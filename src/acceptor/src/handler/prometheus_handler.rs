//! Prometheus Remote Write Handler
//!
//! Accepts Prometheus remote_write protocol (v1 and v2) and converts to OTEL metrics
//! for unified storage. The conversion follows the OpenTelemetry Prometheus compatibility spec.
//!
//! ## Protocol Details
//!
//! - Content-Type: `application/x-protobuf`
//! - Content-Encoding: `snappy` (block format, not framed)
//! - Endpoint: `POST /api/v1/write`
//!
//! ## Remote Write Versions
//!
//! - **v1**: Standard time series with samples
//! - **v2**: Adds native histograms and metric metadata

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use common::{
    auth::TenantContext,
    flight::{
        conversion::{
            conversion_prometheus::{decode_prometheus_remote_write, prometheus_to_otel_metrics},
            otlp_metrics_to_arrow,
        },
        transport::InMemoryFlightTransport,
    },
    ratelimit::TenantRateLimiter,
    storage_usage::StorageUsageTracker,
    wal::{Wal, WalOperation, record_batch_to_bytes},
};
use datafusion::arrow::{error::ArrowError, record_batch::RecordBatch};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use tracing;

use super::WalManager;
use super::forward::forward_batch_to_writer;

/// Content type for Prometheus remote_write requests
pub const PROMETHEUS_CONTENT_TYPE: &str = "application/x-protobuf";

/// Content encoding for Prometheus remote_write (snappy compression)
pub const PROMETHEUS_CONTENT_ENCODING: &str = "snappy";

/// Header indicating remote_write protocol version
pub const HEADER_REMOTE_WRITE_VERSION: &str = "X-Prometheus-Remote-Write-Version";

/// Shared state for the Prometheus handler
#[derive(Clone)]
pub struct PrometheusHandlerState {
    pub handler: Arc<PrometheusHandler>,
}

/// Handler for Prometheus remote_write protocol
pub struct PrometheusHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL manager for multi-tenant WAL isolation
    wal_manager: Arc<WalManager>,
    /// Per-tenant ingest rate limiter (no limiting when unset)
    rate_limiter: Option<Arc<TenantRateLimiter>>,
    /// Per-tenant storage quota enforcement (no quotas when unset)
    storage_quota: Option<Arc<StorageUsageTracker>>,
}

impl PrometheusHandler {
    /// Create a new Prometheus handler with Flight transport and WAL manager
    pub fn new(
        flight_transport: Arc<InMemoryFlightTransport>,
        wal_manager: Arc<WalManager>,
    ) -> Self {
        Self {
            flight_transport,
            wal_manager,
            rate_limiter: None,
            storage_quota: None,
        }
    }

    /// Enforce per-tenant ingest rate limits on remote_write requests.
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<TenantRateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Enforce per-tenant storage quotas on remote_write requests.
    pub fn with_storage_quota(mut self, storage_quota: Arc<StorageUsageTracker>) -> Self {
        self.storage_quota = Some(storage_quota);
        self
    }

    /// Handle Prometheus remote_write request
    ///
    /// 1. Decompress snappy-compressed protobuf
    /// 2. Decode Prometheus WriteRequest
    /// 3. Convert to OTEL ExportMetricsServiceRequest
    /// 4. Write to WAL
    /// 5. Forward to writer via Flight
    #[tracing::instrument(
        skip_all,
        fields(
            signaldb.tenant.id = %tenant_context.tenant_id,
            signaldb.dataset.id = %tenant_context.dataset_id,
            body_size = body.len()
        )
    )]
    pub async fn handle_remote_write(
        &self,
        tenant_context: &TenantContext,
        body: Bytes,
        headers: &HeaderMap,
    ) -> Result<(), PrometheusError> {
        // Per-tenant ingest rate limiting (HTTP 429 with the reason)
        if let Some(limiter) = &self.rate_limiter {
            limiter
                .check_ingest(&tenant_context.tenant_id, body.len())
                .map_err(PrometheusError::RateLimited)?;
        }

        // Per-tenant storage quota: a tenant at or over max_storage_bytes
        // must free space (or get a raised quota) before ingesting more.
        if let Some(quota) = &self.storage_quota {
            quota
                .check_ingest(&tenant_context.tenant_id)
                .map_err(|e| PrometheusError::QuotaExceeded(e.to_string()))?;
        }

        // Log request info
        let version = headers
            .get(HEADER_REMOTE_WRITE_VERSION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("0.1.0");

        tracing::info!(
            tenant_id = %tenant_context.tenant_id,
            dataset = %tenant_context.dataset_id,
            version = %version,
            body_size = body.len(),
            "Handling Prometheus remote_write request"
        );

        // Decode the Prometheus remote_write request (snappy + protobuf)
        let prom_request = decode_prometheus_remote_write(&body).map_err(|e| {
            tracing::error!(error = ?e, "Failed to decode Prometheus remote_write");
            PrometheusError::DecodeError(e.to_string())
        })?;

        if prom_request.timeseries.is_empty() {
            tracing::debug!("Empty remote_write request, skipping");
            return Ok(());
        }

        tracing::debug!(
            timeseries_count = prom_request.timeseries.len(),
            metadata_count = prom_request.metadata.len(),
            "Decoded Prometheus remote_write request"
        );

        // Convert Prometheus → OTEL
        let otel_request = prometheus_to_otel_metrics(&prom_request);

        // Get tenant/dataset-specific WAL
        let wal = self
            .wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                "metrics",
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    error = ?e,
                    tenant_id = %tenant_context.tenant_id,
                    dataset = %tenant_context.dataset_id,
                    "Failed to get WAL"
                );
                PrometheusError::WalError(e.to_string())
            })?;

        // Partition metrics by type for proper schema handling
        let partitions = super::metrics_partition::partition_metrics_by_type(&otel_request);

        if partitions.is_empty() {
            tracing::warn!("No metrics found after conversion");
            return Ok(());
        }

        tracing::debug!(
            partition_count = partitions.len(),
            "Partitioned metrics for storage"
        );

        self.persist_partitions(tenant_context, &wal, partitions, otlp_metrics_to_arrow)
            .await?;

        tracing::info!(
            tenant_id = %tenant_context.tenant_id,
            "Completed Prometheus remote_write processing"
        );

        Ok(())
    }

    /// Convert, then durably persist, every metric-type partition of one
    /// remote_write request.
    ///
    /// Split into two phases so the request is atomic with respect to
    /// `ConversionError` (HTTP 400): Prometheus remote_write treats 400 as
    /// permanent and does not retry, unlike the 503/429/500 paths below,
    /// which it does. Interleaving conversion with WAL appends in a single
    /// loop would let an earlier partition land durably and then fail the
    /// whole request as non-retryable on a later partition — silently
    /// losing that later partition forever, since the client believes 400
    /// means nothing was accepted (the #1385 CodeRabbit finding on M3).
    ///
    /// Phase 1 converts and serializes every partition — no WAL writes —
    /// and returns `ConversionError`/`SerializationError` on the first
    /// failure, before a single append. Phase 2 then appends, flushes, and
    /// forwards each already-converted partition; failures from here on are
    /// all retryable, so at-least-once forwarding tolerates a partial
    /// success on retry.
    ///
    /// `convert` is injectable (a plain function pointer — every call site,
    /// production and test, is non-capturing) so tests can force a
    /// conversion failure deep in a batch without needing an OTLP payload
    /// that genuinely defeats `otlp_metrics_to_arrow` (which is effectively
    /// infallible for protobuf-decoded input in practice — every column is
    /// built with exactly one entry per metric, so `RecordBatch::try_new`
    /// cannot see a length mismatch from realistic data).
    ///
    /// The sibling OTLP `MetricsHandler::handle_grpc_otlp_metrics`
    /// (otlp_metrics_handler.rs) has the same per-partition conversion step
    /// but does not share this fix or this seam: it accumulates failures
    /// with `continue` across the whole partition set instead of
    /// early-returning with `?`, so every partition is always attempted
    /// regardless of another partition's outcome — it was never exposed to
    /// this bug. If that handler's loop shape ever changes toward
    /// fail-fast, re-check it against this same invariant.
    async fn persist_partitions(
        &self,
        tenant_context: &TenantContext,
        wal: &Arc<Wal>,
        partitions: HashMap<String, (String, ExportMetricsServiceRequest)>,
        convert: fn(&ExportMetricsServiceRequest) -> Result<RecordBatch, ArrowError>,
    ) -> Result<(), PrometheusError> {
        // Deterministic order: makes WAL-write ordering predictable for
        // debugging/logs, and lets tests reliably exercise "a partition
        // that would already be durable" ahead of a failing one.
        let mut ordered: Vec<_> = partitions.into_iter().collect();
        ordered.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Phase 1.
        let mut converted = Vec::with_capacity(ordered.len());
        for (metric_type, (target_table, partitioned_request)) in ordered {
            tracing::debug!(
                metric_type = %metric_type,
                target_table = %target_table,
                "Converting metric partition"
            );

            // Convert OTLP metrics to Arrow RecordBatch. A conversion
            // failure must reject the write (client retries) instead of
            // ACKing an empty batch — that would be silent data loss
            // (issue #926).
            let record_batch = convert(&partitioned_request).map_err(|error| {
                tracing::error!(
                    tenant_id = %tenant_context.tenant_id,
                    dataset_id = %tenant_context.dataset_id,
                    signal = "metrics",
                    metric_type = %metric_type,
                    error = %error,
                    "OTLP to Arrow conversion failed - rejecting export"
                );
                PrometheusError::ConversionError(error.to_string())
            })?;

            let batch_bytes = record_batch_to_bytes(&record_batch).map_err(|e| {
                tracing::error!(error = ?e, "Failed to serialize record batch");
                PrometheusError::SerializationError(e.to_string())
            })?;

            converted.push((metric_type, target_table, record_batch, batch_bytes));
        }

        // Phase 2: every partition converted successfully — append, flush,
        // and forward each one. Failures from here on (WalError -> 503,
        // SerializationError -> 500 above) are retryable, so Prometheus
        // retrying the whole request is safe even if an earlier partition
        // in this phase already landed (at-least-once tolerates the
        // duplicate).
        for (metric_type, target_table, record_batch, batch_bytes) in converted {
            let mut wal_metadata = serde_json::json!({
                "schema_version": "v1",
                "signal_type": "metrics",
                "metric_type": metric_type,
                "target_table": target_table,
                "tenant_id": tenant_context.tenant_id,
                "dataset_id": tenant_context.dataset_id,
            });
            // Keep the distributed-trace context with the WAL entry so retry
            // processing after a failed Flight forward retains it.
            if let Some((traceparent, tracestate)) =
                common::flight::trace_context::current_trace_context_fields()
            {
                wal_metadata["traceparent"] = traceparent.into();
                if let Some(tracestate) = tracestate {
                    wal_metadata["tracestate"] = tracestate.into();
                }
            }
            let wal_metadata_str = serde_json::to_string(&wal_metadata).ok();

            let wal_entry_id = wal
                .append(WalOperation::WriteMetrics, batch_bytes, wal_metadata_str)
                .await
                .map_err(|e| {
                    tracing::error!(error = ?e, "Failed to write to WAL");
                    PrometheusError::WalError(e.to_string())
                })?;

            wal.flush().await.map_err(|e| {
                tracing::error!(error = ?e, "Failed to flush WAL");
                PrometheusError::WalError(e.to_string())
            })?;

            tracing::debug!(
                wal_entry_id = %wal_entry_id,
                metric_type = %metric_type,
                "Written to WAL"
            );

            let mut metadata = serde_json::json!({
                "schema_version": "v1",
                "signal_type": "metrics",
                "metric_type": metric_type,
                "target_table": target_table,
                "tenant_id": tenant_context.tenant_id,
                "dataset_id": tenant_context.dataset_id,
                "wal_entry_id": wal_entry_id,
                "source": "prometheus_remote_write"
            });
            if let Some((traceparent, tracestate)) =
                common::flight::trace_context::current_trace_context_fields()
            {
                metadata["traceparent"] = traceparent.into();
                if let Some(tracestate) = tracestate {
                    metadata["tracestate"] = tracestate.into();
                }
            }

            // Forward to writer via Flight
            match forward_batch_to_writer(
                &self.flight_transport,
                record_batch,
                Some(&metadata.to_string()),
            )
            .await
            {
                Ok(()) => {
                    // Mark WAL entry as processed
                    if let Err(e) = wal.mark_processed(wal_entry_id).await {
                        tracing::warn!(error = ?e, wal_entry_id = %wal_entry_id, "Failed to mark WAL entry as processed");
                    }
                    tracing::debug!(
                        metric_type = %metric_type,
                        target_table = %target_table,
                        "Successfully forwarded to writer"
                    );
                }
                Err(e) => {
                    tracing::error!(error = ?e, "Failed to forward via Flight - data remains in WAL for retry");
                }
            }
        }

        Ok(())
    }
}

/// Errors that can occur during Prometheus remote_write handling
#[derive(Debug)]
pub enum PrometheusError {
    DecodeError(String),
    ConversionError(String),
    SerializationError(String),
    WalError(String),
    /// Ingest rate limit exceeded; carries the rejected bucket's state so
    /// the response can attach `Retry-After` / `X-RateLimit-*` headers via
    /// `common::ratelimit::retry_headers`.
    RateLimited(common::ratelimit::RateLimitExceeded),
    QuotaExceeded(String),
}

impl std::fmt::Display for PrometheusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DecodeError(msg) => write!(f, "Decode error: {msg}"),
            Self::ConversionError(msg) => write!(f, "Conversion error: {msg}"),
            Self::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            Self::WalError(msg) => write!(f, "WAL error: {msg}"),
            Self::RateLimited(err) => write!(f, "Rate limited: {err}"),
            Self::QuotaExceeded(msg) => write!(f, "Quota exceeded: {msg}"),
        }
    }
}

impl std::error::Error for PrometheusError {}

impl IntoResponse for PrometheusError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            // A conversion failure is deterministic: the same bytes will
            // fail again, so this is a client error (400), not a 500
            // (finding M3) — a remote_write client backing off and
            // retrying a batch that can never succeed just backs up its
            // queue forever.
            Self::DecodeError(_) | Self::ConversionError(_) => StatusCode::BAD_REQUEST,
            Self::SerializationError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::WalError(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::RateLimited(_) | Self::QuotaExceeded(_) => StatusCode::TOO_MANY_REQUESTS,
        };
        let message = self.to_string();

        // Rate-limit rejections (unlike the count-based storage quota) come
        // from a token bucket, so they carry the `Retry-After` /
        // `X-RateLimit-*` headers every SignalDB rate-limit rejection uses.
        if let Self::RateLimited(err) = &self {
            common::self_monitoring::record_rate_limit_rejection("prometheus", err.kind.as_str());
            let mut response = (status, message).into_response();
            let headers = response.headers_mut();
            for (name, value) in common::ratelimit::retry_headers(err) {
                headers.insert(name, value);
            }
            return response;
        }

        (status, message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_error_display() {
        let err = PrometheusError::DecodeError("invalid protobuf".to_string());
        assert!(err.to_string().contains("invalid protobuf"));
    }

    #[test]
    fn conversion_error_maps_to_http_400() {
        // Finding M3: a failed OTLP → Arrow conversion is deterministic —
        // retrying the same bytes will fail again — so it must surface as
        // a client error (400), not a 500 that invites endless retries.
        let response =
            PrometheusError::ConversionError("arrow batch assembly failed".into()).into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    fn sample_rate_limit_exceeded(
        kind: common::ratelimit::RateLimitKind,
    ) -> common::ratelimit::RateLimitExceeded {
        common::ratelimit::RateLimitExceeded {
            tenant_id: "acme".to_string(),
            kind,
            retry_after: std::time::Duration::from_millis(2_500),
            limit: 1_000.0,
            burst: 1_000.0,
        }
    }

    #[test]
    fn rate_limited_error_maps_to_http_429() {
        let response = PrometheusError::RateLimited(sample_rate_limit_exceeded(
            common::ratelimit::RateLimitKind::Requests,
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn rate_limited_error_carries_retry_after_and_limit_headers() {
        // Bytes-dimension rejection: the headers report bytes/second, not
        // requests/second, matching whichever bucket rejected the request.
        let response = PrometheusError::RateLimited(sample_rate_limit_exceeded(
            common::ratelimit::RateLimitKind::Bytes,
        ))
        .into_response();
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("3")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-limit")
                .and_then(|v| v.to_str().ok()),
            Some("1000")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-burst")
                .and_then(|v| v.to_str().ok()),
            Some("1000")
        );
    }

    #[test]
    fn quota_exceeded_carries_no_rate_limit_headers() {
        // The storage quota is a count check, not a token bucket, so it
        // does not have the header trio.
        let response = PrometheusError::QuotaExceeded("over storage quota".into()).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none()
        );
    }

    // CodeRabbit review of #1385 (M3's ConversionError -> 400 mapping):
    // handle_remote_write converted and persisted each metric-type
    // partition in one loop, so a partition that already reached WAL
    // durability could still be followed by a non-retryable 400 for a
    // later partition that fails conversion — silently losing that later
    // partition forever, since a 400 tells the client not to retry.
    //
    // These tests exercise `persist_partitions` (the phase-1/phase-2 split
    // that fixes this) with an injected `convert` closure, because
    // `otlp_metrics_to_arrow` is effectively infallible for well-formed,
    // protobuf-decoded input: every Arrow column it builds gets exactly one
    // entry per metric processed, so `RecordBatch::try_new` never sees a
    // length mismatch from realistic data. A real failure would have to
    // come from a bug in that function, not from anything a test can craft
    // as input — hence the injectable seam.
    mod partition_atomicity {
        use std::sync::Arc;

        use common::flight::transport::InMemoryFlightTransport;
        use common::service_bootstrap::{ServiceBootstrap, ServiceType};
        use common::wal::{WalConfig, WalOperation};
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric::Data,
            number_data_point,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use tempfile::TempDir;

        use super::*;

        const TEST_TENANT: &str = "test-tenant";
        const TEST_DATASET: &str = "test-dataset";

        fn test_tenant_context() -> TenantContext {
            TenantContext {
                tenant_id: TEST_TENANT.to_string(),
                dataset_id: TEST_DATASET.to_string(),
                tenant_slug: TEST_TENANT.to_string(),
                dataset_slug: TEST_DATASET.to_string(),
                api_key_name: Some("test-key".to_string()),
                api_key_scopes: None,
                api_key_dataset_id: None,
                user_id: None,
                role: None,
                is_instance_admin: false,
                session_id: None,
                source: common::auth::TenantSource::Config,
            }
        }

        /// A one-metric partition request. `metric_type` selects Gauge vs.
        /// Sum purely so `persist_partitions`'s deterministic (sorted by
        /// metric-type key) ordering is exercised the same way it would be
        /// via real `partition_metrics_by_type` output.
        fn one_metric_request(name: &str, data: Data) -> ExportMetricsServiceRequest {
            ExportMetricsServiceRequest {
                resource_metrics: vec![ResourceMetrics {
                    resource: Some(Resource::default()),
                    scope_metrics: vec![ScopeMetrics {
                        scope: None,
                        metrics: vec![Metric {
                            name: name.to_string(),
                            description: String::new(),
                            unit: String::new(),
                            data: Some(data),
                            metadata: vec![],
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            }
        }

        fn gauge_request(name: &str) -> ExportMetricsServiceRequest {
            one_metric_request(
                name,
                Data::Gauge(Gauge {
                    data_points: vec![NumberDataPoint {
                        start_time_unix_nano: 1000,
                        time_unix_nano: 2000,
                        value: Some(number_data_point::Value::AsDouble(1.0)),
                        ..Default::default()
                    }],
                }),
            )
        }

        fn sum_request(name: &str) -> ExportMetricsServiceRequest {
            one_metric_request(
                name,
                Data::Sum(Sum {
                    data_points: vec![NumberDataPoint {
                        start_time_unix_nano: 1000,
                        time_unix_nano: 2000,
                        value: Some(number_data_point::Value::AsInt(1)),
                        ..Default::default()
                    }],
                    aggregation_temporality: 1,
                    is_monotonic: true,
                }),
            )
        }

        /// A real `PrometheusHandler` backed by a temp-dir WAL and an
        /// in-memory Flight transport, plus the WAL manager to inspect
        /// durability directly.
        async fn test_handler(temp_dir: &TempDir) -> (PrometheusHandler, Arc<WalManager>) {
            let service_bootstrap =
                ServiceBootstrap::new_for_test(ServiceType::Acceptor, "127.0.0.1:0")
                    .await
                    .expect("Failed to initialize service bootstrap");

            let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

            let wal_dir = temp_dir.path().join("wal");
            let wal_manager = Arc::new(WalManager::new(
                WalConfig::with_defaults(wal_dir.clone()),
                WalConfig::with_defaults(wal_dir.clone()),
                WalConfig::with_defaults(wal_dir.clone()),
                WalConfig::with_defaults(wal_dir),
            ));

            (
                PrometheusHandler::new(flight_transport, wal_manager.clone()),
                wal_manager,
            )
        }

        async fn metrics_wal_entry_count(wal_manager: &WalManager) -> usize {
            let wal = wal_manager
                .get_wal(TEST_TENANT, TEST_DATASET, "metrics")
                .await
                .expect("Failed to open metrics WAL");
            wal.get_entries()
                .await
                .expect("Failed to read WAL entries")
                .iter()
                .filter(|e| matches!(e.operation, WalOperation::WriteMetrics))
                .count()
        }

        #[tokio::test]
        async fn conversion_failure_after_a_durable_partition_leaves_wal_untouched() {
            // "gauge" sorts before "sum", so — with persist_partitions's
            // deterministic ordering — the gauge partition is converted
            // (and would be durable) before the sum partition's forced
            // conversion failure is even reached.
            let temp_dir = TempDir::new().unwrap();
            let (handler, wal_manager) = test_handler(&temp_dir).await;
            let tenant_context = test_tenant_context();

            let mut partitions = HashMap::new();
            partitions.insert(
                "gauge".to_string(),
                ("otel_metrics_gauge".to_string(), gauge_request("ok")),
            );
            partitions.insert(
                "sum".to_string(),
                ("otel_metrics_sum".to_string(), sum_request("boom")),
            );

            let wal = wal_manager
                .get_wal(TEST_TENANT, TEST_DATASET, "metrics")
                .await
                .expect("Failed to open metrics WAL");

            let result = handler
                .persist_partitions(&tenant_context, &wal, partitions, |req| {
                    let is_boom = req
                        .resource_metrics
                        .first()
                        .and_then(|rm| rm.scope_metrics.first())
                        .and_then(|sm| sm.metrics.first())
                        .map(|m| m.name == "boom")
                        .unwrap_or(false);
                    if is_boom {
                        Err(ArrowError::ComputeError(
                            "forced conversion failure for test".to_string(),
                        ))
                    } else {
                        otlp_metrics_to_arrow(req)
                    }
                })
                .await;

            assert!(
                matches!(result, Err(PrometheusError::ConversionError(_))),
                "expected ConversionError, got {result:?}"
            );
            assert_eq!(
                metrics_wal_entry_count(&wal_manager).await,
                0,
                "the gauge partition converted before the sum partition's \
                 conversion failure must not have reached the WAL — a 400 \
                 here tells the client not to retry, so anything already \
                 durable at that point would be silently stranded"
            );
        }

        #[tokio::test]
        async fn all_partitions_converting_land_in_wal() {
            let temp_dir = TempDir::new().unwrap();
            let (handler, wal_manager) = test_handler(&temp_dir).await;
            let tenant_context = test_tenant_context();

            let mut partitions = HashMap::new();
            partitions.insert(
                "gauge".to_string(),
                ("otel_metrics_gauge".to_string(), gauge_request("ok_gauge")),
            );
            partitions.insert(
                "sum".to_string(),
                ("otel_metrics_sum".to_string(), sum_request("ok_sum")),
            );

            let wal = wal_manager
                .get_wal(TEST_TENANT, TEST_DATASET, "metrics")
                .await
                .expect("Failed to open metrics WAL");

            let result = handler
                .persist_partitions(&tenant_context, &wal, partitions, otlp_metrics_to_arrow)
                .await;

            assert!(result.is_ok(), "expected Ok, got {result:?}");
            assert_eq!(
                metrics_wal_entry_count(&wal_manager).await,
                2,
                "both partitions should have landed durably"
            );
        }
    }
}
