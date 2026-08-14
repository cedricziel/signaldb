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
    wal::{WalOperation, record_batch_to_bytes},
};
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
                .map_err(|e| PrometheusError::RateLimited(e.to_string()))?;
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

        // Process each partition separately (same logic as MetricsHandler)
        for (metric_type, (target_table, partitioned_request)) in partitions {
            tracing::debug!(
                metric_type = %metric_type,
                target_table = %target_table,
                "Processing metric partition"
            );

            // Convert OTLP metrics to Arrow RecordBatch. A conversion
            // failure must reject the write (client retries) instead of
            // ACKing an empty batch — that would be silent data loss
            // (issue #926).
            let record_batch = otlp_metrics_to_arrow(&partitioned_request).map_err(|error| {
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

        tracing::info!(
            tenant_id = %tenant_context.tenant_id,
            "Completed Prometheus remote_write processing"
        );

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
    RateLimited(String),
    QuotaExceeded(String),
}

impl std::fmt::Display for PrometheusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DecodeError(msg) => write!(f, "Decode error: {msg}"),
            Self::ConversionError(msg) => write!(f, "Conversion error: {msg}"),
            Self::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            Self::WalError(msg) => write!(f, "WAL error: {msg}"),
            Self::RateLimited(msg) => write!(f, "Rate limited: {msg}"),
            Self::QuotaExceeded(msg) => write!(f, "Quota exceeded: {msg}"),
        }
    }
}

impl std::error::Error for PrometheusError {}

impl IntoResponse for PrometheusError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match &self {
            Self::DecodeError(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            Self::ConversionError(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            Self::SerializationError(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            Self::WalError(_) => (StatusCode::SERVICE_UNAVAILABLE, self.to_string()),
            Self::RateLimited(_) => (StatusCode::TOO_MANY_REQUESTS, self.to_string()),
            Self::QuotaExceeded(_) => (StatusCode::TOO_MANY_REQUESTS, self.to_string()),
        };

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
    fn conversion_error_maps_to_http_500() {
        // A failed OTLP → Arrow conversion must surface as a server error
        // so the remote-write client retries instead of dropping its copy.
        let response =
            PrometheusError::ConversionError("arrow batch assembly failed".into()).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn rate_limited_error_maps_to_http_429() {
        let response =
            PrometheusError::RateLimited("tenant 'acme' exceeded its request rate limit".into())
                .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
