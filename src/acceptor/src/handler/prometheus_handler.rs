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
    extract::State,
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
        transport::{InMemoryFlightTransport, ServiceCapability},
    },
    wal::{WalOperation, record_batch_to_bytes},
};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use tracing;

use super::WalManager;
use crate::middleware::TenantContextExtractor;
use arrow_flight::utils::batches_to_flight_data;
use futures::{StreamExt, stream};

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
        }
    }

    /// Handle Prometheus remote_write request
    ///
    /// 1. Decompress snappy-compressed protobuf
    /// 2. Decode Prometheus WriteRequest
    /// 3. Convert to OTEL ExportMetricsServiceRequest
    /// 4. Write to WAL
    /// 5. Forward to writer via Flight
    pub async fn handle_remote_write(
        &self,
        tenant_context: &TenantContext,
        body: Bytes,
        headers: &HeaderMap,
    ) -> Result<(), PrometheusError> {
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
        let partitions = Self::partition_metrics_by_type(&otel_request);

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

            // Convert OTLP metrics to Arrow RecordBatch
            let record_batch = otlp_metrics_to_arrow(&partitioned_request);

            let batch_bytes = record_batch_to_bytes(&record_batch).map_err(|e| {
                tracing::error!(error = ?e, "Failed to serialize record batch");
                PrometheusError::SerializationError(e.to_string())
            })?;

            let wal_metadata = serde_json::json!({
                "schema_version": "v1",
                "signal_type": "metrics",
                "metric_type": metric_type,
                "target_table": target_table,
                "tenant_id": tenant_context.tenant_id,
                "dataset_id": tenant_context.dataset_id,
            });
            let wal_metadata_str = serde_json::to_string(&wal_metadata).ok();

            let wal_entry_id = wal
                .append(
                    WalOperation::WriteMetrics,
                    batch_bytes.clone(),
                    wal_metadata_str,
                )
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

            let metadata = serde_json::json!({
                "schema_version": "v1",
                "signal_type": "metrics",
                "metric_type": metric_type,
                "target_table": target_table,
                "tenant_id": tenant_context.tenant_id,
                "dataset_id": tenant_context.dataset_id,
                "wal_entry_id": wal_entry_id,
                "source": "prometheus_remote_write"
            });

            // Forward to writer via Flight
            let mut client = match self
                .flight_transport
                .get_client_for_capability(ServiceCapability::Storage)
                .await
            {
                Ok(client) => client,
                Err(e) => {
                    tracing::error!(error = ?e, "Failed to get Flight client");
                    // Data remains in WAL for retry
                    continue;
                }
            };

            let schema = record_batch.schema();
            let mut flight_data = match batches_to_flight_data(&schema, vec![record_batch]) {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!(error = ?e, "Failed to convert to flight data");
                    continue;
                }
            };

            // Add metadata to first FlightData message
            if !flight_data.is_empty() {
                flight_data[0].app_metadata = Bytes::from(metadata.to_string().into_bytes());
            }

            let flight_stream = stream::iter(flight_data);

            match client.do_put(flight_stream).await {
                Ok(response) => {
                    let mut response_stream = response.into_inner();
                    let mut success = true;
                    while let Some(result) = response_stream.next().await {
                        match result {
                            Ok(put_result) => {
                                tracing::debug!(?put_result, "Flight put response");
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, "Flight put error");
                                success = false;
                                break;
                            }
                        }
                    }

                    if success {
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
                }
                Err(e) => {
                    tracing::error!(error = ?e, "Failed to forward via Flight");
                    // Data remains in WAL for retry
                }
            }
        }

        tracing::info!(
            tenant_id = %tenant_context.tenant_id,
            "Completed Prometheus remote_write processing"
        );

        Ok(())
    }

    /// Partition metrics by type for proper schema handling
    /// Returns: HashMap<metric_type, (table_name, partitioned_request)>
    fn partition_metrics_by_type(
        request: &opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
    ) -> std::collections::HashMap<
        String,
        (
            String,
            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
        ),
    > {
        use std::collections::HashMap;

        let mut partitions: HashMap<String, Vec<(usize, usize, usize)>> = HashMap::new();

        // First pass: detect types and collect indices
        for (res_idx, resource_metrics) in request.resource_metrics.iter().enumerate() {
            for (scope_idx, scope_metrics) in resource_metrics.scope_metrics.iter().enumerate() {
                for (metric_idx, metric) in scope_metrics.metrics.iter().enumerate() {
                    if let Some(data) = &metric.data {
                        let metric_type = match data {
                            Data::Gauge(_) => "gauge",
                            Data::Sum(_) => "sum",
                            Data::Histogram(_) => "histogram",
                            Data::ExponentialHistogram(_) => "exponential_histogram",
                            Data::Summary(_) => "summary",
                        };

                        partitions
                            .entry(metric_type.to_string())
                            .or_default()
                            .push((res_idx, scope_idx, metric_idx));
                    }
                }
            }
        }

        // Second pass: build separate requests for each type
        let mut result = HashMap::new();

        for (metric_type, indices) in partitions {
            let table_name = match metric_type.as_str() {
                "gauge" => "metrics_gauge",
                "sum" => "metrics_sum",
                "histogram" => "metrics_histogram",
                "exponential_histogram" => "metrics_exponential_histogram",
                "summary" => "metrics_summary",
                _ => "metrics_gauge",
            };

            let mut partitioned_resource_metrics = vec![];
            let mut current_resource_idx = None;
            let mut current_scope_idx = None;
            let mut current_scope_metrics = vec![];
            let mut current_resource_scope_metrics = vec![];

            for (res_idx, scope_idx, metric_idx) in indices {
                let resource_metrics = &request.resource_metrics[res_idx];
                let scope_metrics = &resource_metrics.scope_metrics[scope_idx];
                let metric = scope_metrics.metrics[metric_idx].clone();

                if current_resource_idx != Some(res_idx) {
                    // Finalize previous scope and resource
                    if !current_scope_metrics.is_empty() {
                        let src: &opentelemetry_proto::tonic::metrics::v1::ScopeMetrics = &request
                            .resource_metrics[current_resource_idx.unwrap()]
                        .scope_metrics[current_scope_idx.unwrap()];
                        current_resource_scope_metrics.push(
                            opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                                scope: src.scope.clone(),
                                metrics: current_scope_metrics,
                                schema_url: src.schema_url.clone(),
                            },
                        );
                        current_scope_metrics = vec![];
                    }

                    if let Some(res_idx) = current_resource_idx {
                        let src = &request.resource_metrics[res_idx];
                        partitioned_resource_metrics.push(
                            opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
                                resource: src.resource.clone(),
                                scope_metrics: current_resource_scope_metrics,
                                schema_url: src.schema_url.clone(),
                            },
                        );
                        current_resource_scope_metrics = vec![];
                    }

                    current_resource_idx = Some(res_idx);
                    current_scope_idx = Some(scope_idx);
                    current_scope_metrics.push(metric);
                } else if current_scope_idx != Some(scope_idx) {
                    // Finalize previous scope
                    if !current_scope_metrics.is_empty() {
                        let src = &request.resource_metrics[current_resource_idx.unwrap()]
                            .scope_metrics[current_scope_idx.unwrap()];
                        current_resource_scope_metrics.push(
                            opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                                scope: src.scope.clone(),
                                metrics: current_scope_metrics,
                                schema_url: src.schema_url.clone(),
                            },
                        );
                        current_scope_metrics = vec![];
                    }

                    current_scope_idx = Some(scope_idx);
                    current_scope_metrics.push(metric);
                } else {
                    current_scope_metrics.push(metric);
                }
            }

            // Finalize last scope and resource
            if !current_scope_metrics.is_empty() {
                let src = &request.resource_metrics[current_resource_idx.unwrap()].scope_metrics
                    [current_scope_idx.unwrap()];
                current_resource_scope_metrics.push(
                    opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                        scope: src.scope.clone(),
                        metrics: current_scope_metrics,
                        schema_url: src.schema_url.clone(),
                    },
                );
            }

            if !current_resource_scope_metrics.is_empty() {
                partitioned_resource_metrics.push(
                    opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
                        resource: request.resource_metrics[current_resource_idx.unwrap()]
                            .resource
                            .clone(),
                        scope_metrics: current_resource_scope_metrics,
                        schema_url: request.resource_metrics[current_resource_idx.unwrap()]
                            .schema_url
                            .clone(),
                    },
                );
            }

            let partitioned_request =
                opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
                    resource_metrics: partitioned_resource_metrics,
                };

            result.insert(metric_type, (table_name.to_string(), partitioned_request));
        }

        result
    }
}

/// Errors that can occur during Prometheus remote_write handling
#[derive(Debug)]
pub enum PrometheusError {
    DecodeError(String),
    SerializationError(String),
    WalError(String),
    InternalError(String),
}

impl std::fmt::Display for PrometheusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DecodeError(msg) => write!(f, "Decode error: {msg}"),
            Self::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            Self::WalError(msg) => write!(f, "WAL error: {msg}"),
            Self::InternalError(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl std::error::Error for PrometheusError {}

impl IntoResponse for PrometheusError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match &self {
            Self::DecodeError(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            Self::SerializationError(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            Self::WalError(_) => (StatusCode::SERVICE_UNAVAILABLE, self.to_string()),
            Self::InternalError(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        (status, message).into_response()
    }
}

/// Axum handler for POST /api/v1/write
///
/// This is the HTTP endpoint that receives Prometheus remote_write requests.
/// Authentication is expected to be handled by middleware that sets TenantContext.
pub async fn handle_prometheus_write(
    State(state): State<PrometheusHandlerState>,
    headers: HeaderMap,
    TenantContextExtractor(tenant_context): TenantContextExtractor,
    body: Bytes,
) -> Result<StatusCode, PrometheusError> {
    state
        .handler
        .handle_remote_write(&tenant_context, body, &headers)
        .await?;

    // Prometheus expects 204 No Content on success
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_error_display() {
        let err = PrometheusError::DecodeError("invalid protobuf".to_string());
        assert!(err.to_string().contains("invalid protobuf"));
    }
}
