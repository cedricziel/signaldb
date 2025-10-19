use std::sync::Arc;

use common::flight::conversion::otlp_metrics_to_arrow;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::wal::{Wal, WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
// Flight protocol imports
use arrow_flight::utils::batches_to_flight_data;
use bytes::Bytes;
use futures::{StreamExt, stream};

pub struct MetricsHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL for durability
    wal: Arc<Wal>,
}

#[cfg(any(test, feature = "testing"))]
pub struct MockMetricsHandler {
    pub handle_grpc_otlp_metrics_calls: tokio::sync::Mutex<Vec<ExportMetricsServiceRequest>>,
}

#[cfg(any(test, feature = "testing"))]
impl Default for MockMetricsHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockMetricsHandler {
    pub fn new() -> Self {
        Self {
            handle_grpc_otlp_metrics_calls: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn handle_grpc_otlp_metrics(&self, request: ExportMetricsServiceRequest) {
        self.handle_grpc_otlp_metrics_calls
            .lock()
            .await
            .push(request);
    }

    pub fn expect_handle_grpc_otlp_metrics(&mut self) -> &mut Self {
        self
    }
}

impl MetricsHandler {
    /// Create a new handler with Flight transport and WAL
    pub fn new(flight_transport: Arc<InMemoryFlightTransport>, wal: Arc<Wal>) -> Self {
        Self {
            flight_transport,
            wal,
        }
    }

    /// Determine the target table based on metric types in the request
    /// Returns: (dominant_metric_type, target_table_name)
    /// For simplicity, we use the first metric type found
    fn determine_metric_type_and_table(request: &ExportMetricsServiceRequest) -> (String, String) {
        // Scan through metrics to find the first type
        for resource_metrics in &request.resource_metrics {
            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    if let Some(data) = &metric.data {
                        return match data {
                            Data::Gauge(_) => ("gauge".to_string(), "metrics_gauge".to_string()),
                            Data::Sum(_) => ("sum".to_string(), "metrics_sum".to_string()),
                            Data::Histogram(_) => {
                                ("histogram".to_string(), "metrics_histogram".to_string())
                            }
                            Data::ExponentialHistogram(_) => {
                                ("histogram".to_string(), "metrics_histogram".to_string())
                            }
                            Data::Summary(_) => ("sum".to_string(), "metrics_sum".to_string()), // Map summary to sum
                        };
                    }
                }
            }
        }

        // Default to gauge if no metrics found
        ("gauge".to_string(), "metrics_gauge".to_string())
    }

    pub async fn handle_grpc_otlp_metrics(&self, request: ExportMetricsServiceRequest) {
        log::info!("Handling OTLP metrics request");

        // Determine metric type and target table
        let (metric_type, target_table) = Self::determine_metric_type_and_table(&request);
        log::debug!("Routing metrics to table: {target_table} (type: {metric_type})");

        // Convert OTLP metrics to Arrow RecordBatch
        let record_batch = otlp_metrics_to_arrow(&request);

        // Add schema version metadata (v1 for OTLP conversion)
        // Include metric_type and target_table for writer routing
        let metadata = serde_json::json!({
            "schema_version": "v1",
            "signal_type": "metrics",
            "metric_type": metric_type,
            "target_table": target_table
        });

        // Step 1: Write to WAL first for durability
        let batch_bytes = match record_batch_to_bytes(&record_batch) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("Failed to serialize record batch: {e}");
                return;
            }
        };

        let wal_entry_id = match self
            .wal
            .append(WalOperation::WriteMetrics, batch_bytes.clone())
            .await
        {
            Ok(id) => id,
            Err(e) => {
                log::error!("Failed to write metrics to WAL: {e}");
                return;
            }
        };

        // Flush WAL to ensure durability
        if let Err(e) = self.wal.flush().await {
            log::error!("Failed to flush WAL: {e}");
            return;
        }

        log::debug!("Metrics written to WAL with entry ID: {wal_entry_id}");

        // Step 2: Forward from WAL to writer via Flight
        // Get a Flight client for a writer service with storage capability
        let mut client = match self
            .flight_transport
            .get_client_for_capability(ServiceCapability::Storage)
            .await
        {
            Ok(client) => client,
            Err(e) => {
                log::error!("Failed to get Flight client for storage service: {e}");
                // Data remains in WAL for retry by background processor
                return;
            }
        };

        let schema = record_batch.schema();
        let mut flight_data = match batches_to_flight_data(&schema, vec![record_batch]) {
            Ok(data) => data,
            Err(e) => {
                log::error!("Failed to convert batch to flight data: {e}");
                // Data remains in WAL for retry
                return;
            }
        };

        // Add metadata to the first FlightData message (which contains the schema)
        if !flight_data.is_empty() {
            let metadata_bytes = metadata.to_string().into_bytes();
            flight_data[0].app_metadata = Bytes::from(metadata_bytes);
        }

        let flight_stream = stream::iter(flight_data);

        match client.do_put(flight_stream).await {
            Ok(response) => {
                let mut response_stream = response.into_inner();
                let mut success = true;
                while let Some(result) = response_stream.next().await {
                    match result {
                        Ok(put_result) => {
                            log::debug!("Flight put response: {put_result:?}");
                        }
                        Err(e) => {
                            log::error!("Flight put error: {e}");
                            success = false;
                            break;
                        }
                    }
                }

                if success {
                    log::debug!("Successfully forwarded metrics via Flight protocol");
                    // Mark WAL entry as processed after successful forwarding
                    if let Err(e) = self.wal.mark_processed(wal_entry_id).await {
                        log::warn!("Failed to mark WAL entry {wal_entry_id} as processed: {e}");
                    }
                } else {
                    log::error!("Failed to forward metrics - data remains in WAL for retry");
                }
            }
            Err(e) => {
                log::error!("Failed to forward metrics via Flight protocol: {e}");
                // Data remains in WAL for retry by background processor
            }
        }
    }
}
