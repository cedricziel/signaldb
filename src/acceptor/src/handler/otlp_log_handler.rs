use std::sync::Arc;

use common::auth::TenantContext;
use common::flight::conversion::otlp_logs_to_arrow;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::wal::{WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use super::WalManager;
// Flight protocol imports
use arrow_flight::utils::batches_to_flight_data;
use bytes::Bytes;
use futures::{StreamExt, stream};

pub struct LogHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL manager for multi-tenant WAL isolation
    wal_manager: Arc<WalManager>,
}

#[cfg(any(test, feature = "testing"))]
pub struct MockLogHandler {
    pub handle_grpc_otlp_logs_calls: tokio::sync::Mutex<Vec<ExportLogsServiceRequest>>,
}

#[cfg(any(test, feature = "testing"))]
impl Default for MockLogHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockLogHandler {
    pub fn new() -> Self {
        Self {
            handle_grpc_otlp_logs_calls: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn handle_grpc_otlp_logs(
        &self,
        _tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    ) {
        self.handle_grpc_otlp_logs_calls.lock().await.push(request);
    }

    pub fn expect_handle_grpc_otlp_logs(&mut self) -> &mut Self {
        self
    }
}

impl LogHandler {
    /// Create a new handler with Flight transport and WAL manager
    pub fn new(
        flight_transport: Arc<InMemoryFlightTransport>,
        wal_manager: Arc<WalManager>,
    ) -> Self {
        Self {
            flight_transport,
            wal_manager,
        }
    }

    pub async fn handle_grpc_otlp_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    ) {
        log::info!(
            "Handling OTLP log request for tenant='{}', dataset='{}'",
            tenant_context.tenant_id,
            tenant_context.dataset_id
        );

        // Get tenant/dataset-specific WAL
        let wal = match self
            .wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                "logs",
            )
            .await
        {
            Ok(wal) => wal,
            Err(e) => {
                log::error!(
                    "Failed to get WAL for tenant='{}', dataset='{}': {e}",
                    tenant_context.tenant_id,
                    tenant_context.dataset_id
                );
                return;
            }
        };

        // Convert OTLP logs to Arrow RecordBatch
        let record_batch = otlp_logs_to_arrow(&request);

        // Add schema version metadata (v1 for OTLP conversion)
        let metadata = serde_json::json!({
            "schema_version": "v1",
            "signal_type": "logs"
        });

        // Step 1: Write to WAL first for durability
        let batch_bytes = match record_batch_to_bytes(&record_batch) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("Failed to serialize record batch: {e}");
                return;
            }
        };

        let wal_entry_id = match wal
            .append(WalOperation::WriteLogs, batch_bytes.clone())
            .await
        {
            Ok(id) => id,
            Err(e) => {
                log::error!("Failed to write logs to WAL: {e}");
                return;
            }
        };

        // Flush WAL to ensure durability
        if let Err(e) = wal.flush().await {
            log::error!("Failed to flush WAL: {e}");
            return;
        }

        log::debug!("Logs written to WAL with entry ID: {wal_entry_id}");

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
                    log::debug!("Successfully forwarded logs via Flight protocol");
                    // Mark WAL entry as processed after successful forwarding
                    if let Err(e) = wal.mark_processed(wal_entry_id).await {
                        log::warn!("Failed to mark WAL entry {wal_entry_id} as processed: {e}");
                    }
                } else {
                    log::error!("Failed to forward logs - data remains in WAL for retry");
                }
            }
            Err(e) => {
                log::error!("Failed to forward logs via Flight protocol: {e}");
                // Data remains in WAL for retry by background processor
            }
        }
    }
}
