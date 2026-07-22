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

    #[tracing::instrument(
        skip_all,
        fields(
            tenant_id = %tenant_context.tenant_id,
            dataset_id = %tenant_context.dataset_id
        )
    )]
    pub async fn handle_grpc_otlp_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    ) {
        tracing::info!(
            tenant_id = %tenant_context.tenant_id,
            dataset_id = %tenant_context.dataset_id,
            "Handling OTLP log request"
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
                tracing::error!(
                    tenant_id = %tenant_context.tenant_id,
                    dataset_id = %tenant_context.dataset_id,
                    error = %e,
                    "Failed to get WAL"
                );
                return;
            }
        };

        // Convert OTLP logs to Arrow RecordBatch
        let record_batch = otlp_logs_to_arrow(&request);

        // Add schema version metadata (v1 for OTLP conversion)
        let mut metadata = serde_json::json!({
            "schema_version": "v1",
            "signal_type": "logs",
            "tenant_id": tenant_context.tenant_id,
            "dataset_id": tenant_context.dataset_id,
        });
        if let Some((traceparent, tracestate)) =
            common::flight::trace_context::current_trace_context_fields()
        {
            metadata["traceparent"] = traceparent.into();
            if let Some(tracestate) = tracestate {
                metadata["tracestate"] = tracestate.into();
            }
        }

        // Serialize metadata for WAL storage (enables background processor routing)
        let metadata_str = serde_json::to_string(&metadata).ok();

        // Step 1: Write to WAL first for durability
        let batch_bytes = match record_batch_to_bytes(&record_batch) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(error = %e, "Failed to serialize record batch");
                return;
            }
        };

        let wal_entry_id = match wal
            .append(WalOperation::WriteLogs, batch_bytes.clone(), metadata_str)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %e, "Failed to write logs to WAL");
                return;
            }
        };

        // Flush WAL to ensure durability
        if let Err(e) = wal.flush().await {
            tracing::error!(error = %e, "Failed to flush WAL");
            return;
        }

        tracing::debug!(entry_id = %wal_entry_id, "Logs written to WAL");

        // Step 2: Forward from WAL to writer via Flight
        // Get a Flight client for a writer service with storage capability
        let mut client = match self
            .flight_transport
            .get_client_for_capability(ServiceCapability::Storage)
            .await
        {
            Ok(client) => client,
            Err(e) => {
                tracing::error!(error = %e, "Failed to get Flight client for storage service");
                // Data remains in WAL for retry by background processor
                return;
            }
        };

        let schema = record_batch.schema();
        let mut flight_data = match batches_to_flight_data(&schema, vec![record_batch]) {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(error = %e, "Failed to convert batch to flight data");
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
                            tracing::debug!(response = ?put_result, "Flight put response");
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Flight put error");
                            success = false;
                            break;
                        }
                    }
                }

                if success {
                    tracing::debug!("Successfully forwarded logs via Flight protocol");
                    // Mark WAL entry as processed after successful forwarding
                    if let Err(e) = wal.mark_processed(wal_entry_id).await {
                        tracing::warn!(entry_id = %wal_entry_id, error = %e, "Failed to mark WAL entry as processed");
                    }
                } else {
                    tracing::error!("Failed to forward logs - data remains in WAL for retry");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to forward logs via Flight protocol");
                // Data remains in WAL for retry by background processor
            }
        }
    }
}
