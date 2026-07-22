use std::sync::Arc;

use anyhow::Context;
use common::auth::TenantContext;
use common::flight::conversion::otlp_logs_to_arrow;
use common::flight::transport::InMemoryFlightTransport;
use common::wal::{WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use super::WalManager;
use super::forward::forward_batch_to_writer;

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
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_logs_calls.lock().await.push(request);
        Ok(())
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

    /// Handle an OTLP logs export.
    ///
    /// Returns `Ok(())` once the data is durably accepted: written and
    /// flushed to the WAL. A failed Flight forward after that point is not
    /// an error — the WAL retry consumer re-forwards the entry.
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
    ) -> anyhow::Result<()> {
        tracing::info!(
            tenant_id = %tenant_context.tenant_id,
            dataset_id = %tenant_context.dataset_id,
            "Handling OTLP log request"
        );

        // Get tenant/dataset-specific WAL
        let wal = self
            .wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                "logs",
            )
            .await
            .context("Failed to get WAL")?;

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
        let batch_bytes =
            record_batch_to_bytes(&record_batch).context("Failed to serialize record batch")?;

        let wal_entry_id = wal
            .append(WalOperation::WriteLogs, batch_bytes.clone(), metadata_str)
            .await
            .context("Failed to write logs to WAL")?;

        // Flush WAL to ensure durability
        wal.flush().await.context("Failed to flush WAL")?;

        tracing::debug!(entry_id = %wal_entry_id, "Logs written to WAL");

        // Step 2: Forward from WAL to writer via Flight
        match forward_batch_to_writer(
            &self.flight_transport,
            record_batch,
            Some(&metadata.to_string()),
        )
        .await
        {
            Ok(()) => {
                tracing::debug!("Successfully forwarded logs via Flight protocol");
                // Mark WAL entry as processed after successful forwarding
                if let Err(e) = wal.mark_processed(wal_entry_id).await {
                    tracing::warn!(entry_id = %wal_entry_id, error = %e, "Failed to mark WAL entry as processed");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to forward logs - data remains in WAL for retry");
            }
        }

        // Data is durable in the WAL at this point; forward failures are
        // recovered by the retry consumer, so the export is acknowledged.
        Ok(())
    }
}
