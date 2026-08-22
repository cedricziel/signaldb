use std::sync::Arc;

use anyhow::Context;
use common::auth::TenantContext;
use common::flight::conversion::otlp_logs_to_arrow;
use common::flight::transport::InMemoryFlightTransport;
use common::wal::{WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use super::WalManager;
use super::forward::forward_batch_to_writer;
use super::ingest_error::IngestError;

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
    ) -> Result<(), IngestError> {
        self.handle_grpc_otlp_logs_calls.lock().await.push(request);
        Ok(())
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
            signaldb.tenant.id = %tenant_context.tenant_id,
            signaldb.dataset.id = %tenant_context.dataset_id
        )
    )]
    pub async fn handle_grpc_otlp_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    ) -> Result<(), IngestError> {
        tracing::debug!(
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
            .context("Failed to get WAL")
            .map_err(IngestError::Unavailable)?;

        // Convert OTLP logs to Arrow RecordBatch. A conversion failure
        // must reject the export (client retries) instead of ACKing an
        // empty batch — that would be silent data loss (issue #926). It is
        // also deterministic, not a WAL/durability problem, so it is
        // rejected as Invalid rather than Unavailable (finding M3):
        // retrying the same bytes will fail again.
        let record_batch = otlp_logs_to_arrow(&request)
            .inspect_err(|error| {
                tracing::error!(
                    tenant_id = %tenant_context.tenant_id,
                    dataset_id = %tenant_context.dataset_id,
                    signal = "logs",
                    error = %error,
                    "OTLP to Arrow conversion failed - rejecting export"
                );
            })
            .context("Failed to convert OTLP logs to Arrow")
            .map_err(IngestError::Invalid)?;

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
        let batch_bytes = record_batch_to_bytes(&record_batch)
            .context("Failed to serialize record batch")
            .map_err(IngestError::Unavailable)?;

        let wal_entry_id = wal
            .append(WalOperation::WriteLogs, batch_bytes, metadata_str.clone())
            .await
            .context("Failed to write logs to WAL")
            .map_err(IngestError::Unavailable)?;

        // Flush WAL to ensure durability
        wal.flush()
            .await
            .context("Failed to flush WAL")
            .map_err(IngestError::Unavailable)?;

        tracing::debug!(entry_id = %wal_entry_id, "Logs written to WAL");

        // Step 2: Forward from WAL to writer via Flight
        match forward_batch_to_writer(
            &self.flight_transport,
            record_batch,
            metadata_str.as_deref(),
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
