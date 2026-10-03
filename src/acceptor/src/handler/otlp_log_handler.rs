//! # OTLP Log Handler
//!
//! Converts an `ExportLogsServiceRequest` to Arrow, writes it to the
//! tenant/dataset's logs WAL, and forwards it to a writer via Flight.
//! Shared by both the gRPC (`services::otlp_log_service`) and HTTP
//! (`lib::handle_http_logs`) surfaces.

use std::sync::Arc;

use anyhow::Context;
use common::auth::TenantContext;
use common::flight::conversion::otlp_logs_to_arrow;
use common::flight::transport::InMemoryFlightTransport;
use common::processors::ProcessorRegistry;
use common::wal::{WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use super::WalManager;
use super::forward::{spawn_forward_and_mark, spawn_retire_resend};
use super::ingest_error::IngestError;
use super::processors_apply::apply_log_processors;
use super::retry_dedup::{RetryDedup, stamp_batch_fingerprint};
use crate::attribute_limits::{TenantAttributeLimits, cap_logs, record_drops};

pub struct LogHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL manager for multi-tenant WAL isolation
    wal_manager: Arc<WalManager>,
    /// Recognizes a client's resend of a batch already made durable
    retry_dedup: Arc<RetryDedup>,
    /// Per-record attribute guardrails (`[acceptor.attribute_limits]` plus tenant overrides)
    attribute_limits: Arc<TenantAttributeLimits>,
    /// Tenant OTTL processors (change: tenant-ottl-processors)
    processor_registry: Arc<ProcessorRegistry>,
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
        processor_registry: Arc<ProcessorRegistry>,
    ) -> Self {
        Self {
            flight_transport,
            wal_manager,
            retry_dedup: Arc::new(RetryDedup::default()),
            attribute_limits: Arc::new(TenantAttributeLimits::default()),
            processor_registry,
        }
    }

    /// Share one resend-dedup cache (`[acceptor].retry_dedup_window`) with
    /// the acceptor's other handlers; the default is a private cache with
    /// the default window.
    pub fn with_retry_dedup(mut self, retry_dedup: Arc<RetryDedup>) -> Self {
        self.retry_dedup = retry_dedup;
        self
    }

    /// Per-tenant `[acceptor.attribute_limits]`; the default applies the
    /// built-in limits to every tenant.
    pub fn with_attribute_limits(mut self, attribute_limits: Arc<TenantAttributeLimits>) -> Self {
        self.attribute_limits = attribute_limits;
        self
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
        mut request: ExportLogsServiceRequest,
    ) -> Result<(), IngestError> {
        tracing::debug!(
            tenant_id = %tenant_context.tenant_id,
            dataset_id = %tenant_context.dataset_id,
            "Handling OTLP log request"
        );

        apply_log_processors(&self.processor_registry, tenant_context, &mut request).await?;

        let dropped = cap_logs(
            &mut request,
            self.attribute_limits.for_tenant(&tenant_context.tenant_id),
        );
        record_drops(&tenant_context.tenant_id, "logs", &dropped);

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

        // Step 1: Write to WAL first for durability
        let batch_bytes = record_batch_to_bytes(&record_batch)
            .context("Failed to serialize record batch")
            .map_err(IngestError::Unavailable)?;

        let ingest_id = stamp_batch_fingerprint(
            &mut metadata,
            &tenant_context.tenant_id,
            &tenant_context.dataset_id,
            &WalOperation::WriteLogs,
            &batch_bytes,
        );
        // Serialize metadata for WAL storage (enables background processor routing)
        let metadata_str = serde_json::to_string(&metadata).ok();
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

        // Step 2: Forward from WAL to writer via Flight, detached from this
        // request future so a client disconnect cannot cancel it after the
        // flush above (issue #1734). Awaiting the handle keeps behavior for
        // connected clients unchanged. A client's resend of a batch already
        // accepted is retired instead (see `retry_dedup`).
        let forward_task = if self.retry_dedup.is_resend(ingest_id) {
            spawn_retire_resend(
                wal,
                wal_entry_id,
                &tenant_context.tenant_id,
                &WalOperation::WriteLogs,
            )
        } else {
            spawn_forward_and_mark(
                self.flight_transport.clone(),
                wal,
                wal_entry_id,
                ingest_id,
                record_batch,
                metadata_str,
                "logs",
            )
        };
        if let Err(e) = forward_task.await {
            tracing::error!(entry_id = %wal_entry_id, error = %e, "Forward-and-mark task for logs did not complete");
        }

        // Data is durable in the WAL at this point; forward failures are
        // recovered by the retry consumer, so the export is acknowledged.
        Ok(())
    }
}

#[cfg(test)]
mod wal_bytes_tests {
    use std::sync::Arc;

    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use tempfile::TempDir;

    use super::*;
    use crate::handler::test_support::{
        only_wal_entry_bytes, test_tenant_context, test_wal_manager, transport_without_writer,
    };

    fn sample_log_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key_strindex: 0,
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000,
                        observed_time_unix_nano: 1_000,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("hello".to_string())),
                        }),
                        attributes: vec![KeyValue {
                            key_strindex: 0,
                            key: "http.status_code".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::IntValue(200)),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[tokio::test]
    async fn wal_entry_bytes_match_the_unmodified_otlp_to_arrow_conversion() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let (transport, processor_registry) = transport_without_writer().await;
        let handler = LogHandler::new(transport, wal_manager.clone(), processor_registry);
        let tenant_context = test_tenant_context();
        let request = sample_log_request();

        let expected_batch = otlp_logs_to_arrow(&request).unwrap();
        let expected_bytes = record_batch_to_bytes(&expected_batch).unwrap();

        handler
            .handle_grpc_otlp_logs(&tenant_context, request)
            .await
            .unwrap();

        assert_eq!(
            only_wal_entry_bytes(&wal_manager, &tenant_context, "logs").await,
            expected_bytes
        );
    }

    #[tokio::test]
    async fn log_records_over_the_attribute_limit_are_capped_before_the_wal() {
        use crate::handler::test_support::{column_as, string_attrs};
        use common::wal::bytes_to_record_batch;
        use datafusion::arrow::array::{StringArray, UInt32Array};

        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let (transport, processor_registry) = transport_without_writer().await;
        let handler = LogHandler::new(transport, wal_manager.clone(), processor_registry)
            .with_attribute_limits(Arc::new(TenantAttributeLimits::uniform(
                common::config::AttributeLimits {
                    max_attributes: 2,
                    ..Default::default()
                },
            )));
        let tenant_context = test_tenant_context();
        let mut request = sample_log_request();
        let record = &mut request.resource_logs[0].scope_logs[0].log_records[0];
        record.attributes = string_attrs(5);
        record.dropped_attributes_count = 1;

        handler
            .handle_grpc_otlp_logs(&tenant_context, request)
            .await
            .unwrap();

        let batch = bytes_to_record_batch(
            &only_wal_entry_bytes(&wal_manager, &tenant_context, "logs").await,
        )
        .unwrap();
        let attributes: serde_json::Value =
            serde_json::from_str(column_as::<StringArray>(&batch, "attributes_json").value(0))
                .unwrap();
        assert_eq!(attributes.as_object().unwrap().len(), 2);
        let dropped = column_as::<UInt32Array>(&batch, "dropped_attributes_count").value(0);
        assert_eq!(dropped, 1 + 3);
    }
}
