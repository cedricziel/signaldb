//! Writer Flight service: the Storage-capability ingest endpoint.
//!
//! ## Commit contract
//!
//! `do_put` writes each batch to the writer WAL, flushes for durability, and
//! **acknowledges without committing to Iceberg** — the commit is deferred to
//! the background [`WalProcessor`] loop, which coalesces commits per
//! `(tenant, dataset, table)` (see `[writer]` config). This decouples ingest
//! ack latency from Iceberg/catalog latency and caps the snapshot/metadata
//! write rate. Consequently, ingested data is queryable only once the loop
//! commits it (bounded by `commit_interval`).
//!
//! A client needing read-your-writes forces an immediate commit of all pending
//! groups with `do_action(`[`FLUSH_ACTION`]`)` (advertised via `list_actions`),
//! bounded by [`FLUSH_TIMEOUT`].

use crate::processor::{FlushScope, WalProcessor};
use crate::schema_transform::{
    FlightMetadata, determine_wal_operation, extract_flight_metadata, transform_for_signal,
};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    FlightData, FlightDescriptor, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
};
use bytes::Bytes;
use common::CatalogManager;
use common::config::WriterConfig;
use common::flight::decode::flight_data_vec_to_batches;
use common::flight::schema::FlightSchemas;
use common::wal::{Wal, WalOperation, record_batch_to_bytes};
use datafusion::arrow::datatypes::SchemaRef;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::Instrument;

/// Flight `do_action` type that forces an immediate commit of all pending
/// writes (read-your-writes drain), bypassing the commit-coalescing floor.
pub const FLUSH_ACTION: &str = "flush";

/// Upper bound on a client-triggered `do_action("flush")`. The flush commits
/// synchronously while holding the processor mutex, so it must not hang the RPC
/// (or the background loop) forever if the catalog/object store stalls.
const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Parse the required tenant/dataset scope from a `flush` action body
/// from the request's `x-tenant-id` (required) and `x-dataset-id` (optional)
/// gRPC metadata — the same tenant identity the ingest path carries — so the
/// flush can only target the tenant the caller is already acting as, and can
/// never be unscoped (which would force-commit, and amplify catalog writes for,
/// every tenant on this writer). Reading the scope from the request headers
/// rather than a free-form body keeps it consistent with `do_put` and prevents
/// a caller from flushing an arbitrary tenant named in the payload.
fn parse_flush_scope(metadata: &tonic::metadata::MetadataMap) -> Result<FlushScope, Status> {
    let tenant_id = metadata
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if tenant_id.is_empty() {
        return Err(Status::invalid_argument(
            "flush action requires an x-tenant-id metadata header (unscoped flush is not allowed)",
        ));
    }
    let dataset_id = metadata
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .map(str::to_string);
    Ok(FlushScope {
        tenant_id: tenant_id.to_string(),
        dataset_id,
    })
}

/// Enhanced Flight service that uses Iceberg table writer instead of direct Parquet writes
/// This demonstrates the integration of the new Iceberg-based processor
pub struct IcebergWriterFlightService {
    processor: Arc<Mutex<WalProcessor>>,
    wal: Arc<Wal>,
    #[allow(dead_code)]
    schemas: FlightSchemas,
    reconciler: Arc<crate::reconcile::TableReconciler>,
    table_reconcile_interval: std::time::Duration,
}

impl IcebergWriterFlightService {
    /// Create a new IcebergWriterFlightService with CatalogManager
    ///
    /// Uses the shared Iceberg catalog from CatalogManager, ensuring consistent
    /// metadata across all SignalDB components.
    pub fn new(
        catalog_manager: Arc<CatalogManager>,
        object_store: Arc<dyn ObjectStore>,
        wal: Arc<Wal>,
        writer_config: &WriterConfig,
    ) -> Self {
        let processor = WalProcessor::with_config(
            wal.clone(),
            catalog_manager.clone(),
            object_store,
            writer_config,
        );

        Self {
            processor: Arc::new(Mutex::new(processor)),
            wal,
            schemas: FlightSchemas::new(),
            reconciler: Arc::new(crate::reconcile::TableReconciler::new(catalog_manager)),
            table_reconcile_interval: writer_config.table_reconcile_interval,
        }
    }

    /// Start the signal-table reconciler: one pass now, then a pass every
    /// `[writer].table_reconcile_interval` (zero disables the periodic run).
    ///
    /// Mirrors [`Self::start_background_processing`] so the standalone binary
    /// and monolithic mode share one wiring. Failures are logged, never fatal:
    /// the ingest path still creates any table it needs, so a broken pass
    /// degrades to create-on-first-write.
    ///
    /// The caller must `abort()` the returned handle during shutdown.
    pub fn start_table_reconciler(&self) -> tokio::task::JoinHandle<()> {
        let reconciler = self.reconciler.clone();
        let interval = self.table_reconcile_interval;

        let handle = tokio::spawn(async move {
            loop {
                match reconciler.run_pass().await {
                    Ok(summary) => {
                        if summary.tables_created > 0 || summary.tables_failed > 0 {
                            tracing::info!(
                                datasets_checked = summary.datasets_checked,
                                datasets_skipped = summary.datasets_skipped,
                                tables_created = summary.tables_created,
                                tables_failed = summary.tables_failed,
                                "Signal-table reconcile pass complete"
                            );
                        } else {
                            tracing::debug!(
                                datasets_checked = summary.datasets_checked,
                                datasets_skipped = summary.datasets_skipped,
                                "Signal-table reconcile pass complete (converged)"
                            );
                        }
                    }
                    Err(e) => tracing::warn!(
                        error = %e,
                        "Signal-table reconcile pass failed; retrying next interval"
                    ),
                }

                if interval.is_zero() {
                    tracing::info!(
                        "Periodic signal-table reconciliation disabled; startup pass only"
                    );
                    return;
                }
                tokio::time::sleep(interval).await;
            }
        });

        tracing::info!("Started signal-table reconciler");
        handle
    }

    /// Start the background WAL processing loop.
    /// Returns the JoinHandle for the spawned task. Caller must abort() this handle
    /// during shutdown to release the Arc<Wal> reference before calling Arc::try_unwrap.
    pub fn start_background_processing(&self) -> tokio::task::JoinHandle<()> {
        let processor = self.processor.clone();

        let handle = tokio::spawn(async move {
            const BASE_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(5);
            const MAX_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(300);
            let mut consecutive_failures: u32 = 0;
            loop {
                let mut processor_guard = processor.lock().await;
                match processor_guard.process_pending_entries().await {
                    Ok(()) => consecutive_failures = 0,
                    Err(e) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        tracing::error!(
                            error = %e,
                            consecutive_failures,
                            "Background WAL processing error"
                        );
                    }
                }
                drop(processor_guard);

                // Exponential backoff on repeated failures so a persistently
                // failing catalog/store is not hammered every 5 seconds.
                let delay = BASE_INTERVAL
                    .saturating_mul(1u32 << consecutive_failures.min(6))
                    .min(MAX_BACKOFF)
                    .max(BASE_INTERVAL);
                tokio::time::sleep(delay).await;
            }
        });

        tracing::info!("Started background WAL processing task");
        handle
    }
}

/// Logs the received-data event. `signal_type`/`target_table` are recorded
/// via `as_deref()` (tracing's `Value` impl for `Option<T>` skips `None`
/// fields entirely) rather than `?` Debug-formatting the `Option` itself —
/// the latter previously leaked `Some("...")`/`None` into the log's
/// attributes, and from there into query results grouped by those fields (#1072).
fn log_received_data(metadata: &FlightMetadata) {
    tracing::info!(
        schema_version = %metadata.schema_version,
        signal_type = metadata.signal_type.as_deref(),
        target_table = metadata.target_table.as_deref(),
        "Received data"
    );
}

#[tonic::async_trait]
impl FlightService for IcebergWriterFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let resp = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::new(),
        };
        let out = stream::once(async move { Ok(resp) }).boxed();
        Ok(Response::new(out))
    }

    type ListFlightsStream = BoxStream<'static, Result<arrow_flight::FlightInfo, Status>>;

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // No predefined flights
        let out = stream::empty().boxed();
        Ok(Response::new(out))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not supported"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not supported"))
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let remote_addr = request.remote_addr();
        let mut inbound = request.into_inner();
        let mut data_vec = Vec::new();
        let mut flight_metadata: Option<FlightMetadata> = None;
        let mut schema_ref: Option<SchemaRef> = None;
        let put_start = std::time::Instant::now();
        let mut bytes_received: u64 = 0;

        while let Some(msg) = inbound.next().await {
            let d = msg.map_err(|e| Status::internal(e.to_string()))?;
            bytes_received +=
                (d.data_header.len() + d.data_body.len() + d.app_metadata.len()) as u64;

            // Extract full metadata from the first FlightData message (which contains metadata)
            if flight_metadata.is_none() && !d.app_metadata.is_empty() {
                match extract_flight_metadata(&d.app_metadata) {
                    Ok(metadata) => {
                        flight_metadata = Some(metadata);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to extract metadata, using defaults");
                        flight_metadata = Some(FlightMetadata {
                            schema_version: "v1".to_string(),
                            signal_type: Some("traces".to_string()),
                            target_table: None,
                            tenant_id: None,
                            dataset_id: None,
                            traceparent: None,
                            tracestate: None,
                        });
                    }
                }
            }

            data_vec.push(d);
        }

        if data_vec.is_empty() {
            return Err(Status::invalid_argument("No FlightData received"));
        }

        // Anti-loop guard (#760): persisting the _system tenant's own
        // telemetry must not emit logs/spans that get exported and
        // re-ingested as _system telemetry.
        let suppress = flight_metadata
            .as_ref()
            .and_then(|m| m.tenant_id.as_deref())
            .is_some_and(common::self_monitoring::is_self_monitoring_tenant);

        // Process within a semconv RPC SERVER span that joins the sender's
        // distributed trace (parent must be set before the span is first
        // entered). The span is created under the suppression scope so it
        // is itself not exported for _system batches.
        let make_span = || {
            common::self_monitoring::spans::rpc_server_span(
                common::self_monitoring::spans::FLIGHT_DO_PUT,
                None,
            )
        };
        let span = if suppress {
            common::self_monitoring::suppress_self_telemetry_sync(make_span)
        } else {
            make_span()
        };
        if let Some(ref metadata) = flight_metadata {
            common::flight::trace_context::set_parent_from_fields(
                &span,
                metadata.traceparent.as_deref(),
                metadata.tracestate.as_deref(),
            );
        }
        common::self_monitoring::spans::record_network_peer_from_addr(&span, remote_addr);
        // Boxed: the state machine is large, and nesting it by value inside
        // the suppression wrapper overflows rustc's layout-query depth.
        let record_span = span.clone();
        let result = common::self_monitoring::maybe_suppress_self_telemetry(suppress, Box::pin(async move {
        if let Some(ref metadata) = flight_metadata {
            log_received_data(metadata);
        }

        // Convert FlightData stream into Arrow RecordBatches. Dictionary-aware
        // (#951): a hand-rolled decode via `arrow_flight::utils::flight_data_to_batches`
        // silently assumes no dictionary batches are present.
        let batches = flight_data_vec_to_batches(data_vec)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        {
            let app_metrics = common::self_monitoring::app_metrics();
            let attrs = [opentelemetry::KeyValue::new("rpc.method", "do_put")];
            app_metrics
                .flight_request_duration
                .record(put_start.elapsed().as_secs_f64(), &attrs);
            app_metrics
                .flight_bytes_received
                .add(bytes_received, &attrs);
            app_metrics.ingest_batches_written.add(1, &[]);
            let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            app_metrics.ingest_batch_size.record(rows, &[]);
        }

        // Determine WAL operation from metadata
        let wal_operation = if let Some(ref metadata) = flight_metadata {
            determine_wal_operation(metadata.signal_type.as_deref())
        } else {
            WalOperation::WriteTraces // Default fallback
        };

        tracing::debug!(operation = ?wal_operation, "Using WAL operation");

        let transformed_batches = if let Some(ref metadata) = flight_metadata {
            if metadata.schema_version == "v1" {
                let mut transformed = Vec::new();
                for batch in batches {
                    // Per-tenant materialized labels (tenant schema
                    // override replaces the global set).
                    let materialized = metadata
                        .tenant_id
                        .as_deref()
                        .and_then(|t| {
                            common::config::CONFIG
                                .get()
                                .map(|c| c.get_tenant_schema_config(t).materialized_labels)
                        })
                        .unwrap_or_default();
                    match transform_for_signal(
                        metadata.signal_type.as_deref(),
                        metadata.target_table.as_deref(),
                        batch,
                        &materialized,
                    ) {
                        Ok(transformed_batch) => {
                            if schema_ref.is_none() {
                                schema_ref = Some(transformed_batch.schema());
                            }
                            transformed.push(transformed_batch);
                        }
                        Err(e) => {
                            // The batch cannot be shaped into the target
                            // table — a fault in what was sent, not in this
                            // writer, and one that recurs identically on
                            // every retry. It must not be reported as
                            // `internal`, which this service also returns for
                            // its own recoverable WAL failures: the acceptor
                            // classifies forward failures by status code to
                            // decide whether retrying can ever succeed
                            // (#1060), and conflating the two either wedges
                            // the WAL forever or discards good batches.
                            return Err(Status::invalid_argument(format!(
                                "Schema transformation failed: {e}"
                            )));
                        }
                    }
                }
                transformed
            } else {
                batches
            }
        } else {
            batches
        };

        // Write all batches to WAL first for durability
        let mut wal_entry_ids = Vec::new();

        // Persist the active (flight_do_put) trace context alongside the
        // routing metadata so the asynchronous WAL processor can rejoin this
        // ingest trace instead of starting a detached root span.
        // current_trace_context_fields() reads the current span, which here is
        // `flight_do_put`; it is None when self-monitoring is disabled.
        let (traceparent, tracestate) =
            match common::flight::trace_context::current_trace_context_fields() {
                Some((tp, ts)) => (Some(tp), ts),
                None => (None, None),
            };

        // Serialize FlightMetadata to JSON for WAL storage (for writer routing)
        let metadata_json = flight_metadata.as_ref().map(|metadata| {
            serde_json::to_string(&serde_json::json!({
                "schema_version": metadata.schema_version,
                "signal_type": metadata.signal_type,
                "target_table": metadata.target_table,
                "tenant_id": metadata.tenant_id,
                "dataset_id": metadata.dataset_id,
                "traceparent": traceparent,
                "tracestate": tracestate,
            }))
            .unwrap_or_default()
        });

        for batch in &transformed_batches {
            // Serialize RecordBatch for WAL storage
            let batch_bytes = record_batch_to_bytes(batch)
                .map_err(|e| Status::internal(format!("Failed to serialize batch: {e}")))?;

            // Write to WAL with correct operation type determined from metadata
            // Pass metadata to enable proper table routing (e.g., metrics_exponential_histogram)
            let entry_id = self
                .wal
                .append(wal_operation.clone(), batch_bytes, metadata_json.clone())
                .await
                .map_err(|e| Status::internal(format!("Failed to write to WAL: {e}")))?;

            wal_entry_ids.push(entry_id);
        }

        // Acknowledge once the data is durable in the WAL. The Iceberg commit
        // is deferred to the background processing loop, which coalesces
        // commits per (tenant, dataset, table) — this decouples ingest ack
        // latency from Iceberg/catalog latency (#889) and lets the coalescing
        // floor cap the commit rate (#888). Ingested data becomes queryable
        // once the loop commits it (bounded by `commit_interval`); a client
        // needing read-your-writes can force a drain with `do_action("flush")`.
        self.wal
            .flush()
            .await
            .map_err(|e| Status::internal(format!("Failed to flush WAL: {e}")))?;
        tracing::debug!(
            entry_count = wal_entry_ids.len(),
            "Durably buffered ingest entries; Iceberg commit deferred to the background loop"
        );

        let result = PutResult {
            app_metadata: Bytes::new(),
        };
        let out = stream::once(async move { Ok(result) }).boxed();
        Ok(Response::new(out))
        }
        .instrument(span)))
        .await;
        let code = result
            .as_ref()
            .err()
            .map(|s| s.code())
            .unwrap_or(tonic::Code::Ok);
        common::self_monitoring::spans::record_rpc_result(
            &record_span,
            common::self_monitoring::spans::RpcBoundary::Server,
            code,
        );
        result
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    async fn do_get(
        &self,
        _request: Request<arrow_flight::Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get not supported"))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    async fn do_action(
        &self,
        request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // The flush scope is taken from the request's tenant metadata (below),
        // so extract it before consuming the request.
        let metadata = request.metadata().clone();
        let action = request.into_inner();
        match action.r#type.as_str() {
            // Read-your-writes drain: force-commit the pending groups for the
            // requesting tenant (optionally a single dataset), bypassing the
            // coalescing floor for that scope only. Used by tests and by clients
            // that need their just-ingested data queryable at once. The scope is
            // taken from the request's `x-tenant-id` / `x-dataset-id` metadata
            // (the tenant identity the caller is already acting as); an unscoped
            // request is rejected so one caller cannot force-commit — and amplify
            // catalog writes for — every tenant on this writer.
            FLUSH_ACTION => {
                let scope = parse_flush_scope(&metadata)?;
                // Bound the flush: force_commit_pending drives Iceberg/catalog
                // commits while holding the processor mutex, so a stuck catalog
                // or object store must not hang this client-facing RPC (and the
                // background loop behind it) indefinitely.
                let flush = async {
                    self.processor
                        .lock()
                        .await
                        .force_commit_pending(scope)
                        .await
                };
                match tokio::time::timeout(FLUSH_TIMEOUT, flush).await {
                    Ok(Ok(())) => {
                        let out = stream::empty().boxed();
                        Ok(Response::new(out))
                    }
                    Ok(Err(e)) => Err(Status::internal(format!("Flush failed: {e}"))),
                    Err(_) => Err(Status::deadline_exceeded(format!(
                        "Flush did not complete within {FLUSH_TIMEOUT:?}"
                    ))),
                }
            }
            other => Err(Status::unimplemented(format!(
                "do_action does not support {other:?}"
            ))),
        }
    }
    type ListActionsStream = BoxStream<'static, Result<arrow_flight::ActionType, Status>>;
    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let out = stream::once(async {
            Ok(arrow_flight::ActionType {
                r#type: FLUSH_ACTION.to_string(),
                description: "Commit all pending writes immediately (read-your-writes drain)"
                    .to_string(),
            })
        })
        .boxed();
        Ok(Response::new(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::wal::WalConfig;
    use object_store::memory::InMemory;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use tempfile::tempdir;
    use tracing_subscriber::layer::SubscriberExt;

    /// Captures event field values as they are actually recorded by
    /// `tracing::Value`, so tests can tell a raw string field apart from one
    /// that fell through to `Debug` formatting.
    struct FieldCapture(Arc<StdMutex<HashMap<String, String>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for FieldCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = FieldVisitor(self.0.clone());
            event.record(&mut visitor);
        }
    }

    struct FieldVisitor(Arc<StdMutex<HashMap<String, String>>>);

    impl tracing::field::Visit for FieldVisitor {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0
                .lock()
                .unwrap()
                .insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .lock()
                .unwrap()
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    fn capture_log_received_data(metadata: &FlightMetadata) -> HashMap<String, String> {
        let captured = Arc::new(StdMutex::new(HashMap::new()));
        let subscriber = tracing_subscriber::registry().with(FieldCapture(captured.clone()));
        tracing::subscriber::with_default(subscriber, || {
            log_received_data(metadata);
        });
        Arc::try_unwrap(captured)
            .unwrap_or_else(|_| panic!("subscriber must be dropped before this returns"))
            .into_inner()
            .unwrap()
    }

    /// Issue #1072: `target_table`/`signal_type` were logged with a `?`
    /// (Debug) sigil, so `Some("metrics_histogram")` reached the log's
    /// attributes verbatim instead of `metrics_histogram`, and a `None`
    /// rendered as the literal string `"None"` instead of being omitted —
    /// both leaking into query results grouped by these fields.
    #[test]
    fn log_received_data_records_raw_strings_and_omits_none() {
        let fields = capture_log_received_data(&FlightMetadata {
            schema_version: "v1".to_string(),
            signal_type: Some("metrics".to_string()),
            target_table: None,
            tenant_id: None,
            dataset_id: None,
            traceparent: None,
            tracestate: None,
        });

        assert_eq!(
            fields.get("signal_type").map(String::as_str),
            Some("metrics"),
            "fields: {fields:?}"
        );
        assert!(
            !fields.contains_key("target_table"),
            "a None target_table must be omitted, not recorded as \"None\": {fields:?}"
        );
    }

    #[tokio::test]
    async fn test_iceberg_flight_service_creation() {
        let temp_dir = tempdir().unwrap();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024, // 1MB
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());

        let service = IcebergWriterFlightService::new(
            catalog_manager,
            object_store,
            wal,
            &WriterConfig::default(),
        );

        // Verify service was created successfully
        assert!(service.processor.lock().await.get_stats().active_writers == 0);
    }

    #[tokio::test]
    async fn do_action_flush_commits_pending_writes() {
        let temp_dir = tempdir().unwrap();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 8 * 1024 * 1024,
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "acme".to_string(),
            dataset_id: "production".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        // A large interval means the background loop would defer this write; the
        // flush action must commit it regardless.
        let writer_config = WriterConfig {
            commit_interval: std::time::Duration::from_secs(3600),
            max_uncommitted_rows: 1_000_000,
            ..Default::default()
        };
        let service = IcebergWriterFlightService::new(
            catalog_manager,
            object_store,
            wal.clone(),
            &writer_config,
        );

        wal.append(
            WalOperation::WriteMetrics,
            crate::test_support::metrics_gauge_bytes(5),
            Some(r#"{"target_table":"metrics_gauge"}"#.to_string()),
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();

        // Unknown actions are still rejected.
        let unknown = Request::new(arrow_flight::Action {
            r#type: "nope".to_string(),
            body: Bytes::new(),
        });
        match service.do_action(unknown).await {
            Err(status) => assert_eq!(status.code(), tonic::Code::Unimplemented),
            Ok(_) => panic!("unknown do_action type must be Unimplemented"),
        }

        // An unscoped flush (no tenant) is rejected — it must not force-commit
        // every tenant on the writer.
        let unscoped = Request::new(arrow_flight::Action {
            r#type: FLUSH_ACTION.to_string(),
            body: Bytes::new(),
        });
        match service.do_action(unscoped).await {
            Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
            Ok(_) => panic!("unscoped flush must be InvalidArgument"),
        }

        // The flush action drains the pending write for the tenant named in the
        // request metadata.
        let mut flush = Request::new(arrow_flight::Action {
            r#type: FLUSH_ACTION.to_string(),
            body: Bytes::new(),
        });
        flush
            .metadata_mut()
            .insert("x-tenant-id", "acme".parse().unwrap());
        flush
            .metadata_mut()
            .insert("x-dataset-id", "production".parse().unwrap());
        let resp = service.do_action(flush).await.unwrap();
        // Drain the (empty) result stream to completion.
        let _results: Vec<_> = resp.into_inner().collect().await;

        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "flush action must commit the pending write"
        );
    }

    #[tokio::test]
    async fn do_action_flush_surfaces_commit_failure_as_internal() {
        let temp_dir = tempdir().unwrap();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 8 * 1024 * 1024,
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "acme".to_string(),
            dataset_id: "production".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let service = IcebergWriterFlightService::new(
            catalog_manager,
            object_store,
            wal.clone(),
            &WriterConfig::default(),
        );

        // Routes to metrics_gauge but the batch schema does not match, so the
        // commit fails — the flush RPC must report that, not a silent success.
        wal.append(
            WalOperation::WriteMetrics,
            crate::test_support::schema_mismatched_bytes(),
            Some(r#"{"target_table":"metrics_gauge"}"#.to_string()),
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();

        let mut flush = Request::new(arrow_flight::Action {
            r#type: FLUSH_ACTION.to_string(),
            body: Bytes::new(),
        });
        flush
            .metadata_mut()
            .insert("x-tenant-id", "acme".parse().unwrap());
        flush
            .metadata_mut()
            .insert("x-dataset-id", "production".parse().unwrap());
        match service.do_action(flush).await {
            Err(status) => assert_eq!(status.code(), tonic::Code::Internal),
            Ok(_) => panic!("flush must surface the commit failure as an error"),
        }
        // The uncommitted entry remains for retry.
        assert_eq!(wal.get_unprocessed_entries().await.unwrap().len(), 1);
    }
}
