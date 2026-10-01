//! # OTLP Trace Handler
//!
//! Converts an `ExportTraceServiceRequest` to Arrow, writes it to the
//! tenant/dataset's traces WAL, and forwards it to a writer via Flight.
//! Named `otlp_grpc` for its original gRPC-only origin; both the gRPC
//! (`services::otlp_trace_service`) and HTTP (`lib::handle_http_traces`)
//! surfaces share this one handler.
//!
//! A `gen_ai.evaluation.result` span event is also written as a log record
//! (see [`common::evals::span_events`]) through the log handler, since the
//! Evaluate pages read results from `logs`.

use std::sync::Arc;

use anyhow::Context;
use common::auth::TenantContext;
use common::flight::conversion::otlp_traces_to_arrow;
use common::flight::transport::InMemoryFlightTransport;
use common::processors::ProcessorRegistry;
use common::wal::{WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use super::WalManager;
use super::forward::{spawn_forward_and_mark, spawn_retire_resend};
use super::ingest_error::IngestError;
use super::otlp_log_handler::LogHandler;
use super::processors_apply::apply_trace_processors;
use super::retry_dedup::{RetryDedup, stamp_batch_fingerprint};

pub struct TraceHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL manager for multi-tenant WAL isolation
    wal_manager: Arc<WalManager>,
    /// Recognizes a client's resend of a batch already made durable
    retry_dedup: Arc<RetryDedup>,
    /// Tenant OTTL processors (change: tenant-ottl-processors)
    processor_registry: Arc<ProcessorRegistry>,
    /// Writes the log records derived from evaluation-result span events,
    /// through the acceptor's shared log ingest path. `None` makes the
    /// fan-out a no-op, which keeps other constructors and tests unchanged.
    eval_log_handler: Option<Arc<LogHandler>>,
}

#[cfg(any(test, feature = "testing"))]
pub struct MockTraceHandler {
    pub handle_grpc_otlp_traces_calls: tokio::sync::Mutex<Vec<ExportTraceServiceRequest>>,
}

#[cfg(any(test, feature = "testing"))]
impl Default for MockTraceHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockTraceHandler {
    pub fn new() -> Self {
        Self {
            handle_grpc_otlp_traces_calls: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn handle_grpc_otlp_traces(
        &self,
        _tenant_context: &TenantContext,
        request: ExportTraceServiceRequest,
    ) -> Result<(), IngestError> {
        self.handle_grpc_otlp_traces_calls
            .lock()
            .await
            .push(request);
        Ok(())
    }
}

impl TraceHandler {
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
            processor_registry,
            eval_log_handler: None,
        }
    }

    /// Share one resend-dedup cache (`[acceptor].retry_dedup_window`) with
    /// the acceptor's other handlers; the default is a private cache with
    /// the default window.
    pub fn with_retry_dedup(mut self, retry_dedup: Arc<RetryDedup>) -> Self {
        self.retry_dedup = retry_dedup;
        self
    }

    /// Route `gen_ai.evaluation.result` span events to `log_handler` (see
    /// [`common::evals::span_events`]). Without this, the fan-out is a
    /// no-op: the trace export still succeeds, but no derived log is
    /// written.
    pub fn with_evaluation_logs(mut self, log_handler: Arc<LogHandler>) -> Self {
        self.eval_log_handler = Some(log_handler);
        self
    }

    /// Handle an OTLP trace export.
    ///
    /// Returns `Ok(())` once the data is durably accepted: written and
    /// flushed to the WAL. A failed Flight forward after that point is not
    /// an error — the WAL retry consumer re-forwards the entry.
    ///
    /// Any failure before WAL durability is returned as an error so the
    /// service layer can reject the export and the client retries.
    #[tracing::instrument(
        skip_all,
        fields(
            signaldb.tenant.id = %tenant_context.tenant_id,
            signaldb.dataset.id = %tenant_context.dataset_id
        )
    )]
    pub async fn handle_grpc_otlp_traces(
        &self,
        tenant_context: &TenantContext,
        mut request: ExportTraceServiceRequest,
    ) -> Result<(), IngestError> {
        tracing::debug!(
            tenant_id = %tenant_context.tenant_id,
            dataset_id = %tenant_context.dataset_id,
            "Handling OTLP trace request"
        );

        apply_trace_processors(&self.processor_registry, tenant_context, &mut request).await?;

        // Get tenant/dataset-specific WAL
        let wal = self
            .wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                "traces",
            )
            .await
            .context("Failed to get WAL")
            .map_err(IngestError::Unavailable)?;

        // Convert OTLP traces to Arrow RecordBatch. A conversion failure
        // must reject the export (client retries) instead of ACKing an
        // empty batch — that would be silent data loss (issue #926). It is
        // also deterministic, not a WAL/durability problem, so it is
        // rejected as Invalid rather than Unavailable (finding M3):
        // retrying the same bytes will fail again.
        let record_batch = otlp_traces_to_arrow(&request)
            .inspect_err(|error| {
                tracing::error!(
                    tenant_id = %tenant_context.tenant_id,
                    dataset_id = %tenant_context.dataset_id,
                    signal = "traces",
                    error = %error,
                    "OTLP to Arrow conversion failed - rejecting export"
                );
            })
            .context("Failed to convert OTLP traces to Arrow")
            .map_err(IngestError::Invalid)?;

        let mut metadata = serde_json::json!({
            "schema_version": "v1",
            "signal_type": "traces",
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

        let batch_bytes = record_batch_to_bytes(&record_batch)
            .context("Failed to serialize record batch")
            .map_err(IngestError::Unavailable)?;

        let ingest_id = stamp_batch_fingerprint(
            &mut metadata,
            &tenant_context.tenant_id,
            &tenant_context.dataset_id,
            &WalOperation::WriteTraces,
            &batch_bytes,
        );
        let metadata_str = serde_json::to_string(&metadata).ok();
        let wal_entry_id = wal
            .append(WalOperation::WriteTraces, batch_bytes, metadata_str.clone())
            .await
            .context("Failed to write traces to WAL")
            .map_err(IngestError::Unavailable)?;

        // Flush WAL to ensure durability
        wal.flush()
            .await
            .context("Failed to flush WAL")
            .map_err(IngestError::Unavailable)?;

        tracing::debug!(entry_id = %wal_entry_id, "Traces written to WAL");

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
                &WalOperation::WriteTraces,
            )
        } else {
            spawn_forward_and_mark(
                self.flight_transport.clone(),
                wal,
                wal_entry_id,
                ingest_id,
                record_batch,
                metadata_str,
                "traces",
            )
        };
        if let Err(e) = forward_task.await {
            tracing::error!(entry_id = %wal_entry_id, error = %e, "Forward-and-mark task for traces did not complete");
        }

        self.write_evaluation_logs(tenant_context, request).await;

        // Data is durable in the WAL at this point; forward failures are
        // recovered by the retry consumer, so the export is acknowledged.
        Ok(())
    }

    /// Writes each `gen_ai.evaluation.result` span event in `request` as a
    /// log record through the log ingest path (log processors, logs WAL,
    /// forward), when an evaluation log handler is set. Runs after the
    /// traces are durable and never fails the trace export: the spans are
    /// stored either way, and a lost result is logged. The derived batch
    /// depends only on `request`, so a resent trace export fingerprints to
    /// the same logs `ingest_id` and is deduplicated like any resent logs
    /// batch. Takes `request` by value since this is its last use in the
    /// caller, letting the conversion move attributes instead of cloning.
    async fn write_evaluation_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportTraceServiceRequest,
    ) {
        let Some(eval_log_handler) = &self.eval_log_handler else {
            return;
        };
        let Some(logs) = common::evals::span_events::evaluation_logs_from_spans(request) else {
            return;
        };
        if let Err(error) = eval_log_handler
            .handle_grpc_otlp_logs(tenant_context, logs)
            .await
        {
            tracing::warn!(
                tenant_id = %tenant_context.tenant_id,
                dataset_id = %tenant_context.dataset_id,
                error = %error,
                "Failed to write evaluation results derived from span events; the spans were stored"
            );
        }
    }
}

#[cfg(test)]
mod cancellation_safety_tests {
    //! Issue #1734: append -> flush -> forward -> mark_processed used to run
    //! inline in the request future. A client disconnect after the flush
    //! dropped the future before `mark_processed`, leaving the WAL entry
    //! unmarked so the retry consumer forwarded it a second time. These
    //! tests assert the forward-and-mark step now survives that drop.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow_flight::flight_service_server::FlightService;
    use common::auth::TenantContext;
    use common::catalog::Catalog;
    use common::flight::transport::InMemoryFlightTransport;
    use common::processors::ProcessorRegistry;
    use common::service_bootstrap::{ServiceBootstrap, ServiceType};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{
        ResourceSpans, ScopeSpans, Span, Status as SpanStatus, span::SpanKind,
    };
    use tempfile::TempDir;
    use tokio::sync::Notify;

    use super::*;
    use crate::handler::test_support::{
        only_wal_entry_bytes, test_tenant_context, test_wal_manager, transport_without_writer,
    };

    fn sample_trace_request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
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
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: b"1234567890123456".to_vec(),
                        span_id: b"12345678".to_vec(),
                        trace_state: String::new(),
                        parent_span_id: vec![],
                        name: "test-span".to_string(),
                        kind: SpanKind::Server as i32,
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 2_000,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(SpanStatus {
                            code: 1,
                            message: "OK".to_string(),
                        }),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    /// A `FlightService` whose `do_put` blocks on a `Notify` until released,
    /// so the test can hold the writer's response open long enough to drop
    /// the request future in between. Every RPC other than `do_put` is
    /// unimplemented.
    struct BlockingFlightService {
        call_count: Arc<AtomicUsize>,
        saw_request: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[tonic::async_trait]
    impl FlightService for BlockingFlightService {
        type HandshakeStream = futures::stream::BoxStream<
            'static,
            Result<arrow_flight::HandshakeResponse, tonic::Status>,
        >;
        type ListFlightsStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::FlightInfo, tonic::Status>>;
        type DoGetStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::FlightData, tonic::Status>>;
        type DoPutStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::PutResult, tonic::Status>>;
        type DoExchangeStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::FlightData, tonic::Status>>;
        type DoActionStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::Result, tonic::Status>>;
        type ListActionsStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::ActionType, tonic::Status>>;

        async fn handshake(
            &self,
            _request: tonic::Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
        ) -> Result<tonic::Response<Self::HandshakeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("handshake"))
        }
        async fn list_flights(
            &self,
            _request: tonic::Request<arrow_flight::Criteria>,
        ) -> Result<tonic::Response<Self::ListFlightsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("list_flights"))
        }
        async fn get_flight_info(
            &self,
            _request: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::FlightInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented("get_flight_info"))
        }
        async fn poll_flight_info(
            &self,
            _request: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::PollInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented("poll_flight_info"))
        }
        async fn get_schema(
            &self,
            _request: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::SchemaResult>, tonic::Status> {
            Err(tonic::Status::unimplemented("get_schema"))
        }
        async fn do_get(
            &self,
            _request: tonic::Request<arrow_flight::Ticket>,
        ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_get"))
        }
        async fn do_put(
            &self,
            _request: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
        ) -> Result<tonic::Response<Self::DoPutStream>, tonic::Status> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.saw_request.notify_one();
            self.release.notified().await;
            Ok(tonic::Response::new(Box::pin(futures::stream::empty())))
        }
        async fn do_exchange(
            &self,
            _request: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
        ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_exchange"))
        }
        async fn do_action(
            &self,
            _request: tonic::Request<arrow_flight::Action>,
        ) -> Result<tonic::Response<Self::DoActionStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_action"))
        }
        async fn list_actions(
            &self,
            _request: tonic::Request<arrow_flight::Empty>,
        ) -> Result<tonic::Response<Self::ListActionsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("list_actions"))
        }
    }

    #[tokio::test]
    async fn dropping_the_request_future_after_flush_does_not_duplicate_the_forward() {
        let catalog = Catalog::new_in_memory().await.unwrap();

        let call_count = Arc::new(AtomicUsize::new(0));
        let saw_request = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let writer_addr = listener.local_addr().unwrap();
        let service = BlockingFlightService {
            call_count: call_count.clone(),
            saw_request: saw_request.clone(),
            release: release.clone(),
        };
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(common::flight::flight_service_server(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        ServiceBootstrap::new_for_test_with_catalog(
            catalog.clone(),
            ServiceType::Writer,
            &writer_addr.to_string(),
        )
        .await
        .unwrap();

        let acceptor_bootstrap = ServiceBootstrap::new_for_test_with_catalog(
            catalog,
            ServiceType::Acceptor,
            "127.0.0.1:0",
        )
        .await
        .unwrap();
        let flight_transport = Arc::new(InMemoryFlightTransport::new(acceptor_bootstrap));

        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let processor_registry = Arc::new(ProcessorRegistry::new(
            Arc::new(common::catalog::Catalog::new_in_memory().await.unwrap()),
            &common::config::ProcessorsConfig::default(),
        ));

        let handler = TraceHandler::new(flight_transport, wal_manager.clone(), processor_registry);
        let tenant_context = test_tenant_context();

        // Drive the handler future by hand: race it against "the writer saw
        // the DoPut", then drop it — this is the request future a
        // disconnected client leaves behind once WAL flush has already
        // completed.
        let mut handler_future =
            Box::pin(handler.handle_grpc_otlp_traces(&tenant_context, sample_trace_request()));
        tokio::select! {
            _ = &mut handler_future => panic!("handler completed before the writer saw the DoPut"),
            _ = saw_request.notified() => {}
        }
        drop(handler_future);

        // Let the writer's do_put return; the forward-and-mark task must
        // still be running, detached from the future just dropped.
        release.notify_one();

        let wal = wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                "traces",
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if wal.get_unprocessed_entries().await.unwrap().is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("WAL entry was never marked processed after the request future was dropped");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "DoPut must run exactly once even though the request future was dropped"
        );
    }

    async fn handler_without_writer(wal_manager: Arc<WalManager>) -> TraceHandler {
        let (transport, processor_registry) = transport_without_writer().await;
        TraceHandler::new(transport, wal_manager, processor_registry)
    }

    async fn unprocessed_traces(wal_manager: &WalManager, tenant_context: &TenantContext) -> usize {
        let wal = wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                "traces",
            )
            .await
            .unwrap();
        wal.get_unprocessed_entries().await.unwrap().len()
    }

    /// The hive duplicate-spans bug: an OTLP exporter whose `Export` timed
    /// out after the acceptor had already flushed the batch resends the
    /// identical request. The resend must not become a second WAL entry to
    /// forward, or every span of the batch is stored twice.
    #[tokio::test]
    async fn a_resent_identical_export_is_ingested_once() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let handler = handler_without_writer(wal_manager.clone()).await;
        let tenant_context = test_tenant_context();

        for _ in 0..2 {
            handler
                .handle_grpc_otlp_traces(&tenant_context, sample_trace_request())
                .await
                .unwrap();
        }

        assert_eq!(
            unprocessed_traces(&wal_manager, &tenant_context).await,
            1,
            "the resend must be acknowledged without leaving a second entry to forward"
        );
    }

    #[tokio::test]
    async fn distinct_exports_are_each_ingested() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let handler = handler_without_writer(wal_manager.clone()).await;
        let tenant_context = test_tenant_context();

        let first = sample_trace_request();
        let mut second = sample_trace_request();
        second.resource_spans[0].scope_spans[0].spans[0].span_id = b"87654321".to_vec();
        for request in [first, second] {
            handler
                .handle_grpc_otlp_traces(&tenant_context, request)
                .await
                .unwrap();
        }

        assert_eq!(unprocessed_traces(&wal_manager, &tenant_context).await, 2);
    }

    /// The WAL carries the unmodified JSON-in-Utf8 conversion: attribute
    /// typing happens at the writer, never in the acceptor's WAL bytes.
    #[tokio::test]
    async fn wal_entry_bytes_match_the_unmodified_otlp_to_arrow_conversion() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let handler = handler_without_writer(wal_manager.clone()).await;
        let tenant_context = test_tenant_context();
        let request = sample_trace_request();

        let expected_batch = otlp_traces_to_arrow(&request).unwrap();
        let expected_bytes = record_batch_to_bytes(&expected_batch).unwrap();

        handler
            .handle_grpc_otlp_traces(&tenant_context, request)
            .await
            .unwrap();

        assert_eq!(
            only_wal_entry_bytes(&wal_manager, &tenant_context, "traces").await,
            expected_bytes
        );
    }
}

#[cfg(test)]
mod evaluation_span_event_tests {
    //! Change agent-offline-evals, task 7.1: a `gen_ai.evaluation.result`
    //! span event is also written through the log ingest path.

    use std::sync::Arc;

    use common::evals::{EVALUATION_NAME, EVALUATION_RESULT_EVENT};
    use common::wal::bytes_to_record_batch;
    use datafusion::arrow::array::{Array, BinaryArray, StringArray};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::trace::v1::span::Event;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use tempfile::TempDir;

    use super::*;
    use crate::handler::test_support::{
        only_wal_entry_bytes, test_tenant_context, test_wal_manager, transport_without_writer,
    };

    const TRACE_ID: [u8; 16] = [0x42; 16];
    const SPAN_ID: [u8; 8] = [0x24; 8];

    fn trace_request(events: Vec<Event>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: TRACE_ID.to_vec(),
                        span_id: SPAN_ID.to_vec(),
                        name: "invoke_agent triage".to_string(),
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 2_000,
                        events,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn evaluation_event() -> Event {
        Event {
            time_unix_nano: 1_500,
            name: EVALUATION_RESULT_EVENT.to_string(),
            attributes: vec![KeyValue {
                key: EVALUATION_NAME.to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("Correctness".to_string())),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
        }
    }

    async fn unprocessed(wal_manager: &WalManager, signal: &str) -> usize {
        let tenant_context = test_tenant_context();
        wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                signal,
            )
            .await
            .unwrap()
            .get_unprocessed_entries()
            .await
            .unwrap()
            .len()
    }

    async fn handler(wal_manager: Arc<WalManager>) -> TraceHandler {
        let (transport, processor_registry) = transport_without_writer().await;
        let log_handler = Arc::new(LogHandler::new(
            transport.clone(),
            wal_manager.clone(),
            processor_registry.clone(),
        ));
        TraceHandler::new(transport, wal_manager, processor_registry)
            .with_evaluation_logs(log_handler)
    }

    #[tokio::test]
    async fn an_evaluation_span_event_becomes_a_logs_batch_with_the_span_trace_context() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let handler = handler(wal_manager.clone()).await;
        let tenant_context = test_tenant_context();

        handler
            .handle_grpc_otlp_traces(&tenant_context, trace_request(vec![evaluation_event()]))
            .await
            .unwrap();

        assert_eq!(unprocessed(&wal_manager, "traces").await, 1);
        let batch = bytes_to_record_batch(
            &only_wal_entry_bytes(&wal_manager, &tenant_context, "logs").await,
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
        let binary = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0)
                .to_vec()
        };
        assert_eq!(binary("trace_id"), TRACE_ID);
        assert_eq!(binary("span_id"), SPAN_ID);
        let event_name = batch
            .column_by_name("event_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(event_name.value(0), EVALUATION_RESULT_EVENT);
    }

    #[tokio::test]
    async fn a_trace_without_evaluation_events_writes_no_logs() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let handler = handler(wal_manager.clone()).await;
        let other = Event {
            name: "exception".to_string(),
            ..Default::default()
        };

        handler
            .handle_grpc_otlp_traces(&test_tenant_context(), trace_request(vec![other]))
            .await
            .unwrap();

        assert_eq!(unprocessed(&wal_manager, "traces").await, 1);
        assert_eq!(unprocessed(&wal_manager, "logs").await, 0);
    }

    #[tokio::test]
    async fn a_resent_trace_export_writes_its_evaluation_logs_once() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = Arc::new(test_wal_manager(temp_dir.path()));
        let handler = handler(wal_manager.clone()).await;
        let tenant_context = test_tenant_context();

        for _ in 0..2 {
            handler
                .handle_grpc_otlp_traces(&tenant_context, trace_request(vec![evaluation_event()]))
                .await
                .unwrap();
        }

        assert_eq!(unprocessed(&wal_manager, "traces").await, 1);
        assert_eq!(unprocessed(&wal_manager, "logs").await, 1);
    }
}
