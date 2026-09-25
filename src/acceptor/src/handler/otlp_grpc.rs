//! # OTLP Trace Handler
//!
//! Converts an `ExportTraceServiceRequest` to Arrow, writes it to the
//! tenant/dataset's traces WAL, and forwards it to a writer via Flight.
//! Named `otlp_grpc` for its original gRPC-only origin; both the gRPC
//! (`services::otlp_trace_service`) and HTTP (`lib::handle_http_traces`)
//! surfaces share this one handler.

use std::sync::Arc;

use anyhow::Context;
use common::auth::TenantContext;
use common::flight::conversion::otlp_traces_to_arrow;
use common::flight::transport::InMemoryFlightTransport;
use common::processors::ProcessorRegistry;
use common::wal::{WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use super::WalManager;
use super::forward::spawn_forward_and_mark;
use super::ingest_error::IngestError;
use super::processors_apply::apply_trace_processors;

pub struct TraceHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL manager for multi-tenant WAL isolation
    wal_manager: Arc<WalManager>,
    /// Tenant OTTL processors (change: tenant-ottl-processors)
    processor_registry: Arc<ProcessorRegistry>,
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
            processor_registry,
        }
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
        let metadata_str = serde_json::to_string(&metadata).ok();

        let batch_bytes = record_batch_to_bytes(&record_batch)
            .context("Failed to serialize record batch")
            .map_err(IngestError::Unavailable)?;

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
        // connected clients unchanged.
        let forward_task = spawn_forward_and_mark(
            self.flight_transport.clone(),
            wal,
            wal_entry_id,
            record_batch,
            metadata_str,
            "traces",
        );
        if let Err(e) = forward_task.await {
            tracing::error!(entry_id = %wal_entry_id, error = %e, "Forward-and-mark task for traces did not complete");
        }

        // Data is durable in the WAL at this point; forward failures are
        // recovered by the retry consumer, so the export is acknowledged.
        Ok(())
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
    use common::auth::{TenantContext, TenantSource};
    use common::catalog::Catalog;
    use common::flight::transport::InMemoryFlightTransport;
    use common::processors::ProcessorRegistry;
    use common::service_bootstrap::{ServiceBootstrap, ServiceType};
    use common::wal::WalConfig;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{
        ResourceSpans, ScopeSpans, Span, Status as SpanStatus, span::SpanKind,
    };
    use tempfile::TempDir;
    use tokio::sync::Notify;

    use super::*;

    fn test_tenant_context() -> TenantContext {
        TenantContext {
            tenant_id: "acme".to_string(),
            dataset_id: "production".to_string(),
            tenant_slug: "acme".to_string(),
            dataset_slug: "production".to_string(),
            api_key_name: Some("test-key".to_string()),
            api_key_scopes: None,
            api_key_dataset_ids: None,
            oauth_tenant_grants: None,
            api_key_allowed_origins: None,
            user_id: None,
            role: None,
            is_instance_admin: false,
            session_id: None,
            source: TenantSource::Config,
        }
    }

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

    fn test_wal_manager(base_dir: &std::path::Path) -> WalManager {
        let config = WalConfig::with_defaults(base_dir.to_path_buf());
        WalManager::new(config.clone(), config.clone(), config.clone(), config)
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
}
