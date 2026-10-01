//! # Client resends dedup at the writer, across acceptor processes
//!
//! An OTLP exporter that times out after the acceptor already flushed its
//! batch resends the identical export. Each acceptor recognizes a resend it
//! saw itself, but that cache is per process: a resend reaching another
//! replica, or the same acceptor after a restart, gets a fresh WAL entry.
//! The acceptor therefore forwards every batch under its content fingerprint
//! as `ingest_id`, and the writer's ingest-id dedup drops the copies.
//!
//! This drives three byte-identical exports through three acceptor
//! processes, each with its own WAL and resend cache: one forwarded on the
//! hot path, one after a "restart", and one whose forward failed and that
//! only reaches the writer through the WAL retry consumer. Exactly one copy
//! may be left for the writer to commit.

use std::sync::Arc;
use std::time::Duration;

use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::handler::{WalManager, WalRetryConsumer};
use common::auth::{TenantContext, TenantSource};
use common::catalog::Catalog;
use common::catalog_manager::CatalogManager;
use common::config::{ProcessorsConfig, WriterConfig};
use common::flight::transport::InMemoryFlightTransport;
use common::processors::ProcessorRegistry;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, span::SpanKind};
use tempfile::TempDir;
use tests_integration::test_support::writer_service_with_type_authority;

fn tenant_context() -> TenantContext {
    TenantContext::new(
        "acme".to_string(),
        "production".to_string(),
        "acme".to_string(),
        "production".to_string(),
        Some("test-key".to_string()),
        TenantSource::Config,
    )
}

fn export_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key_strindex: 0,
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("checkout".to_string())),
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
                    name: "charge".to_string(),
                    kind: SpanKind::Server as i32,
                    start_time_unix_nano: 1_000,
                    end_time_unix_nano: 2_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: None,
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn wal_manager(dir: &std::path::Path) -> Arc<WalManager> {
    Arc::new(WalManager::uniform(WalConfig::with_defaults(
        dir.to_path_buf(),
    )))
}

async fn acceptor_transport(catalog: Catalog) -> Arc<InMemoryFlightTransport> {
    let bootstrap =
        ServiceBootstrap::new_for_test_with_catalog(catalog, ServiceType::Acceptor, "127.0.0.1:0")
            .await
            .unwrap();
    Arc::new(InMemoryFlightTransport::new(bootstrap))
}

/// A fresh acceptor process: its own WAL, resend cache and trace handler.
async fn acceptor(
    transport: Arc<InMemoryFlightTransport>,
    wal_dir: &std::path::Path,
) -> (TraceHandler, Arc<WalManager>) {
    let wals = wal_manager(wal_dir);
    let processor_registry = Arc::new(ProcessorRegistry::new(
        Arc::new(Catalog::new_in_memory().await.unwrap()),
        &ProcessorsConfig::default(),
    ));
    (
        TraceHandler::new(transport, wals.clone(), processor_registry),
        wals,
    )
}

#[tokio::test]
async fn identical_exports_through_separate_acceptors_commit_once_at_the_writer() {
    let catalog = Catalog::new_in_memory().await.unwrap();

    // A real writer behind Flight, registered for discovery.
    let writer_dir = TempDir::new().unwrap();
    let writer_wals = wal_manager(writer_dir.path());
    let writer_service = writer_service_with_type_authority(
        Arc::new(CatalogManager::new_in_memory().await.unwrap()),
        writer_wals.clone(),
        &WriterConfig::default(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(common::flight::flight_service_server(writer_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .ok();
    });
    ServiceBootstrap::new_for_test_with_catalog(
        catalog.clone(),
        ServiceType::Writer,
        &writer_addr.to_string(),
    )
    .await
    .unwrap();
    let transport = acceptor_transport(catalog).await;
    let tenant = tenant_context();

    // First delivery, and a resend after the acceptor restarted: two
    // processes, two WALs, two empty resend caches.
    for _ in 0..2 {
        let wal_dir = TempDir::new().unwrap();
        let (handler, _) = acceptor(transport.clone(), wal_dir.path()).await;
        handler
            .handle_grpc_otlp_traces(&tenant, export_request())
            .await
            .unwrap();
    }

    // A resend at a replica that cannot reach any writer: the export is
    // acked from its WAL, and only the retry consumer forwards it.
    let replica_dir = TempDir::new().unwrap();
    let unreachable = acceptor_transport(Catalog::new_in_memory().await.unwrap()).await;
    let (replica, replica_wals) = acceptor(unreachable, replica_dir.path()).await;
    replica
        .handle_grpc_otlp_traces(&tenant, export_request())
        .await
        .unwrap();
    let mut consumer = WalRetryConsumer::new(replica_wals.clone(), transport)
        .with_timing(Duration::from_secs(1), Duration::ZERO);
    let stats = consumer.run_once().await.unwrap();
    assert_eq!(stats.retried, 1, "the retry consumer forwards the resend");

    let writer_wal = writer_wals
        .get_wal("acme", "production", "traces")
        .await
        .unwrap();
    assert_eq!(
        writer_wal.get_entries().await.unwrap().len(),
        3,
        "every copy reached the writer"
    );
    assert_eq!(
        writer_wal.get_unprocessed_entries().await.unwrap().len(),
        1,
        "only the first copy is left for the writer to commit"
    );
}
