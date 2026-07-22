//! Integration tests for the self-monitoring epic (#447).
//!
//! Covers the acceptance criteria of #455:
//! - self-monitoring OTLP export is accepted and lands in the `_system` /
//!   `_monitoring` WAL (dogfooding loop works end to end)
//! - the anti-loop guard prevents re-instrumentation of `_system` requests
//! - W3C trace context propagates across Flight metadata (parent/child
//!   spans share a trace)
//! - the configured sampling ratio is respected

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::handler::wal_manager::WalManager;
use acceptor::middleware::grpc_auth_interceptor;
use acceptor::services::otlp_trace_service::TraceAcceptorService;
use common::auth::{Authenticator, TenantContext, TenantSource};
use common::catalog::Catalog;
use common::config::Configuration;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

fn otlp_trace_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![OtlpSpan {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "test-span".to_string(),
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 2,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn wal_config(dir: &TempDir, sub: &str) -> WalConfig {
    WalConfig {
        wal_dir: dir.path().join(sub),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
        ..Default::default()
    }
}

/// End-to-end dogfooding: `init_telemetry` exports spans via OTLP to an
/// in-process acceptor which authenticates the auto-provisioned `_system`
/// tenant and persists to the `_system`/`_monitoring` WAL.
#[tokio::test(flavor = "multi_thread")]
async fn self_monitoring_export_lands_in_system_wal() {
    let temp_dir = TempDir::new().unwrap();

    // Acceptor infrastructure (auth enabled, discovery via sqlite)
    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: format!("sqlite://{}", temp_dir.path().join("catalog.db").display()),
        heartbeat_interval: Duration::from_secs(30),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(300),
    });
    config.auth.enabled = true;
    config.auth.admin_api_key = Some("test-admin-key".to_string());
    config.self_monitoring.enabled = true;
    config.self_monitoring.trace_sample_ratio = 1.0;
    // Auto-provision the _system tenant from the admin key (issue #448)
    config.ensure_self_monitoring_tenant();
    assert!(config.auth.tenants.iter().any(|t| t.id == "_system"));

    let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
    let authenticator = Arc::new(Authenticator::new(config.auth.clone(), catalog));

    let bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:0".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));

    let wal_manager = Arc::new(WalManager::new(
        wal_config(&temp_dir, "traces"),
        wal_config(&temp_dir, "logs"),
        wal_config(&temp_dir, "metrics"),
    ));

    let trace_handler = TraceHandler::new(flight_transport, wal_manager.clone());
    let service = TraceAcceptorService::new(trace_handler);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let acceptor_addr = listener.local_addr().unwrap();
    let auth = authenticator.clone();
    tokio::spawn(
        Server::builder()
            .add_service(TraceServiceServer::with_interceptor(service, move |req| {
                grpc_auth_interceptor(auth.clone(), req)
            }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    // Point self-monitoring at the in-process acceptor and export a span.
    config.self_monitoring.endpoint = format!("http://{acceptor_addr}");
    let telemetry = common::self_monitoring::init_telemetry(&config, "sm-test")
        .unwrap()
        .expect("telemetry enabled");

    {
        use opentelemetry::trace::{Tracer, TracerProvider as _};
        let tracer = telemetry.tracer_provider().tracer("sm-test");
        let span = tracer.start("self-monitoring-test-span");
        drop(span);
    }
    telemetry
        .tracer_provider()
        .force_flush()
        .expect("flush spans");

    // The span must arrive in the _system/_monitoring traces WAL.
    let wal = wal_manager
        .get_wal("_system", "_monitoring", "traces")
        .await
        .unwrap();
    let mut entries = Vec::new();
    for _ in 0..40 {
        entries = wal.get_entries().await.unwrap();
        if !entries.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        !entries.is_empty(),
        "self-monitoring span export did not reach the _system/_monitoring WAL"
    );
}

/// A tracing layer counting spans and events it is allowed to observe.
#[derive(Clone, Default)]
struct CountingLayer(Arc<AtomicUsize>);

impl<S: tracing::Subscriber> Layer<S> for CountingLayer {
    fn on_new_span(
        &self,
        _attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn on_event(
        &self,
        _event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn tenant_context(tenant: &str, dataset: &str) -> TenantContext {
    TenantContext {
        tenant_id: tenant.to_string(),
        dataset_id: dataset.to_string(),
        tenant_slug: tenant.to_string(),
        dataset_slug: dataset.to_string(),
        api_key_name: None,
        source: TenantSource::Config,
    }
}

/// Anti-loop guard (#448): processing a `_system` tenant request must not
/// produce any spans/events on the OTel export layers, while a normal
/// tenant request does.
#[tokio::test(flavor = "multi_thread")]
async fn anti_loop_guard_prevents_reinstrumentation_of_system_requests() {
    use tracing::instrument::WithSubscriber;

    async fn run_export(tenant: &str, counter: &Arc<AtomicUsize>) {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Configuration::default();
        config.discovery = Some(common::config::DiscoveryConfig {
            dsn: format!("sqlite://{}", temp_dir.path().join("catalog.db").display()),
            heartbeat_interval: Duration::from_secs(30),
            poll_interval: Duration::from_secs(60),
            ttl: Duration::from_secs(300),
        });
        let bootstrap = ServiceBootstrap::new(
            config.clone(),
            ServiceType::Acceptor,
            "127.0.0.1:0".to_string(),
        )
        .await
        .unwrap();
        let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
        let wal_manager = Arc::new(WalManager::new(
            wal_config(&temp_dir, "traces"),
            wal_config(&temp_dir, "logs"),
            wal_config(&temp_dir, "metrics"),
        ));
        let service = TraceAcceptorService::new(TraceHandler::new(flight_transport, wal_manager));

        let subscriber = tracing_subscriber::registry().with(
            CountingLayer(counter.clone()).with_filter(common::self_monitoring::OtelExportFilter),
        );

        let mut request = tonic::Request::new(otlp_trace_request());
        request
            .extensions_mut()
            .insert(tenant_context(tenant, "_monitoring"));

        async {
            let _ = service.export(request).await.unwrap();
        }
        .with_subscriber(subscriber)
        .await;
    }

    let system_count = Arc::new(AtomicUsize::new(0));
    run_export("_system", &system_count).await;
    assert_eq!(
        system_count.load(Ordering::SeqCst),
        0,
        "processing a _system request must not emit exportable telemetry"
    );

    let normal_count = Arc::new(AtomicUsize::new(0));
    run_export("acme", &normal_count).await;
    assert!(
        normal_count.load(Ordering::SeqCst) > 0,
        "processing a normal tenant request should emit exportable telemetry"
    );
}

fn in_memory_tracing_setup(
    sampler: opentelemetry_sdk::trace::Sampler,
) -> (
    opentelemetry_sdk::trace::InMemorySpanExporter,
    opentelemetry_sdk::trace::SdkTracerProvider,
    impl tracing::Subscriber,
) {
    use opentelemetry::trace::TracerProvider as _;

    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    (exporter, provider, subscriber)
}

/// Trace context propagation (#451): a "server-side" span that adopts the
/// traceparent carried in Flight app_metadata fields joins the client's
/// trace as a child span.
#[tokio::test(flavor = "multi_thread")]
async fn trace_context_propagates_via_flight_metadata_fields() {
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let (exporter, provider, subscriber) =
        in_memory_tracing_setup(opentelemetry_sdk::trace::Sampler::AlwaysOn);

    let traceparent = tracing::subscriber::with_default(subscriber, || {
        // Client side: capture the current context for app_metadata
        let client_span = tracing::info_span!("client_do_put");
        let fields = {
            let _guard = client_span.enter();
            common::flight::trace_context::current_trace_context_fields()
        };
        let (traceparent, tracestate) = fields.expect("active context to propagate");

        // Server side: a fresh span adopts the propagated parent (set
        // before first enter, as required by tracing-opentelemetry)
        {
            let server_span = tracing::info_span!("server_do_put");
            common::flight::trace_context::set_parent_from_fields(
                &server_span,
                Some(&traceparent),
                tracestate.as_deref(),
            );
            let _guard = server_span.enter();
        }
        traceparent
    });

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 2, "expected client and server spans");

    let client = spans
        .iter()
        .find(|s| s.name == "client_do_put")
        .expect("client span");
    let server = spans
        .iter()
        .find(|s| s.name == "server_do_put")
        .expect("server span");
    assert_eq!(
        client.span_context.trace_id(),
        server.span_context.trace_id(),
        "server span must join the client's trace"
    );
    assert_eq!(
        server.parent_span_id,
        client.span_context.span_id(),
        "server span must be a child of the client span"
    );
    // Sanity: the traceparent embeds the shared trace id
    assert!(
        traceparent.contains(&client.span_context.trace_id().to_string()),
        "traceparent must carry the client trace id"
    );
}

/// Sampling (#450): ratio 1.0 exports every span, ratio 0.0 exports none.
#[tokio::test(flavor = "multi_thread")]
async fn sampling_ratio_respected() {
    use opentelemetry_sdk::trace::Sampler;

    for (ratio, expected) in [(1.0_f64, 100_usize), (0.0_f64, 0_usize)] {
        let (exporter, provider, subscriber) =
            in_memory_tracing_setup(Sampler::TraceIdRatioBased(ratio));
        tracing::subscriber::with_default(subscriber, || {
            for i in 0..100 {
                let span = tracing::info_span!("sampled_span", iteration = i);
                drop(span);
            }
        });
        provider.force_flush().unwrap();
        let count = exporter.get_finished_spans().unwrap().len();
        assert_eq!(
            count, expected,
            "sample ratio {ratio} should export {expected} of 100 spans"
        );
    }
}
