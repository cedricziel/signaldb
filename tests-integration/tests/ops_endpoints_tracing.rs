//! Tracing coverage for the router's `/api/v1/ops/*` operational-control
//! proxy (`router::endpoints::ops::do_compactor_action`).
//!
//! Lives in its own integration-test binary: it installs process-global
//! OTel/tracing state (subscriber + propagator) that must not leak into
//! other tests, matching `http_response_headers.rs`.
//!
//! Verifies the RPC CLIENT span the router opens around `do_action` carries
//! `server.address`/`server.port` (RPC semconv) and that its trace context
//! actually reaches the compactor's SERVER span — otherwise an ops call to
//! the compactor shows up as two disconnected traces instead of one, the
//! same class of gap fixed for the query/search Flight paths.

use std::sync::Arc;

use common::catalog::Catalog;
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use router::RouterState;
use router::discovery::ServiceRegistry;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tracing_subscriber::layer::SubscriberExt;
use uuid::Uuid;

const ADMIN_KEY: &str = "admin-key-123";

#[derive(Clone)]
struct State {
    catalog: Catalog,
    service_registry: ServiceRegistry,
    config: Configuration,
    authenticator: Arc<common::auth::Authenticator>,
}
impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("State")
    }
}
impl RouterState for State {
    fn catalog(&self) -> &Catalog {
        &self.catalog
    }
    fn service_registry(&self) -> &ServiceRegistry {
        &self.service_registry
    }
    fn config(&self) -> &Configuration {
        &self.config
    }
    fn authenticator(&self) -> &Arc<common::auth::Authenticator> {
        &self.authenticator
    }
}

/// Build a real (in-memory-backed) compactor Flight service, the same way
/// `compactor::flight::tests::make_service` does.
async fn make_real_compactor_service() -> compactor::flight::CompactorFlightService {
    use compactor::executor::{CompactionExecutor, ExecutorConfig};
    use compactor::lease::LeaseManager;
    use compactor::metrics::CompactionMetrics;
    use compactor::planner::{CompactionPlanner, PlannerConfig};
    use std::time::Duration;

    let catalog_manager = Arc::new(
        common::catalog_manager::CatalogManager::new_in_memory()
            .await
            .unwrap(),
    );
    let planner = Arc::new(CompactionPlanner::new(
        catalog_manager.clone(),
        PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024,
            target_file_size_bytes: 128 * 1024 * 1024,
            partition_lateness: Duration::from_secs(600),
            max_partition_input_bytes: 0,
        },
    ));
    let metrics = CompactionMetrics::new();
    let executor = Arc::new(CompactionExecutor::new(
        catalog_manager,
        ExecutorConfig::default(),
        metrics.clone(),
    ));
    let catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
    let lease_manager = LeaseManager::new(catalog, Uuid::new_v4(), Duration::from_secs(300));
    compactor::flight::CompactorFlightService::new(planner, executor, lease_manager, metrics)
}

/// The router's `do_action` CLIENT span must carry `server.address`/
/// `server.port` (RPC semconv) and its trace context must actually reach
/// the compactor's SERVER span — otherwise an ops call to the compactor
/// shows up as two disconnected traces instead of one, the same class of
/// gap fixed for the query/search Flight paths.
#[tokio::test(flavor = "multi_thread")]
async fn ops_compact_status_client_span_carries_server_address_and_joins_compactor_trace() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).unwrap();
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let temp_dir = TempDir::new().unwrap();
    let catalog_dsn = format!("sqlite://{}", temp_dir.path().join("catalog.db").display());

    let mut config = Configuration::default();
    config.auth.admin_api_key = Some(ADMIN_KEY.to_string());
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: std::time::Duration::from_secs(5),
        poll_interval: std::time::Duration::from_secs(60),
        ttl: std::time::Duration::from_secs(30),
    });

    let catalog = Catalog::new(&catalog_dsn).await.unwrap();

    // Serve the real compactor Flight service (not a stub) so its SERVER
    // span actually exists to join with.
    let compactor_service = make_real_compactor_service().await;
    let compactor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let compactor_addr = compactor_listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(common::flight::flight_service_server(compactor_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                compactor_listener,
            ))
            .await
            .unwrap();
    });
    catalog
        .register_ingester(
            Uuid::new_v4(),
            &compactor_addr.to_string(),
            ServiceType::Compactor,
            &[ServiceCapability::StorageMaintenance],
        )
        .await
        .expect("compactor registered");

    let router_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Router,
        "127.0.0.1:50054".to_string(),
    )
    .await
    .expect("router bootstrap");
    let service_registry = ServiceRegistry::with_flight_transport(
        catalog.clone(),
        InMemoryFlightTransport::new(router_bootstrap),
    );

    let authenticator = Arc::new(common::auth::Authenticator::new(
        config.auth.clone(),
        Arc::new(catalog.clone()),
    ));
    let state = State {
        catalog,
        service_registry,
        config,
        authenticator,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router::create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/v1/ops/compact/status"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await
        .expect("request sent");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "expected the real compactor to answer"
    );

    // The compactor's SERVER span ends on the tonic server task, which can
    // still be mid-export when the router's HTTP response arrives here —
    // poll instead of assuming one flush already caught both spans.
    let mut spans = Vec::new();
    for _ in 0..50 {
        provider.force_flush().unwrap();
        spans = exporter.get_finished_spans().unwrap();
        let has_client = spans
            .iter()
            .any(|s| s.span_kind == opentelemetry::trace::SpanKind::Client);
        let has_server = spans
            .iter()
            .any(|s| s.span_kind == opentelemetry::trace::SpanKind::Server);
        if has_client && has_server {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Client and server spans share the same name (`{method} {detail}`), so
    // disambiguate by kind rather than relying on find() picking the right one.
    let client_span = spans
        .iter()
        .find(|s| {
            s.span_kind == opentelemetry::trace::SpanKind::Client
                && s.name == "arrow.flight.protocol.FlightService/DoAction compact_status"
        })
        .expect("router's do_action CLIENT span");

    let server_address = client_span
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "server.address")
        .map(|kv| kv.value.as_str().to_string());
    assert_eq!(
        server_address.as_deref(),
        Some(compactor_addr.ip().to_string().as_str()),
        "CLIENT span must carry server.address per RPC semconv"
    );
    let server_port = client_span
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "server.port")
        .map(|kv| kv.value.as_str().to_string());
    assert_eq!(
        server_port.as_deref(),
        Some(compactor_addr.port().to_string().as_str()),
        "CLIENT span must carry server.port per RPC semconv"
    );

    let server_span = spans
        .iter()
        .find(|s| {
            s.span_kind == opentelemetry::trace::SpanKind::Server
                && s.name
                    .starts_with("arrow.flight.protocol.FlightService/DoAction")
        })
        .expect("compactor's do_action SERVER span");
    assert_eq!(
        server_span.span_context.trace_id(),
        client_span.span_context.trace_id(),
        "compactor's SERVER span must join the router's CLIENT trace, not start a new one"
    );
    assert_eq!(
        server_span.parent_span_id,
        client_span.span_context.span_id(),
        "compactor's SERVER span must be a child of the router's CLIENT span"
    );
}
