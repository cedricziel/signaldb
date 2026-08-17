//! End-to-end tests for the `signaldb-sdk` query surface against a live router.
//!
//! Boots the ingest→store→query stack (acceptor metrics handler → WAL →
//! writer → Iceberg → querier), serves the **full router** over real TCP —
//! both its HTTP app and its Arrow Flight endpoint — and drives all four SDK
//! query paths, asserting each returns its native shape:
//!
//! - `QueryClient::sql` (hand-written Flight transport) → Arrow rows
//! - `Client::promql_query` (generated HTTP) → native Prometheus JSON
//! - `Client::logql_query` (generated HTTP) → native Loki JSON
//! - `Client::search` (generated HTTP, TraceQL) → native Tempo JSON
//!
//! The SQL path is the one piece of hand-written SDK code (see `signaldb-sdk`
//! `query.rs`), so it carries the load-bearing assertion here.

use acceptor::handler::WalManager;
use acceptor::handler::otlp_metrics_handler::MetricsHandler;
use common::CatalogManager;
use common::auth::{TenantContext, TenantSource};
use common::catalog::Catalog;
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data,
        number_data_point,
    },
    resource::v1::Resource,
};
use querier::flight::QuerierFlightService;
use router::{RouterState, create_flight_service, create_router, discovery::ServiceRegistry};
use signaldb_sdk::{Client, QueryClient};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::transport::Server;
use writer::IcebergWriterFlightService;

const BASE_NS: u64 = 1_700_000_000_000_000_000;
const API_KEY: &str = "test-key-123";
const TENANT: &str = "test-tenant";
const DATASET: &str = "test-dataset";

struct TestServices {
    flight_transport: Arc<InMemoryFlightTransport>,
    metrics_handler: MetricsHandler,
    config: Configuration,
    _temp_dir: TempDir,
}

fn test_tenant_context() -> TenantContext {
    TenantContext {
        tenant_id: TENANT.to_string(),
        dataset_id: DATASET.to_string(),
        tenant_slug: TENANT.to_string(),
        dataset_slug: DATASET.to_string(),
        api_key_name: Some("test-key".to_string()),
        api_key_scopes: None,
        api_key_dataset_id: None,
        user_id: None,
        role: None,
        is_instance_admin: false,
        session_id: None,
        source: TenantSource::Config,
    }
}

fn string_value(s: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(s.to_string())),
    }
}

/// One gauge metric `requests` for a service.
fn gauge_metrics(service: &str, value: f64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(string_value(service)),
                    ..Default::default()
                }],
                dropped_attributes_count: 0,
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "requests".to_string(),
                    description: String::new(),
                    unit: "1".to_string(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: BASE_NS,
                            time_unix_nano: BASE_NS,
                            value: Some(number_data_point::Value::AsDouble(value)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Concrete `RouterState` for the served router.
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

async fn setup() -> TestServices {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_path).unwrap();
    let storage_dsn = format!("file://{}", storage_path.display());
    let object_store =
        common::storage::create_object_store_from_dsn(&storage_dsn).expect("object store");

    let catalog_dsn = format!("sqlite://{}", temp_dir.path().join("catalog.db").display());
    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn,
        heartbeat_interval: Duration::from_secs(5),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(30),
    });
    config.storage.dsn = storage_dsn;
    config.schema.catalog_uri = format!(
        "sqlite://{}",
        temp_dir.path().join("iceberg_catalog.db").display()
    );
    config.auth = common::config::AuthConfig {
        tenants: vec![common::config::TenantConfig {
            id: TENANT.to_string(),
            slug: TENANT.to_string(),
            name: "Test Tenant".to_string(),
            default_dataset: Some(DATASET.to_string()),
            datasets: vec![],
            api_keys: vec![common::config::ApiKeyConfig {
                key: API_KEY.to_string(),
                name: Some("test-key".to_string()),
            }],
            schema_config: None,
            limits: None,
        }],
        admin_api_key: None,
        internal_service_key: None,
        ..Default::default()
    };

    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
        tenant_id: TENANT.to_string(),
        dataset_id: DATASET.to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    };

    let acceptor_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:50170".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(acceptor_bootstrap));

    // Writer.
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);
    let writer_wal = Arc::new(common::wal::manager::WalManager::uniform(
        wal_config.clone(),
    ));
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("catalog mgr"),
    );
    let writer_service = IcebergWriterFlightService::new(
        catalog_manager.clone(),
        object_store.clone(),
        writer_wal.clone(),
        &common::config::WriterConfig::default(),
    );
    let _writer_bg = writer_service.start_background_processing();
    tokio::spawn(
        Server::builder()
            .add_service(common::flight::flight_service_server(writer_service))
            .serve(writer_addr),
    );
    let _writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();

    // Pre-create the tenant namespace.
    {
        use iceberg_rust::catalog::namespace::Namespace;
        let namespace = Namespace::try_new(&[TENANT.to_string(), DATASET.to_string()]).unwrap();
        catalog_manager
            .catalog()
            .create_namespace(&namespace, None)
            .await
            .expect("pre-create namespace");
    }

    // Querier.
    let querier_service = QuerierFlightService::new_with_catalog_manager(
        flight_transport.clone(),
        catalog_manager,
        common::config::QuerierConfig::default(),
    )
    .await
    .expect("querier service");
    let querier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let querier_addr = querier_listener.local_addr().unwrap();
    drop(querier_listener);
    tokio::spawn(
        Server::builder()
            .add_service(common::flight::flight_service_server(querier_service))
            .serve(querier_addr),
    );
    let _querier_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Querier,
        querier_addr.to_string(),
    )
    .await
    .unwrap();

    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
        wal_config,
    ));
    let metrics_handler = MetricsHandler::new(flight_transport.clone(), wal_manager);

    // Wait for query + storage services to register.
    for attempt in 0..50 {
        let has_query = !flight_transport
            .discover_services_by_capability(ServiceCapability::QueryExecution)
            .await
            .is_empty();
        let has_storage = !flight_transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await
            .is_empty();
        if has_query && has_storage {
            break;
        }
        assert!(attempt < 49, "services failed to register");
        sleep(Duration::from_millis(100)).await;
    }

    TestServices {
        flight_transport,
        metrics_handler,
        config,
        _temp_dir: temp_dir,
    }
}

/// Serve the full router (HTTP app + Flight endpoint) on ephemeral ports.
/// Returns `(http_base_url, flight_url)`.
async fn serve_router(services: &TestServices) -> (String, String) {
    let catalog = Catalog::new(services.config.discovery.as_ref().unwrap().dsn.as_str())
        .await
        .unwrap();
    let service_registry = ServiceRegistry::with_flight_transport(
        catalog.clone(),
        (*services.flight_transport).clone(),
    );
    let authenticator = Arc::new(common::auth::Authenticator::new(
        services.config.auth.clone(),
        Arc::new(catalog.clone()),
    ));
    let state = State {
        catalog,
        service_registry,
        config: services.config.clone(),
        authenticator,
    };

    // HTTP app.
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let app = create_router(state.clone());
    tokio::spawn(async move {
        axum::serve(http_listener, app).await.unwrap();
    });

    // Flight endpoint (SQL).
    let flight_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let flight_addr = flight_listener.local_addr().unwrap();
    drop(flight_listener);
    let flight_service = create_flight_service(state);
    tokio::spawn(
        Server::builder()
            .add_service(common::flight::flight_service_server(flight_service))
            .serve(flight_addr),
    );

    // Wait for both listeners to accept connections before returning, so the
    // SDK doesn't race the servers' bind.
    for addr in [http_addr, flight_addr] {
        for attempt in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            assert!(attempt < 49, "server at {addr} never became reachable");
            sleep(Duration::from_millis(50)).await;
        }
    }

    (
        format!("http://{http_addr}"),
        format!("http://{flight_addr}"),
    )
}

/// Build an SDK HTTP client that carries the bearer key and tenant header on
/// every request, mirroring how the CLI/MCP construct it.
fn sdk_http_client(base_url: &str) -> Client {
    signaldb_sdk::ClientBuilder::new(base_url)
        .bearer(API_KEY)
        .tenant(TENANT)
        .build()
        .unwrap()
}

#[tokio::test]
async fn sdk_query_methods_against_live_router() {
    let services = setup().await;
    let ctx = test_tenant_context();

    // Ingest one metric so the metrics table exists for PromQL.
    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&ctx, gauge_metrics("api", 10.0))
        .await
        .expect("ingest gauge");
    // Force the writer to commit now — deterministic read-your-writes instead
    // of waiting out the asynchronous background commit loop.
    common::testing::flush_storage_writers(&services.flight_transport, TENANT, Some(DATASET))
        .await
        .expect("flush writer");

    let (base_url, flight_url) = serve_router(&services).await;

    // --- SQL over the hand-written Flight transport (the load-bearing case) ---
    let rows = QueryClient::new(&flight_url)
        .with_api_key(API_KEY)
        .with_tenant(TENANT)
        .with_dataset(DATASET)
        .sql("SELECT 1 AS one")
        .await
        .expect("SQL query over Flight");
    let total_rows: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "SELECT 1 must return exactly one row");
    assert_eq!(rows[0].num_columns(), 1, "SELECT 1 must return one column");

    // --- PromQL over the generated HTTP client → native Prometheus JSON ---
    let client = sdk_http_client(&base_url);
    let promql = client
        .promql_query()
        .query("requests")
        .send()
        .await
        .expect("promql_query")
        .into_inner();
    assert_eq!(
        promql.get("status").and_then(|s| s.as_str()),
        Some("success"),
        "PromQL response must be native Prometheus JSON: {promql}"
    );
    assert!(
        promql.pointer("/data/resultType").is_some(),
        "PromQL response must carry data.resultType: {promql}"
    );

    // --- LogQL over the generated HTTP client ---
    // No logs were ingested, so the logs table may be absent and the router
    // returns a native Loki JSON error with a server status. This asserts the
    // SDK builds the right request and reaches the endpoint with an HTTP
    // response (not a transport error); the success-shape path is covered by
    // `logql_queries.rs`.
    match client
        .logql_query()
        .query("{service_name=\"api\"}")
        .send()
        .await
    {
        Ok(resp) => {
            let body = resp.into_inner();
            assert_eq!(
                body.get("status").and_then(|s| s.as_str()),
                Some("success"),
                "LogQL response must be native Loki JSON: {body}"
            );
        }
        Err(signaldb_sdk::Error::UnexpectedResponse(r)) => {
            assert!(
                r.status().is_client_error() || r.status().is_server_error(),
                "expected an HTTP response from the router, got {}",
                r.status()
            );
        }
        Err(e) => panic!("logql_query did not reach the router: {e:?}"),
    }

    // --- TraceQL (Tempo search) over the generated HTTP client ---
    // `search` returns a typed `SearchResult`, so a successful deserialization
    // is itself the native-shape proof. As with LogQL, tolerate the no-data
    // server response; success-shape is covered by `router_tempo_endpoints.rs`.
    match client.search().send().await {
        Ok(resp) => {
            let traces = resp.into_inner();
            assert!(
                traces.traces.is_empty(),
                "no traces ingested, expected empty result: {traces:?}"
            );
        }
        Err(signaldb_sdk::Error::UnexpectedResponse(r)) => {
            assert!(
                r.status().is_client_error() || r.status().is_server_error(),
                "expected an HTTP response from the router, got {}",
                r.status()
            );
        }
        Err(e) => panic!("tempo search did not reach the router: {e:?}"),
    }
}
