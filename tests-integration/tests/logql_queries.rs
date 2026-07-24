//! End-to-end integration tests for the LogQL query interface.
//!
//! Boots the full ingest→store→query stack (acceptor log handler → WAL →
//! writer → Iceberg → querier → router) on filesystem storage, ingests a
//! handful of varied log records, waits for persistence, then exercises
//! the `/loki` HTTP endpoints: stream queries, line filters, label and
//! series discovery, and a metric query.

use acceptor::handler::WalManager;
use acceptor::handler::otlp_log_handler::LogHandler;
use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    middleware,
};
use common::CatalogManager;
use common::auth::{TenantContext, TenantSource, auth_middleware};
use common::catalog::Catalog;
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{Wal, WalConfig};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use querier::flight::QuerierFlightService;
use router::{RouterState, discovery::ServiceRegistry, endpoints::logql};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::transport::Server;
use tower::ServiceExt;
use writer::IcebergWriterFlightService;

/// A base timestamp (2023-11-14T22:13:20Z) shared by the ingested logs.
const BASE_NS: i64 = 1_700_000_000_000_000_000;

struct TestServices {
    object_store: Arc<dyn object_store::ObjectStore>,
    flight_transport: Arc<InMemoryFlightTransport>,
    log_handler: LogHandler,
    config: Configuration,
    _temp_dir: TempDir,
}

fn test_tenant_context() -> TenantContext {
    TenantContext {
        tenant_id: "test-tenant".to_string(),
        dataset_id: "test-dataset".to_string(),
        tenant_slug: "test-tenant".to_string(),
        dataset_slug: "test-dataset".to_string(),
        api_key_name: Some("test-key".to_string()),
        source: TenantSource::Config,
    }
}

fn test_config(catalog_dsn: &str) -> Configuration {
    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.to_string(),
        heartbeat_interval: Duration::from_secs(5),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(30),
    });
    config.auth = common::config::AuthConfig {
        tenants: vec![common::config::TenantConfig {
            id: "test-tenant".to_string(),
            slug: "test-tenant".to_string(),
            name: "Test Tenant".to_string(),
            default_dataset: Some("test-dataset".to_string()),
            datasets: vec![],
            api_keys: vec![common::config::ApiKeyConfig {
                key: "test-key-123".to_string(),
                name: Some("test-key".to_string()),
            }],
            schema_config: None,
            limits: None,
        }],
        admin_api_key: None,
        internal_service_key: None,
        ..Default::default()
    };
    config
}

async fn setup() -> TestServices {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_path).unwrap();
    let storage_dsn = format!("file://{}", storage_path.display());
    let object_store =
        common::storage::create_object_store_from_dsn(&storage_dsn).expect("object store");

    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());
    let mut config = test_config(&catalog_dsn);
    // The writer derives Iceberg table locations from storage.dsn and its
    // catalog from schema.catalog_uri; both must be per-test paths.
    config.storage.dsn = storage_dsn.clone();
    config.schema.catalog_uri = format!(
        "sqlite://{}",
        temp_dir.path().join("iceberg_catalog.db").display()
    );

    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
        tenant_id: "test-tenant".to_string(),
        dataset_id: "test-dataset".to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    };

    let acceptor_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:50158".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(acceptor_bootstrap));

    // Writer Flight service with background WAL processing.
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);
    let writer_wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("catalog mgr"),
    );
    let writer_service = IcebergWriterFlightService::new(
        catalog_manager.clone(),
        object_store.clone(),
        writer_wal.clone(),
    );
    let _writer_bg = writer_service.start_background_processing();
    tokio::spawn(
        Server::builder()
            .add_service(FlightServiceServer::new(writer_service))
            .serve(writer_addr),
    );
    let writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();
    let _writer_id = writer_bootstrap.service_id();

    // Pre-create the Iceberg namespace so the querier's Mirror cache
    // resolves `test-tenant.test-dataset`.
    {
        use iceberg_rust::catalog::namespace::Namespace;
        let namespace =
            Namespace::try_new(&["test-tenant".to_string(), "test-dataset".to_string()]).unwrap();
        catalog_manager
            .catalog()
            .create_namespace(&namespace, None)
            .await
            .expect("pre-create namespace");
    }

    // Querier Flight service.
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
            .add_service(FlightServiceServer::new(querier_service))
            .serve(querier_addr),
    );
    let querier_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Querier,
        querier_addr.to_string(),
    )
    .await
    .unwrap();
    let _querier_id = querier_bootstrap.service_id();

    // Log handler writes to the shared WAL the writer drains.
    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
    ));
    let log_handler = LogHandler::new(flight_transport.clone(), wal_manager);

    // Wait for storage + query services to register.
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
        object_store,
        flight_transport,
        log_handler,
        config,
        _temp_dir: temp_dir,
    }
}

fn string_value(s: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(s.to_string())),
    }
}

/// One log record. `attrs` become log attributes.
fn log_record(offset_ns: i64, severity: &str, body: &str, attrs: &[(&str, &str)]) -> LogRecord {
    let severity_number = match severity {
        "ERROR" => 17,
        "WARN" => 13,
        _ => 9,
    };
    LogRecord {
        time_unix_nano: (BASE_NS + offset_ns) as u64,
        observed_time_unix_nano: (BASE_NS + offset_ns) as u64,
        severity_number,
        severity_text: severity.to_string(),
        body: Some(string_value(body)),
        attributes: attrs
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.to_string(),
                value: Some(string_value(v)),
                ..Default::default()
            })
            .collect(),
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![],
        span_id: vec![],
        event_name: String::new(),
    }
}

fn logs_request(service: &str, records: Vec<LogRecord>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(string_value(service)),
                    ..Default::default()
                }],
                dropped_attributes_count: 0,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build the router with the LogQL routes and test auth.
async fn build_router(services: &TestServices) -> Router {
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

    let state = State {
        catalog,
        service_registry,
        config: services.config.clone(),
        authenticator: authenticator.clone(),
    };
    Router::new()
        .nest("/loki", logql::router().with_state(state))
        .layer(middleware::from_fn(move |req, next| {
            auth_middleware(authenticator.clone(), req, next)
        }))
}

/// GET a `/loki` path and parse the JSON body.
async fn get(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .uri(uri)
        .header("Authorization", "Bearer test-key-123")
        .header("X-Tenant-ID", "test-tenant")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    if !status.is_success() {
        eprintln!(
            "GET {uri} -> {status}: {}",
            std::str::from_utf8(&body).unwrap_or("<non-utf8>")
        );
    }
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// The window bracketing the ingested logs, as URL query params.
fn window() -> String {
    format!(
        "start={}&end={}",
        BASE_NS - 1_000_000_000,
        BASE_NS + 10_000_000_000
    )
}

#[tokio::test]
async fn logql_end_to_end() {
    let services = setup().await;
    let ctx = test_tenant_context();

    // Ingest: api has an error + an info line; web has one warn line.
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "api",
                vec![
                    log_record(0, "ERROR", "boom: connection refused", &[("region", "eu")]),
                    log_record(1_000_000, "INFO", "request served", &[("region", "eu")]),
                ],
            ),
        )
        .await
        .expect("ingest api logs");
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "web",
                vec![log_record(
                    2_000_000,
                    "WARN",
                    "slow response",
                    &[("region", "us")],
                )],
            ),
        )
        .await
        .expect("ingest web logs");

    // Wait for WAL processing and Iceberg persistence.
    sleep(Duration::from_secs(15)).await;
    let objects: Vec<_> = {
        use futures::TryStreamExt;
        services
            .object_store
            .list(None)
            .try_collect()
            .await
            .unwrap()
    };
    assert!(
        objects.iter().any(|o| o.location.as_ref().contains("logs")),
        "expected persisted logs objects, found: {:?}",
        objects
            .iter()
            .map(|o| o.location.to_string())
            .collect::<Vec<_>>()
    );

    let app = build_router(&services).await;
    let w = window();

    // 1. Stream query for one service returns its two lines.
    let (status, body) = get(
        &app,
        &format!("/loki/api/v1/query_range?query=%7Bservice_name%3D%22api%22%7D&{w}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query_range: {body}");
    assert_eq!(body["data"]["resultType"], "streams");
    let entries = count_stream_entries(&body);
    assert_eq!(entries, 2, "api should have two log lines: {body}");

    // 2. Line filter narrows to the error line.
    let (status, body) = get(
        &app,
        &format!(
            "/loki/api/v1/query_range?query=%7Bservice_name%3D%22api%22%7D%20%7C%3D%20%22boom%22&{w}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        count_stream_entries(&body),
        1,
        "line filter should match one: {body}"
    );

    // 3. Labels include the known columns and the ingested attribute key.
    let (status, body) = get(&app, &format!("/loki/api/v1/labels?{w}")).await;
    assert_eq!(status, StatusCode::OK);
    let labels: Vec<String> = serde_json::from_value(body["data"].clone()).unwrap_or_default();
    assert!(labels.contains(&"service_name".to_string()), "{labels:?}");
    assert!(labels.contains(&"region".to_string()), "{labels:?}");

    // 4. Label values for service_name.
    let (status, body) = get(&app, &format!("/loki/api/v1/label/service_name/values?{w}")).await;
    assert_eq!(status, StatusCode::OK);
    let values: Vec<String> = serde_json::from_value(body["data"].clone()).unwrap_or_default();
    assert!(
        values.contains(&"api".to_string()) && values.contains(&"web".to_string()),
        "{values:?}"
    );

    // 5. Series matching a selector.
    let (status, body) = get(
        &app,
        &format!("/loki/api/v1/series?match%5B%5D=%7Bservice_name%3D%22api%22%7D&{w}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let series = body["data"].as_array().cloned().unwrap_or_default();
    assert!(
        series.iter().all(|s| s["service_name"] == "api"),
        "series should all be api: {body}"
    );

    // 6. Metric query: count_over_time returns a matrix.
    let (status, body) = get(
        &app,
        &format!(
            "/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D%22api%22%7D%5B1h%5D)&step=1h&{w}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "metric query: {body}");
    assert_eq!(body["data"]["resultType"], "matrix");
    let total: f64 = matrix_value_sum(&body);
    assert!(
        (total - 2.0).abs() < 1e-9,
        "api count should total 2: {body}"
    );
}

/// Sum the number of `values` entries across all streams in a response.
fn count_stream_entries(body: &serde_json::Value) -> usize {
    body["data"]["result"]
        .as_array()
        .map(|streams| {
            streams
                .iter()
                .map(|s| s["values"].as_array().map(|v| v.len()).unwrap_or(0))
                .sum()
        })
        .unwrap_or(0)
}

/// Sum all sample values across all series in a matrix response.
fn matrix_value_sum(body: &serde_json::Value) -> f64 {
    body["data"]["result"]
        .as_array()
        .map(|series| {
            series
                .iter()
                .flat_map(|s| s["values"].as_array().cloned().unwrap_or_default())
                .filter_map(|sample| sample[1].as_str().and_then(|v| v.parse::<f64>().ok()))
                .sum()
        })
        .unwrap_or(0.0)
}
