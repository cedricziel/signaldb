//! End-to-end integration tests for the native Query IR surface.
//!
//! Boots the full ingest→store→query stack (acceptor log/trace handlers → WAL →
//! writer → Iceberg → querier → router) on filesystem storage, ingests logs and
//! traces, then exercises `POST /api/v1/query` with single-signal IR documents.
//! Proves the cross-service path and that IR results match the dialect
//! equivalents. The existing TraceQL/LogQL/PromQL E2E suites are unchanged
//! (additive, non-regressing — task 10.3).

use acceptor::handler::WalManager;
use acceptor::handler::otlp_grpc::TraceHandler;
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
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use querier::flight::QuerierFlightService;
use router::{RouterState, discovery::ServiceRegistry, endpoints};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::transport::Server;
use tower::ServiceExt;
use writer::IcebergWriterFlightService;

/// A base timestamp (2023-11-14T22:13:20Z) shared by the ingested signals.
const BASE_NS: i64 = 1_700_000_000_000_000_000;

struct TestServices {
    flight_transport: Arc<InMemoryFlightTransport>,
    log_handler: LogHandler,
    trace_handler: TraceHandler,
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
        api_key_scopes: None,
        api_key_dataset_id: None,
        user_id: None,
        role: None,
        is_instance_admin: false,
        session_id: None,
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
        "127.0.0.1:50168".to_string(),
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
        object_store,
        writer_wal,
        &common::config::WriterConfig::default(),
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

    // Pre-create the Iceberg namespace so the querier resolves the dataset.
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

    // Handlers write to the shared WAL the writer drains.
    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
    ));
    let log_handler = LogHandler::new(flight_transport.clone(), wal_manager.clone());
    let trace_handler = TraceHandler::new(flight_transport.clone(), wal_manager);

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
        flight_transport,
        log_handler,
        trace_handler,
        config,
        _temp_dir: temp_dir,
    }
}

fn string_value(s: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(s.to_string())),
    }
}

fn log_record(offset_ns: i64, severity: &str, body: &str) -> LogRecord {
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
        attributes: vec![],
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

fn span(name: &str, seq: u8, dur_ns: i64) -> Span {
    Span {
        trace_id: vec![seq; 16],
        span_id: vec![seq; 8],
        parent_span_id: vec![],
        name: name.to_string(),
        kind: 1,
        start_time_unix_nano: BASE_NS as u64,
        end_time_unix_nano: (BASE_NS + dur_ns) as u64,
        attributes: vec![],
        dropped_attributes_count: 0,
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        trace_state: String::new(),
        flags: 0,
    }
}

fn traces_request(service: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(string_value(service)),
                    ..Default::default()
                }],
                dropped_attributes_count: 0,
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build the router with the native IR endpoint and test auth.
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
        .nest("/api/v1", endpoints::query::router().with_state(state))
        .layer(middleware::from_fn(move |req, next| {
            auth_middleware(authenticator.clone(), req, next)
        }))
}

/// POST an IR document to `/api/v1/query` and parse the JSON body.
async fn post_ir(app: &Router, doc: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/query")
        .header("Authorization", "Bearer test-key-123")
        .header("X-Tenant-ID", "test-tenant")
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(&doc).unwrap()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    if !status.is_success() {
        eprintln!(
            "POST /api/v1/query -> {status}: {}",
            std::str::from_utf8(&body).unwrap_or("<non-utf8>")
        );
    }
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

fn range() -> serde_json::Value {
    // Nanosecond bounds as numeric strings (the `QueryRange` wire type is a
    // string; a numeric string coerces to an absolute timestamp).
    serde_json::json!({
        "from": (BASE_NS - 1_000_000_000).to_string(),
        "to": (BASE_NS + 10_000_000_000).to_string(),
    })
}

/// Poll `POST /api/v1/query` until it returns a non-empty `rows` result or the
/// deadline elapses — the writer's WAL loop persists asynchronously (a ≥5s base
/// interval), so a fixed sleep would race it.
async fn post_ir_until_rows(
    app: &Router,
    doc: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let mut last = (StatusCode::OK, serde_json::Value::Null);
    for _ in 0..40 {
        let (status, body) = post_ir(app, doc.clone()).await;
        let has_rows = body
            .get("rows")
            .and_then(|r| r.as_array())
            .map(|r| !r.is_empty())
            .unwrap_or(false);
        if status == StatusCode::OK && has_rows {
            return (status, body);
        }
        last = (status, body);
        sleep(Duration::from_millis(500)).await;
    }
    last
}

// Task 10.1 — a single-signal logs IR query returns the LogQL equivalent.
#[tokio::test]
async fn logs_ir_query_end_to_end() {
    let services = setup().await;
    let ctx = test_tenant_context();

    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "api",
                vec![
                    log_record(0, "ERROR", "boom happened"),
                    log_record(1_000_000, "INFO", "all good"),
                ],
            ),
        )
        .await
        .expect("ingest api logs");
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request("web", vec![log_record(2_000_000, "WARN", "slow request")]),
        )
        .await
        .expect("ingest web logs");

    let app = build_router(&services).await;

    // `{service_name="api"}` in LogQL == this IR filter; expect the two api lines.
    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "rows",
            "fields": ["service_name", "body"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "api" } }
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "logs IR query: {body}");
    assert_eq!(body["result"], "rows");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 2, "expected the two api log lines: {body}");
    // Both rows belong to the `api` service and never leak other services.
    for row in rows {
        assert_eq!(row[0], "api");
    }
    // The resolved window is echoed for replay.
    assert!(
        body["window"]["end_ns"].as_i64().unwrap() > body["window"]["start_ns"].as_i64().unwrap()
    );
}

// Task 10.2 — a single-signal traces IR query (filter + topk) returns spans.
#[tokio::test]
async fn traces_ir_query_end_to_end() {
    let services = setup().await;
    let ctx = test_tenant_context();

    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "checkout",
                vec![
                    span("GET /a", 1, 100_000_000),
                    span("GET /b", 2, 900_000_000),
                    span("POST /c", 3, 500_000_000),
                ],
            ),
        )
        .await
        .expect("ingest checkout spans");

    let app = build_router(&services).await;

    // Filter to checkout, rank by duration, take the slowest span.
    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "traces",
            "range": range(),
            "result": "rows",
            "fields": ["span_name", "duration"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "checkout" } },
                { "topk": { "n": 1, "of": "duration" } }
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "traces IR query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "topk(1) returns one span: {body}");
    // The slowest checkout span is `GET /b` (900ms).
    assert_eq!(rows[0][0], "GET /b", "expected the slowest span: {body}");
}
