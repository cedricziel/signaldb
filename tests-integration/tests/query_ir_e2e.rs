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
use common::wal::WalConfig;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, KeyValue, KeyValueList, any_value::Value},
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
    tenant_context("test-tenant", "test-dataset", "test-key-123")
}

fn tenant_context(tenant: &str, dataset: &str, key: &str) -> TenantContext {
    TenantContext {
        tenant_id: tenant.to_string(),
        dataset_id: dataset.to_string(),
        tenant_slug: tenant.to_string(),
        dataset_slug: dataset.to_string(),
        api_key_name: Some(key.to_string()),
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
        tenants: vec![
            common::config::TenantConfig {
                id: "test-tenant".to_string(),
                slug: "test-tenant".to_string(),
                name: "Test Tenant".to_string(),
                default_dataset: Some("test-dataset".to_string()),
                datasets: vec![common::config::DatasetConfig {
                    id: "other-dataset".into(),
                    slug: "other-dataset".into(),
                    is_default: false,
                    storage: None,
                }],
                api_keys: vec![common::config::ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: Some("test-key".to_string()),
                }],
                schema_config: None,
                limits: None,
            },
            common::config::TenantConfig {
                id: "other-tenant".into(),
                slug: "other-tenant".into(),
                name: "Other Tenant".into(),
                default_dataset: Some("test-dataset".into()),
                datasets: vec![],
                api_keys: vec![common::config::ApiKeyConfig {
                    key: "other-key-123".into(),
                    name: Some("other-key".into()),
                }],
                schema_config: None,
                limits: None,
            },
        ],
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
    let writer_wal = Arc::new(common::wal::manager::WalManager::uniform(
        tests_integration::test_helpers::writer_wal_config(&wal_config),
    ));
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
            .add_service(common::flight::flight_service_server(writer_service))
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
        for (tenant, dataset) in [
            ("test-tenant", "test-dataset"),
            ("test-tenant", "other-dataset"),
            ("other-tenant", "test-dataset"),
        ] {
            let namespace = Namespace::try_new(&[tenant.to_string(), dataset.to_string()]).unwrap();
            catalog_manager
                .catalog()
                .create_namespace(&namespace, None)
                .await
                .expect("pre-create namespace");
        }
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
            .add_service(common::flight::flight_service_server(querier_service))
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

/// A log record whose body is a structured (kvlist) `AnyValue` rather than a
/// plain string — issue #1410's non-string-body case, which must round-trip
/// as JSON rather than being unwrapped.
fn kvlist_log_record(offset_ns: i64, entries: &[(&str, &str)]) -> LogRecord {
    LogRecord {
        time_unix_nano: (BASE_NS + offset_ns) as u64,
        observed_time_unix_nano: (BASE_NS + offset_ns) as u64,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: entries
                    .iter()
                    .map(|(k, v)| KeyValue {
                        key: k.to_string(),
                        value: Some(string_value(v)),
                        ..Default::default()
                    })
                    .collect(),
            })),
        }),
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
    post_ir_as(app, doc, "test-key-123", "test-tenant", None).await
}

async fn post_ir_as(
    app: &Router,
    doc: serde_json::Value,
    key: &str,
    tenant: &str,
    dataset: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/v1/query")
        .header("Authorization", format!("Bearer {key}"))
        .header("X-Tenant-ID", tenant)
        .header("Content-Type", "application/json");
    if let Some(dataset) = dataset {
        request = request.header("X-Dataset-ID", dataset);
    }
    let request = request
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
            "fields": ["service.name", "body"],
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

// Issue #1410 — ingest JSON-encodes the `body` value so non-string bodies
// (kvlist/array/bytes) survive the Utf8 column; a plain string body must come
// back decoded (no surrounding quotes) while a structured body must still
// round-trip as JSON.
#[tokio::test]
async fn logs_ir_query_body_field_decodes_string_bodies_and_keeps_structured_bodies() {
    let services = setup().await;
    let ctx = test_tenant_context();

    let plain_body =
        "Committed 34 rows in 1 data files to Iceberg table homelab.default.logs (attempt 1)";
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request("committer", vec![log_record(0, "INFO", plain_body)]),
        )
        .await
        .expect("ingest plain-string-body log");
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "structured",
                vec![kvlist_log_record(1_000_000, &[("event", "start")])],
            ),
        )
        .await
        .expect("ingest kvlist-body log");
    let quoted_body = r#""already quoted" said the log"#;
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request("quoted", vec![log_record(2_000_000, "INFO", quoted_body)]),
        )
        .await
        .expect("ingest quoted-message-body log");

    let app = build_router(&services).await;

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "rows",
            "fields": ["service.name", "body"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "committer" } }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "plain-body IR query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "expected the committer log line: {body}");
    assert_eq!(
        rows[0][1], plain_body,
        "body must be decoded, not JSON-string-quoted: {body}"
    );

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "rows",
            "fields": ["service.name", "body"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "structured" } }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "kvlist-body IR query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "expected the structured log line: {body}");
    let kvlist_body: serde_json::Value =
        serde_json::from_str(rows[0][1].as_str().expect("body is a string column"))
            .expect("kvlist body must still round-trip as JSON");
    assert_eq!(kvlist_body, serde_json::json!({"event": "start"}));

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "rows",
            "fields": ["service.name", "body"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "quoted" } }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "quoted-message IR query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "expected the quoted-message log line: {body}"
    );
    assert_eq!(
        rows[0][1], quoted_body,
        "a message that is itself quoted must decode exactly once, not twice: {body}"
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
            "fields": ["span.name", "duration"],
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

#[tokio::test]
async fn traces_ir_query_resolves_numeric_span_kind_status_and_dropped_counts() {
    // iceberg-schema-evolution (#1208) tasks 6.1/6.3: span_kind_number/
    // status_code_number/dropped_*_count are registered in
    // LogicalSchema::core() and given a real physical column + read/write
    // path -- prove the full ingest-through-query-IR path actually
    // surfaces real values for them, not silently-always-zero/null.
    let services = setup().await;
    let ctx = test_tenant_context();

    let mut server_span = span("GET /checkout", 9, 100_000_000);
    server_span.kind = 2; // Server
    server_span.status = Some(Status {
        code: 2, // Error
        message: "boom".to_string(),
    });
    server_span.dropped_attributes_count = 3;
    server_span.dropped_events_count = 5;
    server_span.dropped_links_count = 7;

    services
        .trace_handler
        .handle_grpc_otlp_traces(&ctx, traces_request("checkout", vec![server_span]))
        .await
        .expect("ingest server span");

    let app = build_router(&services).await;

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "traces",
            "range": range(),
            "result": "rows",
            "fields": [
                "span_kind_number",
                "status_code_number",
                "dropped_attributes_count",
                "dropped_events_count",
                "dropped_links_count"
            ],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "checkout" } }
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "numeric fields IR query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "expected exactly the one ingested span: {body}"
    );
    let row = &rows[0];
    assert_eq!(
        row[0], 2,
        "span_kind_number should be the raw OTel int: {body}"
    );
    assert_eq!(
        row[1], 2,
        "status_code_number should be the raw OTel int: {body}"
    );
    assert_eq!(row[2], 3, "dropped_attributes_count: {body}");
    assert_eq!(row[3], 5, "dropped_events_count: {body}");
    assert_eq!(row[4], 7, "dropped_links_count: {body}");
}

#[tokio::test]
async fn traces_ir_query_string_span_kind_and_status_still_resolve_post_1208() {
    // task 6.2: the pre-existing string convenience fields (span_kind/
    // status.code, what TraceQL-style queries filter on) must keep
    // resolving correctly now that they're derived from the numeric
    // columns instead of being the read/write source of truth themselves.
    let services = setup().await;
    let ctx = test_tenant_context();

    let mut server_span = span("GET /checkout", 9, 100_000_000);
    server_span.kind = 2; // Server
    server_span.status = Some(Status {
        code: 2, // Error
        message: "boom".to_string(),
    });

    services
        .trace_handler
        .handle_grpc_otlp_traces(&ctx, traces_request("checkout", vec![server_span]))
        .await
        .expect("ingest server span");

    let app = build_router(&services).await;

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "traces",
            "range": range(),
            "result": "rows",
            "fields": ["span.name"],
            "pipeline": [
                { "where": { "field": "span_kind", "op": "eq", "value": "Server" } },
                { "where": { "field": "status.code", "op": "eq", "value": "Error" } }
            ]
        }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "string span_kind/status.code IR query: {body}"
    );
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "expected the Server/Error span to match: {body}"
    );
    assert_eq!(rows[0][0], "GET /checkout", "{body}");
}

#[tokio::test]
async fn trace_heatmap_end_to_end_uses_native_query_ir_without_list_limit() {
    let services = setup().await;
    let ctx = test_tenant_context();
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "checkout",
                vec![
                    span("fast", 1, 50_000_000),
                    span("edge", 2, 100_000_000),
                    span("overflow", 3, 900_000_000),
                ],
            ),
        )
        .await
        .expect("ingest spans");
    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": 2, "from": "traces", "range": range(), "result": "heatmap",
        "pipeline": [{ "heatmap": {
            "x": { "step": "1m", "align": "epoch" },
            "y": { "of": "duration", "bounds": ["100ms", "500ms"], "overflow": true },
            "value": { "fn": "count", "as": "count" }
        }}]
    });
    let mut last = serde_json::Value::Null;
    for _ in 0..40 {
        let (status, body) = post_ir(&app, document.clone()).await;
        if status == StatusCode::OK
            && body["heatmap"]["cells"]
                .as_array()
                .is_some_and(|cells| !cells.is_empty())
        {
            assert_eq!(body["result"], "heatmap");
            let cells = body["heatmap"]["cells"].as_array().unwrap();
            let mut buckets: Vec<(i64, i64)> = cells
                .iter()
                .map(|cell| {
                    (
                        cell["duration_bucket"].as_i64().unwrap(),
                        cell["count"].as_i64().unwrap(),
                    )
                })
                .collect();
            buckets.sort_unstable();
            assert_eq!(buckets, vec![(0, 1), (1, 1), (2, 1)], "{body}");
            return;
        }
        last = body;
        sleep(Duration::from_millis(500)).await;
    }
    panic!("heatmap did not return persisted cells: {last}");
}

#[tokio::test]
async fn trace_heatmap_isolated_by_authenticated_tenant_and_dataset() {
    let services = setup().await;
    for (context, duration) in [
        (
            tenant_context("test-tenant", "test-dataset", "test-key-123"),
            10_000_000,
        ),
        (
            tenant_context("test-tenant", "other-dataset", "test-key-123"),
            20_000_000,
        ),
        (
            tenant_context("other-tenant", "test-dataset", "other-key-123"),
            30_000_000,
        ),
    ] {
        services
            .trace_handler
            .handle_grpc_otlp_traces(
                &context,
                traces_request("isolated", vec![span("only-here", 1, duration)]),
            )
            .await
            .unwrap();
    }
    let app = build_router(&services).await;
    let doc = serde_json::json!({ "irVersion": 2, "from": "traces", "range": range(), "result": "heatmap", "pipeline": [{ "heatmap": {
        "x": { "step": "1m", "align": "epoch" }, "y": { "of": "duration", "bounds": ["15ms", "25ms"], "overflow": true }, "value": { "fn": "count", "as": "count" }
    }}] });
    let contexts = [
        ("test-key-123", "test-tenant", None, 0),
        ("test-key-123", "test-tenant", Some("other-dataset"), 1),
        ("other-key-123", "other-tenant", None, 2),
    ];
    for (key, tenant, dataset, expected_bucket) in contexts {
        let mut body = serde_json::Value::Null;
        let mut last = serde_json::Value::Null;
        for _ in 0..40 {
            let (status, response) = post_ir_as(&app, doc.clone(), key, tenant, dataset).await;
            if status == StatusCode::OK
                && response["heatmap"]["cells"]
                    .as_array()
                    .is_some_and(|cells| cells.len() == 1)
            {
                body = response;
                break;
            }
            last = response;
            sleep(Duration::from_millis(500)).await;
        }
        let cells = body["heatmap"]["cells"].as_array().unwrap_or_else(|| {
            panic!("{tenant}/{dataset:?} never returned one isolated cell; last: {last}")
        });
        assert_eq!(cells[0]["count"], 1, "context leaked rows: {body}");
        assert_eq!(
            cells[0]["duration_bucket"], expected_bucket,
            "wrong context result: {body}"
        );
    }
}

/// Read a `table` envelope as `(group, measure)` pairs, sorted — addressing
/// columns by name so the assertion does not depend on column or row order.
/// The server answers with *physical* column names for group fields, so the
/// caller passes those, not the logical names the document sent.
fn table_pairs(body: &serde_json::Value, group: &str, measure: &str) -> Vec<(String, i64)> {
    let columns = body["columns"].as_array().expect("columns array");
    let index_of = |name: &str| {
        columns
            .iter()
            .position(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("column '{name}' absent from {columns:?}"))
    };
    let (g, m) = (index_of(group), index_of(measure));
    let mut pairs: Vec<(String, i64)> = body["rows"]
        .as_array()
        .expect("rows array")
        .iter()
        .map(|row| {
            (
                row[g].as_str().unwrap_or_default().to_string(),
                row[m].as_i64().expect("an integer measure"),
            )
        })
        .collect();
    pairs.sort();
    pairs
}

// Task 2.5 — a scoped aggregate measures a subset of each group across the
// full service path, and leaves the group set the unscoped query returns
// untouched.
#[tokio::test]
async fn scoped_aggregate_end_to_end() {
    let services = setup().await;
    let ctx = test_tenant_context();

    // `api`: one of two records is an error. `web`: both are.
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
            logs_request(
                "web",
                vec![
                    log_record(2_000_000, "ERROR", "upstream failed"),
                    log_record(3_000_000, "ERROR", "retry failed"),
                ],
            ),
        )
        .await
        .expect("ingest web logs");

    let app = build_router(&services).await;

    let scoped = serde_json::json!({
        "irVersion": 1,
        "from": "logs",
        "range": range(),
        "result": "table",
        "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
            { "fn": "count", "as": "n" },
            { "fn": "count", "as": "errors",
              "where": { "field": "severity_number", "op": "gte", "value": 17 } }
        ] } } ]
    });
    let (status, body) = post_ir_until_rows(&app, scoped).await;
    assert_eq!(status, StatusCode::OK, "scoped aggregate: {body}");
    assert_eq!(body["result"], "table");

    assert_eq!(
        table_pairs(&body, "service_name", "n"),
        vec![("api".to_string(), 2), ("web".to_string(), 2)],
        "the unscoped count covers every record in the group: {body}"
    );
    assert_eq!(
        table_pairs(&body, "service_name", "errors"),
        vec![("api".to_string(), 1), ("web".to_string(), 2)],
        "the scoped count covers only matching records: {body}"
    );

    // The same query without the scope returns the same groups and totals —
    // scoping narrows one aggregate, never the group set.
    let (status, unscoped) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"],
                "aggs": [ { "fn": "count", "as": "n" } ] } } ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unscoped aggregate: {unscoped}");
    assert_eq!(
        table_pairs(&unscoped, "service_name", "n"),
        table_pairs(&body, "service_name", "n"),
        "the group set and totals are identical with and without a scope"
    );
}

// Task 2.5 — a group no record in it satisfies is still returned, reporting
// zero. A `where` *stage* would have dropped it; a scope must not.
#[tokio::test]
async fn scoped_aggregate_keeps_groups_with_no_match() {
    let services = setup().await;
    let ctx = test_tenant_context();

    // No `api` record is an error; `web` has one.
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "api",
                vec![
                    log_record(0, "INFO", "all good"),
                    log_record(1_000_000, "WARN", "slow request"),
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
                vec![log_record(2_000_000, "ERROR", "upstream failed")],
            ),
        )
        .await
        .expect("ingest web logs");

    let app = build_router(&services).await;

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                { "fn": "count", "as": "n" },
                { "fn": "count", "as": "errors",
                  "where": { "field": "severity_number", "op": "gte", "value": 17 } }
            ] } } ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "scoped aggregate: {body}");
    assert_eq!(
        table_pairs(&body, "service_name", "errors"),
        vec![("api".to_string(), 0), ("web".to_string(), 1)],
        "the error-free group is kept, reporting zero: {body}"
    );
}
