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
use acceptor::handler::otlp_metrics_handler::MetricsHandler;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    middleware,
};
use common::auth::{TenantContext, TenantSource, auth_middleware};
use common::catalog::Catalog;
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::metrics::v1::ExportMetricsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, KeyValue, KeyValueList, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        Exemplar, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, exemplar, metric::Data, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use querier::flight::QuerierFlightService;
use router::{RouterAppState, endpoints};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::transport::Server;
use tower::ServiceExt;

/// A base timestamp (2023-11-14T22:13:20Z) shared by the ingested signals.
pub(crate) const BASE_NS: i64 = 1_700_000_000_000_000_000;

pub(crate) struct TestServices {
    pub(crate) flight_transport: Arc<InMemoryFlightTransport>,
    pub(crate) log_handler: Arc<LogHandler>,
    pub(crate) trace_handler: Arc<TraceHandler>,
    pub(crate) metrics_handler: MetricsHandler,
    /// Shared with the router built by `build_router`, so a processor
    /// created through `POST /api/v1/processors` is visible to the next
    /// `for_request` lookup the ingest handlers make (task 3.3).
    processor_registry: Arc<common::processors::ProcessorRegistry>,
    pub(crate) catalog: Arc<Catalog>,
    config: Configuration,
    _temp_dir: TempDir,
}

pub(crate) fn test_tenant_context() -> TenantContext {
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

pub(crate) async fn setup() -> TestServices {
    setup_with(|_config| {}).await
}

/// Like [`setup`], with a hook to override `config` before the stack boots
/// — e.g. lowering `config.querier.correlate_max_rows` to exercise the
/// truncation path without shrinking it for every other test.
pub(crate) async fn setup_with(config_override: impl FnOnce(&mut Configuration)) -> TestServices {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_path).unwrap();
    let storage_dsn = format!("file://{}", storage_path.display());

    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());
    let mut config = test_config(&catalog_dsn);
    config.storage.dsn = storage_dsn.clone();
    config_override(&mut config);
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
    // `ServiceBootstrap::new` does not itself register config-defined tenants
    // in the `tenants` table (unlike the real binary's startup path); the
    // processors table's `FOREIGN KEY (tenant_id) REFERENCES tenants(id)`
    // needs a real row to insert against.
    acceptor_bootstrap
        .catalog()
        .sync_config_tenants(&config.auth)
        .await
        .expect("sync config tenants");
    let flight_transport = Arc::new(InMemoryFlightTransport::new(acceptor_bootstrap));

    // Writer Flight service with background WAL processing.
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);
    let writer_wal = Arc::new(common::wal::manager::WalManager::uniform(
        tests_integration::test_helpers::writer_wal_config(&wal_config),
    ));
    // Shared with the writer's `TypeAuthority` below, so the querier's
    // `CanonicalTypeLookup` sees the canonical types the writer commits.
    let type_authority_catalog = Catalog::new(&catalog_dsn)
        .await
        .expect("type authority catalog");
    let (catalog_manager, type_authority_catalog) =
        tests_integration::test_support::catalog_manager_with_tenant_source(
            config.clone(),
            type_authority_catalog,
        )
        .await
        .expect("catalog mgr");
    let writer_service =
        tests_integration::test_support::writer_service_with_type_authority_and_catalog(
            catalog_manager.clone(),
            writer_wal,
            &common::config::WriterConfig::default(),
            type_authority_catalog,
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
        config.querier.clone(),
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
    let processor_catalog = Arc::new(Catalog::new(&catalog_dsn).await.expect("catalog"));
    let processor_registry = Arc::new(common::processors::ProcessorRegistry::new(
        processor_catalog.clone(),
        &common::config::ProcessorsConfig::default(),
    ));
    let log_handler = Arc::new(LogHandler::new(
        flight_transport.clone(),
        wal_manager.clone(),
        processor_registry.clone(),
    ));
    let metrics_handler = MetricsHandler::new(
        flight_transport.clone(),
        wal_manager.clone(),
        processor_registry.clone(),
    );
    let trace_handler = Arc::new(
        TraceHandler::new(
            flight_transport.clone(),
            wal_manager,
            processor_registry.clone(),
        )
        .with_evaluation_logs(log_handler.clone()),
    );

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
        metrics_handler,
        processor_registry,
        catalog: processor_catalog,
        config,
        _temp_dir: temp_dir,
    }
}

pub(crate) fn string_value(s: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(s.to_string())),
    }
}

pub(crate) fn log_record(offset_ns: i64, severity: &str, body: &str) -> LogRecord {
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

pub(crate) fn logs_request(service: &str, records: Vec<LogRecord>) -> ExportLogsServiceRequest {
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

pub(crate) fn span(name: &str, seq: u8, dur_ns: i64) -> Span {
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

pub(crate) fn traces_request(service: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
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

/// Build the router with the native IR, processors, eval-sets and eval-results endpoints
/// and test auth.
pub(crate) async fn build_router(services: &TestServices) -> Router {
    let catalog = Catalog::new(services.config.discovery.as_ref().unwrap().dsn.as_str())
        .await
        .unwrap();
    let state = RouterAppState::new_with_flight_transport(
        catalog,
        services.config.clone(),
        (*services.flight_transport).clone(),
    )
    .with_processor_registry(services.processor_registry.clone());
    let authenticator = state.authenticator().clone();
    let traces_http = acceptor::traces_http_router(
        authenticator.clone(),
        services.trace_handler.clone(),
        Arc::new(common::ratelimit::TenantRateLimiter::from_auth_config(
            &services.config.auth,
        )),
        Arc::new(
            common::storage_usage::StorageUsageTracker::from_auth_config(&services.config.auth),
        ),
    );
    Router::new()
        .nest(
            "/api/v1",
            endpoints::query::router()
                .merge(endpoints::processors::router())
                .merge(endpoints::eval_sets::router())
                .merge(endpoints::evals::router())
                .with_state(state),
        )
        .merge(traces_http)
        .layer(middleware::from_fn(move |req, next| {
            auth_middleware(authenticator.clone(), req, next)
        }))
}

/// POST an IR document to `/api/v1/query` and parse the JSON body.
pub(crate) async fn post_ir(
    app: &Router,
    doc: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
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

pub(crate) fn range() -> serde_json::Value {
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
pub(crate) async fn post_ir_until_rows(
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
    // A message whose text is itself a self-contained JSON string literal —
    // the one value class that distinguishes "decoded once" from "decoded
    // twice" (see the matching comment in logql_queries.rs for why).
    let quoted_body = r#""already quoted""#;
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

/// A `count_distinct` aggregate (`irVersion` 9) over ingested OTLP logs:
/// three records carry two distinct `session.id` values, one has no
/// `session.id` at all — the null must not inflate the count.
#[tokio::test]
async fn count_distinct_aggregate_end_to_end() {
    let services = setup().await;
    let ctx = test_tenant_context();

    let with_session = |offset_ns: i64, session_id: &str| LogRecord {
        attributes: vec![KeyValue {
            key: "session.id".to_string(),
            value: Some(string_value(session_id)),
            ..Default::default()
        }],
        ..log_record(offset_ns, "INFO", "page view")
    };

    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "web",
                vec![
                    with_session(0, "s1"),
                    with_session(1_000_000, "s1"),
                    with_session(2_000_000, "s2"),
                    log_record(3_000_000, "INFO", "no session on this one"),
                ],
            ),
        )
        .await
        .expect("ingest web logs");

    let app = build_router(&services).await;

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 9,
            "from": "logs",
            "range": range(),
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                { "fn": "count_distinct", "of": "session.id", "as": "sessions" }
            ] } } ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "count_distinct aggregate: {body}");
    assert_eq!(
        table_pairs(&body, "service_name", "sessions"),
        vec![("web".to_string(), 2)],
        "two distinct sessions (s1, s2); the record with no session.id doesn't count: {body}"
    );
}

/// #1340: `resource.identity` is declared on `logs` (and every other
/// source) but had no producer, so grouping by it always fell into one null
/// bucket with a self-contradicting warning. The writer now materialises it
/// (PRs 1-3 of this stack); this proves the Query IR surface can group by
/// it end to end — two distinct resources land in two distinct, non-null
/// digest groups — and that `describe` advertises a field the query surface
/// can actually answer.
#[tokio::test]
async fn logs_group_by_resource_identity_end_to_end() {
    let services = setup().await;
    let ctx = test_tenant_context();

    // Two distinct resources (only `service.name` differs, so the digest is
    // the identity of that one-attribute resource each).
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "resource-identity-svc-a",
                vec![
                    log_record(0, "INFO", "a1"),
                    log_record(1_000_000, "INFO", "a2"),
                ],
            ),
        )
        .await
        .expect("ingest resource-identity-svc-a logs");
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "resource-identity-svc-b",
                vec![log_record(2_000_000, "INFO", "b1")],
            ),
        )
        .await
        .expect("ingest resource-identity-svc-b logs");

    let app = build_router(&services).await;

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["resource.identity"],
                "aggs": [ { "fn": "count", "as": "n" } ] } } ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "group by resource.identity: {body}");

    let resource_map = |service: &str| {
        let mut map = serde_json::Map::new();
        map.insert(
            "service.name".to_string(),
            serde_json::Value::String(service.to_string()),
        );
        map
    };
    let identity_a = common::schema::resource_identity::resource_identity(&resource_map(
        "resource-identity-svc-a",
    ));
    let identity_b = common::schema::resource_identity::resource_identity(&resource_map(
        "resource-identity-svc-b",
    ));
    assert_ne!(
        identity_a, identity_b,
        "distinct resources digest distinctly"
    );
    for digest in [&identity_a, &identity_b] {
        assert_eq!(
            digest.len(),
            32,
            "digest is 32 lowercase hex chars: {digest}"
        );
        assert!(
            digest
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "digest is lowercase hex: {digest}"
        );
    }

    let groups = table_pairs(&body, "resource_identity", "n");
    let mut expected = vec![(identity_a.clone(), 2_i64), (identity_b.clone(), 1_i64)];
    expected.sort();
    assert_eq!(
        groups, expected,
        "two non-null groups, one per resource: {body}"
    );

    // `describe {"target": "fields"}` advertises the field this query just
    // used successfully — the issue's self-contradiction (advertised but
    // unusable) must not reappear.
    let (status, describe_body) = post_ir(
        &app,
        serde_json::json!({
            "irVersion": 4,
            "from": "logs",
            "range": range(),
            "result": "metadata",
            "pipeline": [ { "describe": { "target": "fields" } } ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "describe fields: {describe_body}");
    let names: Vec<&str> = describe_body["metadata"]["fields"]
        .as_array()
        .expect("fields array")
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"resource.identity"),
        "describe advertises resource.identity: {names:?}"
    );
}

/// `describe fields`, polled until every name in `names` is listed (the writer
/// commits canonical types as it processes the batch), returning the fields.
async fn describe_fields_until_listed(app: &Router, names: &[&str]) -> Vec<serde_json::Value> {
    let mut last = serde_json::Value::Null;
    for _ in 0..40 {
        let (status, body) = post_ir(
            app,
            serde_json::json!({
                "irVersion": 4,
                "from": "logs",
                "range": range(),
                "result": "metadata",
                "pipeline": [ { "describe": { "target": "fields" } } ]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "describe fields: {body}");
        let fields = body["metadata"]["fields"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if names
            .iter()
            .all(|name| fields.iter().any(|f| f["name"] == *name))
        {
            return fields;
        }
        last = body;
        sleep(Duration::from_millis(500)).await;
    }
    panic!("{names:?} never listed: {last}");
}

fn described<'a>(fields: &'a [serde_json::Value], name: &str) -> &'a serde_json::Value {
    fields
        .iter()
        .find(|f| f["name"] == name)
        .unwrap_or_else(|| panic!("{name} is listed"))
}

fn int_attribute(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
        ..Default::default()
    }
}

/// Discovery lists an ingested attribute with the canonical type the type
/// authority committed — the type the planner enforces — not the `string`
/// default an unregistered observed key used to get, and the listed name is
/// valid to query with that type.
#[tokio::test]
async fn describe_lists_an_ingested_int_attribute_as_int64() {
    let services = setup().await;
    let ctx = test_tenant_context();

    let mut record = log_record(0, "INFO", "with an int attribute");
    record.attributes.push(int_attribute("retry.count", 3));
    services
        .log_handler
        .handle_grpc_otlp_logs(&ctx, logs_request("describe-typed-svc", vec![record]))
        .await
        .expect("ingest log with an int attribute");

    let app = build_router(&services).await;
    let fields = describe_fields_until_listed(&app, &["retry.count"]).await;
    let field = described(&fields, "retry.count");
    assert_eq!(field["type"], "int64", "the authority's type: {field}");
    assert_eq!(field["origin"], "authority", "{field}");

    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "rows",
            "fields": ["body"],
            "pipeline": [
                { "where": { "field": "retry.count", "op": "eq", "value": 3 } }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query by the listed name: {body}");
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(1), "{body}");
}

/// A key sent at resource level (string) and record level (int) is listed
/// once per level under the qualified names the planner resolves, each with
/// its own type.
#[tokio::test]
async fn describe_lists_a_multi_level_key_under_names_that_query_each_level() {
    let services = setup().await;
    let ctx = test_tenant_context();

    let mut record = log_record(0, "INFO", "multi-level key");
    record.attributes.push(int_attribute("region", 7));
    let mut request = logs_request("describe-multi-level-svc", vec![record]);
    if let Some(resource) = request.resource_logs[0].resource.as_mut() {
        resource.attributes.push(KeyValue {
            key: "region".to_string(),
            value: Some(string_value("eu")),
            ..Default::default()
        });
    }
    services
        .log_handler
        .handle_grpc_otlp_logs(&ctx, request)
        .await
        .expect("ingest log with a multi-level key");

    let app = build_router(&services).await;
    let fields = describe_fields_until_listed(&app, &["resource.region", "log.region"]).await;
    assert!(fields.iter().all(|f| f["name"] != "region"), "{fields:?}");
    assert_eq!(described(&fields, "resource.region")["type"], "string");
    assert_eq!(described(&fields, "log.region")["type"], "int64");

    for (name, value) in [
        ("log.region", serde_json::json!(7)),
        ("resource.region", serde_json::json!("eu")),
    ] {
        let (status, body) = post_ir_until_rows(
            &app,
            serde_json::json!({
                "irVersion": 1,
                "from": "logs",
                "range": range(),
                "result": "rows",
                "fields": ["body"],
                "pipeline": [
                    { "where": { "field": name, "op": "eq", "value": value } }
                ]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "query by {name}: {body}");
        assert_eq!(
            body["rows"].as_array().map(Vec::len),
            Some(1),
            "{name}: {body}"
        );
    }
}

// Task 3.3 — a processor created through the router HTTP API redacts PII in
// an OTLP/HTTP export before it reaches storage; the Query IR surface never
// sees the original value.
#[tokio::test]
async fn processor_created_via_router_api_redacts_pii_end_to_end() {
    let services = setup().await;
    let app = build_router(&services).await;

    const PII_EMAIL: &str = "alice@example.com";

    // 1. Create the processor through the router HTTP API (not the catalog
    //    directly) — design D6/D7's public contract for this change.
    let create = Request::builder()
        .method("POST")
        .uri("/api/v1/processors")
        .header("authorization", "Bearer test-key-123")
        .header("x-tenant-id", "test-tenant")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "name": "hash-email",
                "signal": "traces",
                "statements": [
                    r#"set(attributes["user.email"], SHA256(attributes["user.email"])) where attributes["user.email"] != nil"#
                ],
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.clone().oneshot(create).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "processor create: {:?}",
        axum::body::to_bytes(response.into_body(), usize::MAX).await
    );

    // 2. Export a span carrying the PII value over OTLP/HTTP.
    let mut pii_span = span("GET /account", 9, 10_000_000);
    pii_span.attributes.push(KeyValue {
        key: "user.email".to_string(),
        value: Some(string_value(PII_EMAIL)),
        ..Default::default()
    });
    let export_body = serde_json::to_vec(&traces_request("checkout", vec![pii_span])).unwrap();
    let export = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("authorization", "Bearer test-key-123")
        .header("x-tenant-id", "test-tenant")
        .header("content-type", "application/json")
        .body(Body::from(export_body))
        .unwrap();
    let response = app.clone().oneshot(export).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 3. Query via the native Query IR: only the redacted value ever
    //    appears, never the original.
    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "traces",
            "range": range(),
            "result": "rows",
            "fields": ["span.name", "user.email"],
            "pipeline": [
                { "where": { "field": "span.name", "op": "eq", "value": "GET /account" } }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "traces IR query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "expected the one redacted span: {body}");
    let queried_email = rows[0][1].as_str().expect("user.email is a string");
    assert_ne!(
        queried_email, PII_EMAIL,
        "the processor must have redacted the value before it was queryable"
    );
    assert!(
        !body.to_string().contains(PII_EMAIL),
        "the raw PII value must never appear anywhere in the query response: {body}"
    );
}

// query-ir-span-join task 2.3 — the span-to-parent `correlate` stage (IR v8)
// across the full ingest→store→query stack: two tenants, three services,
// caller/callee pair counts, and tenant isolation through
// `POST /api/v1/query`.

/// Like [`span`], but with explicit trace/span/parent identity so two spans
/// from different `traces_request` calls (different services, since a
/// resource's `service.name` applies to the whole call) can share one trace.
fn span_with_ids(
    name: &str,
    trace_id: u8,
    span_id: u8,
    parent_span_id: Option<u8>,
    dur_ns: i64,
) -> Span {
    Span {
        trace_id: vec![trace_id; 16],
        span_id: vec![span_id; 8],
        parent_span_id: parent_span_id.map(|p| vec![p; 8]).unwrap_or_default(),
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

/// Poll a tenant-scoped `POST /api/v1/query` until the `caller`/`callee`
/// pair count columns hold at least `min_rows` rows or the deadline elapses
/// (mirrors [`post_ir_until_rows`], parameterized on tenant/key).
async fn post_ir_as_until_rows(
    app: &Router,
    doc: serde_json::Value,
    key: &str,
    tenant: &str,
    min_rows: usize,
) -> (StatusCode, serde_json::Value) {
    let mut last = (StatusCode::OK, serde_json::Value::Null);
    for _ in 0..40 {
        let (status, body) = post_ir_as(app, doc.clone(), key, tenant, None).await;
        let rows = body
            .get("rows")
            .and_then(|r| r.as_array())
            .map(Vec::len)
            .unwrap_or(0);
        if status == StatusCode::OK && rows >= min_rows {
            return (status, body);
        }
        last = (status, body);
        sleep(Duration::from_millis(500)).await;
    }
    last
}

/// Read a `table` envelope's `(caller, callee, count)` triples, sorted by
/// caller then callee — addressing columns by their physical (`safe_ident`)
/// name, same as [`table_pairs`].
fn caller_callee_counts(body: &serde_json::Value) -> Vec<(String, String, i64)> {
    let columns = body["columns"].as_array().expect("columns array");
    let index_of = |name: &str| {
        columns
            .iter()
            .position(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("column '{name}' missing from {body}"))
    };
    let caller_idx = index_of("parent_service_name");
    let callee_idx = index_of("service_name");
    let count_idx = index_of("n");
    let mut pairs: Vec<(String, String, i64)> = body["rows"]
        .as_array()
        .expect("rows array")
        .iter()
        .map(|row| {
            (
                row[caller_idx].as_str().unwrap().to_string(),
                row[callee_idx].as_str().unwrap().to_string(),
                row[count_idx].as_i64().unwrap(),
            )
        })
        .collect();
    pairs.sort();
    pairs
}

fn correlate_pair_counts_document() -> serde_json::Value {
    serde_json::json!({
        "irVersion": 8,
        "from": "traces",
        "range": range(),
        "result": "table",
        "pipeline": [
            { "correlate": { "to": "parent", "kind": "inner" } },
            { "aggregate": {
                "by": ["parent.service.name", "service.name"],
                "aggs": [{ "fn": "count", "as": "n" }]
            } }
        ]
    })
}

#[tokio::test]
async fn correlate_caller_callee_pair_counts_isolated_by_tenant() {
    let services = setup().await;

    // Tenant A ("test-tenant"): gateway calls checkout twice (two distinct
    // traces) and billing once.
    let tenant_a = test_tenant_context();
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &tenant_a,
            traces_request(
                "gateway",
                vec![
                    span_with_ids("GET /checkout", 1, 1, None, 50_000_000),
                    span_with_ids("GET /checkout", 2, 3, None, 50_000_000),
                    span_with_ids("GET /bill", 3, 5, None, 50_000_000),
                ],
            ),
        )
        .await
        .expect("ingest tenant A gateway spans");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &tenant_a,
            traces_request(
                "checkout",
                vec![
                    span_with_ids("POST /charge", 1, 2, Some(1), 20_000_000),
                    span_with_ids("POST /charge", 2, 4, Some(3), 20_000_000),
                ],
            ),
        )
        .await
        .expect("ingest tenant A checkout spans");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &tenant_a,
            traces_request(
                "billing",
                vec![span_with_ids("POST /invoice", 3, 6, Some(5), 20_000_000)],
            ),
        )
        .await
        .expect("ingest tenant A billing span");

    // Tenant B ("other-tenant"): web calls api once, deliberately reusing
    // trace_id=1 — storage is siloed per tenant, so this must not join
    // against tenant A's rows at all.
    let tenant_b = tenant_context("other-tenant", "test-dataset", "other-key-123");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &tenant_b,
            traces_request(
                "web",
                vec![span_with_ids("GET /home", 1, 101, None, 30_000_000)],
            ),
        )
        .await
        .expect("ingest tenant B web span");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &tenant_b,
            traces_request(
                "api",
                vec![span_with_ids("GET /data", 1, 102, Some(101), 15_000_000)],
            ),
        )
        .await
        .expect("ingest tenant B api span");

    let app = build_router(&services).await;

    let (status, body) = post_ir_as_until_rows(
        &app,
        correlate_pair_counts_document(),
        "test-key-123",
        "test-tenant",
        2,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "tenant A correlate query: {body}");
    assert_eq!(
        caller_callee_counts(&body),
        vec![
            ("gateway".to_string(), "billing".to_string(), 1),
            ("gateway".to_string(), "checkout".to_string(), 2),
        ],
        "tenant A must see only its own caller/callee pairs: {body}"
    );

    let (status, body) = post_ir_as_until_rows(
        &app,
        correlate_pair_counts_document(),
        "other-key-123",
        "other-tenant",
        1,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "tenant B correlate query: {body}");
    assert_eq!(
        caller_callee_counts(&body),
        vec![("web".to_string(), "api".to_string(), 1)],
        "tenant B must see only its own caller/callee pair, never tenant A's: {body}"
    );
}

/// Task 4a — a low `[querier].correlate_max_rows` truncates a
/// correlate→aggregate query, and the router surfaces it as a warning
/// (ground truth from the querier, not the document-sniffing heuristic
/// this replaced).
#[tokio::test]
async fn correlate_truncation_warns_through_the_full_stack() {
    let services = setup_with(|config| config.querier.correlate_max_rows = 1).await;
    let ctx = test_tenant_context();
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "gateway",
                vec![
                    span_with_ids("GET /a", 1, 1, None, 50_000_000),
                    span_with_ids("GET /b", 2, 3, None, 50_000_000),
                ],
            ),
        )
        .await
        .expect("ingest gateway spans");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "checkout",
                vec![
                    span_with_ids("POST /charge", 1, 2, Some(1), 20_000_000),
                    span_with_ids("POST /charge", 2, 4, Some(3), 20_000_000),
                ],
            ),
        )
        .await
        .expect("ingest checkout spans");

    let app = build_router(&services).await;
    let (status, body) = post_ir_as_until_rows(
        &app,
        correlate_pair_counts_document(),
        "test-key-123",
        "test-tenant",
        1,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "correlate+aggregate query: {body}");
    let warnings = body["warnings"].as_array().expect("warnings array");
    assert!(
        warnings.iter().any(|w| w["code"] == "correlate_row_limit"),
        "expected a correlate_row_limit warning: {body}"
    );
}

/// Task 4b — a `left` join keeps a root span (no parent) with every
/// `parent.*` field null, end to end.
#[tokio::test]
async fn correlate_left_join_keeps_root_span_with_null_parent_fields() {
    let services = setup().await;
    let ctx = test_tenant_context();
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request("gateway", vec![span("GET /root", 1, 50_000_000)]),
        )
        .await
        .expect("ingest root span");

    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": 8, "from": "traces", "range": range(), "result": "rows",
        "fields": ["span.name", "parent.service.name"],
        "pipeline": [{ "correlate": { "to": "parent", "kind": "left" } }]
    });
    let (status, body) = post_ir_until_rows(&app, document).await;
    assert_eq!(status, StatusCode::OK, "left correlate query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "the root span survives a left join: {body}");
    assert_eq!(rows[0][0], "GET /root");
    assert!(
        rows[0][1].is_null(),
        "a root span has no parent, so parent.service.name is null: {body}"
    );
}

/// Task 4c — a `parent.<key>` attribute reference resolves against the
/// parent side end to end. Sets the attribute via a processor (the same
/// mechanism `processor_created_via_router_api_redacts_pii_end_to_end`
/// already proves round-trips through real ingest→query) rather than the
/// OTLP payload directly, so the fixture reuses an already-proven path.
#[tokio::test]
async fn correlate_resolves_a_parent_span_attribute() {
    let services = setup().await;
    let ctx = test_tenant_context();
    let app = build_router(&services).await;

    let create = Request::builder()
        .method("POST")
        .uri("/api/v1/processors")
        .header("Authorization", "Bearer test-key-123")
        .header("X-Tenant-ID", "test-tenant")
        .header("Content-Type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "name": "set-parent-route",
                "signal": "traces",
                "statements": [
                    r#"set(attributes["checkout.route"], "/checkout")"#
                ],
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED, "processor create");

    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "gateway",
                vec![span_with_ids("GET /checkout", 1, 1, None, 50_000_000)],
            ),
        )
        .await
        .expect("ingest root span");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "checkout",
                vec![span_with_ids("POST /charge", 1, 2, Some(1), 20_000_000)],
            ),
        )
        .await
        .expect("ingest child span");

    let document = serde_json::json!({
        "irVersion": 8, "from": "traces", "range": range(), "result": "rows",
        "fields": ["span.name", "parent.checkout.route"],
        "pipeline": [
            { "correlate": { "to": "parent", "kind": "inner" } },
            { "where": { "field": "service.name", "op": "eq", "value": "checkout" } }
        ]
    });
    let (status, body) = post_ir_until_rows(&app, document).await;
    assert_eq!(status, StatusCode::OK, "parent attribute query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "{body}");
    assert_eq!(rows[0][1], "/checkout", "{body}");
}

/// Task 4d — a parent that starts before the query window is treated as
/// missing, end to end.
#[tokio::test]
async fn correlate_parent_outside_the_window_is_missing() {
    let services = setup().await;
    let ctx = test_tenant_context();
    // The root starts at BASE_NS; the child starts 5s later, so a window
    // opening after BASE_NS but before that excludes only the root.
    let child_offset_ns: i64 = 5_000_000_000;
    let mut root = span_with_ids("GET /a", 1, 1, None, 50_000_000);
    root.start_time_unix_nano = BASE_NS as u64;
    root.end_time_unix_nano = (BASE_NS + 50_000_000) as u64;
    let mut child = span_with_ids("POST /charge", 1, 2, Some(1), 20_000_000);
    child.start_time_unix_nano = (BASE_NS + child_offset_ns) as u64;
    child.end_time_unix_nano = (BASE_NS + child_offset_ns + 20_000_000) as u64;

    services
        .trace_handler
        .handle_grpc_otlp_traces(&ctx, traces_request("gateway", vec![root]))
        .await
        .expect("ingest root span");
    services
        .trace_handler
        .handle_grpc_otlp_traces(&ctx, traces_request("checkout", vec![child]))
        .await
        .expect("ingest child span");

    let app = build_router(&services).await;
    let correlate_where_checkout = |range: serde_json::Value| {
        serde_json::json!({
            "irVersion": 8, "from": "traces", "range": range, "result": "rows",
            "fields": ["span.name"],
            "pipeline": [
                { "correlate": { "to": "parent", "kind": "inner" } },
                { "where": { "field": "service.name", "op": "eq", "value": "checkout" } }
            ]
        })
    };

    // Sanity check over the full window: both spans are persisted and the
    // join finds the parent before narrowing the range.
    let wide_range = serde_json::json!({
        "from": (BASE_NS - 1_000_000_000).to_string(),
        "to": (BASE_NS + child_offset_ns + 10_000_000_000).to_string(),
    });
    let (status, body) = post_ir_until_rows(&app, correlate_where_checkout(wide_range)).await;
    assert_eq!(status, StatusCode::OK, "wide-window sanity query: {body}");
    assert_eq!(
        body["rows"].as_array().expect("rows array").len(),
        1,
        "sanity: the join finds the parent inside the full window: {body}"
    );

    // A window opening after the root but before the child: the parent
    // falls outside it.
    let narrow_range = serde_json::json!({
        "from": (BASE_NS + 1_000_000_000).to_string(),
        "to": (BASE_NS + child_offset_ns + 10_000_000_000).to_string(),
    });
    let (status, body) = post_ir(&app, correlate_where_checkout(narrow_range)).await;
    assert_eq!(status, StatusCode::OK, "narrow-window query: {body}");
    // An empty `rows` is omitted from the JSON entirely
    // (`#[serde(skip_serializing_if = "Vec::is_empty")]`), not serialized
    // as `[]` — a present, non-empty array would be the failure case here.
    let row_count = body["rows"].as_array().map(Vec::len).unwrap_or(0);
    assert_eq!(
        row_count, 0,
        "the parent starts before the narrowed window, so the inner join drops the child: {body}"
    );
}

// service-map task 1.5 — the `graph` envelope (IR v8) across the full
// ingest→store→query stack: three services and a postgres client span per
// trace, a second tenant reusing the same service names.

/// A span with an explicit OTLP kind (2 = server, 3 = client), error
/// status, and string attributes.
fn graph_span(
    trace_id: u8,
    span_id: u8,
    parent_span_id: Option<u8>,
    kind: i32,
    error: bool,
    attrs: &[(&str, &str)],
) -> Span {
    Span {
        kind,
        attributes: attrs
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.to_string(),
                value: Some(string_value(v)),
                ..Default::default()
            })
            .collect(),
        status: Some(Status {
            code: if error { 2 } else { 1 },
            message: String::new(),
        }),
        ..span_with_ids("op", trace_id, span_id, parent_span_id, 10_000_000)
    }
}

/// Ingest one `frontend → checkout → orders → orders-db` trace per
/// `(trace_id, orders_fails)`. Span ids are `trace_id * 10 + n`.
async fn ingest_graph_traces(handler: &TraceHandler, ctx: &TenantContext, traces: &[(u8, bool)]) {
    let mut by_service: Vec<(&str, Vec<Span>)> = vec![
        ("frontend", vec![]),
        ("checkout", vec![]),
        ("orders", vec![]),
    ];
    for &(t, orders_fails) in traces {
        let id = |n: u8| t * 10 + n;
        by_service[0]
            .1
            .push(graph_span(t, id(1), None, 2, false, &[]));
        by_service[0].1.push(graph_span(
            t,
            id(2),
            Some(id(1)),
            3,
            false,
            &[
                ("server.address", "checkout:8080"),
                ("http.request.method", "GET"),
            ],
        ));
        by_service[1]
            .1
            .push(graph_span(t, id(3), Some(id(2)), 2, false, &[]));
        by_service[1]
            .1
            .push(graph_span(t, id(4), Some(id(3)), 3, false, &[]));
        by_service[2]
            .1
            .push(graph_span(t, id(5), Some(id(4)), 2, orders_fails, &[]));
        by_service[2].1.push(graph_span(
            t,
            id(6),
            Some(id(5)),
            3,
            false,
            &[
                ("db.system.name", "postgresql"),
                ("db.namespace", "orders-db"),
            ],
        ));
    }
    for (service, spans) in by_service {
        handler
            .handle_grpc_otlp_traces(ctx, traces_request(service, spans))
            .await
            .expect("ingest graph spans");
    }
}

fn graph_document(extra: serde_json::Value) -> serde_json::Value {
    let mut doc = serde_json::json!({
        "irVersion": 8, "from": "traces", "range": range(),
        "result": "graph", "pipeline": []
    });
    for (k, v) in extra.as_object().into_iter().flatten() {
        doc[k] = v.clone();
    }
    doc
}

/// Poll until the graph holds at least `min_edges` edges.
async fn post_graph_until_edges(
    app: &Router,
    doc: serde_json::Value,
    key: &str,
    tenant: &str,
    min_edges: usize,
) -> serde_json::Value {
    let mut last = serde_json::Value::Null;
    for _ in 0..40 {
        let (status, body) = post_ir_as(app, doc.clone(), key, tenant, None).await;
        assert_eq!(status, StatusCode::OK, "graph query: {body}");
        if body["graph"]["edges"].as_array().map_or(0, Vec::len) >= min_edges {
            return body;
        }
        last = body;
        sleep(Duration::from_millis(500)).await;
    }
    panic!("graph never reached {min_edges} edges: {last}");
}

fn graph_edge<'a>(
    body: &'a serde_json::Value,
    source: &str,
    target: &str,
) -> &'a serde_json::Value {
    body["graph"]["edges"]
        .as_array()
        .expect("edges")
        .iter()
        .find(|e| e["source"] == source && e["target"] == target)
        .unwrap_or_else(|| panic!("no edge {source} -> {target}: {body}"))
}

fn graph_node_ids(body: &serde_json::Value) -> Vec<String> {
    let mut ids: Vec<String> = body["graph"]["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .map(|n| n["id"].as_str().expect("id").to_string())
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn service_graph_end_to_end_isolated_by_tenant() {
    let services = setup().await;
    let tenant_a = test_tenant_context();
    ingest_graph_traces(&services.trace_handler, &tenant_a, &[(1, false), (2, true)]).await;
    // A database named like the `orders` service must stay its own node.
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &tenant_a,
            traces_request(
                "checkout",
                vec![graph_span(
                    3,
                    31,
                    None,
                    3,
                    false,
                    &[("db.system.name", "postgresql"), ("db.namespace", "orders")],
                )],
            ),
        )
        .await
        .expect("ingest tenant A colliding db span");
    // Tenant B reuses tenant A's trace ids, with no failures.
    let tenant_b = tenant_context("other-tenant", "test-dataset", "other-key-123");
    ingest_graph_traces(
        &services.trace_handler,
        &tenant_b,
        &[(1, false), (2, false)],
    )
    .await;
    let app = build_router(&services).await;

    let body = post_graph_until_edges(
        &app,
        graph_document(serde_json::json!({})),
        "test-key-123",
        "test-tenant",
        4,
    )
    .await;
    assert_eq!(
        graph_node_ids(&body),
        [
            "external:database:orders",
            "external:database:orders-db",
            "service:checkout",
            "service:frontend",
            "service:orders",
        ],
        "no external node for the instrumented checkout:8080 call: {body}"
    );
    assert_eq!(
        graph_edge(&body, "service:frontend", "service:checkout")["count"],
        2
    );
    let orders = graph_edge(&body, "service:checkout", "service:orders");
    assert_eq!(
        orders["count"], 2,
        "tenant B's spans must not count: {body}"
    );
    assert_eq!(orders["error_rate"], 0.5);
    assert_eq!(
        graph_edge(&body, "service:checkout", "external:database:orders")["count"],
        1
    );
    let db = body["graph"]["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .find(|n| n["id"] == "external:database:orders-db")
        .expect("orders-db node");
    assert_eq!(db["name"], "orders-db");
    assert_eq!(db["kind"], "external");
    assert_eq!(db["dependency_kind"], "database");
    assert_eq!(
        graph_edge(&body, "service:orders", "external:database:orders-db")["count"],
        2
    );

    let body = post_graph_until_edges(
        &app,
        graph_document(serde_json::json!({ "focus": "checkout" })),
        "test-key-123",
        "test-tenant",
        3,
    )
    .await;
    assert_eq!(
        graph_node_ids(&body),
        [
            "external:database:orders",
            "service:checkout",
            "service:frontend",
            "service:orders",
        ]
    );
    assert_eq!(body["graph"]["edges"].as_array().map(Vec::len), Some(3));

    // Trace 2 exists in both tenants; tenant A's failing call must be the
    // only one counted.
    let body = post_graph_until_edges(
        &app,
        graph_document(serde_json::json!({ "trace_id": "02".repeat(16) })),
        "test-key-123",
        "test-tenant",
        3,
    )
    .await;
    let orders = graph_edge(&body, "service:checkout", "service:orders");
    assert_eq!(
        orders["count"], 1,
        "only tenant A's call in trace 2: {body}"
    );
    assert_eq!(orders["error_rate"], 1.0);

    let body = post_graph_until_edges(
        &app,
        graph_document(serde_json::json!({})),
        "other-key-123",
        "other-tenant",
        3,
    )
    .await;
    assert_eq!(
        graph_edge(&body, "service:frontend", "service:checkout")["count"],
        2
    );
    assert_eq!(
        graph_edge(&body, "service:checkout", "service:orders")["error_rate"],
        0.0
    );
    assert!(
        graph_node_ids(&body)
            .iter()
            .all(|id| id != "external:database:orders"),
        "tenant A's colliding db span must not leak: {body}"
    );
}

// otel-native-schema layer 9 task 9.1 — cross-signal `correlate` (`irVersion`
// 11): `to` a signal source other than `from`, the `semi`/`anti` join kinds,
// a target-side `where` sub-pipeline, a `window` widening the target scan,
// and a per-source-row `fanout` cap on `inner`/`left`. Written test-first
// against `openspec/changes/archive/2026-09-30-otel-native-schema/specs/cross-signal-correlate/
// spec.md` while the feature lands on another branch: every test below is
// expected to fail today, since this server does not accept `irVersion: 11`
// yet. All go through `POST /api/v1/query`, never a compat API.

const CORRELATE_IR_VERSION: i64 = 11;

/// Like [`log_record`], with explicit trace/span identity so a log can be
/// cross-signal-correlated to a span from [`span_with_ids`].
fn log_record_for_trace(
    offset_ns: i64,
    severity: &str,
    body: &str,
    trace_id: u8,
    span_id: u8,
) -> LogRecord {
    LogRecord {
        trace_id: vec![trace_id; 16],
        span_id: vec![span_id; 8],
        ..log_record(offset_ns, severity, body)
    }
}

/// The single-attribute resource shape [`traces_request`]/[`logs_request`]
/// build inline, factored out here so a `resource.identity` correlate can
/// compare a metric against a log or trace from the same nominal resource.
fn resource_with_service(service: &str) -> Resource {
    Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(string_value(service)),
            ..Default::default()
        }],
        dropped_attributes_count: 0,
        ..Default::default()
    }
}

/// One gauge metric data point for `service`.
fn gauge_metric_request(
    service: &str,
    metric_name: &str,
    value: f64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource_with_service(service)),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: "1".to_string(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: BASE_NS as u64,
                            time_unix_nano: BASE_NS as u64,
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

/// A one-point histogram metric for `service`, carrying one exemplar with
/// the given trace/span identity and value — the `exemplars` source's only
/// path to a real trace/span id.
fn histogram_with_exemplar(
    service: &str,
    trace_id: u8,
    span_id: u8,
    exemplar_value: f64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource_with_service(service)),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "latency".to_string(),
                    description: String::new(),
                    unit: "s".to_string(),
                    data: Some(Data::Histogram(Histogram {
                        aggregation_temporality: 1, // delta
                        data_points: vec![HistogramDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: BASE_NS as u64,
                            time_unix_nano: BASE_NS as u64,
                            count: 1,
                            sum: Some(exemplar_value),
                            bucket_counts: vec![0, 1],
                            explicit_bounds: vec![1.0],
                            exemplars: vec![Exemplar {
                                filtered_attributes: vec![],
                                time_unix_nano: BASE_NS as u64,
                                span_id: vec![span_id; 8],
                                trace_id: vec![trace_id; 16],
                                value: Some(exemplar::Value::AsDouble(exemplar_value)),
                            }],
                            flags: 0,
                            min: Some(exemplar_value),
                            max: Some(exemplar_value),
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

/// Read one column of a `"rows"` envelope, addressed positionally by the
/// document's own `fields` list — the same convention the existing
/// single-signal correlate tests above use for `rows[i][j]`.
fn rows_column(body: &serde_json::Value, idx: usize) -> Vec<Option<String>> {
    body["rows"]
        .as_array()
        .expect("rows array")
        .iter()
        .map(|row| row[idx].as_str().map(str::to_string))
        .collect()
}

fn warning_codes(body: &serde_json::Value) -> Vec<String> {
    body["warnings"]
        .as_array()
        .map(|warnings| {
            warnings
                .iter()
                .map(|w| w["code"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Poll a plain `from` query until it returns at least `min_rows` rows, so a
/// correlate query runs only once both sides' writes are readable: an anti
/// join against a not-yet-visible target keeps every source row.
async fn wait_for_rows(app: &Router, from: &str, range: serde_json::Value, min_rows: usize) {
    wait_for_rows_as(app, "test-key-123", "test-tenant", from, range, min_rows).await;
}

async fn wait_for_rows_as(
    app: &Router,
    key: &str,
    tenant: &str,
    from: &str,
    range: serde_json::Value,
    min_rows: usize,
) {
    let doc = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": from, "range": range, "result": "rows"
    });
    for _ in 0..40 {
        let (status, body) = post_ir_as(app, doc.clone(), key, tenant, None).await;
        let rows = body["rows"].as_array().map(Vec::len).unwrap_or(0);
        if status == StatusCode::OK && rows >= min_rows {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("{from} never reached {min_rows} readable rows");
}

/// Scenario 1 — an aggregate/topk pipeline over traces (the "slowest traces"
/// pattern) followed by a signal-target `correlate` to `logs`: the surviving
/// rows carry `logs.body` for exactly the two slowest traces, joined across
/// the traces/logs writers' different trace_id encodings.
#[tokio::test]
async fn correlate_signal_target_joins_slowest_traces_to_their_logs() {
    let services = setup().await;
    let ctx = test_tenant_context();

    for (trace_id, dur_ns) in [(1u8, 50_000_000i64), (2, 200_000_000), (3, 100_000_000)] {
        services
            .trace_handler
            .handle_grpc_otlp_traces(
                &ctx,
                traces_request(
                    "svc",
                    vec![span_with_ids("op", trace_id, trace_id, None, dur_ns)],
                ),
            )
            .await
            .expect("ingest trace");
    }
    for (trace_id, body) in [
        (1u8, "log-for-trace-1"),
        (2, "log-for-trace-2"),
        (3, "log-for-trace-3"),
    ] {
        services
            .log_handler
            .handle_grpc_otlp_logs(
                &ctx,
                logs_request(
                    "svc",
                    vec![log_record_for_trace(
                        1_000_000, "INFO", body, trace_id, trace_id,
                    )],
                ),
            )
            .await
            .expect("ingest log");
    }

    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "table",
        "fields": ["trace_id", "d", "logs.body"],
        "pipeline": [
            { "aggregate": { "by": ["trace_id"], "aggs": [{ "fn": "max", "of": "duration", "as": "d" }] } },
            { "topk": { "n": 2, "of": "d" } },
            { "correlate": { "to": "logs", "on": "trace_id", "kind": "inner" } }
        ]
    });
    let (status, body) = post_ir_until_rows(&app, document).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "slowest-traces correlate query: {body}"
    );
    let bodies: std::collections::BTreeSet<String> =
        rows_column(&body, 2).into_iter().flatten().collect();
    assert_eq!(
        bodies,
        ["log-for-trace-2", "log-for-trace-3"]
            .into_iter()
            .map(String::from)
            .collect(),
        "only the two slowest traces' logs come back: {body}"
    );
}

/// Scenario 2 — `semi` keeps only the source (trace) rows that have a
/// matching target (log) row after the target-side `where` sub-pipeline.
#[tokio::test]
async fn correlate_semi_keeps_only_traces_with_an_error_log() {
    let services = setup().await;
    let ctx = test_tenant_context();

    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "has-error",
                vec![span_with_ids("op", 1, 1, None, 10_000_000)],
            ),
        )
        .await
        .expect("ingest trace with an error log");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request("clean", vec![span_with_ids("op", 2, 2, None, 10_000_000)]),
        )
        .await
        .expect("ingest trace with only an info log");

    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "has-error",
                vec![log_record_for_trace(1_000_000, "ERROR", "boom", 1, 1)],
            ),
        )
        .await
        .expect("ingest error log");
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "clean",
                vec![log_record_for_trace(1_000_000, "INFO", "ok", 2, 2)],
            ),
        )
        .await
        .expect("ingest info log");

    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "rows",
        "fields": ["service.name"],
        "pipeline": [ { "correlate": { "to": "logs", "on": "trace_id", "kind": "semi",
            "pipeline": [ { "where": { "field": "severity_number", "op": "gte", "value": 17 } } ] } } ]
    });
    let (status, body) = post_ir_until_rows(&app, document).await;
    assert_eq!(status, StatusCode::OK, "semi correlate query: {body}");
    assert_eq!(
        rows_column(&body, 0),
        vec![Some("has-error".to_string())],
        "{body}"
    );
    assert!(
        !warning_codes(&body).contains(&"correlate_fanout_limit".to_string()),
        "semi never reports a fanout limit: {body}"
    );
}

/// Scenario 3 — `anti` is the complement of `semi`: it keeps the trace with
/// only an info log, and the trace with no log at all (both "no match"),
/// and drops the trace with an error log.
#[tokio::test]
async fn correlate_anti_keeps_only_traces_without_an_error_log() {
    let services = setup().await;
    let ctx = test_tenant_context();

    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "has-error",
                vec![span_with_ids("op", 1, 1, None, 10_000_000)],
            ),
        )
        .await
        .expect("ingest trace with an error log");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "clean-with-log",
                vec![span_with_ids("op", 2, 2, None, 10_000_000)],
            ),
        )
        .await
        .expect("ingest trace with only an info log");
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "no-logs-at-all",
                vec![span_with_ids("op", 3, 3, None, 10_000_000)],
            ),
        )
        .await
        .expect("ingest trace with no logs");

    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "has-error",
                vec![log_record_for_trace(1_000_000, "ERROR", "boom", 1, 1)],
            ),
        )
        .await
        .expect("ingest error log");
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "clean-with-log",
                vec![log_record_for_trace(1_000_000, "INFO", "ok", 2, 2)],
            ),
        )
        .await
        .expect("ingest info log");

    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "rows",
        "fields": ["service.name"],
        "pipeline": [ { "correlate": { "to": "logs", "on": "trace_id", "kind": "anti",
            "pipeline": [ { "where": { "field": "severity_number", "op": "gte", "value": 17 } } ] } } ]
    });
    wait_for_rows(&app, "traces", range(), 3).await;
    wait_for_rows(&app, "logs", range(), 2).await;
    let (status, body) = post_ir(&app, document).await;
    assert_eq!(status, StatusCode::OK, "anti correlate query: {body}");
    let mut services_seen: Vec<String> = rows_column(&body, 0).into_iter().flatten().collect();
    services_seen.sort();
    assert_eq!(
        services_seen,
        vec!["clean-with-log".to_string(), "no-logs-at-all".to_string()],
        "{body}"
    );
    assert!(
        !warning_codes(&body).contains(&"correlate_fanout_limit".to_string()),
        "anti never reports a fanout limit: {body}"
    );
}

/// Scenario 4 — the default (zero-width) target window treats a log that
/// lands after the trace as "no match"; widening the window with `after`
/// brings it into scope and the response warns with the resulting window.
#[tokio::test]
async fn correlate_anti_window_widens_the_target_scan() {
    let services = setup().await;
    let ctx = test_tenant_context();
    let late_offset_ns: i64 = 10 * 60 * 1_000_000_000; // +10m, after the trace's spans

    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request("svc", vec![span_with_ids("op", 1, 1, None, 10_000_000)]),
        )
        .await
        .expect("ingest trace");
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "svc",
                vec![log_record_for_trace(
                    late_offset_ns,
                    "ERROR",
                    "late-error",
                    1,
                    1,
                )],
            ),
        )
        .await
        .expect("ingest late error log");

    let app = build_router(&services).await;
    let anti_document = |window: Option<serde_json::Value>| {
        let mut correlate = serde_json::json!({ "to": "logs", "on": "trace_id", "kind": "anti",
            "pipeline": [ { "where": { "field": "severity_number", "op": "gte", "value": 17 } } ] });
        if let Some(window) = window {
            correlate["window"] = window;
        }
        serde_json::json!({
            "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "rows",
            "fields": ["service.name"],
            "pipeline": [ { "correlate": correlate } ]
        })
    };

    // The late log lies past `range()`, so wait for it over a wider range.
    let through_late_log = serde_json::json!({
        "from": (BASE_NS - 1_000_000_000).to_string(),
        "to": (BASE_NS + 2 * late_offset_ns).to_string(),
    });
    wait_for_rows(&app, "traces", range(), 1).await;
    wait_for_rows(&app, "logs", through_late_log, 1).await;
    let (status, body) = post_ir(&app, anti_document(None)).await;
    assert_eq!(status, StatusCode::OK, "default-window anti query: {body}");
    assert_eq!(
        rows_column(&body, 0),
        vec![Some("svc".to_string())],
        "the error log lands 10m after the trace, outside the default window, so anti still matches: {body}"
    );

    let (status, body) = post_ir(
        &app,
        anti_document(Some(serde_json::json!({ "after": "15m" }))),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "widened-window anti query: {body}");
    let row_count = body["rows"].as_array().map(Vec::len).unwrap_or(0);
    assert_eq!(
        row_count, 0,
        "widening the window to 15m brings the late error log into scope, so anti now excludes the trace: {body}"
    );
    let warnings = body["warnings"].as_array().expect("warnings array");
    let window_warning = warnings
        .iter()
        .find(|w| w["code"] == "correlate_window")
        .unwrap_or_else(|| panic!("expected a correlate_window warning: {body}"));
    // The message states the resulting absolute window, not the raw `after`
    // operand: a traces source's envelope extends to start+duration (here
    // +10ms), so `after: 15m` from that end lands at 22:28:20.010, 15m10ms
    // past the trace's start (22:13:20).
    assert!(
        window_warning["message"]
            .as_str()
            .unwrap_or_default()
            .contains("22:28:20.010"),
        "the warning states the widened target window: {body}"
    );
}

/// Scenario 5 — `from: exemplars`, `semi`-correlated to `traces` on
/// `trace_id`: only the exemplar whose trace_id resolves to a real trace
/// survives.
#[tokio::test]
async fn correlate_semi_keeps_exemplars_with_a_real_trace() {
    let services = setup().await;
    let ctx = test_tenant_context();

    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request("svc", vec![span_with_ids("op", 1, 1, None, 10_000_000)]),
        )
        .await
        .expect("ingest trace");
    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&ctx, histogram_with_exemplar("svc", 1, 1, 3.0))
        .await
        .expect("ingest exemplar with a real trace");
    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&ctx, histogram_with_exemplar("svc", 9, 9, 9.0))
        .await
        .expect("ingest exemplar with no matching trace");

    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "exemplars", "range": range(), "result": "rows",
        "fields": ["exemplar.value"],
        "pipeline": [ { "correlate": { "to": "traces", "on": "trace_id", "kind": "semi" } } ]
    });
    let (status, body) = post_ir_until_rows(&app, document).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "exemplar semi-correlate query: {body}"
    );
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "only the exemplar whose trace_id resolves to a real trace survives: {body}"
    );
    assert_eq!(rows[0][0].as_f64(), Some(3.0), "{body}");
}

/// Scenario 6 — `resource_identity` matches across signals when the
/// resource's attribute set is the same, regardless of trace/span identity.
#[tokio::test]
async fn correlate_semi_on_resource_identity_matches_same_resource_across_signals() {
    let services = setup().await;
    let ctx = test_tenant_context();

    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&ctx, gauge_metric_request("has-logs", "requests", 1.0))
        .await
        .expect("ingest metric with a resource that also logs");
    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&ctx, gauge_metric_request("no-logs", "requests", 1.0))
        .await
        .expect("ingest metric with a resource that never logs");
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request("has-logs", vec![log_record(1_000_000, "INFO", "hi")]),
        )
        .await
        .expect("ingest log");

    let app = build_router(&services).await;
    // A metrics source's window envelope is the single instant of its data
    // point, not a range (unlike a traces source, which extends to
    // start+duration): the log lands 1ms after that instant, so the
    // correlate stage needs an explicit `after` to bring it into scope.
    let document = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "metrics", "range": range(), "result": "rows",
        "fields": ["service.name"],
        "pipeline": [ { "correlate": { "to": "logs", "on": "resource_identity", "kind": "semi",
            "window": { "after": "1s" } } } ]
    });
    let (status, body) = post_ir_until_rows(&app, document).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "resource_identity semi correlate query: {body}"
    );
    assert_eq!(
        rows_column(&body, 0),
        vec![Some("has-logs".to_string())],
        "{body}"
    );
}

/// Scenario 7 — an `inner` join's per-source-row `fanout` cap keeps only the
/// earliest `fanout` target rows and reports it as a warning (`semi`/`anti`
/// never do, proven above).
#[tokio::test]
async fn correlate_inner_fanout_caps_rows_per_source_and_warns() {
    let services = setup().await;
    let ctx = test_tenant_context();

    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request("svc", vec![span_with_ids("op", 1, 1, None, 10_000_000)]),
        )
        .await
        .expect("ingest trace");
    for i in 0..5u8 {
        services
            .log_handler
            .handle_grpc_otlp_logs(
                &ctx,
                logs_request(
                    "svc",
                    vec![log_record_for_trace(
                        i as i64 * 1_000_000,
                        "INFO",
                        &format!("log-{i}"),
                        1,
                        1,
                    )],
                ),
            )
            .await
            .expect("ingest log");
    }

    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "rows",
        "fields": ["logs.body"],
        "pipeline": [ { "correlate": { "to": "logs", "on": "trace_id", "kind": "inner", "fanout": 2 } } ]
    });
    let (status, body) = post_ir_until_rows(&app, document).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fanout-capped correlate query: {body}"
    );
    let bodies: std::collections::BTreeSet<String> =
        rows_column(&body, 0).into_iter().flatten().collect();
    assert_eq!(
        bodies.len(),
        2,
        "the fanout cap keeps only 2 log rows for the one trace: {body}"
    );
    assert_eq!(
        bodies,
        ["log-0", "log-1"].into_iter().map(String::from).collect(),
        "the fanout cap keeps the earliest 2 logs by target time: {body}"
    );
    assert!(
        warning_codes(&body).contains(&"correlate_fanout_limit".to_string()),
        "expected a correlate_fanout_limit warning: {body}"
    );
}

/// Scenario 8 — three ways a signal-target `correlate` stage is invalid:
/// the join key doesn't exist on one side, a preceding aggregate drops it,
/// and `fanout` is set on a `semi` join. All 400s; the message names the
/// specific reason, not just "unsupported version" (so this test fails for
/// the right reason today, not by accident).
///
/// A `traces`-sourced document is only validated once the `traces` table
/// exists (a dataset with none of a source's tables skips schema-dependent
/// validation along with the scan, per `ir_planner::plan_document`), so this
/// ingests a trace and polls until it is queryable before asserting on the
/// two `traces`-sourced cases below.
#[tokio::test]
async fn correlate_signal_target_validation_errors() {
    let services = setup().await;
    let ctx = test_tenant_context();
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request("svc", vec![span_with_ids("op", 1, 1, None, 10_000_000)]),
        )
        .await
        .expect("ingest trace");

    let app = build_router(&services).await;
    let warmup = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "rows",
        "fields": ["service.name"],
    });
    let (status, body) = post_ir_until_rows(&app, warmup).await;
    assert_eq!(status, StatusCode::OK, "warm-up traces query: {body}");

    let key_missing_on_metrics = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "metrics", "range": range(), "result": "rows",
        "fields": ["metric.name"],
        "pipeline": [ { "correlate": { "to": "traces", "on": "trace_id", "kind": "semi" } } ]
    });
    let key_dropped_by_aggregate = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "table",
        "pipeline": [
            { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }] } },
            { "correlate": { "to": "logs", "on": "trace_id", "kind": "inner" } }
        ]
    });
    let fanout_with_semi = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "rows",
        "fields": ["service.name"],
        "pipeline": [ { "correlate": { "to": "logs", "on": "trace_id", "kind": "semi", "fanout": 2 } } ]
    });

    for (label, doc, expected_phrase) in [
        (
            "key missing on the metrics side",
            key_missing_on_metrics,
            "trace_id",
        ),
        (
            "key dropped by a preceding aggregate",
            key_dropped_by_aggregate,
            "dropped",
        ),
        ("fanout on a semi join", fanout_with_semi, "fanout"),
    ] {
        let (status, body) = post_ir(&app, doc).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {body}");
        let message = body["error"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected_phrase),
            "{label}: expected the error to mention '{expected_phrase}', got: {body}"
        );
    }
}

/// Scenario 9 — a low `[querier].correlate_max_source_rows` rejects a
/// correlate whose source relation exceeds it with a `422` whose
/// `errorType` is `resource_limit`, not a retryable `429` (the same query
/// fails again unchanged), for both `semi` and `anti`.
#[tokio::test]
async fn correlate_source_row_bound_rejects_oversized_source_with_422() {
    let services = setup_with(|config| config.querier.correlate_max_source_rows = 1).await;
    let ctx = test_tenant_context();

    for trace_id in 1u8..=3 {
        services
            .trace_handler
            .handle_grpc_otlp_traces(
                &ctx,
                traces_request(
                    "svc",
                    vec![span_with_ids("op", trace_id, trace_id, None, 10_000_000)],
                ),
            )
            .await
            .expect("ingest trace");
    }

    let app = build_router(&services).await;
    wait_for_rows(&app, "traces", range(), 3).await;
    for kind in ["semi", "anti"] {
        let document = serde_json::json!({
            "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "rows",
            "fields": ["service.name"],
            "pipeline": [ { "correlate": { "to": "logs", "on": "trace_id", "kind": kind } } ]
        });
        let (status, body) = post_ir(&app, document).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{kind} correlate over the source row bound: {body}"
        );
        assert_eq!(
            body["errorType"], "resource_limit",
            "{kind} correlate over the source row bound: {body}"
        );
    }
}

/// A key holding only the source signal's read scope cannot correlate to a
/// target signal it has no read scope for — the router must check every
/// correlate target's scope, not just `from`'s (see
/// `document_read_scopes` in `router::endpoints::query`).
#[tokio::test]
async fn correlate_to_an_unscoped_target_is_forbidden() {
    let services = setup().await;
    services
        .catalog
        .upsert_scoped_api_key(
            "test-tenant",
            &common::auth::Authenticator::hash_api_key("traces-only-key"),
            Some("traces-only"),
            None,
            None,
            Some(&["traces:read".to_string()]),
            None,
        )
        .await
        .expect("create traces-only scoped key");

    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": CORRELATE_IR_VERSION, "from": "traces", "range": range(), "result": "rows",
        "fields": ["service.name"],
        "pipeline": [ { "correlate": { "to": "logs", "on": "trace_id", "kind": "semi" } } ]
    });
    let (status, body) = post_ir_as(&app, document, "traces-only-key", "test-tenant", None).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a traces:read-only key correlating to logs: {body}"
    );
}

const MATCH_IR_VERSION: i64 = 12;

/// Like [`span_with_ids`], plus OTLP events (by name) and links (to a trace id).
fn span_with_events_and_links(
    name: &str,
    trace_id: u8,
    span_id: u8,
    parent_span_id: Option<u8>,
    event_names: &[&str],
    link_trace_ids: &[[u8; 16]],
) -> Span {
    use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
    Span {
        events: event_names
            .iter()
            .map(|event| Event {
                time_unix_nano: BASE_NS as u64,
                name: (*event).to_string(),
                ..Default::default()
            })
            .collect(),
        links: link_trace_ids
            .iter()
            .map(|linked| Link {
                trace_id: linked.to_vec(),
                span_id: vec![7; 8],
                ..Default::default()
            })
            .collect(),
        ..span_with_ids(name, trace_id, span_id, parent_span_id, 10_000_000)
    }
}

fn trace_hex(trace_id: u8) -> String {
    hex::encode([trace_id; 16])
}

/// A `match` document over traces returning the `trace` envelope.
fn match_document(match_stage: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "irVersion": MATCH_IR_VERSION, "from": "traces", "range": range(), "result": "trace",
        "pipeline": [ { "match": match_stage } ]
    })
}

fn do_put_write_match(op: &str) -> serde_json::Value {
    serde_json::json!({
        "spansets": {
            "put": { "field": "span.name", "op": "eq", "value": "DoPut" },
            "write": { "field": "span.name", "op": "eq", "value": "write_parquet_files" }
        },
        "relations": [ { "left": "put", "op": op, "right": "write" } ]
    })
}

fn trace_ids_in(body: &serde_json::Value) -> Vec<String> {
    body["traces"]
        .as_array()
        .expect("traces array")
        .iter()
        .map(|t| t["trace_id"].as_str().expect("string trace_id").to_string())
        .collect()
}

/// Ingest a `DoPut` root with a chain of `depth` child spans; the last child is
/// `write_parquet_files` when `with_write`, else another `step`. Span ids run
/// 1..=depth+1 (root is 1). Returns the number of spans ingested.
async fn ingest_do_put_chain(
    services: &TestServices,
    ctx: &TenantContext,
    trace_id: u8,
    depth: u8,
    with_write: bool,
) -> usize {
    let mut spans = vec![span_with_ids("DoPut", trace_id, 1, None, 10_000_000)];
    for i in 1..=depth {
        let name = if with_write && i == depth {
            "write_parquet_files"
        } else {
            "step"
        };
        spans.push(span_with_ids(name, trace_id, i + 1, Some(i), 10_000_000));
    }
    services
        .trace_handler
        .handle_grpc_otlp_traces(ctx, traces_request("ingester", spans))
        .await
        .expect("ingest DoPut chain");
    usize::from(depth) + 1
}

/// A `DoPut` at depth 0 with a `write_parquet_files` 50 levels below it is
/// returned in the `trace` envelope; a trace with `DoPut` but no write is not,
/// `child` does not reach 50 levels down, and another tenant's identical shape
/// never leaks in.
#[tokio::test]
async fn match_descendant_at_depth_returns_the_trace() {
    let services = setup().await;
    let ctx = test_tenant_context();
    let other = tenant_context("other-tenant", "test-dataset", "other-key-123");
    // Rows visible to the default tenant; the other tenant's are not counted.
    let mut rows = ingest_do_put_chain(&services, &ctx, 1, 50, true).await;
    rows += ingest_do_put_chain(&services, &ctx, 2, 5, false).await;
    let other_rows = ingest_do_put_chain(&services, &other, 9, 3, true).await;

    let app = build_router(&services).await;
    wait_for_rows(&app, "traces", range(), rows).await;
    wait_for_rows_as(
        &app,
        "other-key-123",
        "other-tenant",
        "traces",
        range(),
        other_rows,
    )
    .await;

    let (status, body) = post_ir(&app, match_document(do_put_write_match("descendant"))).await;
    assert_eq!(status, StatusCode::OK, "descendant match: {body}");
    assert_eq!(body["result"], "trace", "trace envelope: {body}");
    assert_eq!(
        trace_ids_in(&body),
        vec![trace_hex(1)],
        "only the trace with the deep write matches: {body}"
    );

    let mut witnesses: Vec<(&str, &str)> = body["traces"][0]["spans"]
        .as_array()
        .expect("spans array")
        .iter()
        .map(|s| {
            (
                s["span_name"].as_str().expect("span_name"),
                s["spansets"].as_str().expect("spansets"),
            )
        })
        .collect();
    witnesses.sort_unstable();
    assert_eq!(
        witnesses,
        [("DoPut", "put"), ("write_parquet_files", "write")],
        "only the two endpoint spans are witnesses"
    );

    // `child` requires a direct parent link; the same data must not match.
    let (status, body) = post_ir(&app, match_document(do_put_write_match("child"))).await;
    assert_eq!(status, StatusCode::OK, "child match: {body}");
    assert!(
        trace_ids_in(&body).is_empty(),
        "a write 50 levels down is not a child: {body}"
    );

    // Tenant isolation: the other tenant sees only its own trace.
    let (status, body) = post_ir_as(
        &app,
        match_document(do_put_write_match("descendant")),
        "other-key-123",
        "other-tenant",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "tenant B match: {body}");
    assert_eq!(trace_ids_in(&body), vec![trace_hex(9)], "isolation: {body}");
}

/// Span-set predicates on `events.name` and `links.trace_id`; a trace with
/// only one of the two, and a plain control trace, are not returned.
#[tokio::test]
async fn match_on_span_events_and_links() {
    let services = setup().await;
    let ctx = test_tenant_context();
    let linked = [0xab_u8; 16];

    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request(
                "svc",
                vec![
                    // Trace 1: an exception event on the root, a link on a child.
                    span_with_events_and_links("op", 1, 1, None, &["exception"], &[]),
                    span_with_events_and_links("call", 1, 2, Some(1), &[], &[linked]),
                    // Trace 2: the event only.
                    span_with_events_and_links("op", 2, 1, None, &["exception"], &[]),
                    // Trace 3: control, neither.
                    span_with_events_and_links("op", 3, 1, None, &["log"], &[]),
                    span_with_events_and_links("call", 3, 2, Some(1), &[], &[]),
                    // Trace 4: both, but on siblings — passes the candidate
                    // filter and fails the relation.
                    span_with_events_and_links("op", 4, 1, None, &[], &[]),
                    span_with_events_and_links("boom", 4, 2, Some(1), &["exception"], &[]),
                    span_with_events_and_links("call", 4, 3, Some(1), &[], &[linked]),
                ],
            ),
        )
        .await
        .expect("ingest event/link spans");

    let app = build_router(&services).await;
    wait_for_rows(&app, "traces", range(), 8).await;

    let document = match_document(serde_json::json!({
        "spansets": {
            "boom": { "field": "events.name", "op": "eq", "value": "exception" },
            "linked": { "field": "links.trace_id", "op": "eq", "value": hex::encode(linked) }
        },
        "relations": [ { "left": "linked", "op": "ancestor", "right": "boom" } ]
    }));
    let (status, body) = post_ir(&app, document).await;
    assert_eq!(status, StatusCode::OK, "events/links match: {body}");
    assert_eq!(
        trace_ids_in(&body),
        vec![trace_hex(1)],
        "only the trace with both the exception event and the link matches: {body}"
    );
}

/// `match` is traces-only: on logs it is a 400 at validation.
#[tokio::test]
async fn match_on_logs_is_rejected() {
    let services = setup().await;
    let app = build_router(&services).await;
    let document = serde_json::json!({
        "irVersion": MATCH_IR_VERSION, "from": "logs", "range": range(), "result": "rows",
        "pipeline": [ { "match": {
            "spansets": { "a": { "field": "body", "op": "contains", "value": "x" } },
            "relations": []
        } } ]
    });
    let (status, body) = post_ir(&app, document).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "match on logs: {body}");
    let message = body["error"].as_str().expect("error message");
    assert!(
        message.contains("source 'logs' does not support match (traces only)"),
        "the documented traces-only message, got: {body}"
    );
}

/// A range that starts after a trace's root: the `match` returns the same
/// witnesses as over the whole trace, and a `match_incomplete_trace` warning
/// names the trace, which the whole-trace query does not carry.
#[tokio::test]
async fn match_incomplete_trace_warns_when_the_range_cuts_a_trace() {
    let services = setup().await;
    let ctx = test_tenant_context();
    let offset_ns: u64 = 5_000_000_000;
    let root = span_with_ids("gateway", 1, 1, None, 10_000_000);
    let mut spans = vec![
        span_with_ids("DoPut", 1, 2, Some(1), 10_000_000),
        span_with_ids("write_parquet_files", 1, 3, Some(2), 10_000_000),
    ];
    for span in &mut spans {
        span.start_time_unix_nano += offset_ns;
        span.end_time_unix_nano += offset_ns;
    }
    spans.push(root);
    services
        .trace_handler
        .handle_grpc_otlp_traces(&ctx, traces_request("ingester", spans))
        .await
        .expect("ingest a trace whose root starts before the narrow range");

    let app = build_router(&services).await;
    wait_for_rows(&app, "traces", range(), 3).await;

    let witnesses = |body: &serde_json::Value| -> Vec<String> {
        let spans = body["traces"][0]["spans"].as_array().expect("spans array");
        spans.iter().map(|s| s["span_name"].to_string()).collect()
    };
    let document = match_document(do_put_write_match("descendant"));
    let (status, body) = post_ir(&app, document.clone()).await;
    assert_eq!(status, StatusCode::OK, "whole-trace match: {body}");
    assert!(
        !warning_codes(&body).contains(&"match_incomplete_trace".to_string()),
        "the whole trace is in range: {body}"
    );
    let whole = witnesses(&body);
    assert_eq!(whole.len(), 2, "{body}");

    let mut narrow = document;
    narrow["range"] = serde_json::json!({
        "from": (BASE_NS + 1_000_000_000).to_string(),
        "to": (BASE_NS + 10_000_000_000).to_string(),
    });
    let (status, body) = post_ir(&app, narrow).await;
    assert_eq!(status, StatusCode::OK, "narrow match: {body}");
    assert_eq!(witnesses(&body), whole, "the warning never changes rows");
    let warnings = body["warnings"].as_array().expect("warnings array");
    let [warning] = warnings.as_slice() else {
        panic!("exactly one warning: {body}");
    };
    assert_eq!(warning["code"], "match_incomplete_trace", "{body}");
    let message = warning["message"].as_str().expect("message");
    assert!(
        message.starts_with("1 matched trace may be missing witness spans: ")
            && message.ends_with(&format!("Examples: {}", trace_hex(1))),
        "{message}"
    );
}

/// Issue #2123: a range that starts after the root, where the relation
/// needs that root, leaves no span for the root's span-set. The trace does
/// not match, and the warning counts it as unmatched.
#[tokio::test]
async fn match_incomplete_trace_counts_a_trace_whose_span_set_is_cut_off() {
    let services = setup().await;
    let ctx = test_tenant_context();
    let root = span_with_ids("gateway", 1, 1, None, 10_000_000_000);
    let mut child = span_with_ids("DoPut", 1, 2, Some(1), 1_000_000_000);
    child.start_time_unix_nano += 5_000_000_000;
    child.end_time_unix_nano += 5_000_000_000;
    services
        .trace_handler
        .handle_grpc_otlp_traces(&ctx, traces_request("ingester", vec![root, child]))
        .await
        .expect("ingest a root with one child");

    let app = build_router(&services).await;
    wait_for_rows(&app, "traces", range(), 2).await;

    let document = match_document(serde_json::json!({
        "spansets": {
            "root": { "field": "span.name", "op": "eq", "value": "gateway" },
            "child": { "field": "span.name", "op": "eq", "value": "DoPut" }
        },
        "relations": [ { "left": "root", "op": "child", "right": "child" } ]
    }));
    let (status, body) = post_ir(&app, document.clone()).await;
    assert_eq!(status, StatusCode::OK, "whole-trace match: {body}");
    assert_eq!(trace_ids_in(&body), [trace_hex(1)], "{body}");
    assert!(
        !warning_codes(&body).contains(&"match_incomplete_trace".to_string()),
        "the whole trace is in range: {body}"
    );

    let mut narrow = document;
    narrow["range"] = serde_json::json!({
        "from": (BASE_NS + 4_000_000_000).to_string(),
        "to": (BASE_NS + 7_000_000_000).to_string(),
    });
    let (status, body) = post_ir(&app, narrow).await;
    assert_eq!(status, StatusCode::OK, "narrow match: {body}");
    assert!(trace_ids_in(&body).is_empty(), "{body}");
    let warnings = body["warnings"].as_array().expect("warnings array");
    let [warning] = warnings.as_slice() else {
        panic!("exactly one warning: {body}");
    };
    assert_eq!(warning["code"], "match_incomplete_trace", "{body}");
    let message = warning["message"].as_str().expect("message");
    assert!(
        message.starts_with("1 trace did not match but may match over a wider range: ")
            && message.ends_with(&format!("Examples: {}", trace_hex(1))),
        "{message}"
    );
}

/// A trace larger than `[querier].match_max_trace_spans` fails the query with
/// a 422 `resource_limit` that names the trace, instead of truncating it.
#[tokio::test]
async fn match_trace_span_bound_is_422() {
    let services = setup_with(|config| config.querier.match_max_trace_spans = 3).await;
    let ctx = test_tenant_context();
    let rows = ingest_do_put_chain(&services, &ctx, 1, 3, true).await;

    let app = build_router(&services).await;
    wait_for_rows(&app, "traces", range(), rows).await;

    let (status, body) = post_ir(&app, match_document(do_put_write_match("descendant"))).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "trace over the span bound: {body}"
    );
    assert_eq!(body["errorType"], "resource_limit", "{body}");
    let message = body["error"].as_str().expect("error message");
    assert!(
        message.contains(&trace_hex(1)),
        "the error names the offending trace, got: {body}"
    );
    assert!(
        message.contains("match_max_trace_spans"),
        "the error names the config key, got: {body}"
    );
}
