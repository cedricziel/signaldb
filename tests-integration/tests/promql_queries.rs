//! End-to-end integration tests for the PromQL query interface.
//!
//! Boots the full ingest→store→query stack (acceptor metrics handler →
//! WAL → writer → Iceberg → querier → router), ingests gauge metrics,
//! waits for persistence, then drives the `/prometheus` HTTP endpoints:
//! range/instant queries, an aggregation, and label/series discovery.

use acceptor::handler::WalManager;
use acceptor::handler::otlp_metrics_handler::MetricsHandler;
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
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, metric::Data, number_data_point,
    },
    resource::v1::Resource,
};
use querier::flight::QuerierFlightService;
use router::{RouterState, discovery::ServiceRegistry, endpoints::promql};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::transport::Server;
use tower::ServiceExt;
use writer::IcebergWriterFlightService;

const BASE_NS: u64 = 1_700_000_000_000_000_000;

struct TestServices {
    object_store: Arc<dyn object_store::ObjectStore>,
    flight_transport: Arc<InMemoryFlightTransport>,
    metrics_handler: MetricsHandler,
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
    let writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();
    let _writer_id = writer_bootstrap.service_id();

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

    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
        wal_config,
    ));
    let metrics_handler = MetricsHandler::new(flight_transport.clone(), wal_manager);

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
        metrics_handler,
        config,
        _temp_dir: temp_dir,
    }
}

fn string_value(s: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(s.to_string())),
    }
}

/// One gauge metric `requests` for a service, with a `code` attribute.
fn gauge_metrics(service: &str, value: f64, code: &str) -> ExportMetricsServiceRequest {
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
                            attributes: vec![KeyValue {
                                key: "code".to_string(),
                                value: Some(string_value(code)),
                                ..Default::default()
                            }],
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

/// A single `latency` histogram data point with bounds [1,2,4] and bucket
/// counts [1,2,3,4] (incl. +Inf) — total 10. The 0.5-quantile interpolates
/// to 2 + 2*(5-3)/3 = 3.333… within the (2,4] bucket.
fn histogram_metrics(service: &str) -> ExportMetricsServiceRequest {
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
                    name: "latency".to_string(),
                    description: String::new(),
                    unit: "s".to_string(),
                    data: Some(Data::Histogram(Histogram {
                        aggregation_temporality: 1, // delta
                        data_points: vec![HistogramDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: BASE_NS,
                            time_unix_nano: BASE_NS,
                            count: 10,
                            sum: Some(20.0),
                            bucket_counts: vec![1, 2, 3, 4],
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            exemplars: vec![],
                            flags: 0,
                            min: Some(0.5),
                            max: Some(6.0),
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
        .nest("/prometheus", promql::router().with_state(state))
        .layer(middleware::from_fn(move |req, next| {
            auth_middleware(authenticator.clone(), req, next)
        }))
}

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
    (
        status,
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
    )
}

/// The window bracketing the ingested metrics.
fn window() -> String {
    // Prometheus params are unix seconds; step 1h covers the point.
    let start = (BASE_NS / 1_000_000_000) as i64 - 60;
    let end = (BASE_NS / 1_000_000_000) as i64 + 60;
    format!("start={start}&end={end}&step=1h")
}

/// The ingested metrics' timestamp, in unix seconds (Prometheus param units).
fn at_timestamp() -> u64 {
    BASE_NS / 1_000_000_000
}

/// Boots the full stack, ingests `requests{job=api}=10`,
/// `requests{job=web}=20`, and a `latency` histogram (buckets [1,2,4],
/// counts [1,2,3,4]), and force-flushes the writer so reads observe the
/// data deterministically. Shared arrange step for every PromQL endpoint
/// test below.
async fn setup_with_ingested_metrics() -> (TestServices, Router) {
    let services = setup().await;
    let ctx = test_tenant_context();

    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&ctx, gauge_metrics("api", 10.0, "200"))
        .await
        .expect("ingest api gauge");
    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&ctx, gauge_metrics("web", 20.0, "500"))
        .await
        .expect("ingest web gauge");
    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&ctx, histogram_metrics("api"))
        .await
        .expect("ingest api histogram");

    // Force the writer to commit now — deterministic read-your-writes instead
    // of waiting out the asynchronous background commit loop.
    common::testing::flush_storage_writers(&services.flight_transport, "test-tenant", None)
        .await
        .expect("flush writer");
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
        objects
            .iter()
            .any(|o| o.location.as_ref().contains("metrics")),
        "expected persisted metrics objects, found: {:?}",
        objects
            .iter()
            .map(|o| o.location.to_string())
            .collect::<Vec<_>>()
    );

    let app = build_router(&services).await;
    (services, app)
}

#[tokio::test]
async fn promql_range_query_returns_matrix_with_all_series() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=requests&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "query_range: {body}");
    assert_eq!(body["data"]["resultType"], "matrix");
    let values = matrix_value_sum(&body);
    assert!(
        (values - 30.0).abs() < 1e-9,
        "api+web should total 30: {body}"
    );
}

#[tokio::test]
async fn promql_range_query_sum_aggregates_across_series() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=sum(requests)&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "sum(requests): {body}");
    let series = body["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(series.len(), 1, "sum collapses to one series: {body}");
    assert!(
        (matrix_value_sum(&body) - 30.0).abs() < 1e-9,
        "sum(requests) should total 30: {body}"
    );
}

#[tokio::test]
async fn promql_instant_query_returns_last_sample_per_series() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let at = at_timestamp();

    // Instant query evaluated at the data's timestamp returns a vector
    // carrying the last sample of each series (10 + 20 = 30).
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query?query=requests&time={at}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "instant query: {body}");
    assert_eq!(body["data"]["resultType"], "vector");
    let vector = body["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(vector.len(), 2, "one instant sample per series: {body}");
    let vsum: f64 = vector
        .iter()
        .filter_map(|s| s["value"][1].as_str().and_then(|v| v.parse::<f64>().ok()))
        .sum();
    assert!(
        (vsum - 30.0).abs() < 1e-9,
        "instant values total 30: {body}"
    );
}

#[tokio::test]
async fn promql_instant_query_empty_window_returns_empty_vector() {
    let (_services, app) = setup_with_ingested_metrics().await;

    // A query whose window contains no data returns an empty vector, not 500.
    let (status, body) = get(&app, "/prometheus/api/v1/query?query=requests&time=100").await;

    assert_eq!(status, StatusCode::OK, "empty-window instant query: {body}");
    assert_eq!(body["data"]["resultType"], "vector");
    assert!(
        body["data"]["result"]
            .as_array()
            .is_none_or(|a| a.is_empty()),
        "empty window yields no series: {body}"
    );
}

#[tokio::test]
async fn promql_labels_endpoint_lists_series_labels() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    let (status, body) = get(&app, &format!("/prometheus/api/v1/labels?{w}")).await;

    assert_eq!(status, StatusCode::OK, "labels: {body}");
    let labels: Vec<String> = serde_json::from_value(body["data"].clone()).unwrap_or_default();
    assert!(labels.contains(&"__name__".to_string()), "{labels:?}");
    assert!(labels.contains(&"job".to_string()), "{labels:?}");
    assert!(labels.contains(&"code".to_string()), "{labels:?}");
}

#[tokio::test]
async fn promql_label_values_endpoint_lists_metric_names() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/label/__name__/values?{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "label values: {body}");
    let names: Vec<String> = serde_json::from_value(body["data"].clone()).unwrap_or_default();
    assert!(names.contains(&"requests".to_string()), "{names:?}");
}

#[tokio::test]
async fn promql_series_endpoint_returns_matching_series() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/series?match%5B%5D=requests&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "series: {body}");
    let series = body["data"].as_array().cloned().unwrap_or_default();
    assert!(
        series.iter().all(|s| s["__name__"] == "requests"),
        "series should all be requests: {body}"
    );
    assert_eq!(series.len(), 2, "one series per job: {body}");
}

#[tokio::test]
async fn promql_histogram_quantile_interpolates_median() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    // histogram_quantile over the stored latency histogram: the median
    // interpolates to 3.333… within the (2,4] bucket.
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=histogram_quantile(0.5,latency)&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "histogram_quantile: {body}");
    assert_eq!(body["data"]["resultType"], "matrix");
    let q = matrix_value_sum(&body);
    assert!(
        (q - (2.0 + 2.0 * 2.0 / 3.0)).abs() < 1e-6,
        "median latency ≈ 3.333, got {q}: {body}"
    );
}

// The remaining tests exercise newer function families end-to-end through
// the router to confirm the query→lowering→execution wiring (the math
// itself is covered by the querier unit tests). `>` is percent-encoded
// (%3E), subquery brackets/colon as %5B/%3A/%5D.

#[tokio::test]
async fn promql_vector_division_yields_one_per_series() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    // Vector-to-vector arithmetic: requests / requests = 1 per series.
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=requests/requests&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "vector division: {body}");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert!(
        matrix_all_values_near(&body, 1.0, 1e-9),
        "a/a should be 1 for every sample: {body}"
    );
}

#[tokio::test]
async fn promql_comparison_filter_keeps_matching_series() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    // Scalar comparison filters to the matching series (web=20 > 15).
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=requests%3E15&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "comparison filter: {body}");
    assert!(
        (matrix_value_sum(&body) - 20.0).abs() < 1e-9,
        "only web (20) survives > 15: {body}"
    );
}

#[tokio::test]
async fn promql_histogram_fraction_computes_bucket_ratio() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    // histogram_fraction over the latency histogram: (0, 2] = 3/10 = 0.3.
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=histogram_fraction(0,2,latency)&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "histogram_fraction: {body}");
    assert!(
        (matrix_value_sum(&body) - 0.3).abs() < 1e-6,
        "fraction in (0,2] ≈ 0.3: {body}"
    );
}

#[tokio::test]
async fn promql_vector_function_produces_constant_series() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    // vector(42): a synthetic constant series.
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=vector(42)&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "vector(): {body}");
    assert!(
        matrix_all_values_near(&body, 42.0, 1e-9),
        "every vector(42) sample must equal 42 exactly: {body}"
    );
}

#[tokio::test]
async fn promql_absent_function_reports_missing_metric() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    // absent() of a missing metric yields 1.
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=absent(does_not_exist)&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "absent(): {body}");
    assert!(
        matrix_all_values_near(&body, 1.0, 1e-9),
        "absent(does_not_exist) must yield exactly 1: {body}"
    );
}

#[tokio::test]
async fn promql_subquery_avg_over_time_executes() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    // Subquery under an over_time reducer lowers and executes cleanly.
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=avg_over_time(requests%5B1h%3A5m%5D)&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "avg_over_time subquery: {body}");
    assert_eq!(body["data"]["resultType"], "matrix");
}

#[tokio::test]
async fn promql_at_modifier_pins_evaluation_time() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();
    let at = at_timestamp();

    // The @ modifier lowers and executes cleanly over the router.
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=requests@{at}&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "@ modifier: {body}");
    assert_eq!(body["data"]["resultType"], "matrix");
}

#[tokio::test]
async fn promql_time_function_executes() {
    let (_services, app) = setup_with_ingested_metrics().await;
    let w = window();

    // time() lowers and executes cleanly over the router.
    let (status, body) = get(
        &app,
        &format!("/prometheus/api/v1/query_range?query=time()&{w}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "time(): {body}");
    assert_eq!(body["data"]["resultType"], "matrix");
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

/// True if every sample value across every series in a matrix response is
/// within `epsilon` of `expected`.
fn matrix_all_values_near(body: &serde_json::Value, expected: f64, epsilon: f64) -> bool {
    let Some(series) = body["data"]["result"].as_array() else {
        return false;
    };
    let samples: Vec<&serde_json::Value> = series
        .iter()
        .flat_map(|s| s["values"].as_array().into_iter().flatten())
        .collect();
    // An empty result or a sample without a parseable numeric value is a
    // failure, not a vacuous pass.
    !samples.is_empty()
        && samples.iter().all(|sample| {
            sample[1]
                .as_str()
                .and_then(|v| v.parse::<f64>().ok())
                .is_some_and(|v| (v - expected).abs() < epsilon)
        })
}
