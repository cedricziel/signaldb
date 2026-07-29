//! Integration tests for the OTLP/HTTP metrics ingestion endpoint
//!
//! Covers the full flow: HTTP request -> auth middleware -> decode
//! (protobuf and JSON) -> metrics handler -> WAL durability.

use std::sync::Arc;
use std::time::Duration;

use acceptor::handler::WalManager;
use acceptor::handler::otlp_metrics_handler::MetricsHandler;
use acceptor::metrics_http_router;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use common::auth::Authenticator;
use common::config::Configuration;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{WalConfig, WalOperation};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use tempfile::TempDir;
use tower::ServiceExt;

const TEST_TENANT: &str = "test-tenant";
const TEST_DATASET: &str = "default";
const TEST_API_KEY: &str = "test-api-key";

/// Build a minimal but valid OTLP metrics export request with one gauge.
fn sample_metrics_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "test.gauge".to_string(),
                    description: "a test gauge".to_string(),
                    unit: "1".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_700_000_000_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(42.0)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Set up the OTLP/HTTP metrics router with an authenticated test tenant.
///
/// Returns the router, the WAL manager (to verify durability), and the
/// temp dir keeping catalog/WAL files alive.
async fn setup_metrics_test() -> (axum::Router, Arc<WalManager>, TempDir) {
    let temp_dir = TempDir::new().unwrap();

    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: Duration::from_secs(30),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(300),
    });

    config.schema = common::config::SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: catalog_dsn,
        default_schemas: common::config::DefaultSchemas::default(),
        materialized_labels: Default::default(),
    };

    config.auth = common::config::AuthConfig {
        admin_api_key: None,
        internal_service_key: None,
        default_limits: Default::default(),
        storage_usage_refresh_interval: Duration::from_secs(60),
        tenants: vec![common::config::TenantConfig {
            id: TEST_TENANT.to_string(),
            slug: TEST_TENANT.to_string(),
            name: "Test Tenant".to_string(),
            default_dataset: Some(TEST_DATASET.to_string()),
            datasets: vec![common::config::DatasetConfig {
                id: TEST_DATASET.to_string(),
                slug: TEST_DATASET.to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![common::config::ApiKeyConfig {
                key: TEST_API_KEY.to_string(),
                name: Some("Test Key".to_string()),
            }],
            schema_config: None,
            limits: None,
        }],
    };

    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:4317".to_string(),
    )
    .await
    .expect("Failed to initialize service bootstrap");

    let catalog = Arc::new(service_bootstrap.catalog().clone());
    let auth_config = service_bootstrap.config().auth.clone();

    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    let wal_dir = temp_dir.path().join("wal");
    let wal_manager = Arc::new(WalManager::new(
        WalConfig::with_defaults(wal_dir.clone()),
        WalConfig::with_defaults(wal_dir.clone()),
        WalConfig::with_defaults(wal_dir.clone()),
        WalConfig::with_defaults(wal_dir),
    ));

    let rate_limiter = Arc::new(common::ratelimit::TenantRateLimiter::from_auth_config(
        &auth_config,
    ));
    let storage_usage =
        Arc::new(common::storage_usage::StorageUsageTracker::from_auth_config(&auth_config));

    let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

    let metrics_handler = Arc::new(MetricsHandler::new(flight_transport, wal_manager.clone()));

    let app = metrics_http_router(authenticator, metrics_handler, rate_limiter, storage_usage);

    (app, wal_manager, temp_dir)
}

/// Count WAL entries recorded for the test tenant's metrics WAL.
async fn metrics_wal_entry_count(wal_manager: &WalManager) -> usize {
    let wal = wal_manager
        .get_wal(TEST_TENANT, TEST_DATASET, "metrics")
        .await
        .expect("Failed to open metrics WAL");
    wal.get_entries()
        .await
        .expect("Failed to read WAL entries")
        .iter()
        .filter(|e| matches!(e.operation, WalOperation::WriteMetrics))
        .count()
}

#[tokio::test]
async fn otlp_http_metrics_protobuf_with_auth_lands_in_wal() {
    let (app, wal_manager, _temp_dir) = setup_metrics_test().await;

    let body = sample_metrics_request().encode_to_vec();

    let request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected 200 OK for authenticated protobuf metrics export"
    );
    assert_eq!(
        metrics_wal_entry_count(&wal_manager).await,
        1,
        "Expected the metrics export to be durably recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_metrics_json_with_auth_lands_in_wal() {
    let (app, wal_manager, _temp_dir) = setup_metrics_test().await;

    let body = serde_json::to_vec(&sample_metrics_request()).unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/json")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected 200 OK for authenticated JSON metrics export"
    );
    assert_eq!(
        metrics_wal_entry_count(&wal_manager).await,
        1,
        "Expected the metrics export to be durably recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_metrics_invalid_api_key_is_unauthorized() {
    let (app, wal_manager, _temp_dir) = setup_metrics_test().await;

    let body = sample_metrics_request().encode_to_vec();

    let request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", "Bearer wrong-key")
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "Expected 401 Unauthorized for an invalid API key"
    );
    assert_eq!(
        metrics_wal_entry_count(&wal_manager).await,
        0,
        "Rejected exports must not be recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_metrics_missing_auth_headers_is_rejected() {
    let (app, wal_manager, _temp_dir) = setup_metrics_test().await;

    let body = sample_metrics_request().encode_to_vec();

    // No Authorization / X-Tenant-ID headers at all.
    let request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "Expected 401 Unauthorized for a request without auth headers"
    );
    assert_eq!(metrics_wal_entry_count(&wal_manager).await, 0);
}

#[tokio::test]
async fn otlp_http_metrics_malformed_protobuf_is_bad_request() {
    let (app, wal_manager, _temp_dir) = setup_metrics_test().await;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(vec![0xff, 0xfe, 0xfd, 0xfc]))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request for a malformed protobuf payload"
    );
    assert_eq!(metrics_wal_entry_count(&wal_manager).await, 0);
}

#[tokio::test]
async fn otlp_http_metrics_malformed_json_is_bad_request() {
    let (app, wal_manager, _temp_dir) = setup_metrics_test().await;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header(header::CONTENT_TYPE, "application/json")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from("{\"resourceMetrics\": \"not-an-array\"}"))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request for a malformed JSON payload"
    );
    assert_eq!(metrics_wal_entry_count(&wal_manager).await, 0);
}
