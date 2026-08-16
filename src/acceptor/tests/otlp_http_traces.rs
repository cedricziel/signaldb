//! Integration tests for the OTLP/HTTP traces ingestion endpoint
//!
//! Covers the full flow: HTTP request -> auth middleware -> decode
//! (protobuf and JSON) -> trace handler -> WAL durability.

use std::sync::Arc;
use std::time::Duration;

use acceptor::handler::WalManager;
use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::traces_http_router;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use common::auth::Authenticator;
use common::config::Configuration;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{WalConfig, WalOperation};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, span};
use prost::Message;
use tempfile::TempDir;
use tower::ServiceExt;

const TEST_TENANT: &str = "test-tenant";
const TEST_DATASET: &str = "default";
const TEST_API_KEY: &str = "test-api-key";

/// Build a minimal but valid OTLP trace export request with one span.
fn sample_trace_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
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
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![
                        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                        0x0d, 0x0e, 0x0f, 0x10,
                    ],
                    span_id: vec![0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18],
                    name: "test-span".to_string(),
                    kind: span::SpanKind::Server as i32,
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_001_000_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Set up the OTLP/HTTP traces router with an authenticated test tenant.
///
/// Returns the router, the WAL manager (to verify durability), and the
/// temp dir keeping catalog/WAL files alive.
async fn setup_traces_test() -> (axum::Router, Arc<WalManager>, TempDir) {
    setup_traces_test_with_limits(common::config::TenantLimits::default()).await
}

/// [`setup_traces_test`] with a configurable per-tenant rate limit, so a
/// test can exercise the ingest rate limiter's `429` path.
async fn setup_traces_test_with_limits(
    limits: common::config::TenantLimits,
) -> (axum::Router, Arc<WalManager>, TempDir) {
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
        default_limits: limits,
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

    let trace_handler = Arc::new(TraceHandler::new(flight_transport, wal_manager.clone()));

    let app = traces_http_router(authenticator, trace_handler, rate_limiter, storage_usage);

    (app, wal_manager, temp_dir)
}

/// Count WAL entries recorded for the test tenant's traces WAL.
async fn traces_wal_entry_count(wal_manager: &WalManager) -> usize {
    let wal = wal_manager
        .get_wal(TEST_TENANT, TEST_DATASET, "traces")
        .await
        .expect("Failed to open traces WAL");
    wal.get_entries()
        .await
        .expect("Failed to read WAL entries")
        .iter()
        .filter(|e| matches!(e.operation, WalOperation::WriteTraces))
        .count()
}

#[tokio::test]
async fn otlp_http_traces_protobuf_with_auth_lands_in_wal() {
    let (app, wal_manager, _temp_dir) = setup_traces_test().await;

    let body = sample_trace_request().encode_to_vec();

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected 200 OK for authenticated protobuf trace export"
    );
    assert_eq!(
        traces_wal_entry_count(&wal_manager).await,
        1,
        "Expected the trace export to be durably recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_traces_json_with_auth_lands_in_wal() {
    let (app, wal_manager, _temp_dir) = setup_traces_test().await;

    // OTLP/JSON (protojson) encoding: trace/span ids are hex strings.
    let body = serde_json::to_vec(&sample_trace_request()).unwrap();
    let json_text = String::from_utf8(body.clone()).unwrap();
    assert!(
        json_text.contains("0102030405060708090a0b0c0d0e0f10"),
        "OTLP/JSON must hex-encode trace ids, got: {json_text}"
    );

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected 200 OK for authenticated JSON trace export"
    );
    assert_eq!(
        traces_wal_entry_count(&wal_manager).await,
        1,
        "Expected the trace export to be durably recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_traces_invalid_api_key_is_unauthorized() {
    let (app, wal_manager, _temp_dir) = setup_traces_test().await;

    let body = sample_trace_request().encode_to_vec();

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
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
        traces_wal_entry_count(&wal_manager).await,
        0,
        "Rejected exports must not be recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_traces_missing_auth_headers_is_rejected() {
    let (app, wal_manager, _temp_dir) = setup_traces_test().await;

    let body = sample_trace_request().encode_to_vec();

    // No Authorization / X-Tenant-ID headers at all.
    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "Expected 401 Unauthorized for a request without auth headers"
    );
    assert_eq!(traces_wal_entry_count(&wal_manager).await, 0);
}

#[tokio::test]
async fn otlp_http_traces_malformed_protobuf_is_bad_request() {
    let (app, wal_manager, _temp_dir) = setup_traces_test().await;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
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
    assert_eq!(traces_wal_entry_count(&wal_manager).await, 0);
}

#[tokio::test]
async fn otlp_http_traces_malformed_json_is_bad_request() {
    let (app, wal_manager, _temp_dir) = setup_traces_test().await;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from("{\"resourceSpans\": \"not-an-array\"}"))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request for a malformed JSON payload"
    );
    assert_eq!(traces_wal_entry_count(&wal_manager).await, 0);
}

#[tokio::test]
async fn otlp_http_traces_rate_limited_carries_retry_after_and_limit_headers() {
    let (app, wal_manager, _temp_dir) =
        setup_traces_test_with_limits(common::config::TenantLimits {
            max_ingest_requests_per_sec: Some(1),
            burst_seconds: 1.0,
            ..Default::default()
        })
        .await;

    let request = ExportTraceServiceRequest::default();
    let build_request = || {
        Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header("Authorization", format!("Bearer {TEST_API_KEY}"))
            .header("X-Tenant-ID", TEST_TENANT)
            .body(Body::from(request.encode_to_vec()))
            .unwrap()
    };

    // First request consumes the single-request burst.
    let first = app.clone().oneshot(build_request()).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    // Second request is rejected with the retry-after contract.
    let second = app.clone().oneshot(build_request()).await.unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after: u64 = second
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .expect("Retry-After header present and numeric");
    assert!(retry_after >= 1);
    assert_eq!(
        second
            .headers()
            .get("x-ratelimit-limit")
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );
    assert_eq!(
        second
            .headers()
            .get("x-ratelimit-burst")
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );
    assert_eq!(traces_wal_entry_count(&wal_manager).await, 1);
}
