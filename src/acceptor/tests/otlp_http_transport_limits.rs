//! Integration tests for the acceptor's OTLP/HTTP transport limits:
//! gzip/zstd request decompression (finding H1) and the decompressed-body
//! size cap (finding H2).
//!
//! These wrap the traces router with [`acceptor::with_http_transport_limits`]
//! — the exact helper `serve_otlp_http` uses — rather than re-implementing
//! the layer wiring, so the test exercises production code, not a copy of it.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use acceptor::handler::WalManager;
use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::{traces_http_router, with_http_transport_limits};
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use common::auth::Authenticator;
use common::config::Configuration;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{WalConfig, WalOperation};
use flate2::Compression;
use flate2::write::GzEncoder;
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

/// A trace export request padded with a long attribute value so gzip
/// compression actually shrinks the wire size (a tiny request compresses to
/// roughly the same size, which would make the "gzip was applied" assertion
/// meaningless).
fn padded_trace_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "padding".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("x".repeat(8192))),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![0x01; 16],
                    span_id: vec![0x02; 8],
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

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

/// Set up the OTLP/HTTP traces router, with the same transport-limit layers
/// `serve_otlp_http` applies, for a given `max_request_body_bytes`.
async fn setup_traces_test_with_limit(
    max_request_body_bytes: usize,
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
        default_limits: common::config::TenantLimits::default(),
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
    let app = with_http_transport_limits(app, max_request_body_bytes);

    (app, wal_manager, temp_dir)
}

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

/// H1: a gzip-encoded protobuf body (matching the OTel Collector's default
/// `otlphttp` exporter config) is decompressed and durably accepted, not
/// rejected with a decode error.
#[tokio::test]
async fn gzip_compressed_protobuf_body_is_accepted() {
    let (app, wal_manager, _temp_dir) = setup_traces_test_with_limit(64 * 1024 * 1024).await;

    let plain = padded_trace_request().encode_to_vec();
    let compressed = gzip(&plain);
    assert!(
        compressed.len() < plain.len(),
        "test payload must actually shrink under gzip, got {} -> {} bytes",
        plain.len(),
        compressed.len()
    );

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "gzip")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(compressed))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a gzip-compressed OTLP/HTTP body must be decompressed and accepted"
    );
    assert_eq!(
        traces_wal_entry_count(&wal_manager).await,
        1,
        "the decompressed export must be durably recorded in the WAL"
    );
}

/// H2: a plain (uncompressed) body over the configured limit is rejected
/// with 413, not silently truncated or accepted.
#[tokio::test]
async fn oversized_uncompressed_body_is_rejected() {
    let limit = 1024;
    let (app, wal_manager, _temp_dir) = setup_traces_test_with_limit(limit).await;

    let oversized = vec![0u8; limit + 1];
    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(oversized))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(traces_wal_entry_count(&wal_manager).await, 0);
}

/// H2 (zip-bomb guard): the body limit must be enforced against the
/// *decompressed* stream. A small compressed payload that decompresses
/// past the limit must still be rejected with 413, proving decompression
/// runs before the size cap rather than after it.
#[tokio::test]
async fn gzip_body_exceeding_limit_after_decompression_is_rejected() {
    let limit = 512;
    let (app, wal_manager, _temp_dir) = setup_traces_test_with_limit(limit).await;

    // Highly compressible: decompresses far past `limit` while the
    // compressed wire size stays tiny.
    let huge_plain = vec![0u8; limit * 20];
    let compressed = gzip(&huge_plain);
    assert!(
        compressed.len() < limit,
        "compressed payload ({} bytes) must be smaller than the limit ({limit}) for this test \
         to prove decompression runs before the size check",
        compressed.len()
    );

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "gzip")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(compressed))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "a small compressed body that decompresses past the limit must still be rejected"
    );
    assert_eq!(traces_wal_entry_count(&wal_manager).await, 0);
}
