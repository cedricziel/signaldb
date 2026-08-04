//! Verifies the OTLP/HTTP ingest endpoint is a proper trace boundary: it
//! emits a semconv SERVER span joined to the caller-supplied W3C trace
//! context, and a client error (401) leaves the span status unset.
//!
//! Lives in its own integration-test binary (separate process) so the
//! process-global OTel/tracing state it installs cannot be poisoned by, or
//! poison, other tests running in parallel.

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
use common::wal::WalConfig;
use opentelemetry::trace::{SpanKind, Status, TracerProvider as _};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, span};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use prost::Message;
use tempfile::TempDir;
use tower::ServiceExt;
use tracing_subscriber::prelude::*;

const TEST_TENANT: &str = "test-tenant";
const TEST_DATASET: &str = "default";
const TEST_API_KEY: &str = "test-api-key";

fn sample_trace_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
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

/// Minimal copy of the OTLP/HTTP traces router setup from
/// `otlp_http_traces.rs` (each test binary is its own process).
async fn setup_traces_test() -> (axum::Router, TempDir) {
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
    let trace_handler = Arc::new(TraceHandler::new(flight_transport, wal_manager));

    let app = traces_http_router(authenticator, trace_handler, rate_limiter, storage_usage);
    (app, temp_dir)
}

#[tokio::test]
async fn emits_server_span_joined_to_caller_trace() {
    // Production installs the W3C propagator in init_telemetry; parent
    // adoption goes through the global propagator, a no-op by default.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let (app, _temp_dir) = setup_traces_test().await;

    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header("Authorization", format!("Bearer {TEST_API_KEY}"))
                .header("X-Tenant-ID", TEST_TENANT)
                .header("traceparent", traceparent)
                .body(Body::from(sample_trace_request().encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Unauthenticated request: 401 is the caller's problem, span status
    // stays unset.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(sample_trace_request().encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();

    let server_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "POST /v1/traces")
        .collect();
    assert_eq!(
        server_spans.len(),
        2,
        "expected two POST /v1/traces server spans; exported = {names:?}"
    );

    // The authenticated request's span joins the caller's trace.
    let joined = server_spans
        .iter()
        .find(|s| {
            s.span_context.trace_id()
                == opentelemetry::trace::TraceId::from_hex("0af7651916cd43dd8448eb211c80319c")
                    .unwrap()
        })
        .expect("no span joined to the caller trace");
    assert_eq!(joined.span_kind, SpanKind::Server);
    assert_eq!(
        joined.parent_span_id,
        opentelemetry::trace::SpanId::from_hex("b7ad6b7169203331").unwrap()
    );
    assert_eq!(joined.status, Status::Unset);

    // The 401 span: recorded, but not an error for the server.
    let unauth = server_spans
        .iter()
        .find(|s| s.span_context.trace_id() != joined.span_context.trace_id())
        .expect("no span for the unauthenticated request");
    assert_eq!(unauth.status, Status::Unset);
    let status_attr = unauth
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "http.response.status_code")
        .map(|kv| kv.value.as_str().to_string());
    assert_eq!(status_attr.as_deref(), Some("401"));
}
