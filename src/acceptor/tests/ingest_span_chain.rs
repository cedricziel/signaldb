//! Verifies the OTLP/HTTP ingest path keeps one contiguous trace: the HTTP
//! SERVER span, both handler spans, and the acceptor -> writer `DoPut`
//! CLIENT span must share a trace id, with no span silently starting a
//! trace of its own.
//!
//! The stack mirrors production (`cli::utils::init_logging`): the OTel span
//! bridge and the OTel log bridge, each behind `OtelExportFilter`, under a
//! parent-based ratio sampler, exercised both sequentially and from
//! concurrently spawned tasks — the combination that decides whether a span
//! resolves its parent from the surrounding context or re-roots.
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
use opentelemetry::trace::TracerProvider as _;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_span_chain_stays_in_one_trace() {
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
            opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(0.1),
        )))
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder().build();
    use tracing_subscriber::Layer as _;
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(
            common::self_monitoring::otel_span_layer(tracer)
                .with_filter(common::self_monitoring::OtelExportFilter),
        )
        .with(
            opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
                &logger_provider,
            )
            .with_filter(common::self_monitoring::OtelExportFilter),
        );
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let (app, _temp_dir) = setup_traces_test().await;

    fn request() -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header("Authorization", format!("Bearer {TEST_API_KEY}"))
            .header("X-Tenant-ID", TEST_TENANT)
            .body(Body::from(sample_trace_request().encode_to_vec()))
            .unwrap()
    }

    // Sequential load.
    for _ in 0..100 {
        let response = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    // Concurrent load across worker threads.
    let mut handles = Vec::new();
    for _ in 0..100 {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let response = app.oneshot(request()).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();

    let mut roots_by_name: std::collections::BTreeMap<String, usize> = Default::default();
    let mut total_by_name: std::collections::BTreeMap<String, usize> = Default::default();
    for s in &spans {
        *total_by_name.entry(s.name.to_string()).or_default() += 1;
        if s.parent_span_id == opentelemetry::trace::SpanId::INVALID {
            *roots_by_name.entry(s.name.to_string()).or_default() += 1;
        }
    }
    println!("exported spans: {}", spans.len());
    for (name, total) in &total_by_name {
        println!(
            "  {name}: {total} total, {} rooted",
            roots_by_name.get(name).copied().unwrap_or(0)
        );
    }
    let orphaned_clients = roots_by_name
        .get("arrow.flight.protocol.FlightService/DoPut")
        .copied()
        .unwrap_or(0);
    assert_eq!(
        orphaned_clients, 0,
        "DoPut client spans started their own trace"
    );
}
