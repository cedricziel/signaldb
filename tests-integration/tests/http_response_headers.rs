//! Cross-service verification that every SignalDB HTTP surface returns the
//! server span's trace context (`Server-Timing: traceparent`, `traceresponse`)
//! and timing metadata, uniformly via the shared middleware.
//!
//! Lives in its own integration-test binary: it installs process-global
//! OTel/tracing state (subscriber + propagator) that must not leak into other
//! tests.

use std::sync::Arc;

use common::catalog::Catalog;
use common::config::Configuration;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use router::RouterState;
use router::discovery::ServiceRegistry;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tracing_subscriber::layer::SubscriberExt;

const CALLER_TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

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

fn install_global_otel() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).unwrap();
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
}

async fn serve(app: axum::Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    format!("http://{addr}")
}

fn assert_trace_headers(headers: &reqwest::header::HeaderMap, surface: &str) -> String {
    let server_timing = headers
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_else(|| panic!("{surface}: no Server-Timing header"));
    assert!(
        server_timing.contains("traceparent;desc=\""),
        "{surface}: no traceparent entry in Server-Timing: {server_timing}"
    );
    assert!(
        server_timing.contains("total;dur="),
        "{surface}: no total;dur entry in Server-Timing: {server_timing}"
    );
    let traceresponse = headers
        .get("traceresponse")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_else(|| panic!("{surface}: no traceresponse header"));
    assert!(
        server_timing.contains(&format!("traceparent;desc=\"{traceresponse}\"")),
        "{surface}: traceresponse {traceresponse} does not match Server-Timing {server_timing}"
    );
    assert_eq!(
        headers
            .get("timing-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("*"),
        "{surface}: missing Timing-Allow-Origin"
    );
    traceresponse.to_string()
}

/// Router and acceptor HTTP surfaces (as combined in monolithic mode) both
/// return trace context and timing headers, and a caller-supplied sampled
/// `traceparent` round-trips its trace id.
#[tokio::test(flavor = "multi_thread")]
async fn all_http_surfaces_return_trace_context_headers() {
    install_global_otel();

    // Router surface.
    let temp_dir = TempDir::new().unwrap();
    let catalog_dsn = format!("sqlite://{}", temp_dir.path().join("catalog.db").display());
    let catalog = Catalog::new(&catalog_dsn).await.unwrap();
    let config = Configuration::default();
    let service_registry = ServiceRegistry::new(catalog.clone());
    let authenticator = Arc::new(common::auth::Authenticator::new(
        config.auth.clone(),
        Arc::new(catalog.clone()),
    ));
    let router_base = serve(router::create_router(State {
        catalog,
        service_registry,
        config,
        authenticator,
    }))
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{router_base}/health"))
        .header("traceparent", CALLER_TRACEPARENT)
        .send()
        .await
        .expect("router health request");
    assert!(response.status().is_success());
    let traceresponse = assert_trace_headers(response.headers(), "router");
    // Caller's sampled trace is joined: same trace id, server's own span id.
    let parts: Vec<&str> = traceresponse.split('-').collect();
    assert_eq!(parts[1], "0af7651916cd43dd8448eb211c80319c");
    assert_ne!(parts[2], "b7ad6b7169203331");
    assert_eq!(parts[3], "01");

    // Acceptor surface (health router; OTLP signal routers share the same
    // middleware pairing).
    let acceptor_base = serve(acceptor::acceptor_router()).await;
    let response = client
        .get(format!("{acceptor_base}/health"))
        .send()
        .await
        .expect("acceptor health request");
    assert!(response.status().is_success());
    assert_trace_headers(response.headers(), "acceptor");
}
