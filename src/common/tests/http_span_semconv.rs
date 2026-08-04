//! End-to-end verification that the router's HTTP trace-context middleware
//! emits a span following the OpenTelemetry HTTP semantic conventions.
//!
//! Lives in its own integration-test binary (separate process) so the
//! process-global OTel/tracing state it installs cannot be poisoned by, or
//! poison, other tests running in parallel.

use axum::{Router, body::Body, http::Request, middleware, routing::get};
use opentelemetry::trace::{SpanKind, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tower::ServiceExt;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn emits_semconv_server_span() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let app = Router::new()
        .nest(
            "/tempo",
            Router::new().route("/api/traces/{trace_id}", get(|| async { "ok" })),
        )
        .route(
            "/boom",
            get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .layer(middleware::from_fn(
            common::self_monitoring::http_trace_context_middleware,
        ));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tempo/api/traces/abc123")
                .header("host", "signaldb.example:3000")
                .header("x-forwarded-for", "203.0.113.9, 10.0.0.1")
                .header("user-agent", "probe/1.0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // A 5xx marks the span failed with the status code as `error.type`.
    let response = app
        .oneshot(Request::builder().uri("/boom").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), 500);

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();
    let span = spans
        .iter()
        .find(|s| s.name.starts_with("GET"))
        .unwrap_or_else(|| panic!("no GET server span; exported spans = {names:?}"));

    // Span name is the low-cardinality route template, never the raw path.
    assert_eq!(span.name, "GET /tempo/api/traces/{trace_id}");
    assert_eq!(span.span_kind, SpanKind::Server);

    let attr = |key: &str| {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.as_str().to_string())
    };
    assert_eq!(
        attr("http.route").as_deref(),
        Some("/tempo/api/traces/{trace_id}")
    );
    assert_eq!(attr("http.request.method").as_deref(), Some("GET"));
    assert_eq!(
        attr("url.path").as_deref(),
        Some("/tempo/api/traces/abc123")
    );
    assert_eq!(attr("url.scheme").as_deref(), Some("http"));
    assert_eq!(attr("http.response.status_code").as_deref(), Some("200"));
    assert_eq!(attr("user_agent.original").as_deref(), Some("probe/1.0"));
    // Host header splits into address and port.
    assert_eq!(attr("server.address").as_deref(), Some("signaldb.example"));
    assert_eq!(attr("server.port").as_deref(), Some("3000"));
    // First (client-most) X-Forwarded-For entry.
    assert_eq!(attr("client.address").as_deref(), Some("203.0.113.9"));
    // Success leaves status unset and no error.type.
    assert_eq!(span.status, opentelemetry::trace::Status::Unset);
    assert_eq!(attr("error.type"), None);

    // The 5xx span: status Error, error.type = status code as a string.
    let err_span = spans
        .iter()
        .find(|s| s.name == "GET /boom")
        .unwrap_or_else(|| panic!("no GET /boom span; exported spans = {names:?}"));
    let err_attr = |key: &str| {
        err_span
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.as_str().to_string())
    };
    assert_eq!(
        err_attr("http.response.status_code").as_deref(),
        Some("500")
    );
    assert!(matches!(
        err_span.status,
        opentelemetry::trace::Status::Error { .. }
    ));
    assert_eq!(err_attr("error.type").as_deref(), Some("500"));
}
