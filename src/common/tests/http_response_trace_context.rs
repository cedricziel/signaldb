//! End-to-end verification that the HTTP trace-context middleware returns the
//! server span's trace context and timing metadata on responses
//! (`Server-Timing`, `traceresponse`, `Timing-Allow-Origin`).
//!
//! Lives in its own integration-test binary (separate process) so the
//! process-global OTel/tracing state it installs cannot be poisoned by, or
//! poison, other tests running in parallel.

use axum::{Router, body::Body, http::Request, middleware, routing::get};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tower::ServiceExt;
use tracing_subscriber::prelude::*;

const CALLER_TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn traceparent_desc(server_timing: &str) -> Option<String> {
    let idx = server_timing.find("traceparent;desc=\"")?;
    let rest = &server_timing[idx + "traceparent;desc=\"".len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

#[tokio::test]
async fn returns_server_trace_context_and_timings() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).unwrap();
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let app = Router::new()
        .route("/ping", get(|| async { "pong" }))
        .route(
            "/timed",
            get(|| async {
                // Handlers opt into stage timings by inserting `ServerTimings`
                // into the response extensions.
                let mut timings = common::self_monitoring::ServerTimings::new();
                timings.push("plan", std::time::Duration::from_millis(12));
                timings.push("storage_scan", std::time::Duration::from_micros(1500));
                (axum::Extension(timings), "ok")
            }),
        )
        .layer(middleware::from_fn(
            common::self_monitoring::http_trace_context_middleware,
        ));

    // Fresh request without caller context: full header set, valid context.
    let response = app
        .clone()
        .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let server_timing = response
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .expect("Server-Timing header present")
        .to_string();
    let desc = traceparent_desc(&server_timing)
        .unwrap_or_else(|| panic!("no traceparent entry in Server-Timing: {server_timing}"));
    // 00-<32 hex>-<16 hex>-<2 hex>, non-zero ids.
    let parts: Vec<&str> = desc.split('-').collect();
    assert_eq!(parts.len(), 4, "malformed traceparent desc: {desc}");
    assert_eq!(parts[0], "00");
    assert_eq!(parts[1].len(), 32);
    assert_eq!(parts[2].len(), 16);
    assert_ne!(parts[1], "0".repeat(32), "all-zero trace id emitted");
    assert_ne!(parts[2], "0".repeat(16), "all-zero span id emitted");

    let traceresponse = response
        .headers()
        .get("traceresponse")
        .and_then(|v| v.to_str().ok())
        .expect("traceresponse header present");
    assert_eq!(
        traceresponse, desc,
        "traceresponse must match Server-Timing"
    );

    assert!(
        server_timing.contains("total;dur="),
        "no total;dur entry in Server-Timing: {server_timing}"
    );
    assert_eq!(
        response
            .headers()
            .get("timing-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("*"),
    );

    // Caller-supplied sampled traceparent: the returned context joins the
    // caller's trace (same trace id) with the server's own span id.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ping")
                .header("traceparent", CALLER_TRACEPARENT)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let server_timing = response
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .expect("Server-Timing header present")
        .to_string();
    let desc = traceparent_desc(&server_timing).expect("traceparent entry present");
    let parts: Vec<&str> = desc.split('-').collect();
    assert_eq!(parts[1], "0af7651916cd43dd8448eb211c80319c");
    assert_ne!(parts[2], "b7ad6b7169203331", "span id must be the server's");
    assert_eq!(parts[3], "01", "sampled caller context stays sampled");

    // Handler-supplied stage timings are drained from response extensions
    // into additional `name;dur=` Server-Timing entries.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/timed")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let server_timing = response
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .expect("Server-Timing header present")
        .to_string();
    assert!(
        server_timing.contains("plan;dur=12"),
        "missing plan stage in: {server_timing}"
    );
    assert!(
        server_timing.contains("storage_scan;dur=1.5"),
        "missing storage_scan stage in: {server_timing}"
    );
    assert!(server_timing.contains("total;dur="));
    assert!(
        response
            .extensions()
            .get::<common::self_monitoring::ServerTimings>()
            .is_none(),
        "timings extension must be drained, not leaked to the client-facing response"
    );

    // Self-monitoring tenant bypass: no span, no trace headers.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ping")
                .header("x-tenant-id", "_system")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().get("server-timing").is_none());
    assert!(response.headers().get("traceresponse").is_none());
    assert!(response.headers().get("timing-allow-origin").is_none());
}
