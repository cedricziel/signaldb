//! Trace continuity from an MCP tool call into the router's own HTTP server
//! span (openspec/changes/mcp-audit-and-concurrency, task 4.1).
//!
//! `signaldb-sdk`'s `tracing` feature (default on, #1260) injects the current
//! tracing span's W3C `traceparent`/`tracestate` into every outbound request.
//! The MCP server opens `mcp_tool_span` (`tools/call {tool}`, INTERNAL) around
//! each tool dispatch (#1255) and instruments the call with it, so the SDK
//! call `get_trace` makes to the router carries that span's trace context.
//! The router, in turn, roots every inbound HTTP request in a SERVER span
//! that adopts the caller's `traceparent` as its parent
//! (`common::self_monitoring::http_trace_context_middleware`). Chained
//! together: `POST /mcp` (root) -> `tools/call get_trace` (child) ->
//! `GET /tempo/api/traces/{trace_id}` (child of the tool span) all share one
//! trace id.
//!
//! This drives a real `router::create_router` and a real `mcp_http_router`
//! in-process, connected over a real TCP socket (so the SDK's HTTP client
//! actually round-trips), with an in-memory OTel exporter capturing both
//! sides' spans. No querier is registered, so the router's trace lookup
//! answers `503` — irrelevant here: the assertion is about the server span
//! the router's own middleware opens for the route, not the query outcome
//! (mirrored by `router::endpoints::tempo::tests::error_bodies::
//! trace_lookup_without_a_querier_explains_itself`, which asserts the same
//! setup yields `503` without a querier).
//!
//! Lives in its own integration-test binary: it installs process-global
//! OTel/tracing state (subscriber + propagator) that must not leak into
//! other tests, matching `http_response_headers.rs` / `ops_endpoints_tracing.rs`.

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::catalog::Catalog;
use common::config::{ApiKeyConfig, AuthConfig, Configuration, TenantConfig};
use futures::StreamExt;
use mcp_server::{McpAppState, mcp_http_router};
use opentelemetry::trace::{SpanKind, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use router::RouterAppState;
use tokio::net::TcpListener;
use tracing_subscriber::layer::SubscriberExt;

const TENANT: &str = "acme";
const API_KEY: &str = "sk-acme-trace-continuity";
const TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";

fn install_global_otel() -> (InMemorySpanExporter, SdkTracerProvider) {
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
    (exporter, provider)
}

fn attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().to_string())
}

/// Spin up a real router (no querier registered — trace lookups 503, which is
/// fine, see module docs) and return its base URL.
async fn spawn_real_router() -> String {
    let catalog = Catalog::new("sqlite::memory:").await.expect("catalog");
    let config = Configuration {
        auth: AuthConfig {
            tenants: vec![TenantConfig {
                id: TENANT.to_string(),
                slug: TENANT.to_string(),
                name: "Acme".to_string(),
                default_dataset: Some("default".to_string()),
                datasets: vec![],
                api_keys: vec![ApiKeyConfig {
                    key: API_KEY.to_string(),
                    name: Some("test".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        },
        ..Default::default()
    };
    let state = RouterAppState::new(catalog, config);
    let app = router::create_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind router");
    let addr = listener.local_addr().expect("router address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("router serves");
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    format!("http://{addr}")
}

fn mcp_request(session_id: Option<&str>, body: serde_json::Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("authorization", format!("Bearer {API_KEY}"))
        .header("x-tenant-id", TENANT)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(session_id) = session_id {
        builder = builder.header("mcp-session-id", session_id);
    }
    builder
        .body(Body::from(body.to_string()))
        .expect("build MCP request")
}

/// Read a Streamable HTTP response (JSON or SSE) until the JSON-RPC message
/// with `id` arrives.
async fn read_jsonrpc_response(response: axum::response::Response, id: u64) -> serde_json::Value {
    let mut stream = response.into_body().into_data_stream();
    let mut buffered = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let chunk = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("response arrives before the deadline");
        let Some(chunk) = chunk else {
            panic!("response stream ended without a reply for id {id}: {buffered}");
        };
        let chunk = chunk.expect("read response chunk");
        buffered.push_str(&String::from_utf8_lossy(&chunk));
        for line in buffered.lines() {
            let candidate = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(candidate)
                && value.get("id").and_then(|v| v.as_u64()) == Some(id)
            {
                return value;
            }
        }
    }
}

/// Open an MCP session and call `get_trace`, returning the JSON-RPC reply.
async fn call_get_trace(app: axum::Router, trace_id: &str) -> serde_json::Value {
    use tower::ServiceExt;

    let init = mcp_request(
        None,
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                       "clientInfo": {"name": "trace-continuity-test", "version": "0"}}
        }),
    );
    let response = app
        .clone()
        .oneshot(init)
        .await
        .expect("initialize responds");
    assert_eq!(response.status(), StatusCode::OK, "initialize");
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .expect("initialize assigns a session id")
        .to_string();
    let _ = read_jsonrpc_response(response, 1).await;

    let initialized = mcp_request(
        Some(&session_id),
        serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    );
    let response = app
        .clone()
        .oneshot(initialized)
        .await
        .expect("initialized responds");
    assert_eq!(response.status(), StatusCode::ACCEPTED, "initialized");

    let call = mcp_request(
        Some(&session_id),
        serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "get_trace",
                "arguments": {"trace_id": trace_id, "tenant": TENANT, "dataset": "default"}
            }
        }),
    );
    let response = app.oneshot(call).await.expect("tools/call responds");
    assert_eq!(response.status(), StatusCode::OK, "tools/call HTTP status");
    read_jsonrpc_response(response, 2).await
}

/// The scenario from `specs/self-monitoring-traces/spec.md`: "A tool call is
/// one span with its router calls beneath it" — an MCP `get_trace` call opens
/// one `tools/call get_trace` INTERNAL span, the outbound request to the
/// router carries that span's W3C trace context, and the router's
/// `GET /tempo/api/traces/{trace_id}` SERVER span is its descendant (same
/// trace id, parented directly to the tool span).
#[tokio::test(flavor = "multi_thread")]
async fn mcp_tool_span_parents_the_router_server_span() {
    let (exporter, provider) = install_global_otel();

    let router_url = spawn_real_router().await;
    let mcp_state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    let mcp_app = mcp_http_router(mcp_state, &[]);

    let reply = call_get_trace(mcp_app, TRACE_ID).await;
    // No querier is registered, so the router answers 503 — the tool call
    // itself is expected to surface that as an error; what this test cares
    // about is the span hierarchy the call produced along the way.
    assert!(
        reply.get("result").is_some() || reply.get("error").is_some(),
        "tools/call must produce a JSON-RPC reply: {reply}"
    );

    provider.force_flush().unwrap();
    let mut spans = exporter.get_finished_spans().unwrap();
    for _ in 0..50 {
        if spans
            .iter()
            .any(|s| s.name == "GET /tempo/api/traces/{trace_id}")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        provider.force_flush().unwrap();
        spans = exporter.get_finished_spans().unwrap();
    }

    let tool_span = spans
        .iter()
        .find(|s| s.name == "tools/call get_trace")
        .unwrap_or_else(|| {
            panic!(
                "no `tools/call get_trace` span; got {:?}",
                spans.iter().map(|s| &s.name).collect::<Vec<_>>()
            )
        });
    assert_eq!(tool_span.span_kind, SpanKind::Internal);
    assert_eq!(
        attr(tool_span, "gen_ai.tool.name").as_deref(),
        Some("get_trace")
    );
    assert_eq!(
        attr(tool_span, "mcp.method.name").as_deref(),
        Some("tools/call")
    );
    assert_eq!(
        attr(tool_span, "signaldb.tenant.id").as_deref(),
        Some(TENANT)
    );

    let router_span = spans
        .iter()
        .find(|s| s.name == "GET /tempo/api/traces/{trace_id}")
        .unwrap_or_else(|| {
            panic!(
                "no router `GET /tempo/api/traces/{{trace_id}}` server span; got {:?}",
                spans.iter().map(|s| &s.name).collect::<Vec<_>>()
            )
        });
    assert_eq!(router_span.span_kind, SpanKind::Server);

    assert_eq!(
        router_span.span_context.trace_id(),
        tool_span.span_context.trace_id(),
        "the router's trace-lookup server span must join the MCP tool span's trace, \
         not start a detached one"
    );
    assert_eq!(
        router_span.parent_span_id,
        tool_span.span_context.span_id(),
        "the router's trace-lookup server span must be a direct child of the tool span, \
         proving the SDK propagated the tool span's W3C trace context on the outbound call"
    );
}
