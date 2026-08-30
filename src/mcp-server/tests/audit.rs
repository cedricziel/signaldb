//! Tool-call audit, spans, metrics, and the per-session concurrency bound
//! (issue #629), asserted end-to-end: a real `mcp_http_router` in front of a
//! mock SignalDB router, driven over Streamable HTTP so the wrapper sees the
//! same `Parts` (session id, tenant, dataset) production does.
//!
//! The mock router picks its behaviour from the `X-Dataset-ID` it receives,
//! which the tools forward from their `dataset` argument: `deny` → 403,
//! `boom` → 500, `throttle` → 429, `throttle-once` → 429 (`Retry-After: 0`)
//! for the first request then 200, `throttle-long` → 429 with
//! `Retry-After: 30` (beyond the SDK's per-attempt cap, so it fails fast),
//! `big` → an oversized payload, `slow` → hold the request until released;
//! anything else → an empty 200.
//!
//! Every test runs on a current-thread runtime with a thread-local tracing
//! subscriber, so the events and spans the session worker emits are captured
//! without any process-global state — except the meter provider, which
//! `app_metrics()` binds once per process and which is therefore installed
//! once for the whole binary.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header::HeaderMap};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::StreamExt;
use mcp_server::{McpAppState, mcp_http_router};
use opentelemetry::trace::{SpanKind, Status, TracerProvider as _};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tokio::sync::watch;
use tracing::Level;
use tracing_subscriber::prelude::*;

// ---------------------------------------------------------------------------
// Capture harness
// ---------------------------------------------------------------------------

/// One captured tracing event: level, target, message, and its fields.
#[derive(Clone, Debug)]
struct CapturedEvent {
    level: Level,
    target: String,
    fields: BTreeMap<String, String>,
}

impl CapturedEvent {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

#[derive(Default)]
struct FieldVisitor(BTreeMap<String, String>);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

/// A `tracing` layer that records every event it sees.
#[derive(Clone, Default)]
struct EventCapture(Arc<Mutex<Vec<CapturedEvent>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.0.lock().unwrap().push(CapturedEvent {
            level: *event.metadata().level(),
            target: event.metadata().target().to_string(),
            fields: visitor.0,
        });
    }
}

impl EventCapture {
    fn events(&self) -> Vec<CapturedEvent> {
        self.0.lock().unwrap().clone()
    }

    fn audit_events(&self) -> Vec<CapturedEvent> {
        self.events()
            .into_iter()
            .filter(|e| e.target == "signaldb_mcp::audit")
            .collect()
    }
}

/// Thread-local subscriber for one test: event capture plus an OTel span
/// bridge into an in-memory exporter. Returns the guard (drop = uninstall),
/// the events, and the span exporter.
fn install_capture() -> (
    tracing::subscriber::DefaultGuard,
    EventCapture,
    InMemorySpanExporter,
    SdkTracerProvider,
) {
    // The W3C propagator is what production installs in `init_telemetry`;
    // parent adoption from `traceparent` is a no-op without it.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let events = EventCapture::default();
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber = tracing_subscriber::registry()
        .with(events.clone())
        .with(tracing_opentelemetry::layer().with_tracer(tracer));
    let guard = tracing::subscriber::set_default(subscriber);
    (guard, events, exporter, provider)
}

/// The process-wide in-memory metric exporter `app_metrics()` binds to.
fn metrics_exporter() -> &'static (InMemoryMetricExporter, SdkMeterProvider) {
    static EXPORTER: OnceLock<(InMemoryMetricExporter, SdkMeterProvider)> = OnceLock::new();
    EXPORTER.get_or_init(|| {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());
        (exporter, provider)
    })
}

fn attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().to_string())
}

// ---------------------------------------------------------------------------
// Mock SignalDB router
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockRouter {
    /// Flipped to `true` by the test to let `slow` requests complete — a
    /// latched state, so a request that arrives after the release passes too.
    release: watch::Sender<bool>,
    /// Requests seen for `throttle-once`; only the first is throttled.
    throttle_once_hits: Arc<std::sync::atomic::AtomicUsize>,
}

async fn whoami() -> Response {
    axum::Json(serde_json::json!({
        "user_id": "user-a",
        "tenant": {"id": "acme", "slug": "acme", "name": "Acme"},
        "dataset": "production",
    }))
    .into_response()
}

async fn behaviour(
    axum::extract::State(mock): axum::extract::State<MockRouter>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    let dataset = headers
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("production");
    match dataset {
        "deny" => (StatusCode::FORBIDDEN, "forbidden").into_response(),
        "boom" => (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response(),
        "throttle" => (StatusCode::TOO_MANY_REQUESTS, "slow down").into_response(),
        "throttle-once" => {
            let hit = mock
                .throttle_once_hits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if hit == 0 {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "0")],
                    "slow down",
                )
                    .into_response()
            } else {
                ok_body(uri.path(), false)
            }
        }
        "throttle-long" => (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "30")],
            "slow down",
        )
            .into_response(),
        "slow" => {
            let mut released = mock.release.subscribe();
            while !*released.borrow_and_update() {
                if released.changed().await.is_err() {
                    break;
                }
            }
            ok_body(uri.path(), false)
        }
        "big" => ok_body(uri.path(), true),
        _ => ok_body(uri.path(), false),
    }
}

fn ok_body(path: &str, big: bool) -> Response {
    let body = if path.starts_with("/tempo/api/search") {
        serde_json::json!({"metrics": {}, "traces": []})
    } else if big {
        // Well past the 256 KiB tool-result cap.
        serde_json::json!({"status": "success", "data": {"resultType": "streams", "result": [{"stream": {}, "values": [["1", "x".repeat(300 * 1024)]]}]}})
    } else {
        serde_json::json!({"status": "success", "data": {"resultType": "streams", "result": []}})
    };
    axum::Json(body).into_response()
}

async fn spawn_mock_router() -> (String, MockRouter) {
    let mock = MockRouter {
        release: watch::Sender::new(false),
        throttle_once_hits: Arc::default(),
    };
    let app = axum::Router::new()
        .route("/api/v1/whoami", get(whoami))
        .fallback(behaviour)
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock router");
    let addr = listener.local_addr().expect("mock router address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock router serves");
    });
    (format!("http://{addr}"), mock)
}

// ---------------------------------------------------------------------------
// Streamable HTTP client, hand-rolled over `oneshot`
// ---------------------------------------------------------------------------

struct McpSession {
    app: axum::Router,
    session_id: String,
    next_id: u64,
}

fn mcp_request(
    session_id: Option<&str>,
    traceparent: Option<&str>,
    body: serde_json::Value,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("authorization", "Bearer sk-acme")
        .header("x-tenant-id", "acme")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(session_id) = session_id {
        builder = builder.header("mcp-session-id", session_id);
    }
    if let Some(traceparent) = traceparent {
        builder = builder.header("traceparent", traceparent);
    }
    builder
        .body(Body::from(body.to_string()))
        .expect("build MCP request")
}

/// Read a Streamable HTTP response (JSON or SSE) until the JSON-RPC message
/// with `id` arrives, then stop — the SSE stream may outlive the response.
async fn read_jsonrpc_response(response: Response, id: u64) -> serde_json::Value {
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

impl McpSession {
    /// Initialize a session (with an optional `traceparent` on the initialize
    /// request) and send `notifications/initialized`.
    async fn open(app: axum::Router, traceparent: Option<&str>) -> Self {
        use tower::ServiceExt;
        let init = mcp_request(
            None,
            traceparent,
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                           "clientInfo": {"name": "audit-test", "version": "0"}}
            }),
        );
        let response = app
            .clone()
            .oneshot(init)
            .await
            .expect("initialize responds");
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            panic!("initialize: {status} {}", String::from_utf8_lossy(&body));
        }
        let session_id = response
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .expect("initialize assigns a session id")
            .to_string();
        let _ = read_jsonrpc_response(response, 1).await;

        let initialized = mcp_request(
            Some(&session_id),
            None,
            serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        );
        let response = app
            .clone()
            .oneshot(initialized)
            .await
            .expect("initialized responds");
        assert_eq!(response.status(), StatusCode::ACCEPTED, "initialized");

        Self {
            app,
            session_id,
            next_id: 2,
        }
    }

    /// Call a tool and return the JSON-RPC reply (`result` or `error`).
    async fn call_tool(&mut self, tool: &str, arguments: serde_json::Value) -> serde_json::Value {
        let request = self.tool_request(tool, arguments);
        Self::send(self.app.clone(), request).await
    }

    /// Build a `tools/call` request without sending it (for concurrent sends).
    /// Every test in this file authenticates as the mock's one tenant
    /// (`acme`) and, absent a behaviour-selecting dataset, gets the mock's
    /// default (`production`) response — so `tenant`/`dataset` are filled in
    /// here when the caller's `arguments` doesn't already set them, rather
    /// than repeating `"tenant": "acme"` at every call site.
    fn tool_request(
        &mut self,
        tool: &str,
        mut arguments: serde_json::Value,
    ) -> (u64, Request<Body>) {
        if let Some(args) = arguments.as_object_mut() {
            args.entry("tenant")
                .or_insert_with(|| serde_json::json!("acme"));
            args.entry("dataset")
                .or_insert_with(|| serde_json::json!("production"));
        }
        let id = self.next_id;
        self.next_id += 1;
        let request = mcp_request(
            Some(&self.session_id),
            None,
            serde_json::json!({
                "jsonrpc": "2.0", "id": id, "method": "tools/call",
                "params": {"name": tool, "arguments": arguments}
            }),
        );
        (id, request)
    }

    async fn send(app: axum::Router, (id, request): (u64, Request<Body>)) -> serde_json::Value {
        use tower::ServiceExt;
        let response = app.oneshot(request).await.expect("tools/call responds");
        assert_eq!(response.status(), StatusCode::OK, "tools/call HTTP status");
        read_jsonrpc_response(response, id).await
    }
}

async fn app_with_limit(limit: usize) -> (axum::Router, MockRouter) {
    metrics_exporter();
    let (router_url, mock) = spawn_mock_router().await;
    let state = McpAppState::new(router_url)
        .with_router_timeout(Duration::from_secs(10))
        .with_max_concurrent_tool_calls(limit);
    (mcp_http_router(state, &[]), mock)
}

fn the_audit_event(events: &EventCapture, tool: &str) -> CapturedEvent {
    let matching: Vec<_> = events
        .audit_events()
        .into_iter()
        .filter(|e| e.field("tool") == Some(tool))
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "exactly one audit event for {tool}: {matching:?}"
    );
    matching.into_iter().next().unwrap()
}

// ---------------------------------------------------------------------------
// Audit events (2.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn successful_call_is_audited_once_at_info_with_identity_and_duration() {
    let (_guard, events, _spans, _provider) = install_capture();
    let (app, _mock) = app_with_limit(8).await;
    let mut session = McpSession::open(app, None).await;

    let reply = session
        .call_tool("search_traces", serde_json::json!({"query": "{}"}))
        .await;
    assert!(reply.get("result").is_some(), "{reply}");

    let event = the_audit_event(&events, "search_traces");
    assert_eq!(event.level, Level::INFO);
    assert_eq!(event.field("tenant_id"), Some("acme"));
    assert_eq!(event.field("session_id"), Some(session.session_id.as_str()));
    assert_eq!(event.field("outcome"), Some("ok"));
    assert!(event.field("duration_ms").is_some(), "{event:?}");
    assert_eq!(event.field("error.type"), None);
    // `dataset` is now a required tool argument (defaulted by `tool_request`
    // to `production`), so it is always recorded.
    assert_eq!(event.field("dataset"), Some("production"));
}

#[tokio::test]
async fn denied_call_is_warn_and_distinguishable_from_a_failed_one() {
    let (_guard, events, _spans, _provider) = install_capture();
    let (app, _mock) = app_with_limit(8).await;
    let mut session = McpSession::open(app, None).await;

    let denied = session
        .call_tool("search_traces", serde_json::json!({"dataset": "deny"}))
        .await;
    assert!(denied.get("error").is_some(), "{denied}");
    let failed = session
        .call_tool(
            "search_logs",
            serde_json::json!({"query": "{app=\"x\"}", "dataset": "boom"}),
        )
        .await;
    assert!(failed.get("error").is_some(), "{failed}");

    let denied = the_audit_event(&events, "search_traces");
    assert_eq!(denied.level, Level::WARN);
    assert_eq!(denied.field("outcome"), Some("denied"));
    assert_eq!(denied.field("dataset"), Some("deny"));
    assert_eq!(denied.field("error.type"), None);

    let failed = the_audit_event(&events, "search_logs");
    assert_eq!(failed.level, Level::ERROR);
    assert_eq!(failed.field("outcome"), Some("error"));
    assert_eq!(failed.field("error.type"), Some("500"));
}

/// A router that never responds (`"slow"` in the mock, never released) must
/// not keep a tool call alive for the SDK retry policy's full worst case
/// (up to `max_attempts` × `router_timeout` plus `total_cap` of retry
/// sleeps — see `mcp_server::tool_call_deadline`). `call_tool` wraps the
/// whole dispatch in a `tokio::time::timeout` at a much shorter deadline
/// here, so this observes that outer bound firing rather than the SDK's own
/// per-attempt timeout or retry budget.
#[tokio::test]
async fn tool_call_past_the_deadline_is_reported_distinctly_and_audited() {
    let (_guard, events, _spans, _provider) = install_capture();
    metrics_exporter();
    let (router_url, _mock) = spawn_mock_router().await;
    let state = McpAppState::new(router_url)
        .with_router_timeout(Duration::from_secs(10))
        .with_tool_call_deadline(Duration::from_millis(200));
    let app = mcp_http_router(state, &[]);
    let mut session = McpSession::open(app, None).await;

    let reply = session
        .call_tool("search_traces", serde_json::json!({"dataset": "slow"}))
        .await;
    let message = reply["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("deadline"),
        "expected a deadline error, got: {reply}"
    );

    let audit = the_audit_event(&events, "search_traces");
    assert_eq!(audit.level, Level::ERROR);
    assert_eq!(audit.field("outcome"), Some("error"));
    assert_eq!(audit.field("error.type"), Some("deadline"));
}

#[tokio::test]
async fn throttled_and_truncated_calls_are_classified() {
    let (_guard, events, _spans, _provider) = install_capture();
    let (app, _mock) = app_with_limit(8).await;
    let mut session = McpSession::open(app, None).await;

    session
        .call_tool("search_traces", serde_json::json!({"dataset": "throttle"}))
        .await;
    let big = session
        .call_tool(
            "search_logs",
            serde_json::json!({"query": "{app=\"x\"}", "dataset": "big"}),
        )
        .await;
    let text = big["result"]["content"][0]["text"]
        .as_str()
        .expect("truncated notice is a text block");
    assert!(text.contains("\"truncated\":true"), "{text}");

    assert_eq!(
        the_audit_event(&events, "search_traces").field("outcome"),
        Some("throttled")
    );
    let truncated = the_audit_event(&events, "search_logs");
    assert_eq!(truncated.level, Level::INFO);
    assert_eq!(truncated.field("outcome"), Some("truncated"));
}

/// `sdk-retry-on-throttle`: a brief 429 is absorbed by the SDK's retry policy
/// (the agent sees the result); an exhausted/unaffordable one is a distinct
/// `throttled:` error carrying `retryAfterMs`, still classified `throttled`.
#[tokio::test]
async fn brief_throttle_is_invisible_and_exhausted_throttle_names_the_wait() {
    let (_guard, events, _spans, _provider) = install_capture();
    let (app, mock) = app_with_limit(8).await;
    let mut session = McpSession::open(app, None).await;

    let ok = session
        .call_tool(
            "search_traces",
            serde_json::json!({"dataset": "throttle-once"}),
        )
        .await;
    assert!(ok.get("error").is_none(), "429 then 200 is a success: {ok}");
    assert_eq!(
        mock.throttle_once_hits
            .load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the SDK retried once"
    );
    assert_eq!(
        the_audit_event(&events, "search_traces").field("outcome"),
        Some("ok")
    );

    let throttled = session
        .call_tool(
            "search_logs",
            serde_json::json!({"query": "{app=\"x\"}", "dataset": "throttle-long"}),
        )
        .await;
    let error = &throttled["error"];
    let message = error["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("throttled:"),
        "distinct throttled prefix: {message}"
    );
    assert!(
        message.contains("retry in 30s"),
        "names the wait: {message}"
    );
    assert_eq!(error["data"]["retryAfterMs"], 30_000);
    assert_eq!(error["data"]["http_status"], 429);
    assert_eq!(
        the_audit_event(&events, "search_logs").field("outcome"),
        Some("throttled")
    );
}

#[tokio::test]
async fn query_text_never_reaches_an_info_level_event() {
    let (_guard, events, _spans, _provider) = install_capture();
    let (app, _mock) = app_with_limit(8).await;
    let mut session = McpSession::open(app, None).await;

    let logql = r#"{service_name="checkout"} |= "SECRET-NEEDLE""#;
    session
        .call_tool("search_logs", serde_json::json!({"query": logql}))
        .await;

    let audit = the_audit_event(&events, "search_logs");
    assert_eq!(audit.field("outcome"), Some("ok"));
    for event in events.events() {
        if event.level <= Level::INFO {
            let rendered = format!("{event:?}");
            assert!(
                !rendered.contains("SECRET-NEEDLE"),
                "query text leaked into an {}-level event: {rendered}",
                event.level
            );
        }
    }
}

#[tokio::test]
async fn metrics_count_calls_by_tool_and_outcome() {
    let (_guard, _events, _spans, _provider) = install_capture();
    let (app, _mock) = app_with_limit(8).await;
    let mut session = McpSession::open(app, None).await;

    // A tool no other test in this binary calls, so the counts are exact
    // even with tests running in parallel.
    session
        .call_tool("discover_metrics", serde_json::json!({}))
        .await;
    session
        .call_tool("discover_metrics", serde_json::json!({"dataset": "deny"}))
        .await;

    let (exporter, provider) = metrics_exporter();
    provider.force_flush().unwrap();
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut duration_count = 0;
    for rm in exporter.get_finished_metrics().unwrap() {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                match (metric.name(), metric.data()) {
                    ("signaldb.mcp.tool_calls", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                        for p in sum.data_points() {
                            let attrs: Vec<_> = p.attributes().cloned().collect();
                            let tool = attrs
                                .iter()
                                .find(|kv| kv.key.as_str() == "gen_ai.tool.name")
                                .map(|kv| kv.value.as_str().to_string());
                            let outcome = attrs
                                .iter()
                                .find(|kv| kv.key.as_str() == "signaldb.mcp.outcome")
                                .map(|kv| kv.value.as_str().to_string());
                            if tool.as_deref() == Some("discover_metrics") {
                                counts.insert(outcome.unwrap_or_default(), p.value());
                            }
                        }
                    }
                    (
                        "signaldb.mcp.tool_call.duration",
                        AggregatedMetrics::F64(MetricData::Histogram(h)),
                    ) => {
                        for p in h.data_points() {
                            if p.attributes().any(|kv| {
                                kv.key.as_str() == "gen_ai.tool.name"
                                    && kv.value.as_str() == "discover_metrics"
                            }) {
                                duration_count = p.count();
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    assert_eq!(counts.get("ok"), Some(&1), "{counts:?}");
    assert_eq!(counts.get("denied"), Some(&1), "{counts:?}");
    assert_eq!(duration_count, 2);
}

// ---------------------------------------------------------------------------
// Spans (2.3, 3.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tool_span_status_is_error_only_for_failed_calls() {
    let (_guard, _events, spans, provider) = install_capture();
    let (app, _mock) = app_with_limit(8).await;
    let mut session = McpSession::open(app, None).await;

    session
        .call_tool("search_traces", serde_json::json!({"dataset": "deny"}))
        .await;
    session
        .call_tool("search_traces", serde_json::json!({"dataset": "throttle"}))
        .await;
    session
        .call_tool(
            "search_logs",
            serde_json::json!({"query": "{}", "dataset": "boom"}),
        )
        .await;
    provider.force_flush().unwrap();

    let spans = spans.get_finished_spans().unwrap();
    let tool_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.name.starts_with("tools/call "))
        .collect();
    assert_eq!(
        tool_spans.len(),
        3,
        "{:?}",
        spans.iter().map(|s| &s.name).collect::<Vec<_>>()
    );
    for span in &tool_spans {
        assert_eq!(span.span_kind, SpanKind::Internal);
        assert_eq!(attr(span, "mcp.method.name").as_deref(), Some("tools/call"));
        assert_eq!(attr(span, "signaldb.tenant.id").as_deref(), Some("acme"));
        assert_eq!(
            attr(span, "mcp.session.id").as_deref(),
            Some(session.session_id.as_str())
        );
    }
    let by_dataset = |dataset: &str| {
        tool_spans
            .iter()
            .find(|s| attr(s, "signaldb.dataset.id").as_deref() == Some(dataset))
            .unwrap_or_else(|| panic!("no tool span for dataset {dataset}"))
    };
    assert_eq!(by_dataset("deny").status, Status::Unset);
    assert_eq!(attr(by_dataset("deny"), "error.type"), None);
    assert_eq!(by_dataset("throttle").status, Status::Unset);
    let failed = by_dataset("boom");
    assert!(matches!(failed.status, Status::Error { .. }));
    assert_eq!(attr(failed, "error.type").as_deref(), Some("500"));
    assert_eq!(
        attr(failed, "gen_ai.tool.name").as_deref(),
        Some("search_logs")
    );
    assert_eq!(failed.name, "tools/call search_logs");
}

#[tokio::test]
async fn post_mcp_is_a_server_span_parented_to_the_caller() {
    let (_guard, _events, spans, provider) = install_capture();
    let (app, _mock) = app_with_limit(8).await;
    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let _session = McpSession::open(app, Some(traceparent)).await;
    provider.force_flush().unwrap();

    let spans = spans.get_finished_spans().unwrap();
    let server = spans
        .iter()
        .find(|s| s.name == "POST /mcp")
        .unwrap_or_else(|| {
            panic!(
                "no `POST /mcp` server span; got {:?}",
                spans.iter().map(|s| &s.name).collect::<Vec<_>>()
            )
        });
    assert_eq!(server.span_kind, SpanKind::Server);
    assert_eq!(attr(server, "http.route").as_deref(), Some("/mcp"));
    assert_eq!(attr(server, "http.request.method").as_deref(), Some("POST"));
    assert_eq!(
        format!("{:032x}", server.span_context.trace_id()),
        "0af7651916cd43dd8448eb211c80319c"
    );
    assert_eq!(
        format!("{:016x}", server.parent_span_id),
        "b7ad6b7169203331"
    );
}

// ---------------------------------------------------------------------------
// Concurrency bound (5.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn excess_concurrent_calls_fail_fast_and_sessions_stay_isolated() {
    let (_guard, events, _spans, _provider) = install_capture();
    let (app, mock) = app_with_limit(2).await;
    let mut session_a = McpSession::open(app.clone(), None).await;
    let mut session_b = McpSession::open(app.clone(), None).await;

    // Two slow calls fill session A's bound.
    let slow_1 = session_a.tool_request("search_traces", serde_json::json!({"dataset": "slow"}));
    let slow_2 = session_a.tool_request("search_traces", serde_json::json!({"dataset": "slow"}));
    let in_flight_1 = tokio::spawn(McpSession::send(app.clone(), slow_1));
    let in_flight_2 = tokio::spawn(McpSession::send(app.clone(), slow_2));
    // Let both reach the router before the third call arrives.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The third call in session A cannot get a permit and fails distinctly.
    let started = std::time::Instant::now();
    let refused = session_a
        .call_tool(
            "discover_attributes",
            serde_json::json!({"signal": "traces"}),
        )
        .await;
    let message = refused["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("too many concurrent tool calls (limit 2)"),
        "{refused}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );
    let refused_audit = the_audit_event(&events, "discover_attributes");
    assert_eq!(refused_audit.field("outcome"), Some("error"));
    assert_eq!(refused_audit.field("error.type"), Some("concurrency_limit"));

    // Session B is unaffected while A is saturated.
    let other = session_b
        .call_tool("search_traces", serde_json::json!({}))
        .await;
    assert!(other.get("result").is_some(), "{other}");

    // Release the slow calls: both complete, and their permits are freed.
    mock.release.send_replace(true);
    let first = in_flight_1.await.unwrap();
    let second = in_flight_2.await.unwrap();
    assert!(first.get("result").is_some(), "{first}");
    assert!(second.get("result").is_some(), "{second}");
    let again = session_a
        .call_tool("search_traces", serde_json::json!({}))
        .await;
    assert!(again.get("result").is_some(), "{again}");

    // A failed call also releases its permit.
    session_a
        .call_tool("search_traces", serde_json::json!({"dataset": "boom"}))
        .await;
    session_a
        .call_tool("search_traces", serde_json::json!({"dataset": "boom"}))
        .await;
    let after_errors = session_a
        .call_tool("search_traces", serde_json::json!({}))
        .await;
    assert!(after_errors.get("result").is_some(), "{after_errors}");
}

#[tokio::test]
async fn calls_within_the_bound_all_run() {
    let (_guard, events, _spans, _provider) = install_capture();
    let (app, mock) = app_with_limit(8).await;
    let mut session = McpSession::open(app.clone(), None).await;

    let mut in_flight = Vec::new();
    for _ in 0..8 {
        let request = session.tool_request("search_traces", serde_json::json!({"dataset": "slow"}));
        in_flight.push(tokio::spawn(McpSession::send(app.clone(), request)));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    mock.release.send_replace(true);
    for handle in in_flight {
        let reply = handle.await.unwrap();
        assert!(reply.get("result").is_some(), "{reply}");
    }
    let outcomes: Vec<_> = events
        .audit_events()
        .into_iter()
        .map(|e| e.field("outcome").unwrap_or_default().to_string())
        .collect();
    assert_eq!(outcomes, vec!["ok"; 8]);
}
