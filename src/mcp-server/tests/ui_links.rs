//! `_links.ui` deep links on `search_traces`/`get_trace`/`search_logs`
//! results, driven end-to-end through a real `mcp_http_router` session (like
//! `dataset_restriction_visibility.rs`) so the tools see the same
//! `Extension<Parts>` production populates. `ui_base_url` is unset by
//! default, so a mismatched-config test needs its own `McpAppState` alongside
//! the configured one used everywhere else.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use std::time::Duration;

use mcp_server::{McpAppState, mcp_http_router};

async fn whoami() -> Response {
    axum::Json(serde_json::json!({
        "user_id": "user-a",
        "tenant": {"id": "acme", "slug": "acme", "name": "Acme"},
        "dataset": "production",
        "granted_tenants": [{"tenant_id": "acme"}],
    }))
    .into_response()
}

/// A minimal Tempo/Loki-shaped body per path, just enough for the SDK's
/// response types to deserialize.
async fn behaviour(uri: axum::http::Uri) -> Response {
    let body = if uri.path().starts_with("/tempo/api/traces/") {
        serde_json::json!({
            "durationMs": 10,
            "rootServiceName": "api",
            "rootTraceName": "GET /",
            "spanSets": [],
            "startTimeUnixNano": "0",
            "traceID": "abc123",
        })
    } else if uri.path().starts_with("/tempo/api/search") {
        serde_json::json!({"metrics": {}, "traces": []})
    } else {
        serde_json::json!({"status": "success", "data": {"resultType": "streams", "result": []}})
    };
    axum::Json(body).into_response()
}

async fn spawn_mock_router() -> String {
    let app = axum::Router::new()
        .route("/api/v1/whoami", axum::routing::get(whoami))
        .fallback(behaviour);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock router");
    let addr = listener.local_addr().expect("mock router address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock router serves");
    });
    format!("http://{addr}")
}

fn mcp_request(session_id: Option<&str>, body: serde_json::Value) -> Request<Body> {
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
    builder
        .body(Body::from(body.to_string()))
        .expect("build MCP request")
}

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

struct McpSession {
    app: axum::Router,
    session_id: String,
    next_id: u64,
}

impl McpSession {
    async fn open(app: axum::Router) -> Self {
        use tower::ServiceExt;
        let init = mcp_request(
            None,
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                           "clientInfo": {"name": "ui-links-test", "version": "0"}}
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

        Self {
            app,
            session_id,
            next_id: 2,
        }
    }

    async fn call_tool(&mut self, tool: &str, arguments: serde_json::Value) -> serde_json::Value {
        use tower::ServiceExt;
        let id = self.next_id;
        self.next_id += 1;
        let request = mcp_request(
            Some(&self.session_id),
            serde_json::json!({
                "jsonrpc": "2.0", "id": id, "method": "tools/call",
                "params": {"name": tool, "arguments": arguments}
            }),
        );
        let response = self
            .app
            .clone()
            .oneshot(request)
            .await
            .expect("tools/call responds");
        assert_eq!(response.status(), StatusCode::OK, "tools/call HTTP status");
        read_jsonrpc_response(response, id).await
    }
}

/// Parse a `tools/call` reply's text content block as JSON.
fn tool_result_json(reply: &serde_json::Value) -> serde_json::Value {
    let text = reply["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool result has a text block: {reply}"));
    serde_json::from_str(text).expect("tool result text is JSON")
}

async fn app_with_ui_base_url(ui_base_url: Option<&str>) -> axum::Router {
    let router_url = spawn_mock_router().await;
    let mut state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    if let Some(ui_base_url) = ui_base_url {
        state = state.with_ui_base_url(Some(ui_base_url.to_string()));
    }
    mcp_http_router(state, &[])
}

const UI_BASE_URL: &str = "https://ui.example.com";

#[tokio::test]
async fn search_traces_carries_a_ui_link_when_ui_base_url_is_configured() {
    let mut session = McpSession::open(app_with_ui_base_url(Some(UI_BASE_URL)).await).await;

    let reply = session
        .call_tool(
            "search_traces",
            serde_json::json!({
                "query": "{ status = error }",
                "tenant": "acme",
                "dataset": "production",
            }),
        )
        .await;
    let result = tool_result_json(&reply);
    assert_eq!(
        result["_links"]["ui"],
        "https://ui.example.com/traces?tenant=acme&dataset=production&q=%7B+status+%3D+error+%7D"
    );
}

#[tokio::test]
async fn get_trace_carries_a_ui_link_when_ui_base_url_is_configured() {
    let mut session = McpSession::open(app_with_ui_base_url(Some(UI_BASE_URL)).await).await;

    let reply = session
        .call_tool(
            "get_trace",
            serde_json::json!({
                "trace_id": "abc123",
                "tenant": "acme",
                "dataset": "production",
            }),
        )
        .await;
    let result = tool_result_json(&reply);
    assert_eq!(
        result["_links"]["ui"],
        "https://ui.example.com/traces/abc123?tenant=acme&dataset=production"
    );
}

#[tokio::test]
async fn search_logs_carries_a_ui_link_when_ui_base_url_is_configured() {
    let mut session = McpSession::open(app_with_ui_base_url(Some(UI_BASE_URL)).await).await;

    let reply = session
        .call_tool(
            "search_logs",
            serde_json::json!({
                "query": "{service_name=\"api\"}",
                "tenant": "acme",
                "dataset": "production",
            }),
        )
        .await;
    let result = tool_result_json(&reply);
    assert_eq!(
        result["_links"]["ui"],
        "https://ui.example.com/logs?tenant=acme&dataset=production&q=%7Bservice_name%3D%22api%22%7D"
    );
}

#[tokio::test]
async fn no_links_key_when_ui_base_url_is_unset() {
    let mut session = McpSession::open(app_with_ui_base_url(None).await).await;

    let reply = session
        .call_tool(
            "search_traces",
            serde_json::json!({
                "tenant": "acme",
                "dataset": "production",
            }),
        )
        .await;
    let result = tool_result_json(&reply);
    assert!(
        result.get("_links").is_none(),
        "no ui_base_url configured, so no _links key: {result}"
    );
}
