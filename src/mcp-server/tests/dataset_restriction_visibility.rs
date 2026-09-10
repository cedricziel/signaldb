//! `multi-dataset-key-restriction` phase 5.2 / design D10: `discover_datasets`
//! and `tenant_list_tables` must filter out any dataset outside the calling
//! credential's dataset-set restriction, driven end to end through the real
//! `mcp_http_router` auth middleware (`whoami()` → `audit::CallerDatasetIds`
//! → the tool handlers), not just the in-crate unit tests that construct the
//! extension directly.

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use std::time::Duration;

use mcp_server::{McpAppState, mcp_http_router};

/// The one credential in this fixture restricted to a single dataset;
/// `sk-acme` (used everywhere else) is unrestricted.
const RESTRICTED_KEY: &str = "sk-acme-restricted";

async fn whoami(headers: HeaderMap) -> Response {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let mut body = serde_json::json!({
        "user_id": "",
        "tenant": {"id": "acme", "slug": "acme", "name": "Acme"},
        "dataset": "production",
    });
    if bearer == format!("Bearer {RESTRICTED_KEY}") {
        body["dataset_ids"] = serde_json::json!(["production"]);
    }
    axum::Json(body).into_response()
}

/// Both provisioned tenant datasets, unfiltered — filtering is the tool
/// handler's job, not the (mock) router's.
async fn behaviour(uri: axum::http::Uri, method: axum::http::Method) -> Response {
    if uri.path().ends_with("/tables") && method == axum::http::Method::GET {
        return axum::Json(serde_json::json!({
            "tenant_id": "acme",
            "tables": [
                {"name": "traces", "schema_type": "traces", "description": "d", "dataset": "production"},
                {"name": "logs", "schema_type": "logs", "description": "d", "dataset": "staging"}
            ],
            "datasets": [
                {"dataset": "production", "tables": [
                    {"name": "traces", "schema_type": "traces", "description": "d", "dataset": "production"}
                ]},
                {"dataset": "staging", "tables": [
                    {"name": "logs", "schema_type": "logs", "description": "d", "dataset": "staging"}
                ]}
            ]
        }))
        .into_response();
    }
    axum::Json(serde_json::json!({})).into_response()
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

fn mcp_request_with_key(
    api_key: &str,
    session_id: Option<&str>,
    body: serde_json::Value,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("authorization", format!("Bearer {api_key}"))
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
    api_key: String,
    session_id: String,
    next_id: u64,
}

impl McpSession {
    async fn open_with_key(app: axum::Router, api_key: &str) -> Self {
        use tower::ServiceExt;
        let init = mcp_request_with_key(
            api_key,
            None,
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                           "clientInfo": {"name": "dataset-restriction-visibility-test", "version": "0"}}
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

        let initialized = mcp_request_with_key(
            api_key,
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
            api_key: api_key.to_string(),
            session_id,
            next_id: 2,
        }
    }

    async fn call_tool(&mut self, tool: &str, arguments: serde_json::Value) -> serde_json::Value {
        let id = self.next_id;
        self.next_id += 1;
        let request = mcp_request_with_key(
            &self.api_key,
            Some(&self.session_id),
            serde_json::json!({
                "jsonrpc": "2.0", "id": id, "method": "tools/call",
                "params": {"name": tool, "arguments": arguments}
            }),
        );
        use tower::ServiceExt;
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

fn tool_text(reply: &serde_json::Value) -> String {
    reply["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

async fn app() -> axum::Router {
    let router_url = spawn_mock_router().await;
    let state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    mcp_http_router(state, &[])
}

#[tokio::test]
async fn discover_datasets_end_to_end_hides_the_unlisted_dataset_for_a_restricted_key() {
    let mut session = McpSession::open_with_key(app().await, RESTRICTED_KEY).await;

    let reply = session
        .call_tool("discover_datasets", serde_json::json!({}))
        .await;
    let text = tool_text(&reply);
    assert!(
        text.contains("production"),
        "the restricted dataset must still be listed: {text}"
    );
    assert!(
        !text.contains("staging"),
        "a dataset outside the restriction must not appear, even by name: {text}"
    );
}

#[tokio::test]
async fn discover_datasets_end_to_end_is_unfiltered_for_an_unrestricted_key() {
    let mut session = McpSession::open_with_key(app().await, "sk-acme").await;

    let reply = session
        .call_tool("discover_datasets", serde_json::json!({}))
        .await;
    let text = tool_text(&reply);
    assert!(text.contains("production"), "got {text}");
    assert!(
        text.contains("staging"),
        "an unrestricted credential sees every dataset, unchanged: {text}"
    );
}

#[tokio::test]
async fn tenant_list_tables_end_to_end_hides_the_unlisted_dataset_for_a_restricted_key() {
    let mut session = McpSession::open_with_key(app().await, RESTRICTED_KEY).await;

    let reply = session
        .call_tool(
            "tenant_list_tables",
            serde_json::json!({"tenant_id": "acme"}),
        )
        .await;
    let text = tool_text(&reply);
    assert!(
        text.contains("\"production\""),
        "the restricted dataset must still be listed: {text}"
    );
    assert!(
        !text.contains("staging"),
        "a dataset outside the restriction must not appear, even by name: {text}"
    );
}

#[tokio::test]
async fn tenant_list_tables_end_to_end_is_unfiltered_for_an_unrestricted_key() {
    let mut session = McpSession::open_with_key(app().await, "sk-acme").await;

    let reply = session
        .call_tool(
            "tenant_list_tables",
            serde_json::json!({"tenant_id": "acme"}),
        )
        .await;
    let text = tool_text(&reply);
    assert!(text.contains("\"production\""), "got {text}");
    assert!(
        text.contains("\"staging\""),
        "an unrestricted credential sees every dataset, unchanged: {text}"
    );
}
