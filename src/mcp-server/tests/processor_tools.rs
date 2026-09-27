//! The tenant OTTL processor tools as an MCP client sees them (openspec
//! change `tenant-ottl-processors`, task 6.1): the seven tools are listed by
//! the router, the tenant-scope check rejects a mismatched `tenant` before
//! any router request is made, and a `processors:read`-only credential can
//! call the read tools but is denied on the write tools.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use std::time::Duration;

mod common;

use common::{McpSession, connect, spawn_router, tool_error_message, tool_is_error};
use mcp_server::{McpAppState, mcp_http_router};

/// (tool, required parameters).
const PROCESSOR_TOOLS: &[(&str, &[&str])] = &[
    ("list_processors", &["tenant"]),
    ("get_processor", &["tenant", "name"]),
    ("validate_processor", &["tenant", "signal", "statements"]),
    ("test_processor", &["tenant", "signal", "payload"]),
    (
        "create_processor",
        &["tenant", "name", "signal", "statements"],
    ),
    (
        "replace_processor",
        &["tenant", "name", "signal", "statements"],
    ),
    ("delete_processor", &["tenant", "name"]),
];

#[tokio::test]
async fn processor_tools_are_listed_with_their_parameters() {
    let client = connect().await;
    let tools = client
        .list_tools(None)
        .await
        .expect("tools/list succeeds")
        .tools;

    for (name, required) in PROCESSOR_TOOLS {
        let tool = tools
            .iter()
            .find(|t| t.name == *name)
            .unwrap_or_else(|| panic!("tool `{name}` is listed"));
        let schema = serde_json::Value::Object((*tool.input_schema).clone());
        let listed_required: Vec<&str> = schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|r| r.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        for param in *required {
            assert!(
                listed_required.contains(param),
                "`{name}` must require `{param}`; schema: {schema}"
            );
        }
    }

    client.cancel().await.ok();
}

// ---------------------------------------------------------------------------
// Full HTTP harness — tenant-scope check and read/write scope enforcement
// need the `Extension<Parts>` only the Streamable HTTP transport populates.
// ---------------------------------------------------------------------------

async fn whoami() -> Response {
    axum::Json(serde_json::json!({
        "user_id": "user-a",
        "tenant": {"id": "acme", "slug": "acme", "name": "Acme"},
        "dataset": "production",
        "granted_tenants": [{"tenant_id": "acme"}],
    }))
    .into_response()
}

/// The processors API stand-in: a bearer of `sk-acme-write` may reach any
/// `/api/v1/processors*` route; `sk-acme-read` (and the default `sk-acme`)
/// may only reach the read-only ones (`GET`s and `:validate`/`:test`),
/// mirroring `require_read`/`require_write` in
/// `router::endpoints::processors`.
async fn behaviour(
    headers: HeaderMap,
    uri: axum::http::Uri,
    method: axum::http::Method,
) -> Response {
    let path = uri.path();
    if path.starts_with("/api/v1/processors") {
        let bearer = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let is_write_route = matches!(method, axum::http::Method::POST | axum::http::Method::PUT)
            && !path.ends_with(":validate")
            && !path.ends_with(":test")
            || method == axum::http::Method::DELETE;
        if is_write_route && bearer != "Bearer sk-acme-write" {
            return (
                StatusCode::FORBIDDEN,
                axum::Json(serde_json::json!({
                    "error": "processors:write scope (and tenant admin role for sessions) required",
                    "errors": []
                })),
            )
                .into_response();
        }
        if path == "/api/v1/processors" && method == axum::http::Method::GET {
            return axum::Json(serde_json::json!({"processors": []})).into_response();
        }
        if path == "/api/v1/processors" && method == axum::http::Method::POST {
            return (
                StatusCode::CREATED,
                axum::Json(serde_json::json!({
                    "name": "redact-pii", "signal": "logs", "enabled": true,
                    "priority": 100, "error_mode": "ignore", "statements": ["set(1,1)"],
                    "tenant_id": "acme", "status": "ok",
                    "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
                    "applies_within_seconds": 300
                })),
            )
                .into_response();
        }
        if path == "/api/v1/processors:validate" {
            return axum::Json(serde_json::json!({"errors": []})).into_response();
        }
        if path == "/api/v1/processors:test" {
            return axum::Json(serde_json::json!({"payload": {}, "statements": []}))
                .into_response();
        }
        if method == axum::http::Method::GET {
            return axum::Json(serde_json::json!({
                "name": "redact-pii", "signal": "logs", "enabled": true,
                "priority": 100, "error_mode": "ignore", "statements": ["set(1,1)"],
                "tenant_id": "acme", "status": "ok",
                "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"
            }))
            .into_response();
        }
        if method == axum::http::Method::PUT {
            return axum::Json(serde_json::json!({
                "name": "redact-pii", "signal": "logs", "enabled": true,
                "priority": 100, "error_mode": "ignore", "statements": ["set(1,1)"],
                "tenant_id": "acme", "status": "ok",
                "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
                "applies_within_seconds": 300
            }))
            .into_response();
        }
        if method == axum::http::Method::DELETE {
            return StatusCode::NO_CONTENT.into_response();
        }
    }
    axum::Json(serde_json::json!({})).into_response()
}

async fn spawn_mock_router() -> String {
    let app = axum::Router::new()
        .route("/api/v1/whoami", axum::routing::get(whoami))
        .fallback(behaviour);
    spawn_router(app).await
}

async fn app() -> axum::Router {
    let router_url = spawn_mock_router().await;
    let state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    mcp_http_router(state, &[])
}

#[tokio::test]
async fn mismatched_tenant_is_rejected_before_any_router_request() {
    let mut session = McpSession::open(app().await, "sk-acme-write").await;

    // The session's `X-Tenant-ID` (set by `mcp_request_with_key`) is
    // `acme`; passing a different `tenant` argument must fail client-side.
    let reply = session
        .call_tool("list_processors", serde_json::json!({"tenant": "other"}))
        .await;
    assert!(
        tool_is_error(&reply),
        "mismatched tenant must be rejected: {reply}"
    );
    assert!(
        tool_error_message(&reply).contains("does not match the authenticated tenant"),
        "error must name the tenant mismatch: {reply}"
    );

    let _ = session;
}

#[tokio::test]
async fn read_scope_credential_can_call_read_tools() {
    let mut session = McpSession::open(app().await, "sk-acme-read").await;

    let reply = session
        .call_tool("list_processors", serde_json::json!({"tenant": "acme"}))
        .await;
    assert!(
        !tool_is_error(&reply),
        "processors:read credential must be able to list processors: {reply}"
    );

    let reply = session
        .call_tool(
            "validate_processor",
            serde_json::json!({"tenant": "acme", "signal": "logs", "statements": ["set(1,1)"]}),
        )
        .await;
    assert!(
        !tool_is_error(&reply),
        "processors:read credential must be able to validate: {reply}"
    );

    let reply = session
        .call_tool(
            "test_processor",
            serde_json::json!({"tenant": "acme", "signal": "logs", "payload": {}}),
        )
        .await;
    assert!(
        !tool_is_error(&reply),
        "processors:read credential must be able to test: {reply}"
    );

    let reply = session
        .call_tool(
            "get_processor",
            serde_json::json!({"tenant": "acme", "name": "redact-pii"}),
        )
        .await;
    assert!(
        !tool_is_error(&reply),
        "processors:read credential must be able to get a processor: {reply}"
    );
}

#[tokio::test]
async fn read_scope_credential_is_denied_on_write_tools() {
    let mut session = McpSession::open(app().await, "sk-acme-read").await;

    let reply = session
        .call_tool(
            "create_processor",
            serde_json::json!({
                "tenant": "acme", "name": "redact-pii", "signal": "logs",
                "statements": ["set(1,1)"]
            }),
        )
        .await;
    assert!(
        tool_is_error(&reply),
        "processors:read-only credential must be denied create_processor: {reply}"
    );

    let reply = session
        .call_tool(
            "replace_processor",
            serde_json::json!({
                "tenant": "acme", "name": "redact-pii", "signal": "logs",
                "statements": ["set(1,1)"]
            }),
        )
        .await;
    assert!(
        tool_is_error(&reply),
        "processors:read-only credential must be denied replace_processor: {reply}"
    );

    let reply = session
        .call_tool(
            "delete_processor",
            serde_json::json!({"tenant": "acme", "name": "redact-pii"}),
        )
        .await;
    assert!(
        tool_is_error(&reply),
        "processors:read-only credential must be denied delete_processor: {reply}"
    );
}

#[tokio::test]
async fn write_scope_credential_can_call_write_tools() {
    let mut session = McpSession::open(app().await, "sk-acme-write").await;

    let reply = session
        .call_tool(
            "create_processor",
            serde_json::json!({
                "tenant": "acme", "name": "redact-pii", "signal": "logs",
                "statements": ["set(1,1)"]
            }),
        )
        .await;
    assert!(
        !tool_is_error(&reply),
        "processors:write credential must be able to create: {reply}"
    );

    let reply = session
        .call_tool(
            "replace_processor",
            serde_json::json!({
                "tenant": "acme", "name": "redact-pii", "signal": "logs",
                "statements": ["set(1,1)"]
            }),
        )
        .await;
    assert!(
        !tool_is_error(&reply),
        "processors:write credential must be able to replace: {reply}"
    );

    let reply = session
        .call_tool(
            "delete_processor",
            serde_json::json!({"tenant": "acme", "name": "redact-pii"}),
        )
        .await;
    assert!(
        !tool_is_error(&reply),
        "processors:write credential must be able to delete: {reply}"
    );
}
