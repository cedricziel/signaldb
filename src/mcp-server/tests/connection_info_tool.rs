//! `connection_info`: a read-only, no-parameter tool available to any valid
//! tenant credential (mirrors `server_info`), forwarding the bearer token to
//! `GET /api/v1/connection` and returning the router's payload unchanged.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use std::time::Duration;

mod common;

use common::{connect, mcp_request_with_key, read_jsonrpc_response, spawn_router};
use mcp_server::server::McpServer;
use mcp_server::{McpAppState, mcp_http_router};

#[test]
fn connection_info_tool_is_registered() {
    assert!(
        McpServer::has_tool("connection_info"),
        "MCP tool `connection_info` is missing"
    );
}

#[tokio::test]
async fn connection_info_is_listed_read_only_with_no_required_parameters() {
    let client = connect().await;
    let tools = client.list_tools(None).await.expect("tools/list succeeds");

    let tool = tools
        .tools
        .iter()
        .find(|t| t.name == "connection_info")
        .expect("connection_info tool listed");

    assert_eq!(
        tool.annotations.as_ref().and_then(|a| a.read_only_hint),
        Some(true),
        "connection_info must carry read_only_hint"
    );

    let schema = serde_json::to_value(&tool.input_schema).expect("schema serializes");
    let required = schema
        .get("required")
        .and_then(|v| v.as_array())
        .map(|items| items.len())
        .unwrap_or(0);
    assert_eq!(
        required, 0,
        "connection_info must take no required parameters: {schema}"
    );

    client.cancel().await.ok();
}

// ---------------------------------------------------------------------------
// Full HTTP harness — confirm the tool forwards the caller's bearer token to
// `GET /api/v1/connection` and returns the router's payload.
// ---------------------------------------------------------------------------

const CONNECTION_PAYLOAD: &str = r#"{
    "tenant_id": "acme",
    "dataset_id": "production",
    "public_endpoints_configured": false,
    "headers": {
        "authorization": "Bearer <api-key>",
        "x-tenant-id": "acme",
        "x-dataset-id": "production"
    },
    "ingest": {
        "otlp_grpc": {"url": "http://localhost:4317", "authority": "localhost:4317", "tls": false, "protocol": "grpc", "signals": ["traces", "logs", "metrics", "profiles"]},
        "otlp_http": {"url": "http://localhost:4318", "tls": false, "protocol": "http/protobuf", "paths": {"traces": "/v1/traces", "logs": "/v1/logs", "metrics": "/v1/metrics", "profiles": "/v1development/profiles"}},
        "prometheus_remote_write": "http://localhost:4318/api/v1/write"
    },
    "query": {
        "api_url": "http://localhost:3000",
        "query_ir": "/api/v1/query",
        "openapi": "/api/v1/openapi.json",
        "compat": {"tempo": "/tempo/api", "loki": "/loki/api/v1", "prometheus": "/prometheus/api/v1", "pyroscope": "/pyroscope"}
    },
    "required_scopes": {
        "ingest": ["metrics:write", "logs:write", "traces:write", "profiles:write"],
        "query": ["traces:read", "logs:read", "metrics:read", "profiles:read"]
    },
    "otel_env": {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4317",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
        "OTEL_EXPORTER_OTLP_HEADERS": "authorization=Bearer <api-key>,x-tenant-id=acme,x-dataset-id=production"
    },
    "notes": ["Public endpoints are not configured ([public] in signaldb.toml); URLs fall back to localhost defaults."]
}"#;

/// The `mcp_auth_middleware` validates every session (including
/// `initialize`) against `GET /api/v1/whoami` before a tool call is ever
/// dispatched, so the mock router must serve a realistic identity there too.
async fn whoami() -> Response {
    axum::Json(serde_json::json!({
        "user_id": "",
        "tenant": {"id": "acme", "slug": "acme", "name": "Acme"},
        "dataset": "production",
    }))
    .into_response()
}

/// Serves `GET /api/v1/connection`, asserting the caller's bearer token was
/// forwarded, then any other path with an empty success body.
async fn behaviour(
    headers: HeaderMap,
    uri: axum::http::Uri,
    method: axum::http::Method,
) -> Response {
    if uri.path() == "/api/v1/connection" && method == axum::http::Method::GET {
        let bearer = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert_eq!(
            bearer, "Bearer sk-acme",
            "connection_info must forward the caller's bearer token"
        );
        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            CONNECTION_PAYLOAD,
        )
            .into_response();
    }
    axum::Json(serde_json::json!({})).into_response()
}

async fn spawn_mock_router() -> String {
    let app = axum::Router::new()
        .route("/api/v1/whoami", axum::routing::get(whoami))
        .fallback(behaviour);
    spawn_router(app).await
}

/// `connection_info` takes no arguments, so every request in this test uses
/// the same fixed bearer credential.
fn mcp_request(
    session_id: Option<&str>,
    body: serde_json::Value,
) -> axum::http::Request<axum::body::Body> {
    mcp_request_with_key("sk-acme", session_id, body)
}

async fn app() -> axum::Router {
    let router_url = spawn_mock_router().await;
    let state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    mcp_http_router(state, &[])
}

#[tokio::test]
async fn connection_info_forwards_bearer_and_returns_router_payload() {
    use tower::ServiceExt;

    let app = app().await;
    let init = mcp_request(
        None,
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                       "clientInfo": {"name": "connection-info-test", "version": "0"}}
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
            "params": {"name": "connection_info", "arguments": {}}
        }),
    );
    let response = app
        .clone()
        .oneshot(call)
        .await
        .expect("tools/call responds");
    assert_eq!(response.status(), StatusCode::OK, "tools/call HTTP status");
    let reply = read_jsonrpc_response(response, 2).await;

    assert!(
        reply["result"]["isError"].as_bool() != Some(true),
        "connection_info must not error: {reply}"
    );
    let text = reply["result"]["content"][0]["text"]
        .as_str()
        .expect("tool result carries text content");
    let payload: serde_json::Value = serde_json::from_str(text).expect("payload is JSON");
    assert_eq!(payload["tenant_id"], "acme");
    assert_eq!(payload["dataset_id"], "production");
    assert_eq!(
        payload["ingest"]["otlp_grpc"]["url"],
        "http://localhost:4317"
    );
    assert_eq!(
        payload["required_scopes"]["ingest"],
        serde_json::json!([
            "metrics:write",
            "logs:write",
            "traces:write",
            "profiles:write"
        ])
    );
}
