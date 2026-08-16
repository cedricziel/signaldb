//! `mcp-admin-tool-parity`: platform-admin tools (admin API, wrap the
//! administrative credential) and tenant self-management tools (management
//! API, act as the caller's own identity). Covers task 3.1/3.3/3.4: tool
//! registration, destructive/read-only annotations, confirm enforcement,
//! clean access-denied on an unauthorized management call, and key material
//! appearing exactly once.

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use rmcp::{ClientHandler, ServiceExt, model::ClientInfo, service::RunningService};
use std::time::Duration;

use mcp_server::server::McpServer;
use mcp_server::{McpAppState, mcp_http_router};

// ---------------------------------------------------------------------------
// Lightweight in-process client (no HTTP layer) — for registration/schema
// checks that never dispatch a tool call.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct TestClient;

impl ClientHandler for TestClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::default()
    }
}

async fn connect() -> RunningService<rmcp::RoleClient, TestClient> {
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let server = McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(5),
        );
        if let Ok(running) = server.serve(server_transport).await {
            let _ = running.waiting().await;
        }
    });
    TestClient
        .serve(client_transport)
        .await
        .expect("client connects to the in-memory server")
}

const PLATFORM_ADMIN_TOOLS: &[&str] = &[
    "list_tenants",
    "get_tenant",
    "create_tenant",
    "update_tenant",
    "delete_tenant",
    "create_user",
    "list_datasets",
    "create_dataset",
    "delete_dataset",
    "revoke_api_key",
];

const TENANT_SELF_TOOLS: &[&str] = &[
    "tenant_list_datasets",
    "tenant_create_dataset",
    "tenant_delete_dataset",
    "tenant_list_api_keys",
    "tenant_create_api_key",
    "tenant_revoke_api_key",
    "tenant_update_api_key",
    "tenant_list_memberships",
    "tenant_upsert_membership",
    "tenant_remove_membership",
    "tenant_get_schema",
    "tenant_info",
    "tenant_list_tables",
    "tenant_create_tables",
    "tenant_list_table_schemas",
    "list_available_table_schemas",
];

/// The tenant tools that wrap the `authorize_tenant`-gated management API:
/// reachable by a human session with the tenant-admin role or by an API key
/// carrying `tenant:manage`.
const TENANT_MANAGE_TOOLS: &[&str] = &[
    "tenant_list_datasets",
    "tenant_create_dataset",
    "tenant_delete_dataset",
    "tenant_list_api_keys",
    "tenant_create_api_key",
    "tenant_revoke_api_key",
    "tenant_update_api_key",
    "tenant_list_memberships",
    "tenant_upsert_membership",
    "tenant_remove_membership",
    "tenant_get_schema",
];

const SCHEMA_EXTRA_TOOLS: &[&str] = &["get_schema_registry", "validate_schema_registry"];

#[test]
fn platform_admin_and_tenant_tools_are_registered() {
    for tool in PLATFORM_ADMIN_TOOLS
        .iter()
        .chain(TENANT_SELF_TOOLS)
        .chain(SCHEMA_EXTRA_TOOLS)
    {
        assert!(McpServer::has_tool(tool), "MCP tool `{tool}` is missing");
    }
}

#[tokio::test]
async fn destructive_and_read_only_tools_carry_the_right_annotations() {
    let client = connect().await;
    let tools = client.list_tools(None).await.expect("tools/list succeeds");

    let destructive = [
        "delete_tenant",
        "delete_dataset",
        "revoke_api_key",
        "tenant_delete_dataset",
        "tenant_revoke_api_key",
        "tenant_remove_membership",
    ];
    let read_only = [
        "list_tenants",
        "get_tenant",
        "list_datasets",
        "tenant_list_datasets",
        "tenant_list_api_keys",
        "tenant_list_memberships",
        "tenant_get_schema",
        "tenant_info",
        "tenant_list_tables",
        "tenant_list_table_schemas",
        "list_available_table_schemas",
        "get_schema_registry",
    ];

    for name in destructive {
        let tool = tools
            .tools
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("{name} listed"));
        let annotations = tool
            .annotations
            .as_ref()
            .unwrap_or_else(|| panic!("{name} must carry annotations"));
        assert_eq!(
            annotations.destructive_hint,
            Some(true),
            "{name} must carry destructive_hint: true"
        );
        assert_eq!(
            annotations.read_only_hint,
            Some(false),
            "{name} must carry read_only_hint: false"
        );
    }

    for name in read_only {
        let tool = tools
            .tools
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("{name} listed"));
        let annotations = tool
            .annotations
            .as_ref()
            .unwrap_or_else(|| panic!("{name} must carry annotations"));
        assert_eq!(
            annotations.read_only_hint,
            Some(true),
            "{name} must carry read_only_hint: true"
        );
    }

    client.cancel().await.ok();
}

#[tokio::test]
async fn tenant_manage_tools_name_the_scope_and_drop_the_human_session_caveat() {
    let client = connect().await;
    let tools = client.list_tools(None).await.expect("tools/list succeeds");
    for name in TENANT_MANAGE_TOOLS {
        let tool = tools
            .tools
            .iter()
            .find(|t| t.name == *name)
            .unwrap_or_else(|| panic!("{name} listed"));
        let description = tool.description.as_deref().unwrap_or_default();
        assert!(
            description.contains("tenant:manage"),
            "{name} must name the tenant:manage scope: {description}"
        );
        for stale in [
            "human-authenticated",
            "plain API key is denied",
            "INSTANCE administrator",
        ] {
            assert!(
                !description.contains(stale),
                "{name} still carries the stale caveat `{stale}`: {description}"
            );
        }
    }
    // The key-creating tools spell out the scope vocabulary including the
    // new management scope.
    for name in ["create_api_key", "tenant_create_api_key"] {
        let tool = tools
            .tools
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("{name} listed"));
        let schema = serde_json::to_string(&tool.input_schema).expect("schema serializes");
        assert!(
            schema.contains("tenant:manage"),
            "{name} scope help must include tenant:manage: {schema}"
        );
    }
    client.cancel().await.ok();
}

#[tokio::test]
async fn tenant_create_api_key_requires_a_confirm_free_scope_list() {
    let client = connect().await;
    let tools = client.list_tools(None).await.expect("tools/list succeeds");
    let tool = tools
        .tools
        .iter()
        .find(|t| t.name == "tenant_create_api_key")
        .expect("tenant_create_api_key listed");
    let schema = serde_json::to_value(&tool.input_schema).expect("schema serializes");
    let required: Vec<&str> = schema["required"]
        .as_array()
        .map(|items| items.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert!(required.contains(&"scopes"), "requires scopes: {schema}");
    assert!(
        required.contains(&"tenant_id"),
        "requires tenant_id: {schema}"
    );

    client.cancel().await.ok();
}

// ---------------------------------------------------------------------------
// Full HTTP harness — confirm enforcement and unauthorized denial need the
// `Extension<Parts>` the Streamable HTTP transport attaches; the in-process
// duplex client above never populates it.
// ---------------------------------------------------------------------------

async fn whoami() -> Response {
    axum::Json(serde_json::json!({
        "user_id": "user-a",
        "tenant": {"id": "acme", "slug": "acme", "name": "Acme"},
        "dataset": "production",
    }))
    .into_response()
}

/// Behaves per path: management API-key list/create endpoints return
/// realistic bodies (list has no key material, create does); everything
/// under `/api/v1/manage/` with tenant `denied` returns 403; anything else
/// gets a generic empty-success body.
async fn behaviour(
    headers: HeaderMap,
    uri: axum::http::Uri,
    method: axum::http::Method,
) -> Response {
    let path = uri.path();
    if path == "/api/v1/tenants/acme" && method == axum::http::Method::GET {
        return axum::Json(serde_json::json!({
            "tenant_id": "acme", "enabled": true, "schema": null
        }))
        .into_response();
    }
    // Scope enforcement stand-in for `authorize_tenant`: only the key that
    // carries `tenant:manage` may create a dataset.
    if path == "/api/v1/manage/tenants/acme/datasets" && method == axum::http::Method::POST {
        let bearer = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        return if bearer == "Bearer sk-acme-manage" {
            (
                StatusCode::CREATED,
                axum::Json(serde_json::json!({"id": "staging", "name": "staging"})),
            )
                .into_response()
        } else {
            (
                StatusCode::FORBIDDEN,
                axum::Json(serde_json::json!({
                    "error": "Tenant administrator role or tenant:manage scope required"
                })),
            )
                .into_response()
        };
    }
    if path.contains("/manage/tenants/denied/") {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({"error": "forbidden"})),
        )
            .into_response();
    }
    // Every admin/management DELETE endpoint returns 204 No Content on
    // success; the generated SDK treats any other status on a DELETE as
    // `UnexpectedResponse`.
    if method == axum::http::Method::DELETE {
        return StatusCode::NO_CONTENT.into_response();
    }
    if path.ends_with("/tables") && method == axum::http::Method::GET {
        return axum::Json(serde_json::json!({
            "tenant_id": "acme",
            "tables": [
                {"name": "traces", "schema_type": "traces", "description": "d", "dataset": "production"}
            ],
            "datasets": [
                {"dataset": "production", "tables": [
                    {"name": "traces", "schema_type": "traces", "description": "d", "dataset": "production"}
                ]}
            ]
        }))
        .into_response();
    }
    if path.ends_with("/api-keys") && method == axum::http::Method::GET {
        return axum::Json(serde_json::json!([
            {"id": "k1", "name": "ci", "scopes": ["traces:write"], "revoked": false, "created_at": "2026-01-01T00:00:00Z"}
        ]))
        .into_response();
    }
    if path.ends_with("/api-keys") && method == axum::http::Method::POST {
        return (
            StatusCode::CREATED,
            axum::Json(serde_json::json!({
                "id": "k2", "key": "sk-new-secret", "name": "ci",
                "scopes": ["traces:write"], "dataset_id": null
            })),
        )
            .into_response();
    }
    let _ = headers;
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
    async fn open(app: axum::Router) -> Self {
        Self::open_with_key(app, "sk-acme").await
    }

    async fn open_with_key(app: axum::Router, api_key: &str) -> Self {
        use tower::ServiceExt;
        let init = mcp_request_with_key(
            api_key,
            None,
            serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                           "clientInfo": {"name": "admin-tenant-tools-test", "version": "0"}}
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

async fn app() -> axum::Router {
    let router_url = spawn_mock_router().await;
    let state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    mcp_http_router(state, &[])
}

fn tool_error_message(reply: &serde_json::Value) -> String {
    // A tool-level failure is a normal `result` with `isError: true`, whose
    // text content carries the message; a protocol-level failure is a
    // JSON-RPC `error`. Either way, collect the human-readable text.
    if let Some(error) = reply.get("error") {
        return error["message"].as_str().unwrap_or_default().to_string();
    }
    reply["result"]["content"]
        .as_array()
        .and_then(|blocks| blocks.first())
        .and_then(|b| b["text"].as_str())
        .unwrap_or_default()
        .to_string()
}

fn tool_is_error(reply: &serde_json::Value) -> bool {
    reply.get("error").is_some() || reply["result"]["isError"].as_bool() == Some(true)
}

#[tokio::test]
async fn destructive_tool_without_matching_confirm_is_refused() {
    let mut session = McpSession::open(app().await).await;

    let reply = session
        .call_tool(
            "delete_tenant",
            serde_json::json!({"tenant_id": "acme", "confirm": "not-acme"}),
        )
        .await;
    assert!(
        tool_is_error(&reply),
        "mismatched confirm must be refused: {reply}"
    );
    assert!(
        tool_error_message(&reply).contains("confirm"),
        "error must name the confirm requirement: {reply}"
    );

    let reply = session
        .call_tool(
            "tenant_delete_dataset",
            serde_json::json!({"tenant_id": "acme", "dataset_name": "prod", "confirm": "staging"}),
        )
        .await;
    assert!(
        tool_is_error(&reply),
        "mismatched confirm must be refused: {reply}"
    );
}

#[tokio::test]
async fn destructive_tool_with_matching_confirm_proceeds() {
    let mut session = McpSession::open(app().await).await;

    let reply = session
        .call_tool(
            "delete_tenant",
            serde_json::json!({"tenant_id": "acme", "confirm": "acme"}),
        )
        .await;
    assert!(
        !tool_is_error(&reply),
        "matching confirm must proceed to the router: {reply}"
    );
}

#[tokio::test]
async fn unauthorized_management_call_is_denied_cleanly() {
    let mut session = McpSession::open(app().await).await;

    let reply = session
        .call_tool(
            "tenant_list_datasets",
            serde_json::json!({"tenant_id": "denied"}),
        )
        .await;
    assert!(
        tool_is_error(&reply),
        "a 403 from the router must surface as a tool error: {reply}"
    );
    assert!(
        tool_error_message(&reply).to_lowercase().contains("denied")
            || tool_error_message(&reply)
                .to_lowercase()
                .contains("forbidden")
            || tool_error_message(&reply).to_lowercase().contains("access"),
        "error must read as access-denied, not an opaque failure: {reply}"
    );
}

/// `tenant_list_tables` must return the SDK's grouped shape — `datasets`
/// alongside the flat `tables` — not just the flat list from before this
/// change.
#[tokio::test]
async fn tenant_list_tables_returns_datasets_grouping() {
    let mut session = McpSession::open(app().await).await;

    let listed = session
        .call_tool(
            "tenant_list_tables",
            serde_json::json!({"tenant_id": "acme"}),
        )
        .await;
    assert!(!tool_is_error(&listed), "list succeeds: {listed}");
    let text = tool_error_message(&listed);
    assert!(
        text.contains("\"datasets\""),
        "SDK shape must include the dataset grouping: {text}"
    );
    assert!(
        text.contains("\"dataset\":\"production\"") || text.contains("\"dataset\": \"production\""),
        "grouped entry must carry its dataset id: {text}"
    );
}

#[tokio::test]
async fn tenant_manage_key_session_creates_a_dataset_and_ingest_key_is_denied() {
    let app = app().await;
    let mut manage = McpSession::open_with_key(app.clone(), "sk-acme-manage").await;
    let reply = manage
        .call_tool(
            "tenant_create_dataset",
            serde_json::json!({"tenant_id": "acme", "name": "staging"}),
        )
        .await;
    assert!(
        !tool_is_error(&reply),
        "tenant:manage key succeeds: {reply}"
    );
    assert!(
        tool_error_message(&reply).contains("staging"),
        "SDK-shaped dataset result: {reply}"
    );

    let mut ingest = McpSession::open_with_key(app, "sk-acme-ingest").await;
    let reply = ingest
        .call_tool(
            "tenant_create_dataset",
            serde_json::json!({"tenant_id": "acme", "name": "staging"}),
        )
        .await;
    assert!(tool_is_error(&reply), "ingest-only key is denied: {reply}");
    assert!(
        tool_error_message(&reply).contains("tenant:manage"),
        "denial names the required scope: {reply}"
    );
}

#[tokio::test]
async fn tenant_info_returns_the_callers_tenant() {
    let mut session = McpSession::open(app().await).await;
    let reply = session
        .call_tool("tenant_info", serde_json::json!({"tenant_id": "acme"}))
        .await;
    assert!(!tool_is_error(&reply), "tenant_info succeeds: {reply}");
    let text = tool_error_message(&reply);
    assert!(text.contains("\"tenant_id\""), "{text}");
    assert!(text.contains("acme"), "{text}");
}

#[tokio::test]
async fn key_material_appears_once_on_create_and_never_on_list() {
    let mut session = McpSession::open(app().await).await;

    let created = session
        .call_tool(
            "tenant_create_api_key",
            serde_json::json!({"tenant_id": "acme", "scopes": ["traces:write"]}),
        )
        .await;
    assert!(!tool_is_error(&created), "create succeeds: {created}");
    let created_text = tool_error_message(&created); // reused: pulls the text block either way
    assert!(
        created_text.contains("sk-new-secret"),
        "create response must contain the key material once: {created_text}"
    );

    let listed = session
        .call_tool(
            "tenant_list_api_keys",
            serde_json::json!({"tenant_id": "acme"}),
        )
        .await;
    assert!(!tool_is_error(&listed), "list succeeds: {listed}");
    let listed_text = tool_error_message(&listed);
    assert!(
        !listed_text.contains("sk-new-secret") && !listed_text.contains("\"key\""),
        "list response must never contain key material: {listed_text}"
    );
}
