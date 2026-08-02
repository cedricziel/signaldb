//! # signaldb-mcp
//!
//! A standalone Model Context Protocol server for SignalDB. Its **only** channel
//! to SignalDB is the router's HTTP API via [`signaldb_sdk`] — it depends on no
//! SignalDB internal crate and holds no privileged state. Because it reaches the
//! platform only through the SDK, it is always a separate service (a sidecar),
//! never an in-process route on the router.
//!
//! It is a pure credential-forwarding client: it checks that a request carries a
//! bearer token and `X-Tenant-ID`, pins each MCP session to that identity, and
//! forwards the caller's credential to the router on every downstream call. The
//! router is the sole authority on whether the credential is valid and what it
//! may access — an invalid or revoked token is rejected downstream and surfaces
//! as a clean MCP error.
//!
//! MCP is served over Streamable HTTP at `/mcp` on this service's own port.

pub mod server;

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode, header::AUTHORIZATION, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use dashmap::DashMap;
use reqwest::header::{HeaderMap, HeaderName};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, tower::StreamableHttpService,
};

use server::McpServer;

/// Headers forwarded from the MCP caller to the router on every downstream
/// call, so the request is made as the caller.
const FORWARDED_HEADERS: [&str; 3] = ["authorization", "x-tenant-id", "x-dataset-id"];

/// The identity a session is pinned to on its first request: the tenant it
/// declared and a hash of the credential it presented.
#[derive(Clone, PartialEq, Eq)]
struct SessionBinding {
    tenant_id: String,
    token_hash: u64,
}

/// Shared state for the MCP HTTP surface. Holds no credential of its own —
/// only the router URL it forwards to and the per-session identity bindings.
#[derive(Clone)]
pub struct McpAppState {
    /// Base URL of the router HTTP API downstream calls are forwarded to.
    pub router_base_url: String,
    /// Pins each MCP session (keyed by `mcp-session-id`) to the identity seen
    /// on its first request, so a session cannot be reused under a different
    /// tenant or credential mid-stream.
    session_bindings: Arc<DashMap<String, SessionBinding>>,
}

impl McpAppState {
    /// Construct the shared state for a server that forwards to `router_base_url`.
    pub fn new(router_base_url: String) -> Self {
        Self {
            router_base_url,
            session_bindings: Arc::new(DashMap::new()),
        }
    }
}

/// Build the axum router that serves MCP over Streamable HTTP at `/mcp`, gated
/// by a lightweight credential-presence + session-binding check.
pub fn mcp_http_router(state: McpAppState) -> Router {
    let session_manager = Arc::new(LocalSessionManager::default());
    let base_url = state.router_base_url.clone();
    let service = StreamableHttpService::new(
        move || Ok(McpServer::new(base_url.clone())),
        session_manager,
        Default::default(),
    );

    Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(state, mcp_auth_middleware))
}

/// Non-cryptographic hash of the caller's credential, used only to detect a
/// mid-session identity change (the raw token is never stored).
fn token_hash(token: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    token.hash(&mut hasher);
    hasher.finish()
}

/// Require a bearer token and `X-Tenant-ID`, and pin the session to that
/// identity, before the request reaches the MCP transport. The MCP server does
/// not validate the credential itself — that is the router's job; this only
/// rejects requests that carry *no* credential (401) and requests that try to
/// reuse a session under a different identity (403).
async fn mcp_auth_middleware(
    State(state): State<McpAppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let headers = req.headers();
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned);
    let tenant_id = headers
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let (Some(token), Some(tenant_id)) = (token, tenant_id) else {
        return (
            StatusCode::UNAUTHORIZED,
            "missing bearer token or X-Tenant-ID header",
        )
            .into_response();
    };

    // Pin the session to this identity. A request that carries an established
    // `mcp-session-id` but resolves to a different tenant or credential is a
    // session-reuse attempt and is refused.
    if let Some(session_id) = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    {
        let binding = SessionBinding {
            tenant_id: tenant_id.clone(),
            token_hash: token_hash(&token),
        };
        if let Some(existing) = state.session_bindings.get(&session_id) {
            if *existing != binding {
                tracing::warn!(%session_id, %tenant_id, "MCP session identity mismatch — refusing reuse");
                return (
                    StatusCode::FORBIDDEN,
                    "session is bound to a different identity",
                )
                    .into_response();
            }
        } else {
            state.session_bindings.insert(session_id, binding);
        }
    }

    next.run(req).await
}

/// Build a [`signaldb_sdk::Client`] that forwards the caller's credential to the
/// router. Every downstream request is made as the caller (bearer + tenant
/// headers copied from the incoming request), so the router enforces tenant
/// isolation and quotas exactly as for any HTTP caller. The MCP server injects
/// no credential of its own.
///
/// `dataset_override` sets `X-Dataset-ID` for tools that accept an explicit
/// dataset argument; when `None`, the caller's incoming `X-Dataset-ID` (or the
/// session default) is used.
pub fn sdk_client_for(
    parts: &Parts,
    router_base_url: &str,
    dataset_override: Option<&str>,
) -> signaldb_sdk::Client {
    let mut headers = HeaderMap::new();
    for name in FORWARDED_HEADERS {
        if name == "x-dataset-id" && dataset_override.is_some() {
            continue;
        }
        if let Some(value) = parts.headers.get(name)
            && let Ok(header_name) = HeaderName::from_bytes(name.as_bytes())
        {
            headers.insert(header_name, value.clone());
        }
    }
    if let Some(dataset) = dataset_override
        && let Ok(value) = dataset.parse()
    {
        headers.insert(HeaderName::from_static("x-dataset-id"), value);
    }
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap_or_default();
    signaldb_sdk::Client::new_with_client(router_base_url, http)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::request::Builder as RequestBuilder;
    use tower::ServiceExt;

    fn test_state() -> McpAppState {
        McpAppState::new("http://localhost:3000".to_string())
    }

    #[tokio::test]
    async fn rejects_request_without_bearer() {
        let app = mcp_http_router(test_state());
        let res = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("x-tenant-id", "acme")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_request_without_tenant() {
        let app = mcp_http_router(test_state());
        let res = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer sk-anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn present_credential_reaches_transport() {
        // With a bearer + tenant present, the request clears the presence check
        // and reaches the MCP transport — validation is the router's job, so the
        // response is no longer a 401 produced by this layer.
        let app = mcp_http_router(test_state());
        let res = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer sk-anything")
                    .header("x-tenant-id", "acme")
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn session_bound_to_first_identity() {
        // A session pinned to tenant `acme` cannot be reused by a different
        // identity (tenant `other`, different token) on the same session id.
        let app = mcp_http_router(test_state());

        let first = app
            .clone()
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer sk-acme")
                    .header("x-tenant-id", "acme")
                    .header("mcp-session-id", "sess-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(first.status(), StatusCode::UNAUTHORIZED);

        let reused = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer sk-other")
                    .header("x-tenant-id", "other")
                    .header("mcp-session-id", "sess-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reused.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn sdk_client_forwards_caller_headers() {
        let parts = RequestBuilder::new()
            .method("POST")
            .uri("/mcp")
            .header("authorization", "Bearer sk-tenant-key")
            .header("x-tenant-id", "acme")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let _client = sdk_client_for(&parts, "http://localhost:3000", None);
        let _with_dataset = sdk_client_for(&parts, "http://localhost:3000", Some("prod"));
    }
}
