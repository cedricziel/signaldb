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
    Json, Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode, Uri, header::AUTHORIZATION, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use dashmap::DashMap;
use reqwest::header::{HeaderMap, HeaderName};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager,
    tower::{StreamableHttpServerConfig, StreamableHttpService},
};

use server::McpServer;

/// Headers forwarded from the MCP caller to the router on every downstream
/// call, so the request is made as the caller.
const FORWARDED_HEADERS: [&str; 3] = ["authorization", "x-tenant-id", "x-dataset-id"];

/// Prefix identifying a SignalDB OAuth 2.1 access token. Mirrors
/// `common::auth::oauth::ACCESS_TOKEN_PREFIX`; duplicated deliberately so the
/// sidecar keeps depending on no SignalDB internal crate.
const OAUTH_ACCESS_TOKEN_PREFIX: &str = "sdb_at_";

/// The OAuth 2.1 resource metadata this sidecar advertises (change:
/// mcp-oauth-dcr). Present only when the deployment enables OAuth.
#[derive(Clone)]
pub struct OAuthResource {
    /// This MCP resource's own public URL — the token audience and the PRM
    /// `resource` value (e.g. `https://signaldb.example.com/mcp`).
    pub resource_url: String,
    /// The authorization server (router) clients are directed to.
    pub issuer_url: String,
}

impl OAuthResource {
    /// Absolute URL of the RFC 9728 Protected Resource Metadata document,
    /// derived from the resource origin. Returns `None` if `resource_url` is
    /// not an absolute URL with an authority.
    fn protected_resource_metadata_url(&self) -> Option<String> {
        let uri: Uri = self.resource_url.parse().ok()?;
        let scheme = uri.scheme_str()?;
        let authority = uri.authority()?;
        Some(format!(
            "{scheme}://{authority}/.well-known/oauth-protected-resource"
        ))
    }
}

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
    /// OAuth resource metadata to advertise, when the deployment enables OAuth.
    oauth: Option<OAuthResource>,
}

impl McpAppState {
    /// Construct the shared state for a server that forwards to `router_base_url`.
    pub fn new(router_base_url: String) -> Self {
        Self {
            router_base_url,
            session_bindings: Arc::new(DashMap::new()),
            oauth: None,
        }
    }

    /// Advertise OAuth resource metadata (the PRM document + `401` challenge)
    /// pointing at the given authorization server.
    pub fn with_oauth(mut self, resource_url: String, issuer_url: String) -> Self {
        self.oauth = Some(OAuthResource {
            resource_url,
            issuer_url,
        });
        self
    }
}

/// Build the axum router that serves MCP over Streamable HTTP at `/mcp`, gated
/// by a lightweight credential-presence + session-binding check.
///
/// `allowed_hosts` configures the Streamable HTTP transport's DNS-rebinding
/// guard, which validates the inbound `Host` header. The transport defaults to
/// loopback only (`localhost`/`127.0.0.1`/`::1`); pass additional authorities
/// (`host` or `host:port`) to reach the server beyond localhost, or the single
/// entry `"*"` to disable the guard entirely. An empty slice keeps the
/// loopback-only default.
pub fn mcp_http_router(state: McpAppState, allowed_hosts: &[String]) -> Router {
    let session_manager = Arc::new(LocalSessionManager::default());
    let base_url = state.router_base_url.clone();

    let mut config = StreamableHttpServerConfig::default();
    if allowed_hosts.iter().any(|h| h == "*") {
        // Explicit opt-out: accept any Host. The server still authenticates
        // every request (bearer + tenant), so this only drops the rebinding
        // guard, not authorization.
        config = config.disable_allowed_hosts();
    } else {
        // Extend the loopback defaults with the operator-provided authorities.
        config.allowed_hosts.extend_from_slice(allowed_hosts);
    }

    let service = StreamableHttpService::new(
        move || Ok(McpServer::new(base_url.clone())),
        session_manager,
        config,
    );

    // The `/mcp` transport is gated by the credential-presence + session-binding
    // check; the Protected Resource Metadata document is public (it is how an
    // unauthenticated client discovers where to authenticate).
    let mcp = Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            mcp_auth_middleware,
        ));

    let oauth = state.oauth.clone();
    let well_known = Router::new().route(
        "/.well-known/oauth-protected-resource",
        get(move || protected_resource_metadata(oauth.clone())),
    );

    mcp.merge(well_known)
}

/// Serve the RFC 9728 Protected Resource Metadata document, naming the
/// authorization server clients should use. Returns `404` when OAuth is not
/// configured for this deployment.
async fn protected_resource_metadata(oauth: Option<OAuthResource>) -> Response {
    match oauth {
        Some(oauth) => Json(serde_json::json!({
            "resource": oauth.resource_url,
            "authorization_servers": [oauth.issuer_url],
        }))
        .into_response(),
        None => (StatusCode::NOT_FOUND, "OAuth is not enabled").into_response(),
    }
}

/// Non-cryptographic hash of the caller's credential, used only to detect a
/// mid-session identity change (the raw token is never stored).
fn token_hash(token: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    token.hash(&mut hasher);
    hasher.finish()
}

/// Build the `401` response for an unauthenticated MCP request, attaching a
/// `WWW-Authenticate: Bearer resource_metadata="…"` challenge that points at
/// the Protected Resource Metadata document (RFC 9728) when OAuth is enabled,
/// so a compliant client discovers where to authenticate.
fn unauthorized_challenge(state: &McpAppState, message: &'static str) -> Response {
    let mut response = (StatusCode::UNAUTHORIZED, message).into_response();
    if let Some(prm_url) = state
        .oauth
        .as_ref()
        .and_then(OAuthResource::protected_resource_metadata_url)
        && let Ok(value) =
            format!("Bearer resource_metadata=\"{prm_url}\"").parse::<axum::http::HeaderValue>()
    {
        response
            .headers_mut()
            .insert(axum::http::header::WWW_AUTHENTICATE, value);
    }
    response
}

/// Require a bearer token before the request reaches the MCP transport, and pin
/// the session to the presented identity. The MCP server does not validate the
/// credential itself — that is the router's job; this only rejects requests
/// that carry no credential (`401`, with a discovery challenge) and refuses a
/// session reused under a different identity (`403`).
///
/// An OAuth access token (recognized by its prefix) carries its own tenant, so
/// `X-Tenant-ID` is not required for it. An API key still requires the tenant
/// header, exactly as before.
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

    let Some(token) = token else {
        return unauthorized_challenge(&state, "missing bearer token");
    };
    let is_oauth = token.starts_with(OAUTH_ACCESS_TOKEN_PREFIX);

    // The session binding pins the credential (and, for API keys, the tenant).
    // An OAuth token carries no tenant here, so it binds on the credential
    // alone.
    let bound_tenant = if is_oauth {
        String::new()
    } else {
        let Some(tenant_id) = tenant_id else {
            return unauthorized_challenge(&state, "missing X-Tenant-ID header");
        };
        tenant_id
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
            tenant_id: bound_tenant.clone(),
            token_hash: token_hash(&token),
        };
        if let Some(existing) = state.session_bindings.get(&session_id) {
            if *existing != binding {
                tracing::warn!(%session_id, "MCP session identity mismatch — refusing reuse");
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

    fn test_state_with_oauth() -> McpAppState {
        McpAppState::new("http://localhost:3000".to_string()).with_oauth(
            "https://signaldb.example.com/mcp".to_string(),
            "https://signaldb.example.com".to_string(),
        )
    }

    #[tokio::test]
    async fn protected_resource_metadata_names_authorization_server() {
        let app = mcp_http_router(test_state_with_oauth(), &[]);
        let res = app
            .oneshot(
                RequestBuilder::new()
                    .method("GET")
                    .uri("/.well-known/oauth-protected-resource")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(doc["resource"], "https://signaldb.example.com/mcp");
        assert_eq!(
            doc["authorization_servers"][0],
            "https://signaldb.example.com"
        );
    }

    #[tokio::test]
    async fn unauthenticated_request_is_challenged_toward_discovery() {
        let app = mcp_http_router(test_state_with_oauth(), &[]);
        let res = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let challenge = res
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(
            challenge.starts_with("Bearer resource_metadata="),
            "{challenge}"
        );
        assert!(
            challenge.contains("https://signaldb.example.com/.well-known/oauth-protected-resource"),
            "{challenge}"
        );
    }

    #[tokio::test]
    async fn oauth_bearer_does_not_require_tenant_header() {
        let app = mcp_http_router(test_state_with_oauth(), &[]);
        // An OAuth access token carries its own tenant; no X-Tenant-ID needed,
        // so the request clears the presence check and reaches the transport.
        let res = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer sdb_at_sometoken")
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
    async fn api_key_bearer_still_requires_tenant_header() {
        // A non-OAuth bearer without X-Tenant-ID is still rejected.
        let app = mcp_http_router(test_state_with_oauth(), &[]);
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
    async fn rejects_request_without_bearer() {
        let app = mcp_http_router(test_state(), &[]);
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
        let app = mcp_http_router(test_state(), &[]);
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
        let app = mcp_http_router(test_state(), &[]);
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
        let app = mcp_http_router(test_state(), &[]);

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

    /// Helper: an authenticated `initialize` POST carrying an explicit `Host`.
    fn init_request_with_host(host: &str) -> Request<Body> {
        RequestBuilder::new()
            .method("POST")
            .uri("/mcp")
            .header("host", host)
            .header("authorization", "Bearer sk-anything")
            .header("x-tenant-id", "acme")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .body(Body::from(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}"#,
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn non_loopback_host_rejected_by_default() {
        // With no configured allowlist, the transport's DNS-rebinding guard
        // accepts only loopback hosts — a non-loopback `Host` is refused.
        let app = mcp_http_router(test_state(), &[]);
        let res = app
            .oneshot(init_request_with_host("signaldb.example.com"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn configured_host_is_allowed() {
        // Naming the host in the allowlist lets its requests clear the guard.
        let app = mcp_http_router(test_state(), &["signaldb.example.com".to_string()]);
        let res = app
            .oneshot(init_request_with_host("signaldb.example.com"))
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn wildcard_disables_host_guard() {
        // The `*` sentinel drops the guard, so any `Host` clears it.
        let app = mcp_http_router(test_state(), &["*".to_string()]);
        let res = app
            .oneshot(init_request_with_host("10.0.0.5:30228"))
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::FORBIDDEN);
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
