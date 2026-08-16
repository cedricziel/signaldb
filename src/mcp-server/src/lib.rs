//! # signaldb-mcp
//!
//! A standalone Model Context Protocol server for SignalDB. Its **only** channel
//! to SignalDB data is the router's HTTP API via [`signaldb_sdk`] — it holds no
//! privileged state and no catalog or storage handle; the one internal crate it
//! uses, `common`, supplies self-monitoring primitives only (span factories,
//! metrics, HTTP middleware). Because it reaches the platform only through the
//! SDK, it is always a separate service (a sidecar), never an in-process route on
//! the router.
//!
//! It validates each caller credential with the router, pins each MCP session to
//! that identity, and forwards the original request headers to the router on
//! every downstream call. The router is the sole authority on whether the
//! credential is valid and what it may access; rejected credentials become an
//! HTTP `401` so MCP clients can refresh them.
//!
//! MCP is served over Streamable HTTP at `/mcp` on this service's own port.

pub mod apps;
pub mod audit;
pub mod cli;
pub mod prompts;
pub mod server;

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
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
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager,
    tower::{StreamableHttpServerConfig, StreamableHttpService},
};

use server::McpServer;

/// Headers forwarded from the MCP caller to the router on every downstream
/// call, so the request is made as the caller.
const FORWARDED_HEADERS: [&str; 3] = ["authorization", "x-tenant-id", "x-dataset-id"];

/// Connect timeout for the HTTP client forwarding to the router. Short and
/// fixed: establishing a TCP connection to the router is a local-network hop.
pub const ROUTER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default overall request timeout for the HTTP client forwarding to the
/// router. Overridable via `SIGNALDB__MCP__ROUTER_TIMEOUT` (seconds) so slow
/// analytical queries can be accommodated; a hung router must never hang MCP
/// tool calls indefinitely (issue #885).
pub const DEFAULT_ROUTER_TIMEOUT: Duration = Duration::from_secs(30);

/// Prefix identifying a SignalDB OAuth 2.1 access token. Mirrors
/// `common::auth::oauth::ACCESS_TOKEN_PREFIX`; duplicated deliberately so the
/// sidecar's trust model does not lean on `common`'s auth module.
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

/// The stable identity a session is pinned to. API keys remain tied to their
/// exact credential, while OAuth access-token refreshes preserve the router-
/// resolved user and tenant.
#[derive(Clone, PartialEq, Eq)]
enum SessionBinding {
    ApiKey { tenant_id: String, token_hash: u64 },
    OAuth { user_id: String, tenant_id: String },
}

/// Shared state for the MCP HTTP surface. Holds no credential of its own —
/// only the router URL it forwards to and the per-session identity bindings.
#[derive(Clone)]
pub struct McpAppState {
    /// Base URL of the router HTTP API downstream calls are forwarded to.
    pub router_base_url: String,
    /// Overall timeout for each downstream request to the router.
    pub router_timeout: Duration,
    /// Pins each MCP session (keyed by `mcp-session-id`) to the identity seen
    /// on its first request, so a session cannot be reused under a different
    /// tenant or credential mid-stream.
    session_bindings: Arc<DashMap<String, SessionBinding>>,
    /// OAuth resource metadata to advertise, when the deployment enables OAuth.
    oauth: Option<OAuthResource>,
    /// Per-session bound on tool calls in flight (`[mcp].max_concurrent_tool_calls`).
    pub max_concurrent_tool_calls: usize,
}

impl McpAppState {
    /// Construct the shared state for a server that forwards to `router_base_url`.
    pub fn new(router_base_url: String) -> Self {
        Self {
            router_base_url,
            router_timeout: DEFAULT_ROUTER_TIMEOUT,
            session_bindings: Arc::new(DashMap::new()),
            oauth: None,
            max_concurrent_tool_calls: audit::DEFAULT_MAX_CONCURRENT_TOOL_CALLS,
        }
    }

    /// Override the per-session bound on tool calls in flight.
    pub fn with_max_concurrent_tool_calls(mut self, limit: usize) -> Self {
        self.max_concurrent_tool_calls = limit;
        self
    }

    /// Override the overall timeout for downstream requests to the router.
    pub fn with_router_timeout(mut self, timeout: Duration) -> Self {
        self.router_timeout = timeout;
        self
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
/// by router-validated credentials and session binding.
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
    let router_timeout = state.router_timeout;
    let max_concurrent_tool_calls = state.max_concurrent_tool_calls;

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
        move || {
            Ok(McpServer::with_max_concurrent_tool_calls(
                base_url.clone(),
                router_timeout,
                max_concurrent_tool_calls,
            ))
        },
        session_manager,
        config,
    );

    // The `/mcp` transport is gated by router-validated credentials and session
    // binding; the Protected Resource Metadata document is public (it is how an
    // unauthenticated client discovers where to authenticate).
    let mcp = Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            mcp_auth_middleware,
        ))
        // OTel HTTP server metrics, then — outermost — the `POST /mcp` server
        // span parented to the caller's W3C trace context, exactly as the
        // router and acceptor layer them (no-op unless self-monitoring is
        // enabled). Layered on the `/mcp` router only, so the public
        // well-known document is not measured.
        .layer(middleware::from_fn(
            common::self_monitoring::http_metrics_middleware,
        ))
        .layer(middleware::from_fn(
            common::self_monitoring::http_trace_context_middleware,
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

/// Require a router-validated bearer token before the request reaches the MCP
/// transport, then pin the session to the presented identity. A router-rejected
/// credential becomes an HTTP `401` with a discovery challenge, allowing MCP
/// clients to refresh it before a JSON-RPC response is produced.
///
/// An OAuth access token (recognized by its prefix) carries its own tenant, so
/// `X-Tenant-ID` is not required for it. An API key still requires the tenant
/// header, exactly as before.
async fn mcp_auth_middleware(
    State(state): State<McpAppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let (mut parts, body) = req.into_parts();
    let token = parts
        .headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned);
    let tenant_id = parts
        .headers
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

    let validation_client =
        match sdk_client_for(&parts, &state.router_base_url, None, state.router_timeout) {
            Ok(client) => client,
            Err(error) => {
                tracing::error!(%error, "failed to construct router validation client");
                return (StatusCode::BAD_GATEWAY, "failed to validate credential").into_response();
            }
        };
    let identity = match validation_client.whoami().send().await {
        Ok(response) => response.into_inner(),
        Err(error) => match error.status().map(|status| status.as_u16()) {
            Some(401) => return unauthorized_challenge(&state, "credential rejected by router"),
            Some(403) => {
                return (StatusCode::FORBIDDEN, "credential denied by router").into_response();
            }
            _ => {
                tracing::warn!(%error, "router credential validation failed");
                return (StatusCode::BAD_GATEWAY, "credential validation unavailable")
                    .into_response();
            }
        },
    };

    // The router-resolved tenant travels with the request so the tool-call
    // audit names it even for OAuth credentials (no `X-Tenant-ID` header).
    parts
        .extensions
        .insert(audit::CallerTenant(identity.tenant.id.clone()));

    // Pin the session to this identity. A request that carries an established
    // `mcp-session-id` but resolves to a different tenant or credential is a
    // session-reuse attempt and is refused.
    if let Some(session_id) = parts
        .headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
    {
        let binding = if is_oauth {
            if identity.user_id.is_empty() {
                tracing::error!("router returned no user ID for an OAuth credential");
                return (StatusCode::BAD_GATEWAY, "credential identity unavailable")
                    .into_response();
            }
            SessionBinding::OAuth {
                user_id: identity.user_id,
                tenant_id: identity.tenant.id,
            }
        } else {
            SessionBinding::ApiKey {
                tenant_id: bound_tenant,
                token_hash: token_hash(&token),
            }
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

    next.run(Request::from_parts(parts, body)).await
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
///
/// `timeout` bounds each downstream request end-to-end (connect is separately
/// capped at [`ROUTER_CONNECT_TIMEOUT`]), so a hung router fails the tool call
/// instead of hanging it indefinitely.
///
/// # Errors
///
/// Returns an error if the underlying HTTP client cannot be constructed. This
/// must not be swallowed: a fallback default client would silently drop the
/// forwarded credential headers.
pub fn sdk_client_for(
    parts: &Parts,
    router_base_url: &str,
    dataset_override: Option<&str>,
    timeout: Duration,
) -> anyhow::Result<signaldb_sdk::Client> {
    // The SDK builder owns the HTTP client (headers, timeouts) and installs
    // the shared retry-on-throttle policy, so a brief 429 from the router is
    // absorbed here and only an exhausted one reaches `map_sdk_err`.
    let mut builder = signaldb_sdk::ClientBuilder::new(router_base_url)
        .connect_timeout(ROUTER_CONNECT_TIMEOUT)
        .timeout(timeout);
    for name in FORWARDED_HEADERS {
        if name == "x-dataset-id" && dataset_override.is_some() {
            continue;
        }
        if let Some(value) = parts.headers.get(name)
            && let Ok(value) = value.to_str()
        {
            builder = builder.header(name, value);
        }
    }
    if let Some(dataset) = dataset_override {
        builder = builder.dataset(dataset);
    }
    builder
        .build()
        .context("Failed to build router HTTP client")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::request::Builder as RequestBuilder;
    use tower::ServiceExt;

    async fn spawn_whoami_router(
        status: &'static str,
        request_count: usize,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(request_count);
            for _ in 0..request_count {
                let (mut socket, _) = listener.accept().await.expect("accept request");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                loop {
                    let read = socket.read(&mut buffer).await.expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let body = if status == "200 OK" {
                    b"{\"user_id\":\"user-a\",\"tenant\":{\"id\":\"acme\",\"slug\":\"acme\",\"name\":\"Acme\"},\"dataset\":\"production\"}".as_slice()
                } else {
                    b"".as_slice()
                };
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write response headers");
                socket.write_all(body).await.expect("write response body");
                requests.push(String::from_utf8(request).expect("request is UTF-8"));
            }
            requests
        });
        (format!("http://{addr}"), handle)
    }

    async fn spawn_oauth_identity_router(
        identities: Vec<(&'static str, &'static str, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let handle = tokio::spawn(async move {
            for (token, user_id, tenant_id) in identities {
                let (mut socket, _) = listener.accept().await.expect("accept request");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                loop {
                    let read = socket.read(&mut buffer).await.expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request).expect("request is UTF-8");
                assert!(
                    request.contains(&format!("authorization: Bearer {token}")),
                    "unexpected router request: {request}"
                );
                let body = format!(
                    "{{\"user_id\":\"{user_id}\",\"tenant\":{{\"id\":\"{tenant_id}\",\"slug\":\"{tenant_id}\",\"name\":\"{tenant_id}\"}},\"dataset\":\"production\"}}"
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write response headers");
                socket
                    .write_all(body.as_bytes())
                    .await
                    .expect("write response body");
            }
        });
        (format!("http://{addr}"), handle)
    }

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
    async fn router_rejected_bearer_is_an_http_401_with_oauth_challenge() {
        let (router_url, router) = spawn_whoami_router("401 Unauthorized", 1).await;
        let state = McpAppState::new(router_url).with_oauth(
            "https://signaldb.example.com/mcp".to_string(),
            "https://signaldb.example.com".to_string(),
        );
        let app = mcp_http_router(state.clone(), &[]);

        let response = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer sdb_at_expired")
                    .header("mcp-session-id", "expired-session")
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}"#,
                    ))
                    .expect("build request"),
            )
            .await
            .expect("MCP response");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(
            response
                .headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value
                    .contains("https://signaldb.example.com/.well-known/oauth-protected-resource"))
        );
        assert!(
            state.session_bindings.is_empty(),
            "a rejected credential must not create a session binding"
        );
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn router_accepted_bearer_reaches_the_mcp_transport_unchanged() {
        let (router_url, router) = spawn_whoami_router("200 OK", 1).await;
        let app = mcp_http_router(McpAppState::new(router_url), &[]);

        let response = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer sk-acme")
                    .header("x-tenant-id", "acme")
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}"#,
                    ))
                    .expect("build request"),
            )
            .await
            .expect("MCP response");

        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
        let mut requests = router.await.expect("mock router task panicked");
        let request = requests.pop().expect("mock router received a request");
        assert!(request.starts_with("GET /api/v1/whoami "), "{request}");
        assert!(
            request.contains("authorization: Bearer sk-acme"),
            "{request}"
        );
        assert!(request.contains("x-tenant-id: acme"), "{request}");
    }

    #[tokio::test]
    async fn oauth_session_allows_token_rotation_only_for_the_same_resolved_identity() {
        let (router_url, router) = spawn_oauth_identity_router(vec![
            ("sdb_at_original", "user-a", "acme"),
            ("sdb_at_refreshed", "user-a", "acme"),
            ("sdb_at_other_user", "user-b", "acme"),
            ("sdb_at_other_tenant", "user-a", "globex"),
        ])
        .await;
        let app = mcp_http_router(McpAppState::new(router_url), &[]);
        let request = |token| {
            RequestBuilder::new()
                .method("POST")
                .uri("/mcp")
                .header("authorization", format!("Bearer {token}"))
                .header("mcp-session-id", "oauth-session")
                .body(Body::empty())
                .expect("build request")
        };

        let original = app
            .clone()
            .oneshot(request("sdb_at_original"))
            .await
            .expect("MCP response");
        assert_ne!(original.status(), StatusCode::FORBIDDEN);

        let refreshed = app
            .clone()
            .oneshot(request("sdb_at_refreshed"))
            .await
            .expect("MCP response");
        assert_ne!(refreshed.status(), StatusCode::FORBIDDEN);

        let other_user = app
            .clone()
            .oneshot(request("sdb_at_other_user"))
            .await
            .expect("MCP response");
        assert_eq!(other_user.status(), StatusCode::FORBIDDEN);

        let other_tenant = app
            .oneshot(request("sdb_at_other_tenant"))
            .await
            .expect("MCP response");
        assert_eq!(other_tenant.status(), StatusCode::FORBIDDEN);
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn oauth_bearer_does_not_require_tenant_header() {
        let (router_url, router) = spawn_whoami_router("200 OK", 1).await;
        let app = mcp_http_router(
            McpAppState::new(router_url).with_oauth(
                "https://signaldb.example.com/mcp".to_string(),
                "https://signaldb.example.com".to_string(),
            ),
            &[],
        );
        // An OAuth access token carries its own tenant, so a router-validated
        // OAuth request reaches the transport without X-Tenant-ID.
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
        router.await.expect("mock router task panicked");
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
        let (router_url, router) = spawn_whoami_router("200 OK", 1).await;
        // A router-validated bearer + tenant reaches the MCP transport.
        let app = mcp_http_router(McpAppState::new(router_url), &[]);
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
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn session_bound_to_first_identity() {
        // A session pinned to tenant `acme` cannot be reused by a different
        // identity (tenant `other`, different token) on the same session id.
        let (router_url, router) = spawn_whoami_router("200 OK", 2).await;
        let app = mcp_http_router(McpAppState::new(router_url), &[]);

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
        router.await.expect("mock router task panicked");
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
        let (router_url, router) = spawn_whoami_router("200 OK", 1).await;
        let app = mcp_http_router(McpAppState::new(router_url), &[]);
        let res = app
            .oneshot(init_request_with_host("signaldb.example.com"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn configured_host_is_allowed() {
        // Naming the host in the allowlist lets its requests clear the guard.
        let (router_url, router) = spawn_whoami_router("200 OK", 1).await;
        let app = mcp_http_router(
            McpAppState::new(router_url),
            &["signaldb.example.com".to_string()],
        );
        let res = app
            .oneshot(init_request_with_host("signaldb.example.com"))
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::FORBIDDEN);
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn wildcard_disables_host_guard() {
        // The `*` sentinel drops the guard, so any `Host` clears it.
        let (router_url, router) = spawn_whoami_router("200 OK", 1).await;
        let app = mcp_http_router(McpAppState::new(router_url), &["*".to_string()]);
        let res = app
            .oneshot(init_request_with_host("10.0.0.5:30228"))
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::FORBIDDEN);
        router.await.expect("mock router task panicked");
    }

    /// Overall request timeout used by tests that expect a response.
    const ROUTER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    #[test]
    fn default_router_timeout_is_30_seconds() {
        assert_eq!(DEFAULT_ROUTER_TIMEOUT, std::time::Duration::from_secs(30));
        assert_eq!(test_state().router_timeout, DEFAULT_ROUTER_TIMEOUT);
    }

    #[tokio::test]
    async fn sdk_client_times_out_instead_of_hanging_on_stalled_router() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        // A router that accepts the connection, reads the request, and never
        // answers. Without a request timeout the tool call hangs forever
        // (issue #885); with one it must fail fast.
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled mock server");
        let addr = listener.local_addr().expect("mock server local addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept connection");
            let mut buf = [0u8; 4096];
            // Keep reading (and never respond) until the client gives up.
            while let Ok(n) = socket.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
        });

        let parts = RequestBuilder::new()
            .method("POST")
            .uri("/mcp")
            .header("authorization", "Bearer sk-tenant-key")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let client = sdk_client_for(
            &parts,
            &format!("http://{addr}"),
            None,
            std::time::Duration::from_millis(200),
        )
        .expect("build forwarding client");

        let started = std::time::Instant::now();
        let result = client.list_tenants().send().await;
        assert!(
            result.is_err(),
            "request against a stalled router must fail, not succeed"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "request must be cut off by the client timeout, took {:?}",
            started.elapsed()
        );
        server.abort();
    }

    /// Spawns a one-shot mock HTTP server, returning its address and a handle
    /// that resolves to the raw request bytes it received once a client has
    /// connected and sent a request.
    async fn spawn_header_capturing_server()
    -> (std::net::SocketAddr, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let addr = listener.local_addr().expect("mock server local addr");

        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept connection");
            let mut received = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = socket.read(&mut buf).await.expect("read request");
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&buf[..n]);
                if received.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let body = b"{\"tenants\":[]}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response headers");
            socket.write_all(body).await.expect("write response body");
            socket.shutdown().await.ok();
            String::from_utf8_lossy(&received).to_string()
        });

        (addr, handle)
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

        let (addr, handle) = spawn_header_capturing_server().await;
        let client = sdk_client_for(&parts, &format!("http://{addr}"), None, ROUTER_TIMEOUT)
            .expect("build forwarding client");
        client
            .list_tenants()
            .send()
            .await
            .expect("mock server responds to list_tenants");

        let received_request = handle.await.expect("mock server task panicked");
        assert!(
            received_request
                .to_lowercase()
                .contains("authorization: bearer sk-tenant-key"),
            "expected forwarded authorization header on the wire, got request:\n{received_request}"
        );
        assert!(
            received_request
                .to_lowercase()
                .contains("x-tenant-id: acme"),
            "expected forwarded x-tenant-id header on the wire, got request:\n{received_request}"
        );
    }

    #[tokio::test]
    async fn sdk_client_forwards_dataset_override_header() {
        let parts = RequestBuilder::new()
            .method("POST")
            .uri("/mcp")
            .header("authorization", "Bearer sk-tenant-key")
            .header("x-tenant-id", "acme")
            .header("x-dataset-id", "staging")
            .body(())
            .unwrap()
            .into_parts()
            .0;

        let (addr, handle) = spawn_header_capturing_server().await;
        let client = sdk_client_for(
            &parts,
            &format!("http://{addr}"),
            Some("prod"),
            ROUTER_TIMEOUT,
        )
        .expect("build forwarding client");
        client
            .list_tenants()
            .send()
            .await
            .expect("mock server responds to list_tenants");

        let received_request = handle.await.expect("mock server task panicked");
        let lower = received_request.to_lowercase();
        // dataset_override must win over the caller's incoming X-Dataset-ID.
        assert!(
            lower.contains("x-dataset-id: prod"),
            "expected dataset_override to set x-dataset-id, got request:\n{received_request}"
        );
        assert!(
            !lower.contains("x-dataset-id: staging"),
            "expected dataset_override to replace the caller's x-dataset-id, got request:\n{received_request}"
        );
    }
}
