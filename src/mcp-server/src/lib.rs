//! # signaldb-mcp
//!
//! A standalone Model Context Protocol server for SignalDB. It is a thin,
//! credential-forwarding client: it authenticates the caller's bearer token,
//! then forwards that same token to the router's HTTP API via
//! [`signaldb_sdk`]. It holds no privileged credential of its own, so tenant
//! isolation and quotas stay enforced by the router — a compromised MCP server
//! grants nothing beyond what the caller's key already grants.
//!
//! MCP is served over Streamable HTTP at `/mcp`, gated by the same bearer +
//! `X-Tenant-ID` scheme as the rest of the platform.

pub mod server;

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode, header::AUTHORIZATION, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use common::auth::Authenticator;
use reqwest::header::{HeaderMap, HeaderName};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, tower::StreamableHttpService,
};

use server::McpServer;

/// Headers forwarded from the MCP caller to the router on every downstream
/// call, so the request is made as the caller.
const FORWARDED_HEADERS: [&str; 3] = ["authorization", "x-tenant-id", "x-dataset-id"];

/// Shared state for the MCP HTTP surface.
#[derive(Clone)]
pub struct McpAppState {
    /// Validates caller bearer tokens and resolves the tenant context.
    pub authenticator: Arc<Authenticator>,
    /// Base URL of the router HTTP API downstream calls are forwarded to.
    pub router_base_url: String,
}

/// Build the axum router that serves MCP over Streamable HTTP at `/mcp`, gated
/// by bearer authentication.
pub fn mcp_http_router(state: McpAppState) -> Router {
    let session_manager = Arc::new(LocalSessionManager::default());
    let service =
        StreamableHttpService::new(|| Ok(McpServer::new()), session_manager, Default::default());

    Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(state, mcp_auth_middleware))
}

/// Authenticate the caller's bearer + `X-Tenant-ID` before the request reaches
/// the MCP transport, and attach the resolved `TenantContext` to the request
/// extensions so tool handlers can read it. Unauthenticated requests are
/// rejected with 401 so no MCP session is established for them.
async fn mcp_auth_middleware(
    State(state): State<McpAppState>,
    mut req: Request<Body>,
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
    let dataset_id = headers
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let (Some(token), Some(tenant_id)) = (token, tenant_id) else {
        return (
            StatusCode::UNAUTHORIZED,
            "missing bearer token or X-Tenant-ID header",
        )
            .into_response();
    };

    match state
        .authenticator
        .authenticate(&token, &tenant_id, dataset_id.as_deref())
        .await
    {
        Ok(ctx) => {
            req.extensions_mut().insert(ctx);
            next.run(req).await
        }
        Err(e) => {
            tracing::warn!(error = %e, tenant_id = %tenant_id, "MCP authentication failed");
            (StatusCode::UNAUTHORIZED, "authentication failed").into_response()
        }
    }
}

/// Build a [`signaldb_sdk::Client`] that forwards the caller's credential to the
/// router. Every downstream request is made as the caller (bearer + tenant
/// headers copied from the incoming request), so the router enforces tenant
/// isolation and quotas exactly as for any HTTP caller. The MCP server injects
/// no credential of its own.
pub fn sdk_client_for(parts: &Parts, router_base_url: &str) -> signaldb_sdk::Client {
    let mut headers = HeaderMap::new();
    for name in FORWARDED_HEADERS {
        if let Some(value) = parts.headers.get(name)
            && let Ok(header_name) = HeaderName::from_bytes(name.as_bytes())
        {
            headers.insert(header_name, value.clone());
        }
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
    use common::catalog::Catalog;
    use common::config::{ApiKeyConfig, AuthConfig, TenantConfig};
    use tower::ServiceExt;

    async fn test_state() -> McpAppState {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let auth = AuthConfig {
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme".to_string(),
                default_dataset: Some("default".to_string()),
                datasets: vec![],
                api_keys: vec![ApiKeyConfig {
                    key: "sk-test-key".to_string(),
                    name: Some("test".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        };
        McpAppState {
            authenticator: Arc::new(Authenticator::new(auth, Arc::new(catalog))),
            router_base_url: "http://localhost:3000".to_string(),
        }
    }

    #[tokio::test]
    async fn rejects_request_without_bearer() {
        let app = mcp_http_router(test_state().await);
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
    }

    #[tokio::test]
    async fn rejects_invalid_bearer() {
        let app = mcp_http_router(test_state().await);
        let res = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer wrong-key")
                    .header("x-tenant-id", "acme")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn valid_bearer_passes_auth_layer() {
        // A valid credential must clear the auth layer and reach the MCP
        // transport — i.e. the response is no longer a 401 produced by us.
        let app = mcp_http_router(test_state().await);
        let res = app
            .oneshot(
                RequestBuilder::new()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", "Bearer sk-test-key")
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
    async fn sdk_client_forwards_caller_headers() {
        // The per-session SDK client is built from the caller's headers so
        // downstream calls are made as the caller. Construction must succeed
        // with the forwarded credential in place.
        let parts = RequestBuilder::new()
            .method("POST")
            .uri("/mcp")
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let _client = sdk_client_for(&parts, "http://localhost:3000");
    }
}
