//! # OAuth 2.1 authorization-server endpoints (change: mcp-oauth-dcr)
//!
//! The router is SignalDB's OAuth 2.1 Authorization Server for the MCP surface.
//! This module serves the pieces that let Claude.ai and OpenAI/ChatGPT register
//! and obtain tokens with no human pre-registration:
//!
//! - `GET /.well-known/oauth-authorization-server` — RFC 8414 metadata
//! - `POST /oauth/register` — RFC 7591 Dynamic Client Registration
//!
//! The authorization (`/oauth/authorize` + consent) and token (`/oauth/token`)
//! endpoints join in later tasks. All endpoints are public: discovery and DCR
//! are unauthenticated by spec, and `/authorize` performs its own login.

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::auth::READ_SCOPES;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::RouterState;

/// An OAuth error rendered in the RFC 6749 §5.2 shape
/// (`{"error": ..., "error_description": ...}`) with an appropriate status.
struct OAuthError {
    status: StatusCode,
    error: &'static str,
    description: String,
}

impl OAuthError {
    fn new(status: StatusCode, error: &'static str, description: impl Into<String>) -> Self {
        Self {
            status,
            error,
            description: description.into(),
        }
    }

    fn bad_request(error: &'static str, description: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, error, description)
    }

    fn server_error(description: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            description,
        )
    }
}

impl IntoResponse for OAuthError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "error": self.error, "error_description": self.description })),
        )
            .into_response()
    }
}

/// Routes mounted at the router root. The caller gates mounting on
/// `config.mcp.oauth.enabled` so a plain deployment exposes no OAuth surface.
pub fn router<S: RouterState>() -> Router<S> {
    Router::new()
        .route(
            "/.well-known/oauth-authorization-server",
            get(authorization_server_metadata::<S>),
        )
        .route("/oauth/register", post(register::<S>))
}

/// RFC 8414 Authorization Server Metadata. Absolute URLs are built from the
/// configured issuer so external clients reach the same authority they came in
/// on (correct behind a TLS terminator, unlike deriving from `Host`).
async fn authorization_server_metadata<S: RouterState>(
    State(state): State<S>,
) -> Result<Response, OAuthError> {
    let oauth = &state.config().mcp.oauth;
    let issuer = oauth.issuer_url.as_deref().ok_or_else(|| {
        OAuthError::server_error("OAuth is enabled but mcp.oauth.issuer_url is not configured")
    })?;
    let issuer = issuer.trim_end_matches('/');

    let doc = json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/oauth/authorize"),
        "token_endpoint": format!("{issuer}/oauth/token"),
        "registration_endpoint": format!("{issuer}/oauth/register"),
        "scopes_supported": READ_SCOPES,
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
    });
    Ok(Json(doc).into_response())
}

/// RFC 7591 Dynamic Client Registration request. Unknown members are ignored.
#[derive(Debug, Default, Deserialize)]
struct RegistrationRequest {
    #[serde(default)]
    redirect_uris: Vec<String>,
    #[serde(default)]
    client_name: Option<String>,
    #[serde(default)]
    grant_types: Option<Vec<String>>,
    #[serde(default)]
    scope: Option<String>,
    // A client-supplied `token_endpoint_auth_method` is ignored (serde drops
    // unknown members): SignalDB registers public PKCE clients only, always
    // `"none"`.
}

/// RFC 7591 registration response.
#[derive(Debug, Serialize)]
struct RegistrationResponse {
    client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_name: Option<String>,
    redirect_uris: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    grant_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
    token_endpoint_auth_method: String,
}

/// Whether a redirect URI is acceptable: an absolute `http`/`https` URL with an
/// authority. (Claude/OpenAI use `https`; `http` is allowed for localhost dev.)
fn is_valid_redirect_uri(uri: &str) -> bool {
    match uri.parse::<axum::http::Uri>() {
        Ok(parsed) => {
            matches!(parsed.scheme_str(), Some("http") | Some("https"))
                && parsed.authority().is_some()
        }
        Err(_) => false,
    }
}

/// Dynamic Client Registration (RFC 7591). Registers a public PKCE client and
/// returns a fresh `client_id`. No client secret is issued.
async fn register<S: RouterState>(
    State(state): State<S>,
    Json(req): Json<RegistrationRequest>,
) -> Result<Response, OAuthError> {
    if req.redirect_uris.is_empty() {
        return Err(OAuthError::bad_request(
            "invalid_redirect_uri",
            "at least one redirect_uri is required",
        ));
    }
    if let Some(bad) = req.redirect_uris.iter().find(|u| !is_valid_redirect_uri(u)) {
        return Err(OAuthError::bad_request(
            "invalid_redirect_uri",
            format!("redirect_uri is not a valid absolute http(s) URL: {bad}"),
        ));
    }
    // SignalDB registers public clients only; PKCE, never a client secret.
    let auth_method = "none".to_string();
    let client_id = Uuid::new_v4().to_string();

    let stored = state
        .catalog()
        .register_oauth_client(
            &client_id,
            req.client_name.as_deref(),
            &req.redirect_uris,
            req.grant_types.as_deref(),
            req.scope.as_deref(),
            &auth_method,
        )
        .await
        .map_err(|e| OAuthError::server_error(format!("failed to persist client: {e}")))?;

    let body = RegistrationResponse {
        client_id: stored.id,
        client_name: stored.client_name,
        redirect_uris: stored.redirect_uris,
        grant_types: stored.grant_types,
        scope: stored.scope,
        token_endpoint_auth_method: stored.token_endpoint_auth_method,
    };
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

#[cfg(test)]
mod tests {
    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use common::catalog::Catalog;
    use common::config::{Configuration, OAuthConfig};
    use serde_json::Value;
    use tower::ServiceExt;

    async fn oauth_app() -> axum::Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration::default();
        config.mcp.oauth = OAuthConfig {
            enabled: true,
            issuer_url: Some("https://signaldb.example.com".to_string()),
            resource_url: Some("https://signaldb.example.com/mcp".to_string()),
            ..Default::default()
        };
        create_router(RouterAppState::new(catalog, config))
    }

    async fn body_json(res: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn metadata_advertises_endpoints_and_pkce() {
        let app = oauth_app().await;
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-authorization-server")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let doc = body_json(res).await;
        assert_eq!(doc["issuer"], "https://signaldb.example.com");
        assert_eq!(
            doc["registration_endpoint"],
            "https://signaldb.example.com/oauth/register"
        );
        assert_eq!(
            doc["authorization_endpoint"],
            "https://signaldb.example.com/oauth/authorize"
        );
        assert_eq!(
            doc["token_endpoint"],
            "https://signaldb.example.com/oauth/token"
        );
        let pkce = doc["code_challenge_methods_supported"].as_array().unwrap();
        assert!(pkce.iter().any(|m| m == "S256"));
        let grants = doc["grant_types_supported"].as_array().unwrap();
        assert!(grants.iter().any(|g| g == "authorization_code"));
        assert!(grants.iter().any(|g| g == "refresh_token"));
    }

    #[tokio::test]
    async fn register_persists_client_and_returns_id() {
        let app = oauth_app().await;
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/register")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"redirect_uris":["https://claude.ai/api/mcp/callback"],"client_name":"Claude"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        let doc = body_json(res).await;
        assert!(doc["client_id"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(doc["token_endpoint_auth_method"], "none");
        assert_eq!(
            doc["redirect_uris"][0],
            "https://claude.ai/api/mcp/callback"
        );
    }

    #[tokio::test]
    async fn register_without_redirect_uri_is_rejected() {
        let app = oauth_app().await;
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/register")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"client_name":"Claude"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let doc = body_json(res).await;
        assert_eq!(doc["error"], "invalid_redirect_uri");
    }

    #[tokio::test]
    async fn register_with_malformed_redirect_uri_is_rejected() {
        let app = oauth_app().await;
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/register")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"redirect_uris":["not-a-url"]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let doc = body_json(res).await;
        assert_eq!(doc["error"], "invalid_redirect_uri");
    }
}
