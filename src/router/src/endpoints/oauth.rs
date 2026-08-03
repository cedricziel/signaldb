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
    Form, Json, Router,
    extract::State,
    http::StatusCode,
    http::header,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::auth::READ_SCOPES;
use common::auth::oauth::{TokenKind, generate_oauth_token, hash_oauth_token, verify_pkce_s256};
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
        .route("/oauth/token", post(token::<S>))
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

/// OAuth token endpoint request (form-encoded, RFC 6749). Fields not relevant
/// to the presented `grant_type` are absent.
#[derive(Debug, Default, Deserialize)]
struct TokenRequest {
    grant_type: String,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    code_verifier: Option<String>,
    #[serde(default)]
    redirect_uri: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
}

/// OAuth token endpoint success response (RFC 6749 §5.1).
#[derive(Debug, Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: &'static str,
    expires_in: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    scope: String,
}

/// Attach `Cache-Control: no-store` (RFC 6749 §5.1) to a token response so
/// credentials are never cached by intermediaries.
fn no_store(body: TokenResponse) -> Response {
    ([(header::CACHE_CONTROL, "no-store")], Json(body)).into_response()
}

/// Token endpoint (RFC 6749). Supports the `authorization_code` grant (with
/// mandatory PKCE) and the `refresh_token` grant. Public clients only — no
/// client authentication is required or accepted.
async fn token<S: RouterState>(
    State(state): State<S>,
    Form(req): Form<TokenRequest>,
) -> Result<Response, OAuthError> {
    match req.grant_type.as_str() {
        "authorization_code" => token_authorization_code(&state, req).await,
        "refresh_token" => token_refresh(&state, req).await,
        other => Err(OAuthError::bad_request(
            "unsupported_grant_type",
            format!("unsupported grant_type: {other}"),
        )),
    }
}

async fn token_authorization_code<S: RouterState>(
    state: &S,
    req: TokenRequest,
) -> Result<Response, OAuthError> {
    let code = req
        .code
        .as_deref()
        .ok_or_else(|| OAuthError::bad_request("invalid_request", "missing code"))?;
    let verifier = req
        .code_verifier
        .as_deref()
        .ok_or_else(|| OAuthError::bad_request("invalid_request", "missing code_verifier"))?;

    // Redeem the code atomically (single-use). An unknown, reused, or expired
    // code returns nothing.
    let grant = state
        .catalog()
        .consume_authorization_code(&hash_oauth_token(code))
        .await
        .map_err(|e| OAuthError::server_error(format!("code lookup failed: {e}")))?
        .ok_or_else(|| {
            OAuthError::bad_request("invalid_grant", "authorization code is invalid or expired")
        })?;

    // The redemption must come from the same client and redirect URI the code
    // was issued to.
    if let Some(client_id) = req.client_id.as_deref()
        && client_id != grant.client_id
    {
        return Err(OAuthError::bad_request(
            "invalid_grant",
            "client_id does not match the authorization code",
        ));
    }
    if let Some(redirect_uri) = req.redirect_uri.as_deref()
        && redirect_uri != grant.redirect_uri
    {
        return Err(OAuthError::bad_request(
            "invalid_grant",
            "redirect_uri does not match the authorization code",
        ));
    }

    // PKCE: the verifier must hash to the challenge bound at /authorize.
    if !verify_pkce_s256(verifier, &grant.code_challenge) {
        return Err(OAuthError::bad_request(
            "invalid_grant",
            "PKCE verification failed",
        ));
    }

    issue_tokens(
        state,
        &grant.client_id,
        &grant.user_id,
        &grant.tenant_id,
        &grant.scopes,
        grant.resource.as_deref(),
        true,
    )
    .await
}

async fn token_refresh<S: RouterState>(
    state: &S,
    req: TokenRequest,
) -> Result<Response, OAuthError> {
    let refresh = req
        .refresh_token
        .as_deref()
        .ok_or_else(|| OAuthError::bad_request("invalid_request", "missing refresh_token"))?;

    let grant = state
        .catalog()
        .get_valid_refresh_token(&hash_oauth_token(refresh))
        .await
        .map_err(|e| OAuthError::server_error(format!("refresh lookup failed: {e}")))?
        .ok_or_else(|| {
            OAuthError::bad_request("invalid_grant", "refresh token is invalid or expired")
        })?;

    if let Some(client_id) = req.client_id.as_deref()
        && client_id != grant.client_id
    {
        return Err(OAuthError::bad_request(
            "invalid_grant",
            "client_id does not match the refresh token",
        ));
    }

    // Mint a fresh access token carrying the same grant; the refresh token is
    // not rotated (the client keeps the one it holds).
    issue_tokens(
        state,
        &grant.client_id,
        &grant.user_id,
        &grant.tenant_id,
        &grant.scopes,
        grant.resource.as_deref(),
        false,
    )
    .await
}

/// Mint an access token (and, when `with_refresh`, a refresh token) for a
/// grant and render the token response.
async fn issue_tokens<S: RouterState>(
    state: &S,
    client_id: &str,
    user_id: &str,
    tenant_id: &str,
    scopes: &[String],
    resource: Option<&str>,
    with_refresh: bool,
) -> Result<Response, OAuthError> {
    let oauth = &state.config().mcp.oauth;
    let now = chrono::Utc::now();
    let access_ttl = chrono::Duration::from_std(oauth.access_token_ttl)
        .map_err(|e| OAuthError::server_error(format!("invalid access_token_ttl: {e}")))?;

    let access_raw = generate_oauth_token(TokenKind::Access);
    state
        .catalog()
        .create_access_token(
            &hash_oauth_token(&access_raw),
            client_id,
            user_id,
            tenant_id,
            scopes,
            resource,
            now + access_ttl,
        )
        .await
        .map_err(|e| OAuthError::server_error(format!("failed to store access token: {e}")))?;

    let refresh_raw = if with_refresh {
        let refresh_ttl = chrono::Duration::from_std(oauth.refresh_token_ttl)
            .map_err(|e| OAuthError::server_error(format!("invalid refresh_token_ttl: {e}")))?;
        let raw = generate_oauth_token(TokenKind::Refresh);
        state
            .catalog()
            .create_refresh_token(
                &hash_oauth_token(&raw),
                client_id,
                user_id,
                tenant_id,
                scopes,
                resource,
                now + refresh_ttl,
            )
            .await
            .map_err(|e| OAuthError::server_error(format!("failed to store refresh token: {e}")))?;
        Some(raw)
    } else {
        None
    };

    Ok(no_store(TokenResponse {
        access_token: access_raw,
        token_type: "Bearer",
        expires_in: oauth.access_token_ttl.as_secs(),
        refresh_token: refresh_raw,
        scope: scopes.join(" "),
    }))
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

    // Canonical RFC 7636 Appendix B PKCE pair.
    const PKCE_VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    const PKCE_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

    fn oauth_config() -> OAuthConfig {
        OAuthConfig {
            enabled: true,
            issuer_url: Some("https://signaldb.example.com".to_string()),
            resource_url: Some("https://signaldb.example.com/mcp".to_string()),
            ..Default::default()
        }
    }

    async fn oauth_app() -> axum::Router {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration::default();
        config.mcp.oauth = oauth_config();
        create_router(RouterAppState::new(catalog, config))
    }

    /// An app plus a catalog handle sharing its in-memory DB, so tests can seed
    /// clients and authorization codes the router then reads.
    async fn app_and_catalog() -> (axum::Router, Catalog) {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let mut config = Configuration::default();
        config.mcp.oauth = oauth_config();
        let app = create_router(RouterAppState::new(catalog.clone(), config));
        (app, catalog)
    }

    /// Seed a user, tenant, client, and an authorization code (bound to the
    /// PKCE challenge), returning the raw code to redeem.
    async fn seed_authorization_code(catalog: &Catalog, code: &str) -> String {
        use common::auth::oauth::hash_oauth_token;
        let user = catalog
            .create_user("agent@example.com", None, "phc", false)
            .await
            .unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        catalog
            .register_oauth_client(
                "client-1",
                Some("Claude"),
                &["https://claude.ai/cb".to_string()],
                None,
                None,
                "none",
            )
            .await
            .unwrap();
        catalog
            .create_authorization_code(
                &hash_oauth_token(code),
                "client-1",
                &user.id,
                "acme",
                &["traces:read".to_string()],
                "https://claude.ai/cb",
                PKCE_CHALLENGE,
                Some("https://signaldb.example.com/mcp"),
                chrono::Utc::now() + chrono::Duration::minutes(1),
            )
            .await
            .unwrap();
        code.to_string()
    }

    async fn post_token(app: &axum::Router, body: String) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/token")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap()
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

    #[tokio::test]
    async fn token_code_with_matching_verifier_yields_tokens() {
        let (app, catalog) = app_and_catalog().await;
        seed_authorization_code(&catalog, "raw-code-1").await;
        let res = post_token(
            &app,
            format!(
                "grant_type=authorization_code&code=raw-code-1&code_verifier={PKCE_VERIFIER}\
                 &redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&client_id=client-1"
            ),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let doc = body_json(res).await;
        assert_eq!(doc["token_type"], "Bearer");
        assert!(doc["access_token"].as_str().is_some_and(|s| !s.is_empty()));
        assert!(doc["refresh_token"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(doc["scope"], "traces:read");
    }

    #[tokio::test]
    async fn token_pkce_mismatch_is_invalid_grant() {
        let (app, catalog) = app_and_catalog().await;
        seed_authorization_code(&catalog, "raw-code-2").await;
        let res = post_token(
            &app,
            "grant_type=authorization_code&code=raw-code-2&code_verifier=wrong-verifier\
             &redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&client_id=client-1"
                .to_string(),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(res).await["error"], "invalid_grant");
    }

    #[tokio::test]
    async fn token_code_is_single_use() {
        let (app, catalog) = app_and_catalog().await;
        seed_authorization_code(&catalog, "raw-code-3").await;
        let body = format!(
            "grant_type=authorization_code&code=raw-code-3&code_verifier={PKCE_VERIFIER}\
             &redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&client_id=client-1"
        );
        let first = post_token(&app, body.clone()).await;
        assert_eq!(first.status(), StatusCode::OK);
        // A replay of the same code fails: it was consumed.
        let second = post_token(&app, body).await;
        assert_eq!(second.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(second).await["error"], "invalid_grant");
    }

    #[tokio::test]
    async fn refresh_grant_mints_new_access_token() {
        let (app, catalog) = app_and_catalog().await;
        seed_authorization_code(&catalog, "raw-code-4").await;
        // Obtain a refresh token first.
        let res = post_token(
            &app,
            format!(
                "grant_type=authorization_code&code=raw-code-4&code_verifier={PKCE_VERIFIER}\
                 &redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&client_id=client-1"
            ),
        )
        .await;
        let refresh = body_json(res).await["refresh_token"]
            .as_str()
            .unwrap()
            .to_string();

        let res = post_token(
            &app,
            format!("grant_type=refresh_token&refresh_token={refresh}&client_id=client-1"),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let doc = body_json(res).await;
        assert!(doc["access_token"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(doc["scope"], "traces:read");
    }
}
