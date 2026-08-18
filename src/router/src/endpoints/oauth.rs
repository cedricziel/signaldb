//! # OAuth 2.1 authorization-server endpoints (change: mcp-oauth-dcr)
//!
//! The router is SignalDB's OAuth 2.1 Authorization Server for the MCP surface.
//! This module serves the pieces that let Claude.ai and OpenAI/ChatGPT register
//! and obtain tokens with no human pre-registration:
//!
//! - `GET /.well-known/oauth-authorization-server` — RFC 8414 metadata
//! - `POST /oauth/register` — RFC 7591 Dynamic Client Registration
//! - `GET /oauth/authorize` — authorization endpoint (PKCE mandatory)
//! - `POST /oauth/token` — token endpoint (authorization-code + refresh)
//! - `GET /oauth/consent/context`, `POST /oauth/authorize/decision` — consent
//!   screen support for the explore-UI
//!
//! Discovery, DCR, `/authorize`, and `/token` are public (unauthenticated by
//! spec; `/authorize` sends the user through login itself). The two consent
//! endpoints are **not** public — they require a valid browser session cookie.

use axum::{
    Form, Json, Router,
    extract::{Query, State},
    http::HeaderMap,
    http::StatusCode,
    http::header,
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use common::auth::oauth::{TokenKind, generate_oauth_token, hash_oauth_token, verify_pkce_s256};
use common::auth::{READ_SCOPES, hash_session_token, session_token_from_headers};
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::Url;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::RouterState;

/// An OAuth error rendered in the RFC 6749 §5.2 shape
/// (`{"error": ..., "error_description": ...}`) with an appropriate status.
pub(crate) struct OAuthError {
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
        .route("/oauth/authorize", get(authorize::<S>))
        .route("/oauth/consent/context", get(consent_context::<S>))
        .route("/oauth/authorize/decision", post(authorize_decision::<S>))
        .route("/oauth/token", post(token::<S>))
}

/// The signal read scopes a consent may grant, as a `Vec` for convenience.
fn all_read_scopes() -> Vec<String> {
    READ_SCOPES.iter().map(|s| s.to_string()).collect()
}

/// The read scopes to grant for a requested `scope` string. `None` (no `scope`
/// requested) grants all read scopes — a sensible default for a read-only
/// connector. A `scope` that names read scopes grants exactly the recognized
/// ones. A `scope` that names *only* unrecognized scopes (e.g. `openid`,
/// `traces:write`) returns `None` so the caller can reject with `invalid_scope`
/// (RFC 6749 §4.1.2.1) — never silently widen the grant to all read scopes.
fn granted_read_scopes(requested: Option<&str>) -> Option<Vec<String>> {
    match requested {
        None => Some(all_read_scopes()),
        Some(scope) => {
            let granted: Vec<String> = scope
                .split_whitespace()
                .filter(|s| READ_SCOPES.contains(s))
                .map(str::to_string)
                .collect();
            if granted.is_empty() {
                None
            } else {
                Some(granted)
            }
        }
    }
}

/// Append query parameters to an absolute redirect URI, preserving any it
/// already carries.
fn redirect_with_params(base: &str, params: &[(&str, &str)]) -> Result<String, OAuthError> {
    let mut url = Url::parse(base).map_err(|_| {
        OAuthError::bad_request("invalid_request", "redirect_uri is not a valid URL")
    })?;
    {
        let mut pairs = url.query_pairs_mut();
        for (k, v) in params {
            pairs.append_pair(k, v);
        }
    }
    Ok(url.to_string())
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

/// Whether a redirect URI is acceptable: an absolute URL with an authority,
/// `https` for any host, or `http` **only for loopback** (`localhost`,
/// `127.0.0.1`, `[::1]`). Cleartext `http` to a non-loopback host would leak the
/// authorization code and `state` (OAuth 2.1 / RFC 8252 §7.3).
fn is_valid_redirect_uri(uri: &str) -> bool {
    let Ok(parsed) = uri.parse::<axum::http::Uri>() else {
        return false;
    };
    let Some(authority) = parsed.authority() else {
        return false;
    };
    match parsed.scheme_str() {
        Some("https") => true,
        Some("http") => matches!(
            authority.host(),
            "localhost" | "127.0.0.1" | "[::1]" | "::1"
        ),
        _ => false,
    }
}

/// Bounds on a dynamic registration, since the endpoint is unauthenticated
/// (RFC 7591 §5). These cap the damage of an anonymous caller: `client_name`
/// renders on the consent screen, so an unbounded value enables spoofing.
/// (Per-IP rate limiting is a further control tracked as a follow-up.)
const MAX_REDIRECT_URIS: usize = 8;
const MAX_REDIRECT_URI_LEN: usize = 2048;
const MAX_CLIENT_NAME_LEN: usize = 256;

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
    if req.redirect_uris.len() > MAX_REDIRECT_URIS {
        return Err(OAuthError::bad_request(
            "invalid_client_metadata",
            format!("at most {MAX_REDIRECT_URIS} redirect_uris are allowed"),
        ));
    }
    if req
        .redirect_uris
        .iter()
        .any(|u| u.len() > MAX_REDIRECT_URI_LEN)
    {
        return Err(OAuthError::bad_request(
            "invalid_redirect_uri",
            "redirect_uri is too long",
        ));
    }
    if req
        .client_name
        .as_deref()
        .is_some_and(|n| n.len() > MAX_CLIENT_NAME_LEN)
    {
        return Err(OAuthError::bad_request(
            "invalid_client_metadata",
            format!("client_name must be at most {MAX_CLIENT_NAME_LEN} characters"),
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

/// Authorization endpoint query parameters (RFC 6749 §4.1.1 + PKCE).
#[derive(Debug, Default, Deserialize)]
struct AuthorizeParams {
    #[serde(default)]
    response_type: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    redirect_uri: Option<String>,
    #[serde(default)]
    code_challenge: Option<String>,
    #[serde(default)]
    code_challenge_method: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    resource: Option<String>,
}

/// Authorization endpoint (RFC 6749 §4.1). Validates the request against the
/// registered client — refusing to redirect anywhere untrusted — then hands off
/// to the consent screen (a route in the explore-UI served at root). The code
/// is minted by [`authorize_decision`] once the human approves. PKCE is
/// mandatory.
async fn authorize<S: RouterState>(
    State(state): State<S>,
    Query(p): Query<AuthorizeParams>,
) -> Result<Response, OAuthError> {
    // Validate client and redirect_uri BEFORE trusting the redirect target;
    // errors here cannot be sent to an unverified URI, so they are direct 400s.
    let client_id = p
        .client_id
        .as_deref()
        .ok_or_else(|| OAuthError::bad_request("invalid_request", "missing client_id"))?;
    let client = state
        .catalog()
        .get_oauth_client(client_id)
        .await
        .map_err(|e| OAuthError::server_error(format!("client lookup failed: {e}")))?
        .ok_or_else(|| OAuthError::bad_request("invalid_client", "unknown client_id"))?;
    let redirect_uri = p
        .redirect_uri
        .as_deref()
        .ok_or_else(|| OAuthError::bad_request("invalid_request", "missing redirect_uri"))?;
    if !client.redirect_uris.iter().any(|u| u == redirect_uri) {
        return Err(OAuthError::bad_request(
            "invalid_request",
            "redirect_uri is not registered for this client",
        ));
    }

    // The redirect target is now trusted; validation failures below are
    // reported to it per RFC 6749 §4.1.2.1.
    let state_param = p.state.as_deref().unwrap_or_default();
    if p.response_type.as_deref() != Some("code") {
        let url = redirect_with_params(
            redirect_uri,
            &[
                ("error", "unsupported_response_type"),
                ("state", state_param),
            ],
        )?;
        return Ok(Redirect::to(&url).into_response());
    }
    let pkce_ok = p.code_challenge.as_deref().is_some_and(|c| !c.is_empty())
        && p.code_challenge_method.as_deref().unwrap_or("S256") == "S256";
    if !pkce_ok {
        let url = redirect_with_params(
            redirect_uri,
            &[("error", "invalid_request"), ("state", state_param)],
        )?;
        return Ok(Redirect::to(&url).into_response());
    }

    // Hand off to the consent screen, echoing the validated request. The
    // decision endpoint re-validates everything, so nothing here is trusted on
    // return.
    let mut consent = url::form_urlencoded::Serializer::new(String::new());
    consent.append_pair("client_id", client_id);
    consent.append_pair("redirect_uri", redirect_uri);
    if let Some(cc) = p.code_challenge.as_deref() {
        consent.append_pair("code_challenge", cc);
    }
    consent.append_pair("code_challenge_method", "S256");
    if let Some(scope) = p.scope.as_deref() {
        consent.append_pair("scope", scope);
    }
    if let Some(st) = p.state.as_deref() {
        consent.append_pair("state", st);
    }
    if let Some(res) = p.resource.as_deref() {
        consent.append_pair("resource", res);
    }
    let consent_url = format!("/oauth/consent?{}", consent.finish());
    Ok(Redirect::to(&consent_url).into_response())
}

/// Consent decision posted by the explore-UI (change: mcp-oauth-dcr). The user
/// is authenticated by their session cookie; `tenant` is their chosen grant.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ConsentDecision {
    /// The requesting client's `client_id`.
    client_id: String,
    /// The redirect URI to return to (must be registered for the client).
    redirect_uri: String,
    /// The PKCE `code_challenge` from the authorization request.
    code_challenge: String,
    #[serde(default)]
    code_challenge_method: Option<String>,
    /// Requested scope (space-delimited); read scopes only are granted.
    #[serde(default)]
    scope: Option<String>,
    /// Opaque `state` to echo back to the client.
    #[serde(default)]
    state: Option<String>,
    /// Requested resource (audience); must match the configured MCP resource.
    #[serde(default)]
    resource: Option<String>,
    /// The tenant the user grants access to (must be one they belong to).
    tenant: String,
    /// Whether the user approved (`true`) or denied (`false`).
    approved: bool,
}

/// Result of a consent decision: the URL the browser should navigate to (the
/// client's redirect URI carrying either the authorization `code` or an
/// `error`).
#[derive(Debug, Serialize, ToSchema)]
pub struct ConsentDecisionResponse {
    /// The absolute redirect URL to navigate to.
    redirect: String,
}

/// Record the human's consent decision and, on approval, mint the single-use
/// authorization code. Authenticated by the browser session cookie; the code is
/// bound to the chosen tenant (which the user must be a member of), the granted
/// read scopes, the client, the redirect URI, the PKCE challenge, and the
/// resource. Returns the URL the SPA should navigate to.
#[utoipa::path(
    post,
    path = "/oauth/authorize/decision",
    tag = "oauth",
    operation_id = "oauth_consent_decision",
    security(()),
    request_body = ConsentDecision,
    responses(
        (status = 200, description = "Decision recorded; navigate to `redirect`", body = ConsentDecisionResponse),
        (status = 401, description = "No active session (login required)"),
        (status = 403, description = "Not a member of the selected tenant"),
        (status = 400, description = "Invalid client, redirect URI, or PKCE"),
    )
)]
pub(crate) async fn authorize_decision<S: RouterState>(
    State(state): State<S>,
    headers: HeaderMap,
    Json(d): Json<ConsentDecision>,
) -> Result<Response, OAuthError> {
    // Authenticate the consenting user from their browser session.
    let token = session_token_from_headers(&headers).ok_or_else(|| {
        OAuthError::new(
            StatusCode::UNAUTHORIZED,
            "login_required",
            "no active session",
        )
    })?;
    let session = state
        .catalog()
        .get_valid_session(&hash_session_token(&token))
        .await
        .map_err(|e| OAuthError::server_error(format!("session lookup failed: {e}")))?
        .ok_or_else(|| {
            OAuthError::new(
                StatusCode::UNAUTHORIZED,
                "login_required",
                "session is invalid or expired",
            )
        })?;
    let user = state
        .catalog()
        .get_user(&session.user_id)
        .await
        .map_err(|e| OAuthError::server_error(format!("user lookup failed: {e}")))?
        .ok_or_else(|| {
            OAuthError::new(
                StatusCode::UNAUTHORIZED,
                "login_required",
                "session user not found",
            )
        })?;

    // Re-validate the client and redirect URI (nothing from the SPA is trusted).
    let client = state
        .catalog()
        .get_oauth_client(&d.client_id)
        .await
        .map_err(|e| OAuthError::server_error(format!("client lookup failed: {e}")))?
        .ok_or_else(|| OAuthError::bad_request("invalid_client", "unknown client_id"))?;
    if !client.redirect_uris.iter().any(|u| u == &d.redirect_uri) {
        return Err(OAuthError::bad_request(
            "invalid_request",
            "redirect_uri is not registered for this client",
        ));
    }
    if d.code_challenge.is_empty() || d.code_challenge_method.as_deref().unwrap_or("S256") != "S256"
    {
        return Err(OAuthError::bad_request(
            "invalid_request",
            "a S256 code_challenge is required",
        ));
    }

    let state_param = d.state.as_deref().unwrap_or_default();

    // Denial: bounce back to the client with an access_denied error.
    if !d.approved {
        let url = redirect_with_params(
            &d.redirect_uri,
            &[("error", "access_denied"), ("state", state_param)],
        )?;
        return Ok(Json(ConsentDecisionResponse { redirect: url }).into_response());
    }

    // The user may only grant a tenant they belong to. An instance admin may
    // grant any tenant, but it must actually exist — otherwise a code would be
    // minted for a non-existent tenant.
    let membership = state
        .catalog()
        .get_tenant_membership(&user.id, &d.tenant)
        .await
        .map_err(|e| OAuthError::server_error(format!("membership lookup failed: {e}")))?;
    if membership.is_none() {
        let grantable = user.is_instance_admin
            && state
                .catalog()
                .get_tenant(&d.tenant)
                .await
                .map_err(|e| OAuthError::server_error(format!("tenant lookup failed: {e}")))?
                .is_some();
        if !grantable {
            return Err(OAuthError::new(
                StatusCode::FORBIDDEN,
                "access_denied",
                "not a member of the selected tenant",
            ));
        }
    }

    // Bind the token audience to the configured MCP resource (RFC 8707): reject
    // a client-supplied `resource` that isn't the one we serve — including when
    // we serve none, in which case a client cannot choose its own audience.
    let configured = state.config().mcp.oauth.resource_url.clone();
    let resource = match (d.resource.as_deref(), configured.as_deref()) {
        (Some(requested), Some(cfg)) if requested != cfg => {
            return Err(OAuthError::bad_request(
                "invalid_target",
                "requested resource is not served here",
            ));
        }
        (Some(_), None) => {
            return Err(OAuthError::bad_request(
                "invalid_target",
                "this server does not serve a configured MCP resource",
            ));
        }
        (Some(requested), Some(_)) => Some(requested.to_string()),
        (None, cfg) => cfg.map(str::to_string),
    };

    // Grant the requested read scopes; a scope that names no read scope is an
    // invalid request rather than a licence to grant everything.
    let scopes = granted_read_scopes(d.scope.as_deref()).ok_or_else(|| {
        OAuthError::bad_request(
            "invalid_scope",
            "requested scope contains no supported read scope",
        )
    })?;
    let code = generate_oauth_token(TokenKind::AuthorizationCode);
    let ttl = chrono::Duration::from_std(state.config().mcp.oauth.authorization_code_ttl)
        .map_err(|e| OAuthError::server_error(format!("invalid authorization_code_ttl: {e}")))?;
    state
        .catalog()
        .create_authorization_code(
            &hash_oauth_token(&code),
            &d.client_id,
            &user.id,
            &d.tenant,
            &scopes,
            &d.redirect_uri,
            &d.code_challenge,
            resource.as_deref(),
            chrono::Utc::now() + ttl,
        )
        .await
        .map_err(|e| {
            OAuthError::server_error(format!("failed to store authorization code: {e}"))
        })?;

    let url = redirect_with_params(&d.redirect_uri, &[("code", &code), ("state", state_param)])?;
    Ok(Json(ConsentDecisionResponse { redirect: url }).into_response())
}

/// Query parameters for the consent-context lookup.
#[derive(Debug, Deserialize, IntoParams)]
pub(crate) struct ConsentContextQuery {
    /// The requesting client's `client_id`.
    client_id: String,
}

/// A tenant the consenting user may grant a connector access to.
#[derive(Debug, Serialize, ToSchema)]
pub struct ConsentTenant {
    /// Tenant id.
    id: String,
    /// The user's role in the tenant.
    role: common::catalog::MembershipRole,
}

/// Context the consent screen renders: the requesting client and the tenants
/// the signed-in user may grant.
#[derive(Debug, Serialize, ToSchema)]
pub struct ConsentContextResponse {
    /// Display name of the requesting client, if it registered one.
    #[serde(skip_serializing_if = "Option::is_none")]
    client_name: Option<String>,
    /// Tenants the user may grant access to.
    tenants: Vec<ConsentTenant>,
}

/// Provide the consent screen's context: the requesting client's name and the
/// tenants the signed-in user may grant. Authenticated by the browser session
/// cookie — the consent screen runs before any tenant is chosen, so unlike
/// `whoami` this needs no tenant context.
#[utoipa::path(
    get,
    path = "/oauth/consent/context",
    tag = "oauth",
    operation_id = "oauth_consent_context",
    security(()),
    params(ConsentContextQuery),
    responses(
        (status = 200, description = "Consent context", body = ConsentContextResponse),
        (status = 401, description = "No active session (login required)"),
        (status = 400, description = "Unknown client"),
    )
)]
pub(crate) async fn consent_context<S: RouterState>(
    State(state): State<S>,
    headers: HeaderMap,
    Query(q): Query<ConsentContextQuery>,
) -> Result<Response, OAuthError> {
    let token = session_token_from_headers(&headers).ok_or_else(|| {
        OAuthError::new(
            StatusCode::UNAUTHORIZED,
            "login_required",
            "no active session",
        )
    })?;
    let session = state
        .catalog()
        .get_valid_session(&hash_session_token(&token))
        .await
        .map_err(|e| OAuthError::server_error(format!("session lookup failed: {e}")))?
        .ok_or_else(|| {
            OAuthError::new(
                StatusCode::UNAUTHORIZED,
                "login_required",
                "session is invalid or expired",
            )
        })?;
    let user = state
        .catalog()
        .get_user(&session.user_id)
        .await
        .map_err(|e| OAuthError::server_error(format!("user lookup failed: {e}")))?
        .ok_or_else(|| {
            OAuthError::new(
                StatusCode::UNAUTHORIZED,
                "login_required",
                "session user not found",
            )
        })?;

    let client = state
        .catalog()
        .get_oauth_client(&q.client_id)
        .await
        .map_err(|e| OAuthError::server_error(format!("client lookup failed: {e}")))?
        .ok_or_else(|| OAuthError::bad_request("invalid_client", "unknown client_id"))?;

    let tenants = if user.is_instance_admin {
        state
            .catalog()
            .list_tenants()
            .await
            .map_err(|e| OAuthError::server_error(format!("tenant lookup failed: {e}")))?
            .into_iter()
            .map(|t| ConsentTenant {
                id: t.id,
                role: common::catalog::MembershipRole::Admin,
            })
            .collect()
    } else {
        state
            .catalog()
            .list_memberships_for_user(&user.id)
            .await
            .map_err(|e| OAuthError::server_error(format!("membership lookup failed: {e}")))?
            .into_iter()
            .map(|m| ConsentTenant {
                id: m.tenant_id,
                role: m.role,
            })
            .collect()
    };

    Ok(Json(ConsentContextResponse {
        client_name: client.client_name,
        tenants,
    })
    .into_response())
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
    // was issued to. `client_id` is required for public clients (RFC 6749
    // §4.1.3), and `redirect_uri` must match — the grant always records one.
    let client_id = req
        .client_id
        .as_deref()
        .ok_or_else(|| OAuthError::bad_request("invalid_request", "missing client_id"))?;
    if client_id != grant.client_id {
        return Err(OAuthError::bad_request(
            "invalid_grant",
            "client_id does not match the authorization code",
        ));
    }
    let redirect_uri = req
        .redirect_uri
        .as_deref()
        .ok_or_else(|| OAuthError::bad_request("invalid_request", "missing redirect_uri"))?;
    if redirect_uri != grant.redirect_uri {
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

    let client_id = req
        .client_id
        .as_deref()
        .ok_or_else(|| OAuthError::bad_request("invalid_request", "missing client_id"))?;
    if client_id != grant.client_id {
        return Err(OAuthError::bad_request(
            "invalid_grant",
            "client_id does not match the refresh token",
        ));
    }

    // Rotate the refresh token (OAuth 2.1 §4.3.1 for public clients): the
    // presented token is single-use, so revoke it before issuing a fresh
    // access + refresh pair. Detecting replay of an already-consumed token to
    // revoke the whole grant family is a tracked follow-up.
    state
        .catalog()
        .revoke_refresh_token(&hash_oauth_token(refresh))
        .await
        .map_err(|e| OAuthError::server_error(format!("failed to revoke refresh token: {e}")))?;

    issue_tokens(
        state,
        &grant.client_id,
        &grant.user_id,
        &grant.tenant_id,
        &grant.scopes,
        grant.resource.as_deref(),
    )
    .await
}

/// Mint an access token and a refresh token for a grant and render the
/// token response.
async fn issue_tokens<S: RouterState>(
    state: &S,
    client_id: &str,
    user_id: &str,
    tenant_id: &str,
    scopes: &[String],
    resource: Option<&str>,
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

    let refresh_ttl = chrono::Duration::from_std(oauth.refresh_token_ttl)
        .map_err(|e| OAuthError::server_error(format!("invalid refresh_token_ttl: {e}")))?;
    let refresh_raw = generate_oauth_token(TokenKind::Refresh);
    state
        .catalog()
        .create_refresh_token(
            &hash_oauth_token(&refresh_raw),
            client_id,
            user_id,
            tenant_id,
            scopes,
            resource,
            now + refresh_ttl,
        )
        .await
        .map_err(|e| OAuthError::server_error(format!("failed to store refresh token: {e}")))?;

    Ok(no_store(TokenResponse {
        access_token: access_raw,
        token_type: "Bearer",
        expires_in: oauth.access_token_ttl.as_secs(),
        refresh_token: Some(refresh_raw),
        scope: scopes.join(" "),
    }))
}

#[cfg(test)]
mod tests {
    use super::hash_oauth_token;
    use crate::{RouterAppState, create_router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use common::catalog::Catalog;
    use common::config::{Configuration, OAuthConfig};
    use serde_json::Value;
    use tower::ServiceExt;
    use url::Url;

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

    async fn seed_client(catalog: &Catalog) {
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
    }

    /// Create a user who is a member of `tenant`, with a live session; return
    /// the raw session cookie value.
    async fn seed_user_session(catalog: &Catalog, tenant: &str) -> String {
        use common::auth::{generate_session_token, hash_session_token};
        catalog
            .upsert_tenant(tenant, tenant, Some("production"), "database")
            .await
            .unwrap();
        let user = catalog
            .create_user("human@example.com", None, "phc", false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, tenant, common::catalog::MembershipRole::Member)
            .await
            .unwrap();
        let cookie = generate_session_token();
        catalog
            .create_user_session(
                &user.id,
                &hash_session_token(&cookie),
                chrono::Utc::now() + chrono::Duration::hours(1),
            )
            .await
            .unwrap();
        cookie
    }

    #[tokio::test]
    async fn authorize_without_pkce_redirects_with_error() {
        let (app, catalog) = app_and_catalog().await;
        seed_client(&catalog).await;
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/oauth/authorize?response_type=code&client_id=client-1&redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&state=xyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(res.status().is_redirection());
        let location = res
            .headers()
            .get(axum::http::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(location.starts_with("https://claude.ai/cb"), "{location}");
        assert!(location.contains("error=invalid_request"), "{location}");
    }

    #[tokio::test]
    async fn authorize_with_valid_request_redirects_to_consent() {
        let (app, catalog) = app_and_catalog().await;
        seed_client(&catalog).await;
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/oauth/authorize?response_type=code&client_id=client-1&redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&code_challenge=abc&code_challenge_method=S256")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(res.status().is_redirection());
        let location = res
            .headers()
            .get(axum::http::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(location.starts_with("/oauth/consent?"), "{location}");
        assert!(location.contains("client_id=client-1"), "{location}");
    }

    #[tokio::test]
    async fn approved_decision_issues_a_bound_code() {
        let (app, catalog) = app_and_catalog().await;
        seed_client(&catalog).await;
        let cookie = seed_user_session(&catalog, "acme").await;

        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/authorize/decision")
                    .header("content-type", "application/json")
                    .header("cookie", format!("signaldb_session={cookie}"))
                    .body(Body::from(
                        r#"{"client_id":"client-1","redirect_uri":"https://claude.ai/cb","code_challenge":"chal-1","scope":"traces:read","state":"st","tenant":"acme","approved":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let doc = body_json(res).await;
        let redirect = doc["redirect"].as_str().unwrap();
        assert!(redirect.starts_with("https://claude.ai/cb?"), "{redirect}");
        assert!(redirect.contains("code="), "{redirect}");
        assert!(redirect.contains("state=st"), "{redirect}");

        // The issued code is bound to the chosen tenant, scope, and challenge.
        let code = Url::parse(redirect)
            .unwrap()
            .query_pairs()
            .find(|(k, _)| k == "code")
            .map(|(_, v)| v.into_owned())
            .unwrap();
        let grant = catalog
            .consume_authorization_code(&hash_oauth_token(&code))
            .await
            .unwrap()
            .expect("code was stored");
        assert_eq!(grant.tenant_id, "acme");
        assert_eq!(grant.scopes, vec!["traces:read".to_string()]);
        assert_eq!(grant.code_challenge, "chal-1");
        assert_eq!(grant.client_id, "client-1");
    }

    #[tokio::test]
    async fn decision_rejects_tenant_the_user_is_not_a_member_of() {
        let (app, catalog) = app_and_catalog().await;
        seed_client(&catalog).await;
        let cookie = seed_user_session(&catalog, "acme").await;
        // The user is a member of acme only; granting globex must fail even
        // though the tenant row exists.
        catalog
            .upsert_tenant("globex", "Globex", Some("production"), "database")
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/authorize/decision")
                    .header("content-type", "application/json")
                    .header("cookie", format!("signaldb_session={cookie}"))
                    .body(Body::from(
                        r#"{"client_id":"client-1","redirect_uri":"https://claude.ai/cb","code_challenge":"chal-1","tenant":"globex","approved":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn decision_without_session_requires_login() {
        let (app, catalog) = app_and_catalog().await;
        seed_client(&catalog).await;
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/authorize/decision")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"client_id":"client-1","redirect_uri":"https://claude.ai/cb","code_challenge":"chal-1","tenant":"acme","approved":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    // ---- CodeRabbit review hardening (change: mcp-oauth-dcr) ----

    async fn register_with(app: &axum::Router, body: &str) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/register")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn register_rejects_non_loopback_http_redirect() {
        let app = oauth_app().await;
        let res = register_with(&app, r#"{"redirect_uris":["http://attacker.example/cb"]}"#).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(res).await["error"], "invalid_redirect_uri");
    }

    #[tokio::test]
    async fn register_accepts_loopback_http_redirect() {
        let app = oauth_app().await;
        let res = register_with(&app, r#"{"redirect_uris":["http://127.0.0.1:9273/cb"]}"#).await;
        assert_eq!(res.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn register_rejects_too_many_redirect_uris() {
        let app = oauth_app().await;
        let uris: Vec<String> = (0..9)
            .map(|i| format!("\"https://c.example/{i}\""))
            .collect();
        let body = format!("{{\"redirect_uris\":[{}]}}", uris.join(","));
        let res = register_with(&app, &body).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(res).await["error"], "invalid_client_metadata");
    }

    #[tokio::test]
    async fn decision_rejects_scope_with_no_read_scope() {
        let (app, catalog) = app_and_catalog().await;
        seed_client(&catalog).await;
        let cookie = seed_user_session(&catalog, "acme").await;
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/authorize/decision")
                    .header("content-type", "application/json")
                    .header("cookie", format!("signaldb_session={cookie}"))
                    .body(Body::from(
                        r#"{"client_id":"client-1","redirect_uri":"https://claude.ai/cb","code_challenge":"c","scope":"openid profile","tenant":"acme","approved":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(res).await["error"], "invalid_scope");
    }

    #[test]
    fn default_grant_includes_schema_read_but_never_schema_write() {
        let granted = super::granted_read_scopes(None).expect("default grant");
        assert!(granted.iter().any(|s| s == "schema:read"));
        assert!(!granted.iter().any(|s| s == "schema:write"));

        assert_eq!(
            super::granted_read_scopes(Some("schema:read traces:read")),
            Some(vec!["schema:read".to_string(), "traces:read".to_string()])
        );
        // schema:write is not a read scope: a request naming only it is
        // rejected instead of silently widened.
        assert_eq!(super::granted_read_scopes(Some("schema:write")), None);
    }

    #[test]
    fn oauth_consent_never_grants_tenant_manage() {
        use common::auth::TENANT_MANAGE_SCOPE;
        let granted = super::granted_read_scopes(None).expect("default grant");
        assert!(!granted.iter().any(|s| s == TENANT_MANAGE_SCOPE));
        assert_eq!(super::granted_read_scopes(Some(TENANT_MANAGE_SCOPE)), None);
        assert_eq!(
            super::granted_read_scopes(Some("traces:read tenant:manage")),
            Some(vec!["traces:read".to_string()])
        );
        assert!(!common::auth::READ_SCOPES.contains(&TENANT_MANAGE_SCOPE));
    }

    #[tokio::test]
    async fn decision_rejects_schema_write_only_scope() {
        let (app, catalog) = app_and_catalog().await;
        seed_client(&catalog).await;
        let cookie = seed_user_session(&catalog, "acme").await;
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/authorize/decision")
                    .header("content-type", "application/json")
                    .header("cookie", format!("signaldb_session={cookie}"))
                    .body(Body::from(
                        r#"{"client_id":"client-1","redirect_uri":"https://claude.ai/cb","code_challenge":"c","scope":"schema:write","tenant":"acme","approved":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(res).await["error"], "invalid_scope");
    }

    #[tokio::test]
    async fn token_exchange_requires_client_id() {
        let (app, catalog) = app_and_catalog().await;
        seed_authorization_code(&catalog, "raw-code-noclient").await;
        let res = post_token(
            &app,
            format!(
                "grant_type=authorization_code&code=raw-code-noclient&code_verifier={PKCE_VERIFIER}\
                 &redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb"
            ),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(res).await["error"], "invalid_request");
    }

    #[tokio::test]
    async fn refresh_rotation_revokes_the_presented_token() {
        let (app, catalog) = app_and_catalog().await;
        seed_authorization_code(&catalog, "raw-code-rot").await;
        let tokens = body_json(
            post_token(
                &app,
                format!(
                    "grant_type=authorization_code&code=raw-code-rot&code_verifier={PKCE_VERIFIER}\
                     &redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&client_id=client-1"
                ),
            )
            .await,
        )
        .await;
        let refresh1 = tokens["refresh_token"].as_str().unwrap().to_string();

        // First refresh succeeds and returns a NEW refresh token.
        let r = body_json(
            post_token(
                &app,
                format!("grant_type=refresh_token&refresh_token={refresh1}&client_id=client-1"),
            )
            .await,
        )
        .await;
        let refresh2 = r["refresh_token"].as_str().unwrap().to_string();
        assert_ne!(refresh1, refresh2, "refresh token must rotate");

        // The original refresh token is now revoked (single-use).
        let replay = post_token(
            &app,
            format!("grant_type=refresh_token&refresh_token={refresh1}&client_id=client-1"),
        )
        .await;
        assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(replay).await["error"], "invalid_grant");
    }
}
