//! # Demo-account write guard
//!
//! Defense in depth for `[demo]` (change: demo-mode), on top of the
//! ordinary `MembershipRole::Viewer` write denial `TenantContext::can_write`
//! already enforces: an axum middleware that refuses every non-read request
//! from the demo user's session outright, before the request reaches a
//! handler that might not itself be tenant-scoped (session cookie
//! management, API key creation, and any endpoint the Viewer check does not
//! cover).
//!
//! A no-op when `[demo]` is disabled, and a no-op for any request that
//! isn't carrying the demo user's own session cookie — every other caller
//! is unaffected.

use crate::RouterState;
use axum::{
    Json,
    body::Body,
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use common::auth::{hash_session_token, session_token_from_headers};
use serde_json::json;

/// Read-only POST endpoints the Explore UI calls that must stay reachable
/// for the demo account even though the method is POST:
///
/// - `/ui/session` (login) — a demo visitor must be able to sign in at all;
///   `DELETE /ui/session` (logout) is exempted the same way.
/// - `/api/v1/query` — the Query IR is the read surface itself.
/// - `/api/v1/tenants/{tenant_id}/source-context` — fetches a read-only
///   GitHub source snippet for a span/log attribute; persists nothing.
const READ_ONLY_POST_PATHS: &[&str] = &["/ui/session", "/api/v1/query"];

/// Matches `/api/v1/tenants/{tenant_id}/source-context` for any
/// `tenant_id`, since the allowlist otherwise only compares exact paths.
fn is_source_context_path(path: &str) -> bool {
    path.starts_with("/api/v1/tenants/") && path.ends_with("/source-context")
}

fn is_allowlisted(method: &Method, path: &str) -> bool {
    if method == Method::DELETE && path == "/ui/session" {
        return true;
    }
    method == Method::POST && (READ_ONLY_POST_PATHS.contains(&path) || is_source_context_path(path))
}

/// Rejects every request from the demo user's session other than
/// GET/HEAD/OPTIONS and the read-only allowlist above.
pub async fn demo_write_guard<S: RouterState>(
    State(state): State<S>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let demo = &state.config().demo;
    if !demo.enabled
        || matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        )
        || is_allowlisted(request.method(), request.uri().path())
    {
        return next.run(request).await;
    }

    let is_demo_session = match session_token_from_headers(request.headers()) {
        Some(token) => {
            let token_hash = hash_session_token(&token);
            match state.catalog().get_valid_session(&token_hash).await {
                Ok(Some(session)) => match state.catalog().get_user(&session.user_id).await {
                    Ok(Some(user)) => user.email == demo.username.trim().to_lowercase(),
                    Ok(None) => false,
                    Err(error) => {
                        tracing::error!(error = %error, "demo_write_guard: user lookup failed");
                        false
                    }
                },
                Ok(None) => false,
                Err(error) => {
                    tracing::error!(error = %error, "demo_write_guard: session lookup failed");
                    false
                }
            }
        }
        None => false,
    };

    if is_demo_session {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "The demo account is read-only"})),
        )
            .into_response();
    }

    next.run(request).await
}
