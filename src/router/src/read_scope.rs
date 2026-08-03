//! # Read-scope enforcement for query surfaces (change: mcp-oauth-dcr)
//!
//! A per-signal guard layered on the Tempo/Loki/Prometheus query routers. A
//! caller whose credential carries explicit scopes (an OAuth access token, or a
//! scoped API key) must hold the matching `<signal>:read` scope; a legacy
//! unscoped key or a human session is unrestricted (see
//! [`common::auth::TenantContext::can_read`]). The guard runs after
//! authentication, reading the resolved [`TenantContext`] from request
//! extensions.

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use common::auth::TenantContext;

/// Reject the request with `403` unless the authenticated principal may read
/// `signal`. Intended to wrap a query router via
/// [`axum::middleware::from_fn`].
pub async fn require_read_scope(signal: &'static str, request: Request, next: Next) -> Response {
    let permitted = request
        .extensions()
        .get::<TenantContext>()
        .is_some_and(|ctx| ctx.can_read(signal));
    if !permitted {
        return (
            StatusCode::FORBIDDEN,
            format!("missing {signal}:read scope"),
        )
            .into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, http::Request as HttpRequest, middleware, routing::get};
    use common::auth::TenantSource;
    use common::catalog::MembershipRole;
    use tower::ServiceExt;

    /// A context carrying the given explicit scopes (an OAuth-token-like
    /// principal), as the auth layer would insert.
    fn context_with_scopes(scopes: Option<Vec<String>>) -> TenantContext {
        TenantContext::new(
            "acme".into(),
            "production".into(),
            "acme".into(),
            "production".into(),
            None,
            TenantSource::Database,
        )
        .with_user("u1".into(), MembershipRole::Member, false, None)
        .with_api_key_restrictions(scopes, None)
    }

    /// Build a one-route app gated by `signal`, injecting `ctx` into extensions
    /// the way the auth middleware would.
    fn guarded_app(signal: &'static str, ctx: TenantContext) -> Router {
        Router::new()
            .route("/q", get(|| async { "ok" }))
            .layer(middleware::from_fn(move |req, next| {
                require_read_scope(signal, req, next)
            }))
            .layer(middleware::from_fn(move |mut req: Request, next: Next| {
                let ctx = ctx.clone();
                async move {
                    req.extensions_mut().insert(ctx);
                    next.run(req).await
                }
            }))
    }

    async fn status(app: Router) -> StatusCode {
        app.oneshot(
            HttpRequest::builder()
                .uri("/q")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
    }

    #[tokio::test]
    async fn token_with_matching_read_scope_is_allowed() {
        let ctx = context_with_scopes(Some(vec!["traces:read".into()]));
        assert_eq!(status(guarded_app("traces", ctx)).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn token_lacking_the_read_scope_is_forbidden() {
        let ctx = context_with_scopes(Some(vec!["traces:read".into()]));
        assert_eq!(
            status(guarded_app("metrics", ctx)).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn legacy_unscoped_principal_is_unrestricted() {
        let ctx = context_with_scopes(None);
        assert_eq!(status(guarded_app("metrics", ctx)).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_context_is_forbidden() {
        // No TenantContext in extensions (defensive): deny.
        let app = Router::new()
            .route("/q", get(|| async { "ok" }))
            .layer(middleware::from_fn(move |req, next| {
                require_read_scope("traces", req, next)
            }));
        assert_eq!(status(app).await, StatusCode::FORBIDDEN);
    }
}
