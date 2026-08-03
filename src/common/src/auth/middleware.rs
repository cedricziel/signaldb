//! HTTP authentication middleware for Axum
//!
//! This module provides Tower middleware for extracting and validating
//! authentication headers on HTTP requests.

use super::{
    AuthError, Authenticator, TenantContext, session_token_from_headers, validate_dataset_id,
    validate_tenant_id,
};
use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// Extract authentication headers from HTTP request
///
/// When the request carries no `Authorization` header, falls back to the
/// opaque UI session cookie (see [`super::session`]). Tenant and dataset
/// context must still be supplied through headers.
#[derive(Debug)]
enum RequestCredentials {
    ApiKey(String),
    UserSession(String),
    /// An opaque OAuth 2.1 access token (change: mcp-oauth-dcr). Its tenant and
    /// scopes come from the token record, not from headers.
    OAuthToken(String),
}

#[cfg(test)]
impl PartialEq<&str> for RequestCredentials {
    fn eq(&self, other: &&str) -> bool {
        match self {
            RequestCredentials::ApiKey(value)
            | RequestCredentials::UserSession(value)
            | RequestCredentials::OAuthToken(value) => value == other,
        }
    }
}

fn extract_auth_headers(
    headers: &HeaderMap,
) -> Result<(RequestCredentials, String, Option<String>), AuthError> {
    // Extract Authorization header, falling back to the session cookie.
    let Some(auth_value) = headers.get("authorization") else {
        return extract_auth_from_session(headers);
    };
    let auth_header = auth_value
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid Authorization header"))?;

    // Parse Bearer token
    let bearer = auth_header
        .strip_prefix("Bearer ")
        .ok_or_else(|| AuthError::bad_request("Authorization header must use Bearer scheme"))?
        .to_string();

    // Extract and validate the optional X-Dataset-ID header (shared by all
    // bearer kinds).
    let dataset_id = match headers.get("x-dataset-id").and_then(|v| v.to_str().ok()) {
        Some(id) => Some(validate_dataset_id(id)?),
        None => None,
    };

    // An OAuth access token carries its own tenant, so X-Tenant-ID is neither
    // required nor consulted — an OAuth session cannot be pointed at a tenant
    // it was not granted. The tenant field is unused for this credential kind.
    if bearer.starts_with(super::oauth::ACCESS_TOKEN_PREFIX) {
        return Ok((
            RequestCredentials::OAuthToken(bearer),
            String::new(),
            dataset_id,
        ));
    }

    // Extract and validate X-Tenant-ID header. Absent credentials are 401
    // (unauthenticated), not 400 — the embedded UI's login gate keys on 401.
    let tenant_id_raw = headers
        .get("x-tenant-id")
        .ok_or_else(|| AuthError::unauthorized("Missing X-Tenant-ID header"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid X-Tenant-ID header"))?;

    let tenant_id = validate_tenant_id(tenant_id_raw)?;

    Ok((RequestCredentials::ApiKey(bearer), tenant_id, dataset_id))
}

/// Resolve credentials from the UI session cookie for requests without an
/// `Authorization` header.
///
/// The cookie supplies only an opaque token. Tenant and dataset come from
/// `X-Tenant-ID`/`X-Dataset-ID`; all values pass the same validation as
/// API-key requests. A missing or malformed cookie yields the same error
/// as a missing Authorization header.
fn extract_auth_from_session(
    headers: &HeaderMap,
) -> Result<(RequestCredentials, String, Option<String>), AuthError> {
    let token = super::session::session_token_from_headers(headers)
        .ok_or_else(|| AuthError::unauthorized("Missing Authorization header"))?;

    let tenant_id_raw = headers
        .get("x-tenant-id")
        .ok_or_else(|| AuthError::unauthorized("Missing X-Tenant-ID header"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid X-Tenant-ID header"))?;
    let tenant_id = validate_tenant_id(tenant_id_raw)?;

    let dataset_raw = headers
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let dataset_id = match dataset_raw {
        Some(id) => Some(validate_dataset_id(&id)?),
        None => None,
    };

    Ok((
        RequestCredentials::UserSession(token),
        tenant_id,
        dataset_id,
    ))
}

/// Axum middleware function for HTTP authentication
///
/// Extracts authentication headers, validates them using the Authenticator,
/// and inserts TenantContext into request extensions on success.
///
/// Returns appropriate HTTP error responses (400/401/403) on auth failure.
pub async fn auth_middleware(
    authenticator: Arc<Authenticator>,
    mut request: Request,
    next: Next,
) -> Response {
    // Extract authentication headers
    let (credentials, tenant_id, dataset_id) = match extract_auth_headers(request.headers()) {
        Ok(headers) => headers,
        Err(err) => {
            return (
                StatusCode::from_u16(err.status_code).unwrap_or(StatusCode::BAD_REQUEST),
                err.message,
            )
                .into_response();
        }
    };

    // Authenticate using the Authenticator
    let auth_result = match credentials {
        RequestCredentials::ApiKey(api_key) => {
            authenticator
                .authenticate(&api_key, &tenant_id, dataset_id.as_deref())
                .await
        }
        RequestCredentials::UserSession(token) => {
            authenticator
                .authenticate_session(&token, &tenant_id, dataset_id.as_deref())
                .await
        }
        RequestCredentials::OAuthToken(token) => {
            // Tenant and scopes come from the token; audience is bound to the
            // configured MCP resource.
            let resource = authenticator.mcp_resource().map(str::to_owned);
            authenticator
                .authenticate_oauth_token(&token, dataset_id.as_deref(), resource.as_deref())
                .await
        }
    };
    let tenant_context = match auth_result {
        Ok(ctx) => ctx,
        Err(err) => {
            log::warn!(
                "Authentication failed for tenant '{}': {}",
                tenant_id,
                err.message
            );
            return (
                StatusCode::from_u16(err.status_code).unwrap_or(StatusCode::UNAUTHORIZED),
                err.message,
            )
                .into_response();
        }
    };

    // Anti-loop guard: processing the _system tenant's own telemetry must not
    // generate more self-monitoring telemetry (infinite feedback loop). The
    // suppression scope covers everything from here through the handler.
    if crate::self_monitoring::is_self_monitoring_tenant(&tenant_context.tenant_id) {
        crate::self_monitoring::suppress_self_telemetry(async move {
            log::debug!(
                "Authenticated request for tenant '{}', dataset '{}' (source: {})",
                tenant_context.tenant_id,
                tenant_context.dataset_id,
                tenant_context.source
            );
            request.extensions_mut().insert(tenant_context);
            next.run(request).await
        })
        .await
    } else {
        log::debug!(
            "Authenticated request for tenant '{}', dataset '{}' (source: {})",
            tenant_context.tenant_id,
            tenant_context.dataset_id,
            tenant_context.source
        );
        // Insert TenantContext into request extensions
        request.extensions_mut().insert(tenant_context);

        // Continue to next middleware/handler
        next.run(request).await
    }
}

/// Axum middleware function for admin API authentication
///
/// Validates the request carries a Bearer token matching the configured admin API key.
/// Returns 401 if missing/invalid, 403 if admin key is not configured.
pub async fn admin_auth_middleware(
    admin_key_hash: Option<String>,
    authenticator: Arc<Authenticator>,
    mut request: Request,
    next: Next,
) -> Response {
    if let Some(auth_header) = request.headers().get("authorization") {
        let Ok(auth_header) = auth_header.to_str() else {
            return (StatusCode::BAD_REQUEST, "Invalid Authorization header").into_response();
        };
        let Some(token) = auth_header.strip_prefix("Bearer ") else {
            return (
                StatusCode::BAD_REQUEST,
                "Authorization header must use Bearer scheme",
            )
                .into_response();
        };
        let Some(expected_hash) = admin_key_hash else {
            return (StatusCode::FORBIDDEN, "Admin API key is not configured").into_response();
        };
        if Authenticator::hash_api_key(token) != expected_hash {
            return (StatusCode::UNAUTHORIZED, "Invalid admin API key").into_response();
        }
        return next.run(request).await;
    }

    let Some(token) = session_token_from_headers(request.headers()) else {
        return (
            StatusCode::UNAUTHORIZED,
            "Missing administrator credentials",
        )
            .into_response();
    };
    match authenticator
        .authenticate_instance_admin_session(&token)
        .await
    {
        Ok(user) => {
            request.extensions_mut().insert(user);
            next.run(request).await
        }
        Err(error) => (
            StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::UNAUTHORIZED),
            error.message,
        )
            .into_response(),
    }
}

/// Axum extractor for TenantContext from request extensions
///
/// Use this in handler functions to extract the authenticated tenant context:
///
/// ```ignore
/// async fn handler(tenant_ctx: TenantContextExtractor) -> Response {
///     let tenant_id = tenant_ctx.0.tenant_id;
///     // ... use tenant context
/// }
/// ```
pub struct TenantContextExtractor(pub TenantContext);

impl<S> axum::extract::FromRequestParts<S> for TenantContextExtractor
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<TenantContext>()
            .cloned()
            .map(TenantContextExtractor)
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "TenantContext not found in request extensions".to_string(),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::config::{ApiKeyConfig, AuthConfig, DatasetConfig, TenantConfig};
    use axum::http::HeaderValue;

    #[test]
    fn test_extract_auth_headers_success() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));
        headers.insert("x-dataset-id", HeaderValue::from_static("production"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());

        let (api_key, tenant_id, dataset_id) = result.unwrap();
        assert_eq!(api_key, "test-api-key-123");
        assert_eq!(tenant_id, "acme");
        assert_eq!(dataset_id, Some("production".to_string()));
    }

    #[test]
    fn test_extract_auth_headers_no_dataset() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());

        let (api_key, tenant_id, dataset_id) = result.unwrap();
        assert_eq!(api_key, "test-api-key-123");
        assert_eq!(tenant_id, "acme");
        assert_eq!(dataset_id, None);
    }

    #[test]
    fn test_extract_auth_headers_missing_authorization() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 401);
        assert!(err.message.contains("Authorization"));
    }

    #[test]
    fn test_extract_auth_headers_missing_tenant_id() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-123"),
        );

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 401);
        assert!(err.message.contains("X-Tenant-ID"));
    }

    #[test]
    fn test_extract_auth_headers_no_credentials_at_all_is_unauthorized() {
        // A fresh browser hitting the API sends neither headers nor a
        // session cookie; the UI login gate keys on 401.
        let err = extract_auth_headers(&HeaderMap::new()).unwrap_err();
        assert_eq!(err.status_code, 401);
        assert!(err.message.contains("Authorization"));
    }

    #[test]
    fn test_extract_auth_headers_invalid_bearer_format() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Basic test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("Bearer"));
    }

    #[tokio::test]
    async fn test_auth_middleware_integration() {
        use axum::{
            Router,
            body::Body,
            http::{Request, StatusCode},
            middleware,
            routing::get,
        };
        use tower::ServiceExt;

        // Setup authenticator
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![DatasetConfig {
                    id: "production".to_string(),
                    slug: "production".to_string(),
                    is_default: true,
                    storage: None,
                }],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: Some("test-key".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        };
        let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

        // Create test handler
        async fn test_handler(tenant_ctx: TenantContextExtractor) -> String {
            format!(
                "tenant={},dataset={}",
                tenant_ctx.0.tenant_id, tenant_ctx.0.dataset_id
            )
        }

        // Create router with auth middleware
        let auth = authenticator.clone();
        let app = Router::new()
            .route("/test", get(test_handler))
            .layer(middleware::from_fn(move |req, next| {
                auth_middleware(auth.clone(), req, next)
            }));

        // Test successful authentication
        let request = Request::builder()
            .uri("/test")
            .header("authorization", "Bearer test-key-123")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test missing authorization header
        let request = Request::builder()
            .uri("/test")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Test invalid API key
        let request = Request::builder()
            .uri("/test")
            .header("authorization", "Bearer invalid-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_extract_auth_headers_uses_session_cookie_without_authorization() {
        use crate::auth::{SESSION_COOKIE, generate_session_token};

        let cookie = generate_session_token();
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}")).unwrap(),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));
        headers.insert("x-dataset-id", HeaderValue::from_static("production"));

        let (credentials, tenant_id, dataset_id) = extract_auth_headers(&headers).unwrap();
        assert_eq!(credentials, cookie.as_str());
        assert_eq!(tenant_id, "acme");
        assert_eq!(dataset_id, Some("production".to_string()));
    }

    #[test]
    fn test_extract_auth_headers_explicit_headers_win_over_cookie() {
        use crate::auth::{SESSION_COOKIE, generate_session_token};

        let cookie = generate_session_token();

        // Tenant/dataset headers provide context for an opaque session.
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}")).unwrap(),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("other"));
        headers.insert("x-dataset-id", HeaderValue::from_static("staging"));

        let (credentials, tenant_id, dataset_id) = extract_auth_headers(&headers).unwrap();
        assert_eq!(credentials, cookie.as_str());
        assert_eq!(tenant_id, "other");
        assert_eq!(dataset_id, Some("staging".to_string()));

        // An Authorization header disables the cookie entirely.
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}")).unwrap(),
        );
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer header-key"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("other"));

        let (credentials, tenant_id, dataset_id) = extract_auth_headers(&headers).unwrap();
        assert_eq!(credentials, "header-key");
        assert_eq!(tenant_id, "other");
        assert_eq!(dataset_id, None);
    }

    #[test]
    fn test_extract_auth_headers_malformed_cookie_is_missing_auth() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_static("signaldb_session=not-a-session"),
        );

        let err = extract_auth_headers(&headers).unwrap_err();
        assert_eq!(err.status_code, 401);
        assert!(err.message.contains("Authorization"));
    }

    #[test]
    fn test_extract_auth_headers_cookie_values_are_validated() {
        use crate::auth::{SESSION_COOKIE, generate_session_token};

        let cookie = generate_session_token();
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={cookie}")).unwrap(),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("../evil"));

        let err = extract_auth_headers(&headers).unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("path traversal"));
    }

    #[tokio::test]
    async fn session_cookie_round_trip_authenticates_request() {
        use crate::auth::{SESSION_COOKIE, generate_session_token, hash_session_token};
        use crate::catalog::MembershipRole;
        use axum::{
            Router,
            body::Body,
            http::{Request, StatusCode},
            middleware,
            routing::get,
        };
        use chrono::{Duration, Utc};
        use tower::ServiceExt;

        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![DatasetConfig {
                    id: "production".to_string(),
                    slug: "production".to_string(),
                    is_default: true,
                    storage: None,
                }],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: Some("test-key".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        };
        catalog.sync_config_tenants(&auth_config).await.unwrap();
        let user = catalog
            .create_user("user@example.com", None, "unused", false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Viewer)
            .await
            .unwrap();
        let cookie = generate_session_token();
        catalog
            .create_user_session(
                &user.id,
                &hash_session_token(&cookie),
                Utc::now() + Duration::hours(1),
            )
            .await
            .unwrap();
        let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

        async fn test_handler(tenant_ctx: TenantContextExtractor) -> String {
            format!(
                "tenant={},dataset={}",
                tenant_ctx.0.tenant_id, tenant_ctx.0.dataset_id
            )
        }

        let auth = authenticator.clone();
        let app = Router::new()
            .route("/test", get(test_handler))
            .layer(middleware::from_fn(move |req, next| {
                auth_middleware(auth.clone(), req, next)
            }));

        // The opaque cookie plus tenant context resolves the default dataset.
        let request = Request::builder()
            .uri("/test")
            .header("cookie", format!("{SESSION_COOKIE}={cookie}"))
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, "tenant=acme,dataset=production");

        // An unknown opaque session token fails authentication.
        let bad_cookie = generate_session_token();
        let request = Request::builder()
            .uri("/test")
            .header("cookie", format!("{SESSION_COOKIE}={bad_cookie}"))
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn system_tenant_requests_are_suppressed_from_otel_export() {
        use axum::{Router, body::Body, http::Request, middleware, routing::get};
        use tower::ServiceExt;

        // Regression test for issue #760: the router's HTTP query handling
        // goes through this middleware, so processing a _system-tenant
        // request must not emit spans/log records that pass the OTel
        // export filter (they would be re-ingested as _system telemetry).
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let make_tenant = |id: &str, key: &str| TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: id.to_string(),
            default_dataset: Some("production".to_string()),
            datasets: vec![DatasetConfig {
                id: "production".to_string(),
                slug: "production".to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![ApiKeyConfig {
                key: key.to_string(),
                name: Some("test-key".to_string()),
            }],
            schema_config: None,
            limits: None,
        };
        let auth_config = AuthConfig {
            tenants: vec![
                make_tenant("_system", "system-key"),
                make_tenant("acme", "acme-key"),
            ],
            ..Default::default()
        };
        let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

        async fn handler() -> &'static str {
            tracing::info!("handling query request");
            "ok"
        }

        let auth = authenticator.clone();
        let app = Router::new()
            .route("/query", get(handler))
            .layer(middleware::from_fn(move |req, next| {
                auth_middleware(auth.clone(), req, next)
            }));

        let send = |app: Router, key: &'static str, tenant: &'static str| async move {
            let request = Request::builder()
                .uri("/query")
                .header("authorization", format!("Bearer {key}"))
                .header("x-tenant-id", tenant)
                .body(Body::empty())
                .unwrap();
            app.oneshot(request).await.unwrap()
        };

        // _system-tenant request: nothing may pass the export filter.
        let probe = crate::testing::OtelExportProbe::new();
        {
            let _guard = probe.install();
            let response = send(app.clone(), "system-key", "_system").await;
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert_eq!(
            probe.exported_events(),
            0,
            "_system request processing must not export log records"
        );
        assert_eq!(
            probe.exported_spans(),
            0,
            "_system request processing must not export spans"
        );

        // Normal tenant: the handler's telemetry still exports.
        let probe = crate::testing::OtelExportProbe::new();
        {
            let _guard = probe.install();
            let response = send(app.clone(), "acme-key", "acme").await;
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert!(
            probe.exported_events() > 0,
            "normal tenant request processing must still export telemetry"
        );
    }

    #[tokio::test]
    async fn oauth_bearer_authenticates_without_tenant_header_and_ignores_it() {
        use crate::auth::oauth::{TokenKind, generate_oauth_token, hash_oauth_token};
        use crate::catalog::MembershipRole;
        use axum::{Router, body::Body, http::Request, middleware, routing::get};
        use chrono::{Duration, Utc};
        use tower::ServiceExt;

        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        catalog.create_dataset("acme", "production").await.unwrap();
        let user = catalog
            .create_user("agent@example.com", None, "phc", false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Member)
            .await
            .unwrap();
        let token = generate_oauth_token(TokenKind::Access);
        catalog
            .create_access_token(
                &hash_oauth_token(&token),
                "client-1",
                &user.id,
                "acme",
                &["traces:read".to_string()],
                None,
                Utc::now() + Duration::hours(1),
            )
            .await
            .unwrap();
        let authenticator = Arc::new(Authenticator::new(AuthConfig::default(), catalog));

        async fn handler(tenant_ctx: TenantContextExtractor) -> String {
            tenant_ctx.0.tenant_id
        }
        let auth = authenticator.clone();
        let app = Router::new()
            .route("/test", get(handler))
            .layer(middleware::from_fn(move |req, next| {
                auth_middleware(auth.clone(), req, next)
            }));

        // No X-Tenant-ID: the OAuth token still authenticates and resolves its
        // bound tenant.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, "acme");

        // A conflicting X-Tenant-ID is ignored — the token's tenant wins.
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("authorization", format!("Bearer {token}"))
                    .header("x-tenant-id", "globex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, "acme");
    }

    #[test]
    fn test_extract_auth_headers_validates_tenant_id() {
        // Test empty tenant ID
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("   "));

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("Invalid tenant ID"));

        // Test path traversal in tenant ID
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("../evil"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("path traversal"));
    }

    #[test]
    fn test_extract_auth_headers_validates_dataset_id() {
        // Test invalid characters in dataset ID
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));
        headers.insert("x-dataset-id", HeaderValue::from_static("prod@ction"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("Invalid dataset ID"));
    }

    #[test]
    fn test_extract_auth_headers_trims_whitespace() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("  acme  "));
        headers.insert("x-dataset-id", HeaderValue::from_static("  production  "));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());

        let (_, tenant_id, dataset_id) = result.unwrap();
        assert_eq!(tenant_id, "acme");
        assert_eq!(dataset_id, Some("production".to_string()));
    }
}
