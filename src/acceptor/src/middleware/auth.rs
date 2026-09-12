//! HTTP authentication middleware for Axum
//!
//! This module provides Tower middleware for extracting and validating
//! authentication headers on HTTP requests.

use axum::{
    Json,
    extract::Request,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use common::auth::{
    AuthError, Authenticator, TenantContext, origin_allowed, parse_bearer_token,
    validate_dataset_id, validate_tenant_id,
};
use std::sync::Arc;

/// Extract authentication headers from HTTP request
fn extract_auth_headers(
    headers: &HeaderMap,
) -> Result<(String, String, Option<String>), AuthError> {
    // Extract Authorization header. Absent credentials are 401
    // (unauthenticated), not 400; malformed ones stay 400.
    let auth_header = headers
        .get("authorization")
        .ok_or_else(|| AuthError::unauthorized("Missing Authorization header"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid Authorization header"))?;

    let api_key = parse_bearer_token(auth_header)?;

    // Extract and validate X-Tenant-ID header
    let tenant_id_raw = headers
        .get("x-tenant-id")
        .ok_or_else(|| AuthError::unauthorized("Missing X-Tenant-ID header"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid X-Tenant-ID header"))?;

    let tenant_id = validate_tenant_id(tenant_id_raw)?;

    // Extract and validate optional X-Dataset-ID header. A non-UTF-8 value
    // is a malformed header (400), not "the header is absent" (which would
    // silently ignore it and fall back to the tenant's default dataset).
    let dataset_id = match headers.get("x-dataset-id") {
        Some(value) => {
            let id = value
                .to_str()
                .map_err(|_| AuthError::bad_request("Invalid X-Dataset-ID header"))?;
            Some(validate_dataset_id(id)?)
        }
        None => None,
    };

    Ok((api_key, tenant_id, dataset_id))
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
    let (api_key, tenant_id, dataset_id) = match extract_auth_headers(request.headers()) {
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
    let tenant_context = match authenticator
        .authenticate(&api_key, &tenant_id, dataset_id.as_deref())
        .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            tracing::warn!(
                tenant_id = %tenant_id,
                error = %err.message,
                "Authentication failed"
            );
            return (
                StatusCode::from_u16(err.status_code).unwrap_or(StatusCode::UNAUTHORIZED),
                err.message,
            )
                .into_response();
        }
    };

    let required_signal = match request.uri().path() {
        common::endpoints::PROMETHEUS_REMOTE_WRITE_PATH
        | common::endpoints::OTLP_HTTP_METRICS_PATH => Some("metrics"),
        common::endpoints::OTLP_HTTP_LOGS_PATH => Some("logs"),
        common::endpoints::OTLP_HTTP_TRACES_PATH => Some("traces"),
        common::endpoints::OTLP_HTTP_PROFILES_PATH => Some("profiles"),
        _ => None,
    };
    if let Some(signal) = required_signal
        && !tenant_context.can_ingest(signal)
    {
        tracing::warn!(
            tenant_id = %tenant_context.tenant_id,
            dataset_id = %tenant_context.dataset_id,
            signal,
            "API key scope denied ingestion"
        );
        return (
            StatusCode::FORBIDDEN,
            format!("API key requires scope '{signal}:write'"),
        )
            .into_response();
    }

    // Per-key CORS enforcement (D-allowed-origins): a request with no
    // `Origin` header is not a browser request — SDKs, collectors, and
    // server-to-server callers are the vast majority of ingest traffic and
    // are completely unaffected. The outer `otlp_cors_layer` only ever
    // answers the unauthenticated `OPTIONS` preflight (which grants no
    // authority by itself); the actual request's origin is checked here,
    // once the API key is known, and the `Access-Control-Allow-Origin` the
    // browser needs to read the response is set only on a match.
    let origin_header = request.headers().get(header::ORIGIN).cloned();
    if let Some(origin_value) = &origin_header {
        let allowed = origin_value.to_str().is_ok_and(|origin| {
            origin_allowed(tenant_context.api_key_allowed_origins.as_deref(), origin)
        });
        if !allowed {
            tracing::warn!(
                tenant_id = %tenant_context.tenant_id,
                "origin not allowed for this API key"
            );
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({ "error": "origin not allowed for this API key" })),
            )
                .into_response();
        }
    }

    let is_system = common::self_monitoring::is_self_monitoring_tenant(&tenant_context.tenant_id);

    // Anti-loop guard: processing the _system tenant's own telemetry must not
    // generate more self-monitoring telemetry (infinite feedback loop). The
    // suppression scope covers everything from the auth event onwards.
    let mut response = if is_system {
        common::self_monitoring::suppress_self_telemetry(async move {
            tracing::debug!(
                tenant_id = %tenant_context.tenant_id,
                dataset_id = %tenant_context.dataset_id,
                source = %tenant_context.source,
                "Authenticated request"
            );
            request.extensions_mut().insert(tenant_context);
            next.run(request).await
        })
        .await
    } else {
        tracing::debug!(
            tenant_id = %tenant_context.tenant_id,
            dataset_id = %tenant_context.dataset_id,
            source = %tenant_context.source,
            "Authenticated request"
        );
        request.extensions_mut().insert(tenant_context);
        next.run(request).await
    };

    if let Some(origin_value) = origin_header {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin_value);
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static("Origin"));
    }
    response
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

impl axum::extract::FromRequestParts<()> for TenantContextExtractor {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &(),
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
    use axum::http::HeaderValue;
    use common::catalog::Catalog;
    use common::config::{ApiKeyConfig, AuthConfig, DatasetConfig, TenantConfig};

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

    #[test]
    fn test_extract_auth_headers_case_insensitive_bearer() {
        // Test lowercase "bearer"
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("bearer test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());
        let (api_key, _, _) = result.unwrap();
        assert_eq!(api_key, "test-api-key-123");

        // Test uppercase "BEARER"
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("BEARER test-api-key-456"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());
        let (api_key, _, _) = result.unwrap();
        assert_eq!(api_key, "test-api-key-456");

        // Test mixed case "BeArEr"
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("BeArEr test-api-key-789"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());
        let (api_key, _, _) = result.unwrap();
        assert_eq!(api_key, "test-api-key-789");
    }

    #[test]
    fn test_extract_auth_headers_trim_whitespace() {
        // Test leading whitespace in token
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer   test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());
        let (api_key, _, _) = result.unwrap();
        assert_eq!(api_key, "test-api-key-123");

        // Test trailing whitespace in token
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-456   "),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());
        let (api_key, _, _) = result.unwrap();
        assert_eq!(api_key, "test-api-key-456");

        // Test both leading and trailing whitespace
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer   test-api-key-789   "),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_ok());
        let (api_key, _, _) = result.unwrap();
        assert_eq!(api_key, "test-api-key-789");
    }

    #[test]
    fn test_extract_auth_headers_empty_token() {
        // Test empty token after trimming
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer    "));
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn test_extract_auth_headers_missing_token() {
        // Test missing token (only scheme)
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer"));
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
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
    fn test_extract_auth_headers_rejects_non_utf8_dataset_id() {
        // Finding L2: a non-UTF-8 X-Dataset-ID must be a 400, matching
        // X-Tenant-ID's existing behavior — not silently treated as absent
        // (which would fall back to the tenant's default dataset).
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer test-api-key-123"),
        );
        headers.insert("x-tenant-id", HeaderValue::from_static("acme"));
        headers.insert(
            "x-dataset-id",
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );

        let result = extract_auth_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("X-Dataset-ID"));
    }

    #[test]
    fn test_extract_auth_headers_tenant_whitespace_trimmed() {
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

    /// Per-key CORS enforcement: whether the actual (non-preflight) request
    /// gets an `Access-Control-Allow-Origin` (and `Vary: Origin`), a 403 with
    /// no CORS header, or is unaffected entirely, depending on the
    /// authenticated key's `allowed_origins` and whether the request even
    /// carries an `Origin` header. Preflight itself (always permissive,
    /// regardless of any key) is covered by `otlp_cors_layer`'s own tests in
    /// `crate::cors_tests`.
    mod origin_enforcement_tests {
        use super::*;
        use axum::body::Body;
        use axum::http::{Request, StatusCode, header};
        use axum::routing::post;
        use axum::{Router, middleware};
        use common::catalog::Catalog;
        use tower::ServiceExt;

        /// A database-backed tenant/key pair scoped to ingest traces, with
        /// `allowed_origins` set as given (`None` is unrestricted). Returns
        /// the router (auth-gated `POST /v1/traces`, matching the real
        /// route the enforcement lives on) and the raw key.
        async fn app_with_key(allowed_origins: Option<Vec<String>>) -> (Router, String) {
            let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
            catalog
                .upsert_tenant("acme", "Acme Corp", Some("production"), "database")
                .await
                .unwrap();
            catalog.create_dataset("acme", "production").await.unwrap();
            let raw_key = "sdbk_test_origin_key".to_string();
            let key_hash = Authenticator::hash_api_key(&raw_key);
            catalog
                .upsert_scoped_api_key(
                    "acme",
                    &key_hash,
                    Some("origin-test"),
                    None,
                    allowed_origins.as_deref(),
                    Some(&["traces:write".to_string()]),
                    None,
                )
                .await
                .unwrap();
            let authenticator = Arc::new(Authenticator::new(AuthConfig::default(), catalog));

            async fn handler() -> &'static str {
                "ok"
            }
            let app = Router::new()
                .route(common::endpoints::OTLP_HTTP_TRACES_PATH, post(handler))
                .layer(middleware::from_fn(move |req, next| {
                    let auth = authenticator.clone();
                    async move { auth_middleware(auth, req, next).await }
                }));
            (app, raw_key)
        }

        fn request(raw_key: &str, origin: Option<&str>) -> Request<Body> {
            let mut builder = Request::builder()
                .method("POST")
                .uri(common::endpoints::OTLP_HTTP_TRACES_PATH)
                .header("authorization", format!("Bearer {raw_key}"))
                .header("x-tenant-id", "acme");
            if let Some(origin) = origin {
                builder = builder.header(header::ORIGIN, origin);
            }
            builder.body(Body::empty()).unwrap()
        }

        #[tokio::test]
        async fn unrestricted_key_allows_any_origin_and_reflects_it() {
            let (app, raw_key) = app_with_key(None).await;
            let res = app
                .oneshot(request(&raw_key, Some("https://ui.example")))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            assert_eq!(
                res.headers()
                    .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .map(|v| v.to_str().unwrap().to_string()),
                Some("https://ui.example".to_string())
            );
            assert_eq!(
                res.headers()
                    .get(header::VARY)
                    .map(|v| v.to_str().unwrap().to_string()),
                Some("Origin".to_string())
            );
        }

        #[tokio::test]
        async fn restricted_key_allows_matching_origin() {
            let (app, raw_key) =
                app_with_key(Some(vec!["https://allowed.example".to_string()])).await;
            let res = app
                .oneshot(request(&raw_key, Some("https://allowed.example")))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            assert_eq!(
                res.headers()
                    .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .map(|v| v.to_str().unwrap().to_string()),
                Some("https://allowed.example".to_string())
            );
        }

        #[tokio::test]
        async fn restricted_key_rejects_non_matching_origin_without_cors_header() {
            let (app, raw_key) =
                app_with_key(Some(vec!["https://allowed.example".to_string()])).await;
            let res = app
                .oneshot(request(&raw_key, Some("https://evil.example")))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::FORBIDDEN);
            assert!(
                res.headers()
                    .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .is_none(),
                "a rejected origin must not be granted CORS access"
            );
        }

        #[tokio::test]
        async fn request_without_origin_header_is_unaffected_by_restriction() {
            let (app, raw_key) =
                app_with_key(Some(vec!["https://allowed.example".to_string()])).await;
            let res = app.oneshot(request(&raw_key, None)).await.unwrap();
            assert_eq!(
                res.status(),
                StatusCode::OK,
                "a non-browser request (no Origin header) must be unaffected by the restriction"
            );
            assert!(
                res.headers()
                    .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .is_none()
            );
        }
    }
}
