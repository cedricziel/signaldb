//! HTTP authentication middleware for Axum
//!
//! This module provides Tower middleware for extracting and validating
//! authentication headers on HTTP requests.

use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use common::auth::{
    AuthError, Authenticator, TenantContext, validate_dataset_id, validate_tenant_id,
};
use std::sync::Arc;

/// Extract authentication headers from HTTP request
fn extract_auth_headers(
    headers: &HeaderMap,
) -> Result<(String, String, Option<String>), AuthError> {
    // Extract Authorization header
    let auth_header = headers
        .get("authorization")
        .ok_or_else(|| AuthError::bad_request("Missing Authorization header"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid Authorization header"))?;

    // Parse Bearer token (case-insensitive scheme, trim whitespace)
    let api_key = {
        // Find first whitespace to split scheme from token
        let parts: Vec<&str> = auth_header.splitn(2, char::is_whitespace).collect();

        if parts.len() != 2 {
            return Err(AuthError::bad_request(
                "Authorization header must be in format: Bearer <token>",
            ));
        }

        let scheme = parts[0];
        let token = parts[1].trim();

        // Verify scheme is "bearer" (case-insensitive)
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(AuthError::bad_request(
                "Authorization header must use Bearer scheme",
            ));
        }

        // Verify token is not empty after trimming
        if token.is_empty() {
            return Err(AuthError::bad_request(
                "Authorization token cannot be empty",
            ));
        }

        token.to_string()
    };

    // Extract and validate X-Tenant-ID header
    let tenant_id_raw = headers
        .get("x-tenant-id")
        .ok_or_else(|| AuthError::bad_request("Missing X-Tenant-ID header"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid X-Tenant-ID header"))?;

    let tenant_id = validate_tenant_id(tenant_id_raw)?;

    // Extract and validate optional X-Dataset-ID header
    let dataset_id = match headers.get("x-dataset-id").and_then(|v| v.to_str().ok()) {
        Some(id) => Some(validate_dataset_id(id)?),
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
        assert_eq!(err.status_code, 400);
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
        assert_eq!(err.status_code, 400);
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
            enabled: true,
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![DatasetConfig {
                    id: "production".to_string(),
                    slug: "production".to_string(),
                    is_default: true,
                }],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: Some("test-key".to_string()),
                }],
                schema_config: None,
            }],
            admin_api_key: None,
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
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

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
}
