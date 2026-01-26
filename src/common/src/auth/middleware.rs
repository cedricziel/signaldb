//! HTTP authentication middleware for Axum
//!
//! This module provides Tower middleware for extracting and validating
//! authentication headers on HTTP requests.

use super::{AuthError, Authenticator, TenantContext};
use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
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

    // Parse Bearer token
    let api_key = auth_header
        .strip_prefix("Bearer ")
        .ok_or_else(|| AuthError::bad_request("Authorization header must use Bearer scheme"))?
        .to_string();

    // Extract X-Tenant-ID header
    let tenant_id = headers
        .get("x-tenant-id")
        .ok_or_else(|| AuthError::bad_request("Missing X-Tenant-ID header"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid X-Tenant-ID header"))?
        .to_string();

    // Extract optional X-Dataset-ID header
    let dataset_id = headers
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

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
}
