//! gRPC authentication interceptor for Tonic
//!
//! This module provides an interceptor for extracting and validating
//! authentication headers on gRPC/OTLP requests.

use common::auth::{AuthError, Authenticator, TenantContext};
use std::sync::Arc;
use tonic::{Request, Status};

/// Extract authentication headers from gRPC metadata
fn extract_grpc_auth_headers(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<(String, String, Option<String>), AuthError> {
    // Extract authorization header (Bearer token)
    let auth_header = metadata
        .get("authorization")
        .ok_or_else(|| AuthError::bad_request("Missing authorization metadata"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid authorization metadata"))?;

    // Parse Bearer token
    let api_key = auth_header
        .strip_prefix("Bearer ")
        .or_else(|| auth_header.strip_prefix("bearer "))
        .ok_or_else(|| AuthError::bad_request("Authorization must use Bearer scheme"))?
        .to_string();

    // Extract x-tenant-id header
    let tenant_id = metadata
        .get("x-tenant-id")
        .ok_or_else(|| AuthError::bad_request("Missing x-tenant-id metadata"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid x-tenant-id metadata"))?
        .to_string();

    // Extract optional x-dataset-id header
    let dataset_id = metadata
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    Ok((api_key, tenant_id, dataset_id))
}

/// gRPC interceptor function for authentication
///
/// This interceptor validates authentication headers and inserts TenantContext
/// into the request extensions. Returns Status::unauthenticated or
/// Status::permission_denied on authentication failure.
///
/// # Usage
///
/// ```ignore
/// let authenticator = Arc::new(Authenticator::new(auth_config, catalog));
/// let server = tonic::transport::Server::builder()
///     .layer(tower::ServiceBuilder::new()
///         .layer(tonic::service::interceptor(move |req| {
///             grpc_auth_interceptor(authenticator.clone(), req)
///         })))
///     .add_service(service)
///     .serve(addr);
/// ```
#[allow(clippy::result_large_err)]
pub fn grpc_auth_interceptor<T>(
    authenticator: Arc<Authenticator>,
    mut request: Request<T>,
) -> Result<Request<T>, Status> {
    // Extract authentication headers
    let (api_key, tenant_id, dataset_id) =
        extract_grpc_auth_headers(request.metadata()).map_err(|err| match err.status_code {
            400 => Status::invalid_argument(err.message),
            401 => Status::unauthenticated(err.message),
            403 => Status::permission_denied(err.message),
            _ => Status::internal("Authentication error"),
        })?;

    // Authenticate using the Authenticator (blocking version)
    // Note: We need to use blocking context here since interceptors are not async
    let tenant_context = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            authenticator
                .authenticate(&api_key, &tenant_id, dataset_id.as_deref())
                .await
        })
    })
    .map_err(|err| {
        log::warn!(
            "gRPC authentication failed for tenant '{}': {}",
            tenant_id,
            err.message
        );
        match err.status_code {
            400 => Status::invalid_argument(err.message),
            401 => Status::unauthenticated(err.message),
            403 => Status::permission_denied(err.message),
            _ => Status::internal("Authentication error"),
        }
    })?;

    log::debug!(
        "Authenticated gRPC request for tenant '{}', dataset '{}' (source: {})",
        tenant_context.tenant_id,
        tenant_context.dataset_id,
        tenant_context.source
    );

    // Insert TenantContext into request extensions
    request.extensions_mut().insert(tenant_context);

    Ok(request)
}

/// Helper to extract TenantContext from gRPC request extensions
///
/// Use this in service implementations to access the authenticated tenant context:
///
/// ```ignore
/// async fn export(&self, request: Request<ExportTraceServiceRequest>) -> Result<Response<...>, Status> {
///     let tenant_ctx = get_tenant_context(&request)?;
///     // ... use tenant context
/// }
/// ```
#[allow(clippy::result_large_err)]
pub fn get_tenant_context<T>(request: &Request<T>) -> Result<TenantContext, Status> {
    request
        .extensions()
        .get::<TenantContext>()
        .cloned()
        .ok_or_else(|| {
            Status::internal("TenantContext not found in request extensions (auth not configured?)")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::catalog::Catalog;
    use common::config::{ApiKeyConfig, AuthConfig, DatasetConfig, TenantConfig};
    use tonic::metadata::{MetadataMap, MetadataValue};

    #[test]
    fn test_extract_grpc_auth_headers_success() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("Bearer test-api-key-123"),
        );
        metadata.insert("x-tenant-id", MetadataValue::from_static("acme"));
        metadata.insert("x-dataset-id", MetadataValue::from_static("production"));

        let result = extract_grpc_auth_headers(&metadata);
        assert!(result.is_ok());

        let (api_key, tenant_id, dataset_id) = result.unwrap();
        assert_eq!(api_key, "test-api-key-123");
        assert_eq!(tenant_id, "acme");
        assert_eq!(dataset_id, Some("production".to_string()));
    }

    #[test]
    fn test_extract_grpc_auth_headers_no_dataset() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("Bearer test-api-key-123"),
        );
        metadata.insert("x-tenant-id", MetadataValue::from_static("acme"));

        let result = extract_grpc_auth_headers(&metadata);
        assert!(result.is_ok());

        let (api_key, tenant_id, dataset_id) = result.unwrap();
        assert_eq!(api_key, "test-api-key-123");
        assert_eq!(tenant_id, "acme");
        assert_eq!(dataset_id, None);
    }

    #[test]
    fn test_extract_grpc_auth_headers_missing_authorization() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-tenant-id", MetadataValue::from_static("acme"));

        let result = extract_grpc_auth_headers(&metadata);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("authorization"));
    }

    #[test]
    fn test_extract_grpc_auth_headers_missing_tenant_id() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("Bearer test-api-key-123"),
        );

        let result = extract_grpc_auth_headers(&metadata);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("x-tenant-id"));
    }

    #[test]
    fn test_extract_grpc_auth_headers_invalid_bearer_format() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("Basic test-api-key-123"),
        );
        metadata.insert("x-tenant-id", MetadataValue::from_static("acme"));

        let result = extract_grpc_auth_headers(&metadata);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("Bearer"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_grpc_auth_interceptor_success() {
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

        // Create test request
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer test-key-123"),
        );
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("acme"));

        // Test interceptor
        let result = grpc_auth_interceptor(authenticator, request);
        assert!(result.is_ok());

        let request = result.unwrap();
        let tenant_ctx = get_tenant_context(&request).unwrap();
        assert_eq!(tenant_ctx.tenant_id, "acme");
        assert_eq!(tenant_ctx.dataset_id, "production");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_grpc_auth_interceptor_missing_header() {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
            enabled: true,
            tenants: vec![],
        };
        let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

        // Create request missing headers
        let request = Request::new(());

        let result = grpc_auth_interceptor(authenticator, request);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_grpc_auth_interceptor_invalid_key() {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
            enabled: true,
            tenants: vec![],
        };
        let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

        // Create request with invalid API key
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer invalid-key"),
        );
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("acme"));

        let result = grpc_auth_interceptor(authenticator, request);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }
}
