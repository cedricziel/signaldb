//! # Flight Authentication
//!
//! Authentication for the Arrow Flight ports (router, querier, writer,
//! compactor).
//!
//! The Flight servers are reachable over the network, and the querier
//! executes SQL against a session where every tenant catalog is registered.
//! Without authentication, anyone who can reach the port can read any
//! tenant's data.
//!
//! [`FlightAuthInterceptor`] enforces two caller classes:
//!
//! - **Internal services** present the shared `[auth].internal_service_key`
//!   as a Bearer token. They are trusted to act on behalf of any tenant
//!   (the router queries the querier on behalf of HTTP-authenticated
//!   tenants), marked via the [`InternalService`] request extension.
//! - **Tenants** present their own API key plus `x-tenant-id` /
//!   `x-dataset-id`, validated by the [`Authenticator`]. The resulting
//!   [`TenantContext`] is inserted into request extensions and servers must
//!   scope all work to it.
//!
//! The interceptor is only installed when `internal_service_key` is
//! configured; otherwise the Flight ports remain unauthenticated for
//! backward compatibility and servers log a prominent warning that the
//! ports must be network-isolated.

use std::sync::Arc;

use tonic::{Request, Status};

use crate::auth::{Authenticator, validate_dataset_id, validate_tenant_id};

/// Request-extension marker for callers that presented the internal
/// service key. Such callers are trusted to specify tenant scoping
/// themselves (e.g. in ticket contents).
#[derive(Clone, Copy, Debug)]
pub struct InternalService;

/// Interceptor enforcing authentication on Flight servers.
#[derive(Clone)]
pub struct FlightAuthInterceptor {
    /// When None, only the internal service key is accepted (used by the
    /// writer, whose Flight surface is not meant for tenant clients).
    authenticator: Option<Arc<Authenticator>>,
    internal_key: String,
}

impl FlightAuthInterceptor {
    /// Accept the internal service key or tenant API keys.
    pub fn new(authenticator: Arc<Authenticator>, internal_key: String) -> Self {
        Self {
            authenticator: Some(authenticator),
            internal_key,
        }
    }

    /// Accept only the internal service key.
    pub fn internal_only(internal_key: String) -> Self {
        Self {
            authenticator: None,
            internal_key,
        }
    }

    /// Intercept a Flight request.
    ///
    /// Requires a `Bearer` token: either the internal service key
    /// (inserts [`InternalService`]) or a tenant API key together with
    /// `x-tenant-id` (inserts [`TenantContext`]).
    ///
    /// Note: interceptors are synchronous, so tenant authentication
    /// bridges to async via `block_in_place` — requires a multi-threaded
    /// tokio runtime.
    #[allow(clippy::result_large_err)]
    pub fn intercept<T>(&self, mut request: Request<T>) -> Result<Request<T>, Status> {
        let metadata = request.metadata();

        let auth_header = metadata
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("Missing authorization metadata"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("Invalid authorization metadata"))?;

        let bearer = auth_header
            .strip_prefix("Bearer ")
            .or_else(|| auth_header.strip_prefix("bearer "))
            .ok_or_else(|| Status::unauthenticated("Authorization must use Bearer scheme"))?;

        // Internal service caller
        if constant_time_eq(bearer.as_bytes(), self.internal_key.as_bytes()) {
            request.extensions_mut().insert(InternalService);
            return Ok(request);
        }

        // Tenant caller: same header contract as the OTLP gRPC ingest path
        let Some(authenticator) = &self.authenticator else {
            return Err(Status::permission_denied(
                "this Flight endpoint accepts only internal service callers",
            ));
        };

        let tenant_id_raw = metadata
            .get("x-tenant-id")
            .ok_or_else(|| Status::unauthenticated("Missing x-tenant-id metadata"))?
            .to_str()
            .map_err(|_| Status::invalid_argument("Invalid x-tenant-id metadata"))?;
        let tenant_id =
            validate_tenant_id(tenant_id_raw).map_err(|e| Status::invalid_argument(e.message))?;

        let dataset_id = match metadata.get("x-dataset-id").and_then(|v| v.to_str().ok()) {
            Some(id) => {
                Some(validate_dataset_id(id).map_err(|e| Status::invalid_argument(e.message))?)
            }
            None => None,
        };

        let api_key = bearer.to_string();
        let authenticator = authenticator.clone();
        let tenant_context = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                authenticator
                    .authenticate(&api_key, &tenant_id, dataset_id.as_deref())
                    .await
            })
        })
        .map_err(|err| {
            tracing::warn!(
                tenant_id = %tenant_id,
                error = %err.message,
                "Flight authentication failed"
            );
            match err.status_code {
                400 => Status::invalid_argument(err.message),
                401 => Status::unauthenticated(err.message),
                403 => Status::permission_denied(err.message),
                _ => Status::internal("Authentication error"),
            }
        })?;

        request.extensions_mut().insert(tenant_context);
        Ok(request)
    }
}

/// Attach the internal service key as Bearer auth to an outbound request.
pub fn attach_internal_auth<T>(request: &mut Request<T>, internal_key: &str) {
    match format!("Bearer {internal_key}").parse() {
        Ok(value) => {
            request.metadata_mut().insert("authorization", value);
        }
        Err(e) => {
            tracing::error!(error = %e, "Internal service key is not a valid header value");
        }
    }
}

/// Constant-time byte comparison to avoid leaking key prefixes via timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::TenantContext;
    use crate::catalog::Catalog;
    use crate::config::{ApiKeyConfig, AuthConfig, DatasetConfig, TenantConfig};
    use tonic::metadata::MetadataValue;

    async fn interceptor() -> FlightAuthInterceptor {
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
                    storage: None,
                }],
                api_keys: vec![ApiKeyConfig {
                    key: "tenant-key-123".to_string(),
                    name: Some("test".to_string()),
                }],
                schema_config: None,
            }],
            admin_api_key: None,
            internal_service_key: Some("internal-secret".to_string()),
        };
        let authenticator = Arc::new(Authenticator::new(auth_config, catalog));
        FlightAuthInterceptor::new(authenticator, "internal-secret".to_string())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn internal_key_is_accepted_and_marked() {
        let interceptor = interceptor().await;
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer internal-secret"),
        );

        let request = interceptor.intercept(request).unwrap();
        assert!(request.extensions().get::<InternalService>().is_some());
        assert!(request.extensions().get::<TenantContext>().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tenant_key_yields_tenant_context() {
        let interceptor = interceptor().await;
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer tenant-key-123"),
        );
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("acme"));

        let request = interceptor.intercept(request).unwrap();
        assert!(request.extensions().get::<InternalService>().is_none());
        let ctx = request.extensions().get::<TenantContext>().unwrap();
        assert_eq!(ctx.tenant_slug, "acme");
        assert_eq!(ctx.dataset_slug, "production");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_authorization_is_rejected() {
        let interceptor = interceptor().await;
        let status = interceptor.intercept(Request::new(())).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wrong_tenant_key_is_rejected() {
        let interceptor = interceptor().await;
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer wrong-key"),
        );
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("acme"));

        let status = interceptor.intercept(request).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn internal_only_rejects_tenant_keys() {
        let interceptor = FlightAuthInterceptor::internal_only("internal-secret".to_string());

        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer tenant-key-123"),
        );
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("acme"));

        let status = interceptor.intercept(request).unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);

        // The internal key still works
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer internal-secret"),
        );
        assert!(interceptor.intercept(request).is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attach_internal_auth_round_trips() {
        let interceptor = interceptor().await;
        let mut request = Request::new(());
        attach_internal_auth(&mut request, "internal-secret");

        let request = interceptor.intercept(request).unwrap();
        assert!(request.extensions().get::<InternalService>().is_some());
    }
}
