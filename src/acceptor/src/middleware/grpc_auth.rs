//! gRPC authentication layer for Tonic
//!
//! [`GrpcAuthLayer`] is an async tower layer, applied once around the whole
//! `tonic::transport::Server` stack (mirroring [`crate::middleware::GrpcTraceLayer`]),
//! that extracts and validates authentication headers on every gRPC/OTLP
//! request and injects the resulting [`TenantContext`] into the request's
//! extensions before it reaches a service.
//!
//! This replaces a prior synchronous [`tonic::service::Interceptor`] that
//! called the async `Authenticator::authenticate` via
//! `tokio::task::block_in_place` + `Handle::block_on` per request — a
//! worker-thread migration to the blocking pool for every single RPC, and
//! an outright panic on a current-thread runtime (`block_in_place` is only
//! valid on a multi-thread runtime). A tower layer can simply be `async`.

use common::auth::{
    AuthError, Authenticator, TenantContext, parse_bearer_token, validate_dataset_id,
    validate_tenant_id,
};
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::{Request, Status};

/// Extract authentication headers from gRPC metadata
fn extract_grpc_auth_headers(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<(String, String, Option<String>), AuthError> {
    // Extract authorization header (Bearer token). Absent credentials map
    // to UNAUTHENTICATED (401); malformed ones stay INVALID_ARGUMENT (400).
    let auth_header = metadata
        .get("authorization")
        .ok_or_else(|| AuthError::unauthorized("Missing authorization metadata"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid authorization metadata"))?;

    // Shared with the HTTP middleware's own Bearer parsing: case-insensitive
    // scheme, tolerant of extra whitespace around the token (finding L1 —
    // this used to accept only the literal "Bearer "/"bearer " prefixes,
    // diverging from the HTTP side).
    let api_key = parse_bearer_token(auth_header)?;

    // Extract and validate x-tenant-id header (same validator as the HTTP
    // middleware: charset allowlist, length cap, path-traversal rejection —
    // these IDs end up in WAL paths and Iceberg namespaces)
    let tenant_id_raw = metadata
        .get("x-tenant-id")
        .ok_or_else(|| AuthError::unauthorized("Missing x-tenant-id metadata"))?
        .to_str()
        .map_err(|_| AuthError::bad_request("Invalid x-tenant-id metadata"))?;
    let tenant_id = validate_tenant_id(tenant_id_raw)?;

    // Extract and validate optional x-dataset-id header. A non-UTF-8 value
    // is a malformed header (400), not "the header is absent" (finding L2 —
    // matches x-tenant-id's existing behavior).
    let dataset_id = match metadata.get("x-dataset-id") {
        Some(value) => {
            let id = value
                .to_str()
                .map_err(|_| AuthError::bad_request("Invalid x-dataset-id metadata"))?;
            Some(validate_dataset_id(id)?)
        }
        None => None,
    };

    Ok((api_key, tenant_id, dataset_id))
}

/// Map an [`AuthError`] to the equivalent gRPC [`Status`].
fn auth_error_to_status(err: AuthError) -> Status {
    match err.status_code {
        400 => Status::invalid_argument(err.message),
        401 => Status::unauthenticated(err.message),
        403 => Status::permission_denied(err.message),
        _ => Status::internal("Authentication error"),
    }
}

/// Layer applying [`GrpcAuth`] to the tonic server stack.
///
/// Model this the same way as [`crate::middleware::GrpcTraceLayer`]: apply
/// once around the whole `Server::builder()`, inside the trace layer so
/// the RPC SERVER span covers authentication too.
///
/// # Usage
///
/// ```ignore
/// let authenticator = Arc::new(Authenticator::new(auth_config, catalog));
/// let server = tonic::transport::Server::builder()
///     .layer(crate::middleware::GrpcTraceLayer)
///     .layer(GrpcAuthLayer::new(authenticator))
///     .add_service(service)
///     .serve(addr);
/// ```
#[derive(Clone)]
pub struct GrpcAuthLayer {
    authenticator: Arc<Authenticator>,
}

impl GrpcAuthLayer {
    pub fn new(authenticator: Arc<Authenticator>) -> Self {
        Self { authenticator }
    }
}

impl<S> tower::Layer<S> for GrpcAuthLayer {
    type Service = GrpcAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcAuth {
            inner,
            authenticator: self.authenticator.clone(),
        }
    }
}

/// Service wrapper authenticating every inbound gRPC call and injecting
/// [`TenantContext`] into its extensions before it reaches `inner`.
#[derive(Clone)]
pub struct GrpcAuth<S> {
    inner: S,
    authenticator: Arc<Authenticator>,
}

impl<S, ReqBody, ResBody> tower::Service<tonic::codegen::http::Request<ReqBody>> for GrpcAuth<S>
where
    S: tower::Service<
            tonic::codegen::http::Request<ReqBody>,
            Response = tonic::codegen::http::Response<ResBody>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + Default + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: tonic::codegen::http::Request<ReqBody>) -> Self::Future {
        let authenticator = self.authenticator.clone();
        // Standard tower pattern for async pre-call work: the returned
        // future must be 'static, so it cannot borrow `&mut self.inner`.
        // Clone the (cheap, tonic-generated) service and swap it in,
        // leaving `self.inner` ready for the next `call`.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let (mut parts, body) = req.into_parts();

            let metadata = tonic::metadata::MetadataMap::from_headers(parts.headers.clone());
            let (api_key, tenant_id, dataset_id) =
                match extract_grpc_auth_headers(&metadata).map_err(auth_error_to_status) {
                    Ok(v) => v,
                    Err(status) => return Ok(status.into_http()),
                };

            let tenant_context = match authenticator
                .authenticate(&api_key, &tenant_id, dataset_id.as_deref())
                .await
            {
                Ok(ctx) => ctx,
                Err(err) => {
                    tracing::warn!(
                        tenant_id = %tenant_id,
                        error = %err.message,
                        "gRPC authentication failed"
                    );
                    return Ok(auth_error_to_status(err).into_http());
                }
            };

            // Anti-loop guard: don't emit exportable auth telemetry for the
            // _system tenant's own requests.
            let log_auth = async {
                tracing::debug!(
                    tenant_id = %tenant_context.tenant_id,
                    dataset_id = %tenant_context.dataset_id,
                    source = %tenant_context.source,
                    "Authenticated gRPC request"
                );
            };
            if common::self_monitoring::is_self_monitoring_tenant(&tenant_context.tenant_id) {
                common::self_monitoring::suppress_self_telemetry(log_auth).await;
            } else {
                log_auth.await;
            }

            // Insert TenantContext into the request extensions; tonic
            // carries `http::Request` extensions through into the
            // `tonic::Request<T>` it decodes, so services read this back
            // via `get_tenant_context`.
            parts.extensions.insert(tenant_context);

            inner
                .call(tonic::codegen::http::Request::from_parts(parts, body))
                .await
        })
    }
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
        assert_eq!(err.status_code, 401);
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
        assert_eq!(err.status_code, 401);
        assert!(err.message.contains("x-tenant-id"));
    }

    #[test]
    fn extract_grpc_auth_headers_rejects_path_traversal_tenant_id() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("Bearer test-api-key-123"),
        );
        metadata.insert("x-tenant-id", MetadataValue::from_static("..evil"));

        let result = extract_grpc_auth_headers(&metadata);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("Invalid tenant ID"));
    }

    #[test]
    fn extract_grpc_auth_headers_rejects_invalid_dataset_id() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("Bearer test-api-key-123"),
        );
        metadata.insert("x-tenant-id", MetadataValue::from_static("acme"));
        metadata.insert("x-dataset-id", MetadataValue::from_static("data set"));

        let result = extract_grpc_auth_headers(&metadata);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("Invalid dataset ID"));
    }

    #[test]
    fn extract_grpc_auth_headers_rejects_non_utf8_dataset_id() {
        // Finding L2: a non-UTF-8 x-dataset-id must be a 400, matching
        // x-tenant-id's existing behavior — not silently treated as absent.
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("Bearer test-api-key-123"),
        );
        metadata.insert("x-tenant-id", MetadataValue::from_static("acme"));
        metadata.insert(
            "x-dataset-id",
            MetadataValue::try_from(&[0xff, 0xfe][..]).unwrap(),
        );

        let result = extract_grpc_auth_headers(&metadata);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("x-dataset-id"));
    }

    #[test]
    fn extract_grpc_auth_headers_accepts_case_insensitive_bearer_scheme() {
        // Finding L1: the gRPC side used to accept only the literal
        // "Bearer "/"bearer " prefixes; it now shares the HTTP side's
        // case-insensitive, whitespace-tolerant parser.
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("BEARER   test-api-key-123   "),
        );
        metadata.insert("x-tenant-id", MetadataValue::from_static("acme"));

        let (api_key, _, _) = extract_grpc_auth_headers(&metadata).unwrap();
        assert_eq!(api_key, "test-api-key-123");
    }

    #[test]
    fn extract_grpc_auth_headers_trims_tenant_whitespace() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::from_static("Bearer test-api-key-123"),
        );
        metadata.insert("x-tenant-id", MetadataValue::from_static("  acme  "));

        let (_, tenant_id, _) = extract_grpc_auth_headers(&metadata).unwrap();
        assert_eq!(tenant_id, "acme");
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

    mod layer_tests {
        use super::*;
        use std::sync::Mutex;
        use tonic::codegen::http;
        use tower::{ServiceBuilder, ServiceExt};

        /// Authenticator with one tenant ("acme", key "test-key-123",
        /// default dataset "production"), shared by the layer tests below.
        async fn test_authenticator() -> Arc<Authenticator> {
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
            Arc::new(Authenticator::new(auth_config, catalog))
        }

        /// Mock inner gRPC service that records the `TenantContext` it saw
        /// (or its absence) into `captured`, so a test can assert on what
        /// the auth layer injected without needing a real tonic service.
        fn mock_service(
            captured: Arc<Mutex<Option<TenantContext>>>,
        ) -> impl tower::Service<
            http::Request<String>,
            Response = http::Response<String>,
            Error = std::convert::Infallible,
            Future: Send + 'static,
        > + Clone {
            tower::service_fn(move |req: http::Request<String>| {
                let captured = captured.clone();
                async move {
                    *captured.lock().unwrap() = req.extensions().get::<TenantContext>().cloned();
                    Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .status(200)
                            .body(String::new())
                            .unwrap(),
                    )
                }
            })
        }

        fn request_with_headers(headers: &[(&str, &str)]) -> http::Request<String> {
            let mut builder = http::Request::builder().uri("/svc/Method");
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            builder.body(String::new()).unwrap()
        }

        /// `#[tokio::test]`'s default flavor is a **current-thread**
        /// runtime — exactly where the old `block_in_place`-based
        /// interceptor would panic ("can call blocking only when running
        /// on the multi-threaded runtime"). This test's flavor is the
        /// regression check for finding M2: it merely needs to run to
        /// completion without panicking.
        #[tokio::test]
        async fn authenticates_without_blocking_the_runtime() {
            let authenticator = test_authenticator().await;
            let captured = Arc::new(Mutex::new(None));

            let svc = ServiceBuilder::new()
                .layer(GrpcAuthLayer::new(authenticator))
                .service(mock_service(captured.clone()));

            let request = request_with_headers(&[
                ("authorization", "Bearer test-key-123"),
                ("x-tenant-id", "acme"),
            ]);
            let response = svc.oneshot(request).await.unwrap();

            assert_eq!(response.status(), 200);
            let ctx = captured.lock().unwrap().clone();
            let ctx = ctx.expect("authenticated request must carry a TenantContext");
            assert_eq!(ctx.tenant_id, "acme");
            assert_eq!(ctx.dataset_id, "production");
        }

        #[tokio::test]
        async fn missing_headers_is_rejected_before_reaching_inner_service() {
            let authenticator = test_authenticator().await;
            let captured = Arc::new(Mutex::new(None));

            let svc = ServiceBuilder::new()
                .layer(GrpcAuthLayer::new(authenticator))
                .service(mock_service(captured.clone()));

            let response = svc.oneshot(request_with_headers(&[])).await.unwrap();

            let status_header = response
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i32>().ok())
                .map(tonic::Code::from_i32);
            assert_eq!(status_header, Some(tonic::Code::Unauthenticated));
            assert!(
                captured.lock().unwrap().is_none(),
                "rejected request must never reach the inner service"
            );
        }

        #[tokio::test]
        async fn invalid_key_is_rejected_before_reaching_inner_service() {
            let authenticator = test_authenticator().await;
            let captured = Arc::new(Mutex::new(None));

            let svc = ServiceBuilder::new()
                .layer(GrpcAuthLayer::new(authenticator))
                .service(mock_service(captured.clone()));

            let request = request_with_headers(&[
                ("authorization", "Bearer invalid-key"),
                ("x-tenant-id", "acme"),
            ]);
            let response = svc.oneshot(request).await.unwrap();

            let status_header = response
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i32>().ok())
                .map(tonic::Code::from_i32);
            assert_eq!(status_header, Some(tonic::Code::Unauthenticated));
            assert!(captured.lock().unwrap().is_none());
        }
    }
}
