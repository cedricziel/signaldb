use axum::{
    Json, Router,
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use common::auth::{Authenticator, TenantContext, admin_auth_middleware, auth_middleware};
use common::catalog::Catalog;
use common::config::Configuration;
use common::ratelimit::TenantRateLimiter;
use std::sync::Arc;

pub mod discovery;
pub mod endpoints;

pub trait RouterState: std::fmt::Debug + Clone + Send + Sync + 'static {
    fn catalog(&self) -> &Catalog;
    fn service_registry(&self) -> &discovery::ServiceRegistry;
    fn config(&self) -> &Configuration;
    fn authenticator(&self) -> &Arc<Authenticator>;
}

/// RouterState holds any shared state that needs to be accessed by route handlers
#[derive(Clone)]
pub struct InMemoryStateImpl {
    catalog: Catalog,
    service_registry: discovery::ServiceRegistry,
    config: Configuration,
    authenticator: Arc<Authenticator>,
}

impl std::fmt::Debug for InMemoryStateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryStateImpl")
            .field("catalog", &"Catalog")
            .field("service_registry", &self.service_registry)
            .field("config", &"Configuration")
            .field("authenticator", &"Authenticator")
            .finish()
    }
}

impl InMemoryStateImpl {
    pub fn new(catalog: Catalog, config: Configuration) -> Self {
        let mut service_registry = discovery::ServiceRegistry::new(catalog.clone());
        if let Some(discovery_config) = &config.discovery {
            service_registry = service_registry.with_discovery_ttl(discovery_config.ttl);
        }
        let authenticator = Arc::new(Authenticator::new(
            config.auth.clone(),
            Arc::new(catalog.clone()),
        ));

        Self {
            catalog,
            service_registry,
            config,
            authenticator,
        }
    }

    pub fn new_with_flight_transport(
        catalog: Catalog,
        config: Configuration,
        flight_transport: common::flight::transport::InMemoryFlightTransport,
    ) -> Self {
        let mut service_registry =
            discovery::ServiceRegistry::with_flight_transport(catalog.clone(), flight_transport);
        if let Some(discovery_config) = &config.discovery {
            service_registry = service_registry.with_discovery_ttl(discovery_config.ttl);
        }
        let authenticator = Arc::new(Authenticator::new(
            config.auth.clone(),
            Arc::new(catalog.clone()),
        ));

        Self {
            catalog,
            service_registry,
            config,
            authenticator,
        }
    }
}

impl RouterState for InMemoryStateImpl {
    fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    fn service_registry(&self) -> &discovery::ServiceRegistry {
        &self.service_registry
    }

    fn config(&self) -> &Configuration {
        &self.config
    }

    fn authenticator(&self) -> &Arc<Authenticator> {
        &self.authenticator
    }
}

/// Create a new router instance with all routes configured
pub fn create_router<S: RouterState>(state: S) -> Router {
    // Create auth middleware layer
    let authenticator = state.authenticator().clone();
    let auth_layer =
        middleware::from_fn(move |req, next| auth_middleware(authenticator.clone(), req, next));

    // Per-tenant query request-rate limiting, applied after authentication
    // (it reads the TenantContext the auth layer inserts). Tenants without
    // a max_query_requests_per_sec limit are unaffected.
    let query_limiter = Arc::new(TenantRateLimiter::from_auth_config(&state.config().auth));
    let query_rate_layer =
        middleware::from_fn(move |req: axum::extract::Request, next: middleware::Next| {
            let limiter = query_limiter.clone();
            async move {
                if let Some(ctx) = req.extensions().get::<TenantContext>()
                    && let Err(e) = limiter.check_query(&ctx.tenant_id)
                {
                    tracing::warn!(tenant_id = %ctx.tenant_id, "Query request rate limited");
                    return (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response();
                }
                next.run(req).await
            }
        });

    // Create admin auth middleware layer
    let admin_key_hash = state
        .config()
        .auth
        .admin_api_key
        .as_ref()
        .map(|key| Authenticator::hash_api_key(key));
    let admin_auth_layer = middleware::from_fn(move |req, next| {
        admin_auth_middleware(admin_key_hash.clone(), req, next)
    });

    // Build admin routes
    let admin_router = Router::new()
        .route("/tenants", get(endpoints::admin::list_tenants::<S>))
        .route("/tenants", post(endpoints::admin::create_tenant::<S>))
        .route(
            "/tenants/{tenant_id}",
            get(endpoints::admin::get_tenant::<S>),
        )
        .route(
            "/tenants/{tenant_id}",
            put(endpoints::admin::update_tenant::<S>),
        )
        .route(
            "/tenants/{tenant_id}",
            delete(endpoints::admin::delete_tenant::<S>),
        )
        .route(
            "/tenants/{tenant_id}/api-keys",
            get(endpoints::admin::list_api_keys::<S>),
        )
        .route(
            "/tenants/{tenant_id}/api-keys",
            post(endpoints::admin::create_api_key::<S>),
        )
        .route(
            "/tenants/{tenant_id}/api-keys/{key_id}",
            delete(endpoints::admin::revoke_api_key::<S>),
        )
        .route(
            "/tenants/{tenant_id}/datasets",
            get(endpoints::admin::list_datasets::<S>),
        )
        .route(
            "/tenants/{tenant_id}/datasets",
            post(endpoints::admin::create_dataset::<S>),
        )
        .route(
            "/tenants/{tenant_id}/datasets/{dataset_id}",
            delete(endpoints::admin::delete_dataset::<S>),
        )
        .layer(admin_auth_layer);

    // Load OpenAPI spec from the generated JSON file
    let openapi_spec = load_openapi_spec();

    Router::new()
        // Public health check endpoint (no authentication)
        .route("/health", get(health_check))
        // OpenAPI spec endpoint (public)
        .route(
            "/api/v1/openapi.json",
            get(move || {
                let spec = openapi_spec.clone();
                async move { Json(spec) }
            }),
        )
        // Protected routes with authentication
        .nest(
            "/tempo",
            endpoints::tempo::router()
                .layer(query_rate_layer.clone())
                .layer(auth_layer.clone()),
        )
        // Pyroscope-compatible profile query API
        .nest(
            "/pyroscope",
            endpoints::pyroscope::router()
                .layer(query_rate_layer.clone())
                .layer(auth_layer.clone()),
        )
        // Trace-to-profile correlation
        .nest(
            "/api/profiles",
            endpoints::pyroscope::profiles_router()
                .layer(query_rate_layer.clone())
                .layer(auth_layer.clone()),
        )
        // Admin routes with admin authentication
        .nest("/api/v1/admin", admin_router)
        .nest(
            "/api/v1",
            endpoints::tenant::router()
                .layer(query_rate_layer)
                .layer(auth_layer),
        )
        // OTel HTTP server metrics for all routes (no-op unless
        // self-monitoring is enabled)
        .layer(middleware::from_fn(
            common::self_monitoring::http_metrics_middleware,
        ))
        .with_state(state)
}

/// Create a new Flight service instance
pub fn create_flight_service<S: RouterState>(
    state: S,
) -> endpoints::flight::SignalDBFlightService<S> {
    endpoints::flight::SignalDBFlightService::new(state)
}

/// Basic health check endpoint
async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}

/// Load the OpenAPI specification from the generated JSON file
fn load_openapi_spec() -> serde_json::Value {
    serde_json::from_str(include_str!("../../../api/admin-api.json"))
        .expect("api/admin-api.json must be valid JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use common::config::{ApiKeyConfig, TenantConfig, TenantLimits};
    use tower::ServiceExt;

    fn test_config(query_limit: Option<u32>) -> Configuration {
        let mut config = Configuration::default();
        config.auth = common::config::AuthConfig {
            default_limits: TenantLimits {
                max_query_requests_per_sec: query_limit,
                burst_seconds: 1.0,
                ..Default::default()
            },
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme".to_string(),
                default_dataset: Some("default".to_string()),
                datasets: vec![],
                api_keys: vec![ApiKeyConfig {
                    key: "sk-test-key".to_string(),
                    name: Some("test".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        };
        config
    }

    async fn echo_request(app: &Router) -> StatusCode {
        let request = Request::builder()
            .uri("/tempo/api/echo")
            .header("authorization", "Bearer sk-test-key")
            .header("x-tenant-id", "acme")
            .body(Body::empty())
            .unwrap();
        app.clone().oneshot(request).await.unwrap().status()
    }

    #[tokio::test]
    async fn query_requests_are_rate_limited_per_tenant() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let state = InMemoryStateImpl::new(catalog, test_config(Some(2)));
        let app = create_router(state);

        assert_eq!(echo_request(&app).await, StatusCode::OK);
        assert_eq!(echo_request(&app).await, StatusCode::OK);
        assert_eq!(echo_request(&app).await, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn query_requests_unlimited_without_configured_limit() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let state = InMemoryStateImpl::new(catalog, test_config(None));
        let app = create_router(state);

        for _ in 0..50 {
            assert_eq!(echo_request(&app).await, StatusCode::OK);
        }
    }
}
