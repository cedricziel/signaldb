use axum::{
    Json, Router,
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use common::auth::{Authenticator, admin_auth_middleware, auth_middleware};
use common::catalog::Catalog;
use common::config::Configuration;
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
        let service_registry = discovery::ServiceRegistry::new(catalog.clone());
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
        let service_registry =
            discovery::ServiceRegistry::with_flight_transport(catalog.clone(), flight_transport);
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
            endpoints::tempo::router().layer(auth_layer.clone()),
        )
        // Admin routes with admin authentication
        .nest("/api/v1/admin", admin_router)
        .nest("/api/v1", endpoints::tenant::router().layer(auth_layer))
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
