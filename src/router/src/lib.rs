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
use utoipa::OpenApi;

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
    /// Create a new InMemoryStateImpl with the given catalog and configuration
    pub fn new(catalog: Catalog, config: Configuration) -> Self {
        let service_registry = discovery::ServiceRegistry::new(catalog.clone());

        // Create authenticator from config
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

    // Build OpenAPI spec
    let openapi_spec = build_openapi_spec();

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

/// Build the OpenAPI specification for the admin API
fn build_openapi_spec() -> utoipa::openapi::OpenApi {
    #[derive(OpenApi)]
    #[openapi(
        info(
            title = "SignalDB Admin API",
            version = "1.0.0",
            description = "Tenant management API for SignalDB"
        ),
        paths(
            endpoints::admin::list_tenants,
            endpoints::admin::create_tenant,
            endpoints::admin::get_tenant,
            endpoints::admin::update_tenant,
            endpoints::admin::delete_tenant,
            endpoints::admin::list_api_keys,
            endpoints::admin::create_api_key,
            endpoints::admin::revoke_api_key,
            endpoints::admin::list_datasets,
            endpoints::admin::create_dataset,
            endpoints::admin::delete_dataset,
        ),
        components(schemas(
            signaldb_api::CreateTenantRequest,
            signaldb_api::UpdateTenantRequest,
            signaldb_api::TenantResponse,
            signaldb_api::ListTenantsResponse,
            signaldb_api::CreateApiKeyRequest,
            signaldb_api::CreateApiKeyResponse,
            signaldb_api::ApiKeyResponse,
            signaldb_api::ListApiKeysResponse,
            signaldb_api::CreateDatasetRequest,
            signaldb_api::DatasetResponse,
            signaldb_api::ListDatasetsResponse,
            signaldb_api::ApiError,
        )),
        tags(
            (name = "tenants", description = "Tenant management"),
            (name = "api-keys", description = "API key management"),
            (name = "datasets", description = "Dataset management"),
        )
    )]
    struct AdminApiDoc;

    AdminApiDoc::openapi()
}
