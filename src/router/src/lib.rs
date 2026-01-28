use axum::{Router, http::StatusCode, middleware, response::IntoResponse, routing::get};
use common::auth::{Authenticator, auth_middleware};
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

    Router::new()
        // Public health check endpoint (no authentication)
        .route("/health", get(health_check))
        // Protected routes with authentication
        .nest(
            "/tempo",
            endpoints::tempo::router().layer(auth_layer.clone()),
        )
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
