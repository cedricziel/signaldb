use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use common::catalog::Catalog;
use common::config::Configuration;

pub mod discovery;
pub mod endpoints;

pub trait RouterState: std::fmt::Debug + Clone + Send + Sync + 'static {
    fn catalog(&self) -> &Catalog;
    fn service_registry(&self) -> &discovery::ServiceRegistry;
    fn config(&self) -> &Configuration;
}

/// RouterState holds any shared state that needs to be accessed by route handlers
#[derive(Clone)]
pub struct InMemoryStateImpl {
    catalog: Catalog,
    service_registry: discovery::ServiceRegistry,
    config: Configuration,
}

impl std::fmt::Debug for InMemoryStateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryStateImpl")
            .field("catalog", &"Catalog")
            .field("service_registry", &self.service_registry)
            .field("config", &"Configuration")
            .finish()
    }
}

impl InMemoryStateImpl {
    /// Create a new InMemoryStateImpl with the given catalog and configuration
    pub fn new(catalog: Catalog, config: Configuration) -> Self {
        let service_registry = discovery::ServiceRegistry::new(catalog.clone());
        Self {
            catalog,
            service_registry,
            config,
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
}

/// Create a new router instance with all routes configured
pub fn create_router<S: RouterState>(state: S) -> Router<S> {
    Router::new()
        .with_state(state.clone())
        .route("/health", get(health_check))
        .nest("/tempo", endpoints::tempo::router())
        .nest("/api/v1", endpoints::tenant::router())
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
