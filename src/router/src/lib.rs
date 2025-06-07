use std::sync::Arc;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use common::catalog::Catalog;
use messaging::{backend::nats::NatsBackend, MessagingBackend};

pub mod discovery;
pub mod endpoints;

/// RouterState implementation for NATS
#[derive(Clone, Debug)]
pub struct NatsStateImpl {
    queue: Arc<NatsBackend>,
}

impl NatsStateImpl {
    /// Create a new NatsStateImpl with the given queue
    pub fn new(queue: NatsBackend) -> Self {
        Self {
            queue: Arc::new(queue),
        }
    }
}

impl RouterState for NatsStateImpl {
    type Q = NatsBackend;

    fn queue(&self) -> &Self::Q {
        &self.queue
    }

    fn catalog(&self) -> &Catalog {
        // TODO: NatsStateImpl needs catalog integration
        unimplemented!("Catalog not implemented for NatsStateImpl")
    }

    fn service_registry(&self) -> &discovery::ServiceRegistry {
        // TODO: NatsStateImpl needs service registry integration
        unimplemented!("ServiceRegistry not implemented for NatsStateImpl")
    }
}

pub trait RouterState: std::fmt::Debug + Clone + Send + Sync + 'static {
    type Q: MessagingBackend;

    fn queue(&self) -> &Self::Q;
    fn catalog(&self) -> &Catalog;
    fn service_registry(&self) -> &discovery::ServiceRegistry;
}

/// RouterState holds any shared state that needs to be accessed by route handlers
#[derive(Clone)]
pub struct InMemoryStateImpl {
    queue: Arc<messaging::backend::memory::InMemoryStreamingBackend>,
    catalog: Catalog,
    service_registry: discovery::ServiceRegistry,
}

impl std::fmt::Debug for InMemoryStateImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryStateImpl")
            .field("queue", &"Arc<InMemoryStreamingBackend>")
            .field("catalog", &"Catalog")
            .field("service_registry", &self.service_registry)
            .finish()
    }
}

impl InMemoryStateImpl {
    /// Create a new InMemoryStateImpl with the given queue and catalog
    pub fn new(
        queue: messaging::backend::memory::InMemoryStreamingBackend,
        catalog: Catalog,
    ) -> Self {
        let service_registry = discovery::ServiceRegistry::new(catalog.clone());
        Self {
            queue: Arc::new(queue),
            catalog,
            service_registry,
        }
    }
}

impl RouterState for InMemoryStateImpl {
    type Q = messaging::backend::memory::InMemoryStreamingBackend;

    fn queue(&self) -> &Self::Q {
        &self.queue
    }

    fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    fn service_registry(&self) -> &discovery::ServiceRegistry {
        &self.service_registry
    }
}

/// Create a new router instance with all routes configured
pub fn create_router<S: RouterState>(state: S) -> Router<S> {
    Router::new()
        .with_state(state.clone())
        .route("/health", get(health_check))
        .nest("/tempo", endpoints::tempo::router())
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
