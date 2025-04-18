use std::sync::Arc;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use messaging::{backend::nats::NatsBackend, MessagingBackend};
use tonic::transport::Server;

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
}

pub trait RouterState: std::fmt::Debug + Clone + Send + Sync + 'static {
    type Q: MessagingBackend;

    fn queue(&self) -> &Self::Q;
}

/// RouterState holds any shared state that needs to be accessed by route handlers
#[derive(Clone, Debug)]
pub struct InMemoryStateImpl {
    queue: Arc<messaging::backend::memory::InMemoryStreamingBackend>,
}

impl InMemoryStateImpl {
    /// Create a new InMemoryStateImpl with the given queue
    pub fn new(queue: messaging::backend::memory::InMemoryStreamingBackend) -> Self {
        Self {
            queue: Arc::new(queue),
        }
    }
}

impl RouterState for InMemoryStateImpl {
    type Q = messaging::backend::memory::InMemoryStreamingBackend;

    fn queue(&self) -> &Self::Q {
        &self.queue
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
pub fn create_flight_service<S: RouterState>(state: S) -> endpoints::flight::SignalDBFlightService<S> {
    endpoints::flight::SignalDBFlightService::new(state)
}

/// Basic health check endpoint
async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}
