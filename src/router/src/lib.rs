use std::sync::Arc;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use common::queue::{NatsQueue, Queue};

mod endpoints;

/// RouterState implementation for NATS
#[derive(Clone, Debug)]
pub struct NatsStateImpl {
    queue: Arc<NatsQueue>,
}

impl NatsStateImpl {
    /// Create a new NatsStateImpl with the given queue
    pub fn new(queue: NatsQueue) -> Self {
        Self {
            queue: Arc::new(queue),
        }
    }
}

impl RouterState for NatsStateImpl {
    type Q = NatsQueue;

    fn queue(&self) -> &Self::Q {
        &self.queue
    }
}

pub trait RouterState: std::fmt::Debug + Clone + Send + Sync + 'static {
    type Q: Queue;

    fn queue(&self) -> &Self::Q;
}

/// RouterState holds any shared state that needs to be accessed by route handlers
#[derive(Clone, Debug)]
pub struct InMemoryStateImpl {
    queue: Arc<common::queue::InMemoryQueue>,
}

impl InMemoryStateImpl {
    /// Create a new InMemoryStateImpl with the given queue
    pub fn new(queue: common::queue::InMemoryQueue) -> Self {
        Self {
            queue: Arc::new(queue),
        }
    }
}

impl RouterState for InMemoryStateImpl {
    type Q = common::queue::InMemoryQueue;

    fn queue(&self) -> &Self::Q {
        &self.queue
    }
}

/// Create a new router instance with all routes configured
pub fn create_router<S: RouterState>(state: S) -> Router<S> {
    Router::new()
        .with_state(state)
        .route("/health", get(health_check))
        .nest("/tempo", endpoints::tempo::router())
}

/// Basic health check endpoint
async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}
