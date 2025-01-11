use std::sync::Arc;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use common::queue::Queue;
use tokio::sync::Mutex;

mod endpoints;

/// RouterState holds any shared state that needs to be accessed by route handlers
#[derive(Clone, Debug)]
pub struct RouterState {
    queue: Arc<dyn Queue>,
}

/// Create a new router instance with all routes configured
pub fn create_router(state: RouterState) -> Router<RouterState> {
    Router::new()
        .with_state(state)
        .route("/health", get(health_check))
        .nest("/tempo", endpoints::tempo::router())
}

/// Basic health check endpoint
async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}
