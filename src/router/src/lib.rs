use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use std::sync::Arc;
use tower_http::trace::TraceLayer;

/// RouterState holds any shared state that needs to be accessed by route handlers
#[derive(Clone)]
pub struct RouterState {
    // Add any shared state here
}

/// Create a new router instance with all routes configured
pub fn create_router() -> Router<Arc<RouterState>> {
    let state = Arc::new(RouterState {
        // Initialize state here
    });

    Router::new()
        .route("/health", get(health_check))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Basic health check endpoint
async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}
