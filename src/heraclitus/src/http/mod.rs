pub mod health;
pub mod metrics;

use crate::error::Result;
use axum::{Router, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

/// HTTP server state shared across all handlers
#[derive(Clone)]
pub struct HttpState {
    pub metrics_registry: Arc<prometheus::Registry>,
    pub service_start_time: std::time::Instant,
}

/// Create the HTTP router with all endpoints
pub fn create_router(state: HttpState) -> Router {
    Router::new()
        .route("/health", get(health::health_handler))
        .route("/metrics", get(metrics::metrics_handler))
        .with_state(state)
}

/// Run the HTTP server
pub async fn run_http_server(port: u16, metrics_registry: Arc<prometheus::Registry>) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let state = HttpState {
        metrics_registry,
        service_start_time: std::time::Instant::now(),
    };

    let app = create_router(state);

    info!("Starting HTTP server on {}", addr);

    let listener = TcpListener::bind(addr).await.map_err(|e| {
        crate::error::HeraclitusError::Network(format!("Failed to bind HTTP server: {e}"))
    })?;

    axum::serve(listener, app)
        .await
        .map_err(|e| crate::error::HeraclitusError::Network(format!("HTTP server error: {e}")))?;

    Ok(())
}
