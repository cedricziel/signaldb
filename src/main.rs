use acceptor::{serve_otlp_grpc, serve_otlp_http};
use anyhow::{Context, Result};
use messaging::backend::memory::InMemoryStreamingBackend;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Initialize queue for future use
    let _queue = Arc::new(Mutex::new(InMemoryStreamingBackend::new(10)));

    let (otlp_grpc_init_tx, otlp_grpc_init_rx) = oneshot::channel();
    let (_otlp_grpc_shutdown_tx, otlp_grpc_shutdown_rx) = oneshot::channel();
    let (otlp_grpc_stopped_tx, _otlp_grpc_stopped_rx) = oneshot::channel();

    let (otlp_http_init_tx, otlp_http_init_rx) = oneshot::channel();
    let (_otlp_http_shutdown_tx, otlp_http_shutdown_rx) = oneshot::channel();
    let (otlp_http_stopped_tx, _otlp_http_stopped_rx) = oneshot::channel();

    // Start OTLP/gRPC server
    let grpc_handle = tokio::spawn(async move {
        serve_otlp_grpc(
            otlp_grpc_init_tx,
            otlp_grpc_shutdown_rx,
            otlp_grpc_stopped_tx,
        )
        .await
        .expect("Failed to start OTLP/gRPC server");
    });

    // Start OTLP/HTTP server
    let http_handle = tokio::spawn(async move {
        serve_otlp_http(
            otlp_http_init_tx,
            otlp_http_shutdown_rx,
            otlp_http_stopped_tx,
        )
        .await
        .expect("Failed to start OTLP/HTTP server");
    });

    // Wait for both servers to initialize
    otlp_grpc_init_rx
        .await
        .context("Failed to receive init signal from OTLP/gRPC server")?;
    otlp_http_init_rx
        .await
        .context("Failed to receive init signal from OTLP/HTTP server")?;

    // Wait for ctrl+c
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for ctrl+c signal")?;

    // Wait for servers to stop
    let _ = grpc_handle.await;
    let _ = http_handle.await;

    Ok(())
}
