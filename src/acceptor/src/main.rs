use anyhow::{anyhow, Context, Result};
use acceptor::{serve_otlp_grpc, serve_otlp_http};
use common::config::{Configuration, CONFIG};
use common::discovery::{register, deregister, Instance};
use std::env;
use tokio::signal;
use tokio::sync::oneshot;
use tracing_subscriber;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load configuration
    let config = Configuration::load().context("Failed to load configuration")?;
    CONFIG
        .set(config)
        .map_err(|_| anyhow!("Configuration already set"))?;

    // Channels for OTLP/gRPC
    let (grpc_init_tx, grpc_init_rx) = oneshot::channel();
    let (grpc_shutdown_tx, grpc_shutdown_rx) = oneshot::channel();
    let (grpc_stopped_tx, grpc_stopped_rx) = oneshot::channel();

    // Channels for OTLP/HTTP
    let (http_init_tx, http_init_rx) = oneshot::channel();
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel();
    let (http_stopped_tx, http_stopped_rx) = oneshot::channel();

    // Start OTLP/gRPC server
    let grpc_handle = tokio::spawn(async move {
        serve_otlp_grpc(grpc_init_tx, grpc_shutdown_rx, grpc_stopped_tx)
            .await
            .expect("Failed to start OTLP/gRPC server");
    });

    // Start OTLP/HTTP server
    let http_handle = tokio::spawn(async move {
        serve_otlp_http(http_init_tx, http_shutdown_rx, http_stopped_tx)
            .await
            .expect("Failed to start OTLP/HTTP server");
    });

    // Wait for both services to initialize
    grpc_init_rx
        .await
        .context("Failed to receive init signal from OTLP/gRPC server")?;
    http_init_rx
        .await
        .context("Failed to receive init signal from OTLP/HTTP server")?;

    // Register this acceptor instance with service discovery
    let instance_id = Uuid::new_v4().to_string();
    let host = env::var("SERVICE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    // The OTLP/gRPC port is fixed at 4317
    let port: u16 = env::var("OTLP_GRPC_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(4317);
    register(
        "acceptor",
        Instance {
            id: instance_id.clone(),
            host,
            port,
        },
    )
    .await
    .context("Service discovery register failed")?;

    // Wait for shutdown signal (Ctrl+C)
    signal::ctrl_c()
        .await
        .context("Failed to listen for ctrl+c signal")?;

    // Deregister this acceptor instance
    deregister("acceptor", &instance_id)
        .await
        .context("Service discovery deregister failed")?;

    // Signal servers to shut down
    let _ = grpc_shutdown_tx.send(());
    let _ = http_shutdown_tx.send(());

    // Wait for servers to stop
    let _ = grpc_stopped_rx.await;
    let _ = http_stopped_rx.await;

    // Await server tasks
    let _ = grpc_handle.await;
    let _ = http_handle.await;

    Ok(())
}