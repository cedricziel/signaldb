use acceptor::{serve_otlp_grpc, serve_otlp_http};
use anyhow::{Context, Result};
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use std::net::SocketAddr;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load application configuration
    let config = Configuration::load().context("Failed to load configuration")?;

    log::info!("Starting SignalDB Acceptor Service");

    // Standard OTLP ports
    let grpc_addr = SocketAddr::from(([0, 0, 0, 0], 4317)); // Standard OTLP gRPC port
    let http_addr = SocketAddr::from(([0, 0, 0, 0], 4318)); // Standard OTLP HTTP port

    // Initialize acceptor service bootstrap for catalog-based discovery
    let acceptor_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        grpc_addr.to_string(), // Register the main gRPC endpoint
    )
    .await
    .context("Failed to initialize acceptor service bootstrap")?;

    log::info!("Acceptor service registered with catalog");

    // Channels for OTLP/gRPC server signals
    let (grpc_init_tx, grpc_init_rx) = oneshot::channel::<()>();
    let (grpc_shutdown_tx, grpc_shutdown_rx) = oneshot::channel::<()>();
    let (grpc_stopped_tx, grpc_stopped_rx) = oneshot::channel::<()>();

    // Channels for OTLP/HTTP server signals
    let (http_init_tx, http_init_rx) = oneshot::channel::<()>();
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel::<()>();
    let (http_stopped_tx, http_stopped_rx) = oneshot::channel::<()>();

    // Spawn OTLP/gRPC acceptor
    let grpc_handle = tokio::spawn(async move {
        if let Err(e) = serve_otlp_grpc(grpc_init_tx, grpc_shutdown_rx, grpc_stopped_tx).await {
            log::error!("OTLP/gRPC server error: {e}");
        }
    });

    // Spawn OTLP/HTTP acceptor
    let http_handle = tokio::spawn(async move {
        if let Err(e) = serve_otlp_http(http_init_tx, http_shutdown_rx, http_stopped_tx).await {
            log::error!("OTLP/HTTP server error: {e}");
        }
    });

    // Await initialization signals
    grpc_init_rx
        .await
        .context("Failed to initialize OTLP/gRPC server")?;
    http_init_rx
        .await
        .context("Failed to initialize OTLP/HTTP server")?;

    log::info!("✅ Acceptor service started successfully");
    log::info!("📡 OTLP gRPC server listening on {grpc_addr}");
    log::info!("🌐 OTLP HTTP server listening on {http_addr}");

    // Wait for shutdown signal (Ctrl+C)
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for shutdown signal")?;

    log::info!("🛑 Shutting down acceptor service...");

    // Graceful deregistration using service bootstrap
    if let Err(e) = acceptor_bootstrap.shutdown().await {
        log::error!("Failed to shutdown acceptor service bootstrap: {e}");
    }

    // Trigger shutdown
    let _ = grpc_shutdown_tx.send(());
    let _ = http_shutdown_tx.send(());

    // Wait for services to stop
    let _ = grpc_stopped_rx.await;
    let _ = http_stopped_rx.await;

    // Await spawned tasks
    let _ = grpc_handle.await;
    let _ = http_handle.await;

    log::info!("✅ Acceptor service stopped gracefully");

    Ok(())
}
