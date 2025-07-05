use anyhow::{Context, Result};
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use router::{create_flight_service, create_router, InMemoryStateImpl, RouterState};
use std::net::SocketAddr;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load application configuration
    let config = Configuration::load().context("Failed to load configuration")?;

    log::info!("Starting SignalDB Router Service");

    // Standard ports for router services
    let flight_addr = SocketAddr::from(([0, 0, 0, 0], 50053)); // Arrow Flight
    let http_addr = SocketAddr::from(([0, 0, 0, 0], 3000)); // Tempo API

    // Initialize router service bootstrap for catalog-based discovery
    let router_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Router, flight_addr.to_string())
            .await
            .context("Failed to initialize router service bootstrap")?;

    // Create router state with catalog access
    let state = InMemoryStateImpl::new(router_bootstrap.catalog().clone());

    // Start background service discovery polling
    if config.discovery.is_some() {
        let poll_interval = config.discovery.as_ref().unwrap().poll_interval;
        state
            .service_registry()
            .start_background_polling(poll_interval)
            .await;
        log::info!("Started service registry background polling with interval: {poll_interval:?}");
    }

    log::info!("Router service registered with catalog");

    // Create HTTP router
    let _app = create_router(state.clone());
    let http_handle = tokio::spawn(async move {
        log::info!("Starting HTTP router on {http_addr}");

        // For now, we'll skip starting the HTTP server due to compatibility issues with axum 0.7.9
        // This will be fixed in a future update
        log::warn!("HTTP router is disabled due to compatibility issues with axum 0.7.9");

        // To enable the HTTP router, you would need to update the axum dependency to a version
        // that supports the current usage pattern
    });

    // Start Flight service
    let flight_service = create_flight_service(state);
    let flight_handle = tokio::spawn(async move {
        log::info!("Starting Flight service on {flight_addr}");

        match Server::builder()
            .add_service(
                arrow_flight::flight_service_server::FlightServiceServer::new(flight_service),
            )
            .serve(flight_addr)
            .await
        {
            Ok(_) => log::info!("Flight service stopped"),
            Err(e) => log::error!("Flight service error: {e}"),
        }
    });

    log::info!("✅ Router service started successfully");
    log::info!("🛩️  Arrow Flight server listening on {flight_addr}");
    log::info!("🌐 HTTP API server on {http_addr} (currently disabled)");

    // Wait for ctrl+c
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for ctrl+c signal")?;

    log::info!("🛑 Shutting down router service...");

    // Graceful deregistration using service bootstrap
    if let Err(e) = router_bootstrap.shutdown().await {
        log::error!("Failed to shutdown router service bootstrap: {e}");
    }

    // Wait for servers to stop
    let _ = http_handle.await;
    let _ = flight_handle.await;

    log::info!("✅ Router service stopped gracefully");

    Ok(())
}
