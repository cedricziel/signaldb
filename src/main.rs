use acceptor::{serve_otlp_grpc, serve_otlp_http};
use anyhow::{Context, Result};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::config::Configuration;
use messaging::backend::memory::InMemoryStreamingBackend;
use router::{create_flight_service, create_router, InMemoryStateImpl};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{oneshot, Mutex};
use tokio::time::{sleep, Duration};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load application configuration
    let config = Configuration::load().context("Failed to load configuration")?;
    
    // Initialize router service bootstrap for catalog-based discovery
    let flight_addr = SocketAddr::from(([0, 0, 0, 0], 50053));
    let router_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Router,
        flight_addr.to_string(),
    ).await
    .context("Failed to initialize router service bootstrap")?;
    
    // Spawn a task to periodically log catalog state (similar to previous behavior)
    if config.discovery.is_some() {
        let catalog = router_bootstrap.catalog().clone();
        let poll_interval = config.discovery.as_ref().unwrap().poll_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(poll_interval);
            loop {
                ticker.tick().await;
                if let Ok(ings) = catalog.list_ingesters().await {
                    log::info!("Catalog ingesters: {:#?}", ings);
                }
                if let Ok(shs) = catalog.list_shards().await {
                    log::info!("Catalog shards: {:#?}", shs);
                }
                if let Ok(owns) = catalog.list_shard_owners().await {
                    log::info!("Catalog shard owners: {:#?}", owns);
                }
            }
        });
    }

    // Initialize queue for future use
    let queue = Arc::new(Mutex::new(InMemoryStreamingBackend::new(10)));

    // Create router state
    let state = InMemoryStateImpl::new(InMemoryStreamingBackend::new(10));

    let (otlp_grpc_init_tx, otlp_grpc_init_rx) = oneshot::channel::<()>();
    let (_otlp_grpc_shutdown_tx, otlp_grpc_shutdown_rx) = oneshot::channel::<()>();
    let (otlp_grpc_stopped_tx, _otlp_grpc_stopped_rx) = oneshot::channel::<()>();

    let (otlp_http_init_tx, otlp_http_init_rx) = oneshot::channel::<()>();
    let (_otlp_http_shutdown_tx, otlp_http_shutdown_rx) = oneshot::channel::<()>();
    let (otlp_http_stopped_tx, _otlp_http_stopped_rx) = oneshot::channel::<()>();

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

    // Start HTTP router
    let app = create_router(state.clone());
    let http_router_addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let http_router_handle = tokio::spawn(async move {
        log::info!("Starting HTTP router on {}", http_router_addr);

        // For now, we'll skip starting the HTTP server due to compatibility issues with axum 0.7.9
        // This will be fixed in a future update
        log::warn!("HTTP router is disabled due to compatibility issues with axum 0.7.9");

        // To enable the HTTP router, you would need to update the axum dependency to a version
        // that supports the current usage pattern
    });

    // Start Flight service
    let flight_service = create_flight_service(state);
    let flight_addr = SocketAddr::from(([0, 0, 0, 0], 50053));
    let flight_handle = tokio::spawn(async move {
        log::info!("Starting Flight service on {}", flight_addr);

        match Server::builder()
            .add_service(
                arrow_flight::flight_service_server::FlightServiceServer::new(flight_service),
            )
            .serve(flight_addr)
            .await
        {
            Ok(_) => log::info!("Flight service stopped"),
            Err(e) => log::error!("Flight service error: {}", e),
        }
    });

    // Wait for OTLP servers to initialize
    otlp_grpc_init_rx
        .await
        .context("Failed to receive init signal from OTLP/gRPC server")?;
    otlp_http_init_rx
        .await
        .context("Failed to receive init signal from OTLP/HTTP server")?;

    log::info!("All services started successfully");

    // Wait for ctrl+c
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for ctrl+c signal")?;
    log::info!("Shutting down service discovery and other services");
    // Graceful deregistration using service bootstrap
    if let Err(e) = router_bootstrap.shutdown().await {
        log::error!("Failed to shutdown router service bootstrap: {}", e);
    }

    // Wait for servers to stop
    let _ = grpc_handle.await;
    let _ = http_handle.await;
    let _ = http_router_handle.await;
    let _ = flight_handle.await;

    Ok(())
}
