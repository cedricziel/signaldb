use acceptor::{serve_otlp_grpc, serve_otlp_http};
use anyhow::{Context, Result};
use common::catalog::Catalog;
use common::config::Configuration;
use messaging::backend::memory::InMemoryStreamingBackend;
use router::{create_flight_service, create_router, InMemoryStateImpl};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{oneshot, Mutex};
use tokio::time::{sleep, Duration};
use tonic::transport::Server;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load application configuration (including optional service discovery)
    let config = common::config::Configuration::load().context("Failed to load configuration")?;
    // Optional service discovery setup using Catalog
    // Tracks optional registry state: (catalog client, instance id, heartbeat task)
    let mut registry: Option<(Catalog, Uuid, tokio::task::JoinHandle<()>)> = None;
    if let Some(disc) = config.discovery.clone() {
        // Initialize Catalog client
        let catalog = Catalog::new(&disc.dsn)
            .await
            .context("Failed to initialize Catalog client")?;
        // Register this ingester instance
        let ingester_id = Uuid::new_v4();
        let ingester_address =
            std::env::var("INGESTER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:4317".to_string());
        catalog
            .register_ingester(ingester_id, &ingester_address)
            .await
            .context("Failed to register ingester in Catalog")?;
        // Spawn heartbeat task
        let hb_handle = catalog.spawn_ingester_heartbeat(ingester_id, disc.heartbeat_interval);
        // Periodically poll and log Catalog state
        let catalog_poll = catalog.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(disc.poll_interval);
            loop {
                ticker.tick().await;
                if let Ok(ings) = catalog_poll.list_ingesters().await {
                    log::info!("Catalog ingesters: {:#?}", ings);
                }
                if let Ok(shs) = catalog_poll.list_shards().await {
                    log::info!("Catalog shards: {:#?}", shs);
                }
                if let Ok(owns) = catalog_poll.list_shard_owners().await {
                    log::info!("Catalog shard owners: {:#?}", owns);
                }
            }
        });
        registry = Some((catalog, ingester_id, hb_handle));
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
    // Graceful deregistration if service discovery was enabled
    if let Some((catalog, ingester_id, hb_handle)) = registry {
        // stop heartbeat task
        hb_handle.abort();
        // deregister this ingester instance
        if let Err(e) = catalog.deregister_ingester(ingester_id).await {
            log::error!("Failed to deregister ingester {}: {}", ingester_id, e);
        }
    }

    // Wait for servers to stop
    let _ = grpc_handle.await;
    let _ = http_handle.await;
    let _ = http_router_handle.await;
    let _ = flight_handle.await;

    Ok(())
}
