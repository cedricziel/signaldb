use anyhow::Context;
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use arrow_flight::flight_service_server::FlightServiceServer;
use writer::WriterFlightService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load configuration
    let config = Configuration::load().context("Failed to load configuration")?;

    // Get Flight address for service registration
    let flight_addr = std::env::var("WRITER_FLIGHT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50061".to_string())
        .parse::<std::net::SocketAddr>()
        .context("Invalid WRITER_FLIGHT_ADDR")?;

    // Initialize service bootstrap for catalog-based discovery
    let advertise_addr = std::env::var("WRITER_ADVERTISE_ADDR")
        .unwrap_or_else(|_| flight_addr.to_string());
    
    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Writer,
        advertise_addr,
    ).await
    .context("Failed to initialize service bootstrap")?;

    // Initialize object store (local filesystem)
    let prefix = config.default_storage_prefix();
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(&prefix)
            .context("Failed to initialize local object store")?,
    );

    // Create Flight ingestion service
    let flight_service = WriterFlightService::new(object_store.clone());
    log::info!("Starting Flight ingest service on {}", flight_addr);
    let flight_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(flight_service))
            .serve(flight_addr)
            .await
            .unwrap();
    });

    // Await shutdown signal
    signal::ctrl_c()
        .await
        .context("Failed to listen for shutdown signal")?;
    log::info!("Shutting down writer service");

    // Graceful shutdown: deregister from catalog first, then stop Flight server
    service_bootstrap.shutdown().await
        .context("Failed to shutdown service bootstrap")?;
    
    // Shutdown Flight server
    flight_handle.abort();

    Ok(())
}
