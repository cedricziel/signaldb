use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
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
    let advertise_addr =
        std::env::var("WRITER_ADVERTISE_ADDR").unwrap_or_else(|_| flight_addr.to_string());

    let service_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, advertise_addr.clone())
            .await
            .context("Failed to initialize service bootstrap")?;

    // Create Flight transport and register this writer's Flight service
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Register this writer's Flight service with its capabilities
    let service_id = flight_transport
        .register_flight_service(
            ServiceType::Writer,
            flight_addr.ip().to_string(),
            flight_addr.port(),
            vec![
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register Flight service: {}", e))?;

    log::info!("Writer Flight service registered with ID: {}", service_id);

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

    // Graceful shutdown: unregister Flight service
    flight_transport
        .unregister_service(service_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to unregister Flight service: {}", e))?;

    // Note: ServiceBootstrap shutdown is handled via drop impl when flight_transport is dropped

    // Shutdown Flight server
    flight_handle.abort();

    Ok(())
}
