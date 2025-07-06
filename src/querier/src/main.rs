use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use querier::QuerierFlightService;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load configuration
    let config = Configuration::load().context("Failed to load configuration")?;

    // Get Flight address for service registration
    let flight_addr = std::env::var("QUERIER_FLIGHT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50054".to_string())
        .parse::<std::net::SocketAddr>()
        .context("Invalid QUERIER_FLIGHT_ADDR")?;

    // Initialize service bootstrap for catalog-based discovery
    let advertise_addr =
        std::env::var("QUERIER_ADVERTISE_ADDR").unwrap_or_else(|_| flight_addr.to_string());

    let service_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Querier, advertise_addr.clone())
            .await
            .context("Failed to initialize service bootstrap")?;

    // Create Flight transport and register this querier's Flight service
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Register this querier's Flight service with its capabilities
    let service_id = flight_transport
        .register_flight_service(
            ServiceType::Querier,
            flight_addr.ip().to_string(),
            flight_addr.port(),
            vec![ServiceCapability::QueryExecution],
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register Flight service: {}", e))?;

    log::info!("Querier Flight service registered with ID: {service_id}");

    // Initialize object store from configuration for reading historical data
    let object_store = common::storage::create_object_store(&config.storage)
        .context("Failed to initialize object store")?;

    // Create Flight query service
    let flight_service = QuerierFlightService::new(object_store.clone(), flight_transport.clone());
    log::info!("Starting Flight query service on {flight_addr}");
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
    log::info!("Shutting down querier service");

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
