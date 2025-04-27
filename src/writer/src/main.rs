use anyhow::Context;
use common::config::Configuration;
use common::discovery::{Instance as DiscoveryInstance, NatsDiscovery};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::task::JoinHandle;
use uuid::Uuid;
use tonic::transport::Server;
use arrow_flight::flight_service_server::FlightServiceServer;
use writer::WriterFlightService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load configuration
    let config = Configuration::load().context("Failed to load configuration")?;

    // Service discovery (NATS subject-based)
    // Service discovery (optional, default to NATS)
    let mut discovery_handle: Option<(NatsDiscovery, JoinHandle<()>)> = None;
    let discovery_kind = std::env::var("DISCOVERY_KIND").unwrap_or_else(|_| "nats".to_string());
    log::info!("Writer service discovery mode: {}", discovery_kind);
    if discovery_kind.eq_ignore_ascii_case("nats") {
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".to_string());
        let advertise_addr = std::env::var("WRITER_ADVERTISE_ADDR").unwrap_or_else(|_| {
            // Use local host with no real port (writer is headless)
            "127.0.0.1:0".to_string()
        });
        // Heartbeat interval for service discovery
        let heartbeat_interval = Duration::from_secs(10);
        let instance_id = Uuid::new_v4().to_string();
        let inst = DiscoveryInstance {
            id: instance_id.clone(),
            host: advertise_addr,
            port: 0,
        };
        let nd = NatsDiscovery::new(&nats_url, "writer", inst)
            .await
            .context("Failed to initialize NATS discovery")?;
        nd.register().await.context("Failed to register writer")?;
        let hb = nd.spawn_heartbeat(heartbeat_interval);
        discovery_handle = Some((nd, hb));
    }

    // Initialize object store (local filesystem)
    let prefix = config.default_storage_prefix();
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(&prefix)
            .context("Failed to initialize local object store")?,
    );

    // Create Flight ingestion service
    let flight_service = WriterFlightService::new(object_store.clone());
    let flight_addr = std::env::var("WRITER_FLIGHT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50061".to_string())
        .parse()
        .context("Invalid WRITER_FLIGHT_ADDR")?;
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

    // Deregister discovery
    if let Some((nd, hb)) = discovery_handle {
        hb.abort();
        nd.deregister()
            .await
            .context("Failed to deregister writer")?;
    }
    // Shutdown Flight server
    flight_handle.abort();

    Ok(())
}
