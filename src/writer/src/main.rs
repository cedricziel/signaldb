use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::{Parser, Subcommand};
use common::cli::{CommonArgs, CommonCommands, utils};
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{Wal, WalConfig};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use writer::IcebergWriterFlightService;

#[derive(Parser)]
#[command(name = "signaldb-writer")]
#[command(about = "SignalDB Writer Service - ingests and stores observability data")]
#[command(version)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<WriterCommands>,

    #[arg(long, help = "Arrow Flight server port", default_value = "50061")]
    flight_port: u16,

    #[arg(
        long,
        env = "WRITER_WAL_DIR",
        help = "WAL directory path",
        default_value = ".wal/writer"
    )]
    wal_dir: PathBuf,

    #[arg(long, help = "Bind address for servers", default_value = "0.0.0.0")]
    bind: String,
}

#[derive(Subcommand)]
enum WriterCommands {
    #[command(flatten)]
    Common(CommonCommands),
}

impl Default for WriterCommands {
    fn default() -> Self {
        Self::Common(CommonCommands::Start)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize logging based on CLI arguments
    utils::init_logging(&cli.common);

    // Load configuration
    let config = utils::load_config(cli.common.config.as_ref())?;

    // Handle common commands that don't require starting the service
    let command = cli.command.unwrap_or_default();
    let WriterCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    // Get Flight address for service registration
    let bind_ip = cli
        .bind
        .parse::<std::net::IpAddr>()
        .context("Invalid bind address")?;
    let flight_addr = std::net::SocketAddr::new(bind_ip, cli.flight_port);

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

    log::info!("Writer Flight service registered with ID: {service_id}");

    // Initialize object store from configuration
    let object_store = common::storage::create_object_store(&config.storage)
        .context("Failed to initialize object store")?;

    // Initialize WAL for durability
    let wal_config = WalConfig {
        wal_dir: cli.wal_dir,
        ..Default::default()
    };

    let mut wal = Wal::new(wal_config)
        .await
        .context("Failed to initialize WAL")?;

    // Start background WAL flush task
    wal.start_background_flush();
    let wal = Arc::new(wal);

    // Create Iceberg-based Flight ingestion service with WAL
    let flight_service =
        IcebergWriterFlightService::new(config.clone(), object_store.clone(), wal.clone());

    // Start background WAL processing for Iceberg writes
    let writer_bg_handle = flight_service.start_background_processing();

    log::info!("Starting Flight ingest service on {flight_addr}");
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

    // Shutdown Flight server first
    flight_handle.abort();
    let _ = flight_handle.await;

    // Stop background WAL processing task to release Arc<Wal> reference
    log::info!("Stopping background WAL processing task");
    writer_bg_handle.abort();
    let _ = writer_bg_handle.await;

    // Shutdown WAL and flush any remaining data
    if let Ok(wal) = Arc::try_unwrap(wal) {
        wal.shutdown().await.context("Failed to shutdown WAL")?;
    } else {
        log::warn!("Could not get exclusive access to WAL for shutdown - forcing flush");
        // WAL will be dropped and cleaned up automatically
    }

    Ok(())
}
