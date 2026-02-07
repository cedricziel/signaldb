use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::{Parser, Subcommand};
use common::CatalogManager;
use common::cli::{CommonArgs, CommonCommands, utils};
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use querier::QuerierFlightService;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;

#[derive(Parser)]
#[command(name = "signaldb-querier")]
#[command(about = "SignalDB Querier Service - executes queries on stored observability data")]
#[command(version)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<QuerierCommands>,

    #[arg(long, help = "Arrow Flight server port", default_value = "50054")]
    flight_port: u16,

    #[arg(long, help = "Bind address for servers", default_value = "0.0.0.0")]
    bind: String,
}

#[derive(Subcommand)]
enum QuerierCommands {
    #[command(flatten)]
    Common(CommonCommands),
}

impl Default for QuerierCommands {
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
    let QuerierCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    let _telemetry = match common::self_monitoring::init_telemetry(&config, "signaldb-querier") {
        Ok(t) => {
            if t.is_some() {
                log::info!("Self-monitoring telemetry initialized");
            }
            t
        }
        Err(e) => {
            log::warn!("Self-monitoring init failed, continuing without it: {e}");
            None
        }
    };

    let bind_ip = cli
        .bind
        .parse::<std::net::IpAddr>()
        .context("Invalid bind address")?;
    let flight_addr = std::net::SocketAddr::new(bind_ip, cli.flight_port);

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

    // Create shared catalog manager
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .context("Failed to create catalog manager")?,
    );

    // Create Flight query service with CatalogManager for per-tenant catalog support
    let flight_service =
        QuerierFlightService::new_with_catalog_manager(flight_transport.clone(), catalog_manager)
            .await
            .context("Failed to create querier flight service with CatalogManager")?;
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

    if let Some(telemetry) = _telemetry {
        telemetry.shutdown();
    }

    flight_handle.abort();

    Ok(())
}
