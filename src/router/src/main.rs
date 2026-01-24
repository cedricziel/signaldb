use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use common::cli::{CommonArgs, CommonCommands, utils};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use router::{InMemoryStateImpl, RouterState, create_flight_service, create_router};
use std::net::SocketAddr;
use tonic::transport::Server;

#[derive(Parser)]
#[command(name = "signaldb-router")]
#[command(about = "SignalDB Router Service - routes queries and provides HTTP/Flight APIs")]
#[command(version)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<RouterCommands>,

    #[arg(long, help = "Arrow Flight server port", default_value = "50053")]
    flight_port: u16,

    #[arg(long, help = "HTTP API server port", default_value = "3000")]
    http_port: u16,

    #[arg(long, help = "Bind address for servers", default_value = "0.0.0.0")]
    bind: String,
}

#[derive(Subcommand)]
enum RouterCommands {
    #[command(flatten)]
    Common(CommonCommands),
}

impl Default for RouterCommands {
    fn default() -> Self {
        Self::Common(CommonCommands::Start)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging based on CLI arguments
    utils::init_logging(&cli.common);

    // Load application configuration
    let config = utils::load_config(cli.common.config.as_ref())?;

    // Handle common commands that don't require starting the service
    let command = cli.command.unwrap_or_default();
    let RouterCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    log::info!("Starting SignalDB Router Service");

    // Use CLI-provided ports or defaults
    let bind_ip = cli
        .bind
        .parse::<std::net::IpAddr>()
        .context("Invalid bind address")?;
    let flight_addr = SocketAddr::new(bind_ip, cli.flight_port);
    let http_addr = SocketAddr::new(bind_ip, cli.http_port);

    // Initialize router service bootstrap for catalog-based discovery
    let router_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Router, flight_addr.to_string())
            .await
            .context("Failed to initialize router service bootstrap")?;

    // Create router state with catalog access and configuration
    let state = InMemoryStateImpl::new(router_bootstrap.catalog().clone(), config.clone());

    // Start background service discovery polling
    if let Some(discovery) = &config.discovery {
        let poll_interval = discovery.poll_interval;
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
