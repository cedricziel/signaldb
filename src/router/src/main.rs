use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use common::cli::{CommonArgs, CommonCommands, utils};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use router::{InMemoryStateImpl, RouterState, create_flight_service, create_router};
use std::net::SocketAddr;
use tokio::sync::oneshot;
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

// Heap profiling: install jemalloc as global allocator when built with
// the jemalloc-profiling feature (see [profiling] config)
#[cfg(feature = "jemalloc-profiling")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load application configuration
    let config = utils::load_config(cli.common.config.as_ref())?;

    // Handle common commands that don't require starting the service
    let command = cli.command.unwrap_or_default();
    let RouterCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    // Initialize self-monitoring telemetry first so the OTel bridge layers
    // can be attached to the tracing subscriber, then initialize logging.
    let (telemetry, telemetry_error) =
        match common::self_monitoring::init_telemetry(&config, "signaldb-router") {
            Ok(t) => (t, None),
            Err(e) => (None, Some(e)),
        };
    utils::init_logging(&cli.common, telemetry.as_ref());
    if let Some(e) = telemetry_error {
        tracing::warn!(error = %e, "Self-monitoring init failed, continuing without it");
    } else if let Some(ref t) = telemetry {
        tracing::info!(
            sampler = %t.sampler_description(),
            "Self-monitoring telemetry initialized"
        );
    }
    let _telemetry = telemetry;

    let _profiling = match common::self_monitoring::init_profiling(&config, "signaldb-router") {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "Profiling init failed, continuing without it");
            None
        }
    };

    tracing::info!("Starting SignalDB Router Service");

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

    router_bootstrap
        .catalog()
        .sync_config_tenants(&config.auth)
        .await
        .context("Failed to sync config tenants to catalog")?;
    tracing::info!(
        tenant_count = config.auth.tenants.len(),
        "Synced config tenants to catalog"
    );

    // Create router state with catalog access and configuration
    let state = InMemoryStateImpl::new(router_bootstrap.catalog().clone(), config.clone());

    // Start background service discovery polling
    if let Some(discovery) = &config.discovery {
        let poll_interval = discovery.poll_interval;
        state
            .service_registry()
            .start_background_polling(poll_interval)
            .await;
        tracing::info!(interval = ?poll_interval, "Started service registry background polling");
    }

    tracing::info!("Router service registered with catalog");

    // Create HTTP router
    let app = create_router(state.clone());
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel::<()>();
    let http_handle = tokio::spawn(async move {
        tracing::info!(address = %http_addr, "Starting HTTP router");
        let listener = tokio::net::TcpListener::bind(http_addr)
            .await
            .expect("Failed to bind HTTP router");
        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(async {
                http_shutdown_rx.await.ok();
                tracing::info!("HTTP router shutting down gracefully");
            })
            .await
            .expect("HTTP router error");
    });

    // Start Flight service
    let flight_service = create_flight_service(state);
    let flight_handle = tokio::spawn(async move {
        tracing::info!(address = %flight_addr, "Starting Flight service");

        match Server::builder()
            .add_service(
                arrow_flight::flight_service_server::FlightServiceServer::new(flight_service),
            )
            .serve(flight_addr)
            .await
        {
            Ok(_) => tracing::info!("Flight service stopped"),
            Err(e) => tracing::error!(error = %e, "Flight service error"),
        }
    });

    tracing::info!("Router service started successfully");
    tracing::info!(address = %flight_addr, "Arrow Flight server listening");
    tracing::info!(address = %http_addr, "HTTP API server listening");

    // Wait for ctrl+c
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for ctrl+c signal")?;

    tracing::info!("Shutting down router service...");

    // Graceful deregistration using service bootstrap
    if let Err(e) = router_bootstrap.shutdown().await {
        tracing::error!(error = %e, "Failed to shutdown router service bootstrap");
    }

    // Signal HTTP router to shutdown gracefully
    let _ = http_shutdown_tx.send(());

    // Wait for servers to stop
    let _ = http_handle.await;
    let _ = flight_handle.await;

    if let Some(telemetry) = _telemetry {
        telemetry.shutdown();
    }

    tracing::info!("Router service stopped gracefully");

    Ok(())
}
