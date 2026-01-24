use acceptor::{serve_otlp_grpc, serve_otlp_http};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use common::cli::{CommonArgs, CommonCommands, utils};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{Wal, WalConfig};
use router::{InMemoryStateImpl, RouterState, create_flight_service, create_router};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::transport::Server;
use writer::IcebergWriterFlightService;

#[derive(Parser)]
#[command(name = "signaldb")]
#[command(about = "SignalDB - distributed observability signal database (monolithic mode)")]
#[command(version)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<SignalDbCommands>,
}

#[derive(Subcommand)]
enum SignalDbCommands {
    #[command(flatten)]
    Common(CommonCommands),
}

impl Default for SignalDbCommands {
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
    let SignalDbCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    log::info!("Loaded configuration:");
    log::info!("  Database DSN: {}", config.database.dsn);
    if let Some(discovery) = &config.discovery {
        log::info!("  Discovery DSN: {}", discovery.dsn);
    } else {
        log::info!("  No discovery configuration");
    }

    // Initialize router service bootstrap for catalog-based discovery
    let flight_addr = SocketAddr::from(([0, 0, 0, 0], 50053));
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

    let (otlp_grpc_init_tx, otlp_grpc_init_rx) = oneshot::channel::<()>();
    let (_otlp_grpc_shutdown_tx, otlp_grpc_shutdown_rx) = oneshot::channel::<()>();
    let (otlp_grpc_stopped_tx, _otlp_grpc_stopped_rx) = oneshot::channel::<()>();

    let (otlp_http_init_tx, otlp_http_init_rx) = oneshot::channel::<()>();
    let (_otlp_http_shutdown_tx, otlp_http_shutdown_rx) = oneshot::channel::<()>();
    let (otlp_http_stopped_tx, _otlp_http_stopped_rx) = oneshot::channel::<()>();

    // Shutdown channels for Flight services
    let (router_flight_shutdown_tx, router_flight_shutdown_rx) = oneshot::channel::<()>();
    let (writer_flight_shutdown_tx, writer_flight_shutdown_rx) = oneshot::channel::<()>();

    // Initialize Writer components
    let object_store = common::storage::create_object_store(&config.storage)
        .context("Failed to initialize object store")?;

    let writer_wal_dir =
        std::env::var("WRITER_WAL_DIR").unwrap_or_else(|_| ".data/wal/writer".to_string());
    let writer_wal_config = WalConfig {
        wal_dir: writer_wal_dir.into(),
        ..Default::default()
    };

    let mut writer_wal = Wal::new(writer_wal_config)
        .await
        .context("Failed to initialize Writer WAL")?;
    writer_wal.start_background_flush();
    let writer_wal = Arc::new(writer_wal);

    // Create Iceberg-based Flight ingestion service with WAL
    let writer_flight_service =
        IcebergWriterFlightService::new(config.clone(), object_store.clone(), writer_wal.clone());

    // Start background WAL processing for Iceberg writes
    let writer_bg_handle = writer_flight_service.start_background_processing();

    // Start OTLP/gRPC server
    let wal_dir = std::env::var("ACCEPTOR_WAL_DIR")
        .unwrap_or_else(|_| ".wal/acceptor".to_string())
        .into();
    let grpc_handle = tokio::spawn(async move {
        serve_otlp_grpc(
            otlp_grpc_init_tx,
            otlp_grpc_shutdown_rx,
            otlp_grpc_stopped_tx,
            wal_dir,
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
    let _app = create_router(state.clone());
    let http_router_addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let http_router_handle = tokio::spawn(async move {
        log::info!("Starting HTTP router on {http_router_addr}");

        // For now, we'll skip starting the HTTP server due to compatibility issues with axum 0.7.9
        // This will be fixed in a future update
        log::warn!("HTTP router is disabled due to compatibility issues with axum 0.7.9");

        // To enable the HTTP router, you would need to update the axum dependency to a version
        // that supports the current usage pattern
    });

    // Start Router Flight service
    let flight_service = create_flight_service(state);
    let flight_addr = SocketAddr::from(([0, 0, 0, 0], 50053));
    let flight_handle = tokio::spawn(async move {
        log::info!("Starting Router Flight service on {flight_addr}");

        match Server::builder()
            .add_service(
                arrow_flight::flight_service_server::FlightServiceServer::new(flight_service),
            )
            .serve_with_shutdown(flight_addr, async {
                router_flight_shutdown_rx.await.ok();
                log::info!("Router Flight service shutting down gracefully");
            })
            .await
        {
            Ok(_) => log::info!("Router Flight service stopped"),
            Err(e) => log::error!("Router Flight service error: {e}"),
        }
    });

    // Start Writer Flight service
    let writer_flight_addr = SocketAddr::from(([0, 0, 0, 0], 50051));
    let writer_flight_handle = tokio::spawn(async move {
        log::info!("Starting Writer Flight service on {writer_flight_addr}");

        match Server::builder()
            .add_service(
                arrow_flight::flight_service_server::FlightServiceServer::new(
                    writer_flight_service,
                ),
            )
            .serve_with_shutdown(writer_flight_addr, async {
                writer_flight_shutdown_rx.await.ok();
                log::info!("Writer Flight service shutting down gracefully");
            })
            .await
        {
            Ok(_) => log::info!("Writer Flight service stopped"),
            Err(e) => log::error!("Writer Flight service error: {e}"),
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
        log::error!("Failed to shutdown router service bootstrap: {e}");
    }

    // Signal Flight servers to shutdown gracefully
    let _ = router_flight_shutdown_tx.send(());
    let _ = writer_flight_shutdown_tx.send(());

    // Wait for servers to stop
    let _ = grpc_handle.await;
    let _ = http_handle.await;
    let _ = http_router_handle.await;
    let _ = flight_handle.await;
    let _ = writer_flight_handle.await;

    // Stop background WAL processing task to release Arc<Wal> reference
    log::info!("Stopping background WAL processing task");
    writer_bg_handle.abort();
    let _ = writer_bg_handle.await;

    // Shutdown Writer WAL and flush any remaining data
    if let Ok(wal) = Arc::try_unwrap(writer_wal) {
        wal.shutdown()
            .await
            .context("Failed to shutdown Writer WAL")?;
    } else {
        log::warn!("Could not get exclusive access to Writer WAL for shutdown - forcing flush");
    }

    Ok(())
}
