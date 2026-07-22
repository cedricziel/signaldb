use acceptor::{
    AcceptorResources, GrpcAcceptorConfig, HttpAcceptorConfig, init_acceptor_resources,
    serve_otlp_grpc, serve_otlp_http,
};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use common::cli::{CommonArgs, CommonCommands, utils};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Parser)]
#[command(name = "signaldb-acceptor")]
#[command(about = "SignalDB OTLP Acceptor Service - ingests observability data via OTLP protocols")]
#[command(version)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<AcceptorCommands>,

    #[arg(long, help = "OTLP gRPC server port", default_value = "4317")]
    grpc_port: u16,

    #[arg(long, help = "OTLP HTTP server port", default_value = "4318")]
    http_port: u16,

    #[arg(long, help = "Bind address for servers", default_value = "0.0.0.0")]
    bind: String,

    #[arg(
        long,
        env = "ACCEPTOR_WAL_DIR",
        help = "WAL directory path",
        default_value = ".wal/acceptor"
    )]
    wal_dir: PathBuf,
}

#[derive(Subcommand)]
enum AcceptorCommands {
    #[command(flatten)]
    Common(CommonCommands),
}

impl Default for AcceptorCommands {
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
    // Standalone services need shared discovery/catalog backends; the
    // in-memory defaults only make sense in monolithic mode (issue #554).
    config.validate_for_distributed("acceptor")?;

    // Handle common commands that don't require starting the service
    let command = cli.command.unwrap_or_default();
    let AcceptorCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    // Initialize self-monitoring telemetry first so the OTel bridge layers
    // can be attached to the tracing subscriber, then initialize logging.
    let (telemetry, telemetry_error) =
        match common::self_monitoring::init_telemetry(&config, "signaldb-acceptor") {
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

    let _profiling = match common::self_monitoring::init_profiling(&config, "signaldb-acceptor") {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "Profiling init failed, continuing without it");
            None
        }
    };

    tracing::info!("Starting SignalDB Acceptor Service");

    // Use CLI-provided ports or defaults
    let bind_ip = cli
        .bind
        .parse::<std::net::IpAddr>()
        .context("Invalid bind address")?;
    let grpc_addr = SocketAddr::new(bind_ip, cli.grpc_port);
    let http_addr = SocketAddr::new(bind_ip, cli.http_port);

    // Initialize shared resources for both gRPC and HTTP servers
    let advertise_addr =
        std::env::var("ACCEPTOR_ADVERTISE_ADDR").unwrap_or_else(|_| grpc_addr.to_string());

    let resources = init_acceptor_resources(config.clone(), advertise_addr, cli.wal_dir.clone())
        .await
        .context("Failed to initialize acceptor resources")?;

    tracing::info!("Acceptor resources initialized successfully");

    // Clone resources for HTTP server (Arc refs are cheap to clone)
    let grpc_resources = AcceptorResources {
        flight_transport: Arc::clone(&resources.flight_transport),
        wal_manager: Arc::clone(&resources.wal_manager),
        authenticator: Arc::clone(&resources.authenticator),
        rate_limiter: Arc::clone(&resources.rate_limiter),
    };

    let http_resources = AcceptorResources {
        flight_transport: Arc::clone(&resources.flight_transport),
        wal_manager: Arc::clone(&resources.wal_manager),
        authenticator: Arc::clone(&resources.authenticator),
        rate_limiter: Arc::clone(&resources.rate_limiter),
    };

    // Channels for OTLP/gRPC server signals
    let (grpc_init_tx, grpc_init_rx) = oneshot::channel::<()>();
    let (grpc_shutdown_tx, grpc_shutdown_rx) = oneshot::channel::<()>();
    let (grpc_stopped_tx, grpc_stopped_rx) = oneshot::channel::<()>();

    // Channels for OTLP/HTTP server signals
    let (http_init_tx, http_init_rx) = oneshot::channel::<()>();
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel::<()>();
    let (http_stopped_tx, http_stopped_rx) = oneshot::channel::<()>();

    // Spawn OTLP/gRPC acceptor
    let grpc_config = GrpcAcceptorConfig {
        addr: grpc_addr,
        resources: grpc_resources,
    };
    let grpc_handle = tokio::spawn(async move {
        if let Err(e) =
            serve_otlp_grpc(grpc_config, grpc_init_tx, grpc_shutdown_rx, grpc_stopped_tx).await
        {
            tracing::error!(error = %e, "OTLP/gRPC server error");
        }
    });

    // Spawn OTLP/HTTP acceptor (with Prometheus remote_write support)
    let http_config = HttpAcceptorConfig {
        addr: http_addr,
        flight_transport: http_resources.flight_transport,
        wal_manager: http_resources.wal_manager,
        authenticator: http_resources.authenticator,
        rate_limiter: http_resources.rate_limiter,
    };
    let http_handle = tokio::spawn(async move {
        if let Err(e) =
            serve_otlp_http(http_config, http_init_tx, http_shutdown_rx, http_stopped_tx).await
        {
            tracing::error!(error = %e, "OTLP/HTTP server error");
        }
    });

    // Await initialization signals
    grpc_init_rx
        .await
        .context("Failed to initialize OTLP/gRPC server")?;
    http_init_rx
        .await
        .context("Failed to initialize OTLP/HTTP server")?;

    tracing::info!("Acceptor service started successfully");
    tracing::info!(address = %grpc_addr, "OTLP gRPC server listening");
    tracing::info!(address = %http_addr, "OTLP HTTP server listening");
    tracing::info!(address = %http_addr, "Prometheus remote_write available at /api/v1/write");

    // Wait for shutdown signal (Ctrl+C)
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for shutdown signal")?;

    tracing::info!("Shutting down acceptor service...");

    // Trigger shutdown
    let _ = grpc_shutdown_tx.send(());
    let _ = http_shutdown_tx.send(());

    // Wait for services to stop
    let _ = grpc_stopped_rx.await;
    let _ = http_stopped_rx.await;

    // Await spawned tasks
    let _ = grpc_handle.await;
    let _ = http_handle.await;

    if let Some(profiling) = _profiling {
        profiling.shutdown();
    }
    if let Some(telemetry) = _telemetry {
        telemetry.shutdown();
    }

    tracing::info!("Acceptor service stopped gracefully");

    Ok(())
}
