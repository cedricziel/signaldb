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

// Heap profiling: install jemalloc as global allocator when built with
// the jemalloc-profiling feature (see [profiling] config)
#[cfg(feature = "jemalloc-profiling")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load configuration
    let config = utils::load_config(cli.common.config.as_ref())?;
    // Standalone services need shared discovery/catalog backends; the
    // in-memory defaults only make sense in monolithic mode (issue #554).
    config.validate_for_distributed("querier")?;

    // Handle common commands that don't require starting the service
    let command = cli.command.unwrap_or_default();
    let QuerierCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    // Initialize self-monitoring telemetry first so the OTel bridge layers
    // can be attached to the tracing subscriber, then initialize logging.
    let (telemetry, telemetry_error) =
        match common::self_monitoring::init_telemetry(&config, "signaldb-querier") {
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

    let _profiling = match common::self_monitoring::init_profiling(&config, "signaldb-querier") {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "Profiling init failed, continuing without it");
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

    // Keep a handle on the SQL catalog for Flight authentication before the
    // bootstrap moves into the transport
    let sql_catalog = Arc::new(service_bootstrap.catalog().clone());

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

    tracing::info!(service_id = %service_id, "Querier Flight service registered");

    // Create shared catalog manager
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .context("Failed to create catalog manager")?,
    );

    // Create Flight query service with CatalogManager for per-tenant catalog support
    tracing::info!(
        memory_limit_mb = ?config.querier.memory_limit_mb,
        query_timeout = ?config.querier.query_timeout,
        max_sql_rows = config.querier.max_sql_rows,
        max_search_limit = config.querier.max_search_limit,
        "Querier resource limits"
    );
    let flight_service = QuerierFlightService::new_with_catalog_manager(
        flight_transport.clone(),
        catalog_manager,
        config.querier.clone(),
    )
    .await
    .context("Failed to create querier flight service with CatalogManager")?;
    tracing::info!(address = %flight_addr, "Starting Flight query service");
    let flight_auth = config.auth.internal_service_key.clone().map(|key| {
        let authenticator = Arc::new(common::auth::Authenticator::new(
            config.auth.clone(),
            sql_catalog,
        ));
        common::flight::auth::FlightAuthInterceptor::new(authenticator, key)
    });
    if flight_auth.is_none() {
        tracing::warn!(
            "Flight port is UNAUTHENTICATED ([auth].internal_service_key is not set); \
             it must be restricted to a trusted network"
        );
    }
    let flight_handle = tokio::spawn(async move {
        let builder = Server::builder();
        let serve = match flight_auth {
            Some(interceptor) => {
                let mut builder = builder;
                builder
                    .add_service(FlightServiceServer::with_interceptor(
                        flight_service,
                        move |req| interceptor.intercept(req),
                    ))
                    .serve(flight_addr)
                    .await
            }
            None => {
                let mut builder = builder;
                builder
                    .add_service(FlightServiceServer::new(flight_service))
                    .serve(flight_addr)
                    .await
            }
        };
        if let Err(e) = serve {
            tracing::error!(error = %e, "Flight server exited with error");
        }
    });

    // Await shutdown signal
    signal::ctrl_c()
        .await
        .context("Failed to listen for shutdown signal")?;
    tracing::info!("Shutting down querier service");

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
