use acceptor::{
    GrpcAcceptorConfig, HttpAcceptorConfig, init_acceptor_resources, serve_otlp_grpc,
    serve_otlp_http,
};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use common::CatalogManager;
use common::cli::{CommonArgs, CommonCommands, utils};
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use common::wal::manager::WalManager;
use compactor::service::CompactorService;
use querier::QuerierFlightService;
use router::{RouterAppState, RouterState, create_flight_service, create_router};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::transport::Server;
use writer::IcebergWriterFlightService;

/// The `signaldb` command line: the monolith by default, or one service via
/// its subcommand. The shared options (`--config`, `-v`, `-q`) are global, so
/// `signaldb --config x router` and `signaldb router --config x` are the same.
#[derive(Parser, Debug)]
#[command(name = "signaldb")]
#[command(
    about = "SignalDB - distributed observability signal database (monolithic mode by default; pick a service subcommand to run one service)"
)]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub common: CommonArgs,

    #[command(subcommand)]
    pub command: Option<SignalDbCommands>,
}

#[derive(Subcommand, Debug)]
pub enum SignalDbCommands {
    #[command(flatten)]
    Common(CommonCommands),

    /// Run only the OTLP acceptor (ingest) service
    #[command(version)]
    Acceptor(acceptor::cli::Args),
    /// Run only the router (HTTP API + Flight) service
    #[command(version)]
    Router(router::cli::Args),
    /// Run only the writer (Iceberg persistence) service
    #[command(version)]
    Writer(writer::cli::Args),
    /// Run only the querier (query execution) service
    #[command(version)]
    Querier(querier::cli::Args),
    /// Run only the compactor (compaction, retention, cleanup) service
    #[command(version)]
    Compactor(compactor::cli::Args),
    /// Run only the MCP server sidecar
    #[command(version)]
    Mcp(mcp_server::cli::Args),
}

impl Default for SignalDbCommands {
    fn default() -> Self {
        Self::Common(CommonCommands::Start)
    }
}

// jemalloc as global allocator: the Linux release/Docker builds enable the
// `jemalloc` feature because musl's (and to a lesser degree glibc's)
// allocator degrades under multithreaded Arrow allocation churn.
// `jemalloc-profiling` implies it and adds heap self-profiling.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Single-service mode: hand off to that service's own entry point. The
    // service crates no longer have binaries of their own — this is the one
    // executable that ships (see openspec change multi-call-binary).
    let common_cmd = match cli.command.unwrap_or_default() {
        SignalDbCommands::Acceptor(args) => return acceptor::cli::run(&cli.common, args).await,
        SignalDbCommands::Router(args) => return router::cli::run(&cli.common, args).await,
        SignalDbCommands::Writer(args) => return writer::cli::run(&cli.common, args).await,
        SignalDbCommands::Querier(args) => return querier::cli::run(&cli.common, args).await,
        SignalDbCommands::Compactor(args) => {
            return compactor::cli::run(&cli.common, args).await;
        }
        SignalDbCommands::Mcp(args) => return mcp_server::cli::run(&cli.common, args).await,
        SignalDbCommands::Common(common_cmd) => common_cmd,
    };

    // Load application configuration
    let config = utils::load_config(cli.common.config.as_ref())?;

    // Handle common commands that don't require starting the service
    if utils::handle_common_command(&common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    // Initialize self-monitoring telemetry first so the OTel bridge layers
    // can be attached to the tracing subscriber, then initialize logging.
    let (telemetry, telemetry_error) =
        match common::self_monitoring::init_telemetry(&config, "signaldb") {
            Ok(t) => (t, None),
            Err(e) => (None, Some(e)),
        };
    utils::init_logging(&cli.common, telemetry.as_ref());
    if let Some(e) = telemetry_error {
        tracing::warn!("Self-monitoring init failed, continuing without it: {e}");
    } else if let Some(ref t) = telemetry {
        tracing::info!(
            "Self-monitoring telemetry initialized (sampler: {})",
            t.sampler_description()
        );
    }
    let _telemetry = telemetry;

    let _profiling = match common::self_monitoring::init_profiling(&config, "signaldb") {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "Profiling init failed, continuing without it");
            None
        }
    };

    tracing::info!("Loaded configuration:");
    tracing::info!("  Database DSN: {}", config.database.dsn);
    if let Some(discovery) = &config.discovery {
        tracing::info!("  Discovery DSN: {}", discovery.dsn);
    } else {
        tracing::info!("  No discovery configuration");
    }

    // Initialize router service bootstrap for catalog-based discovery
    let flight_addr = SocketAddr::from(([0, 0, 0, 0], 50053));
    let router_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Router, flight_addr.to_string())
            .await
            .context("Failed to initialize router service bootstrap")?;

    // Sync config-defined tenants, API keys, and datasets into the catalog
    router_bootstrap
        .catalog()
        .sync_config_tenants(&config.auth)
        .await
        .context("Failed to sync config tenants to catalog")?;
    tracing::info!(
        "Synced {} config tenant(s) to catalog",
        config.auth.tenants.len()
    );

    // Converge tenants created before the default dataset was materialized at
    // write time: without a dataset row they cannot authenticate and are
    // invisible to compaction, retention, and orphan cleanup (issue #1066).
    // A no-op once converged.
    let materialized = router_bootstrap
        .catalog()
        .backfill_default_datasets()
        .await
        .context("Failed to backfill default dataset rows")?;
    if materialized > 0 {
        tracing::info!("Materialized {materialized} missing default dataset row(s)");
    }

    // First boot with no tenants at all (none in config, none in the
    // catalog): auto-provision a default tenant and print its API key once.
    if let Some(api_key) =
        common::bootstrap::bootstrap_default_tenant(router_bootstrap.catalog(), &config)
            .await
            .context("Failed to bootstrap default tenant")?
    {
        tracing::info!(
            "\n============================================================\n\
             First boot: no tenants were configured or provisioned, so a\n\
             default tenant was created automatically.\n\
             \n\
               Tenant:  default\n\
               Dataset: default\n\
               API key: {api_key}\n\
             \n\
             The key is stored hashed and printed only this once - save it.\n\
             \n\
             Point any OpenTelemetry SDK or Collector at SignalDB:\n\
             \n\
               export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317\n\
               export OTEL_EXPORTER_OTLP_HEADERS=\"authorization=Bearer {api_key},x-tenant-id=default\"\n\
             \n\
             (OTLP/HTTP uses port 4318 instead.)\n\
             To create a UI user: signaldb-cli user create <email> --tenant default\n\
             ============================================================"
        );
    }

    let (otlp_grpc_init_tx, otlp_grpc_init_rx) = oneshot::channel::<()>();
    let (_otlp_grpc_shutdown_tx, otlp_grpc_shutdown_rx) = oneshot::channel::<()>();
    let (otlp_grpc_stopped_tx, _otlp_grpc_stopped_rx) = oneshot::channel::<()>();

    let (otlp_http_init_tx, otlp_http_init_rx) = oneshot::channel::<()>();
    let (_otlp_http_shutdown_tx, otlp_http_shutdown_rx) = oneshot::channel::<()>();
    let (otlp_http_stopped_tx, _otlp_http_stopped_rx) = oneshot::channel::<()>();

    // Shutdown channels for Flight services and HTTP router
    let (router_flight_shutdown_tx, router_flight_shutdown_rx) = oneshot::channel::<()>();
    let (writer_flight_shutdown_tx, writer_flight_shutdown_rx) = oneshot::channel::<()>();
    let (querier_flight_shutdown_tx, querier_flight_shutdown_rx) = oneshot::channel::<()>();
    let (http_router_shutdown_tx, http_router_shutdown_rx) = oneshot::channel::<()>();

    // Create shared catalog manager for consistent metadata across services.
    // Attach the SQL catalog as the tenant source so admin-API (database)
    // tenants are registered for querying alongside config-defined ones.
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .context("Failed to create catalog manager")?
            .with_tenant_source(Arc::new(router_bootstrap.catalog().clone())),
    );
    tracing::info!("Created shared catalog manager");

    // Initialize Writer components
    let object_store = common::storage::create_object_store(&config.storage)
        .context("Failed to initialize object store")?;

    // WRITER_WAL_DIR override wins, otherwise [wal].wal_dir + "/writer".
    let writer_wal_dir = config.wal.wal_dir_for_service(
        "writer",
        std::env::var("WRITER_WAL_DIR").ok().map(Into::into),
    );
    let writer_wal_config = WalConfig {
        wal_dir: writer_wal_dir,
        ..Default::default()
    };

    // One WAL per tenant/dataset/signal (#932); WALs left by a previous run
    // are opened now so their pending entries drain.
    let writer_wal_manager = Arc::new(
        WalManager::uniform(writer_wal_config).with_max_instances(config.wal.max_instances),
    );
    writer::cli::open_existing_writer_wals(&writer_wal_manager).await;
    writer_wal_manager.warn_if_fd_headroom_thin("writer").await;

    // Create Iceberg-based Flight ingestion service with CatalogManager
    let writer_flight_service = IcebergWriterFlightService::new(
        catalog_manager.clone(),
        object_store.clone(),
        writer_wal_manager.clone(),
        &config.writer,
    );

    // Start background WAL processing for Iceberg writes
    let writer_bg_handle = writer_flight_service.start_background_processing();

    // Converge every registered tenant/dataset on its enabled signal tables.
    // The standalone writer binary and this one are independent wirings, so
    // monolithic mode does not inherit this for free.
    let writer_reconciler_handle = writer_flight_service.start_table_reconciler();

    // Initialize Writer service bootstrap for catalog-based discovery
    // This registers the Writer with Storage capability so the Acceptor can discover it
    let writer_flight_addr = SocketAddr::from(([0, 0, 0, 0], 50051));
    let writer_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Writer,
        writer_flight_addr.to_string(),
    )
    .await
    .context("Failed to initialize writer service bootstrap")?;
    tracing::info!(
        "Writer service registered with ID: {}",
        writer_bootstrap.service_id()
    );

    // Initialize Querier service bootstrap for catalog-based discovery
    let querier_flight_addr = SocketAddr::from(([0, 0, 0, 0], 50054));
    let querier_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Querier,
        querier_flight_addr.to_string(),
    )
    .await
    .context("Failed to initialize querier service bootstrap")?;

    // Periodically flush attribute query-demand counters (epic #737, #733)
    // into the catalog's advisory `attribute_stats` table.
    let _demand_flusher = common::attr_demand::spawn_flusher(
        Arc::new(querier_bootstrap.catalog().clone()),
        std::time::Duration::from_secs(60),
    );

    // Create Flight transport and register querier with QueryExecution capability
    let querier_flight_transport = Arc::new(InMemoryFlightTransport::new(querier_bootstrap));
    let querier_service_id = querier_flight_transport
        .register_flight_service(
            ServiceType::Querier,
            querier_flight_addr.ip().to_string(),
            querier_flight_addr.port(),
            vec![ServiceCapability::QueryExecution],
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register querier Flight service: {e}"))?;
    tracing::info!("Querier Flight service registered with ID: {querier_service_id}");

    let state = RouterAppState::new_with_flight_transport(
        router_bootstrap.catalog().clone(),
        config.clone(),
        (*querier_flight_transport).clone(),
    );

    if let Some(discovery) = &config.discovery {
        let poll_interval = discovery.poll_interval;
        state
            .service_registry()
            .start_background_polling(poll_interval)
            .await;
        tracing::info!(
            "Started service registry background polling with interval: {poll_interval:?}"
        );
    }

    // Create QuerierFlightService with shared CatalogManager for per-tenant catalog support
    let querier_flight_service = QuerierFlightService::new_with_catalog_manager(
        querier_flight_transport.clone(),
        catalog_manager.clone(),
        config.querier.clone(),
    )
    .await
    .context("Failed to create querier flight service")?;

    // Initialize compactor service (optional, controlled by config)
    let compactor_handle = if config.compactor.enabled {
        tracing::info!("Compactor enabled, initializing service");

        // Register with the real Flight address so operators can reach the
        // compactor's do_action control surface through the router ops API.
        let compactor_flight_addr = SocketAddr::from(([0, 0, 0, 0], 50055));
        let compactor_bootstrap = ServiceBootstrap::new(
            config.clone(),
            ServiceType::Compactor,
            compactor_flight_addr.to_string(),
        )
        .await
        .context("Failed to initialize compactor service bootstrap")?;

        tracing::info!(
            "Compactor service registered with ID: {} (Flight: {compactor_flight_addr})",
            compactor_bootstrap.service_id()
        );

        // Full compactor assembly shared with the standalone binary:
        // planner, executor, leases, retention enforcement, orphan cleanup.
        let compactor_service = CompactorService::new(
            &config,
            catalog_manager.clone(),
            Arc::new(compactor_bootstrap.catalog().clone()),
            compactor_bootstrap.service_id(),
        )
        .context("Failed to assemble compactor service")?;

        let compactor_flight_service = compactor_service.flight_service();
        let compactor_flight_auth = config
            .auth
            .internal_service_key
            .clone()
            .map(common::flight::auth::FlightAuthInterceptor::internal_only);
        // The blanket "Flight ports are UNAUTHENTICATED" startup warning
        // below covers this port too ([auth].internal_service_key gates all
        // in-process Flight servers identically).
        let compactor_flight_handle = tokio::spawn(async move {
            tracing::info!("Starting Compactor Flight service on {compactor_flight_addr}");
            let serve = match compactor_flight_auth {
                Some(interceptor) => {
                    Server::builder()
                        .add_service(common::flight::flight_service_server_with_interceptor(
                            compactor_flight_service,
                            move |req| interceptor.intercept(req),
                        ))
                        .serve(compactor_flight_addr)
                        .await
                }
                None => {
                    Server::builder()
                        .add_service(common::flight::flight_service_server(
                            compactor_flight_service,
                        ))
                        .serve(compactor_flight_addr)
                        .await
                }
            };
            if let Err(e) = serve {
                tracing::error!("Compactor Flight service error: {e}");
            }
        });

        let mut compactor_tasks = vec![compactor_flight_handle];

        // Observability endpoint (Prometheus /metrics, /status, /health).
        if !config.compactor.metrics_addr.is_empty() {
            let metrics_addr: SocketAddr =
                config.compactor.metrics_addr.parse().with_context(|| {
                    format!(
                        "Invalid compactor metrics_addr: {}",
                        config.compactor.metrics_addr
                    )
                })?;
            let observability_state =
                compactor_service.observability_state(compactor_bootstrap.service_id());
            compactor_tasks.push(tokio::spawn(async move {
                if let Err(e) = compactor::http::serve(metrics_addr, observability_state).await {
                    tracing::error!("Compactor observability HTTP endpoint error: {e:#}");
                }
            }));
        }

        // The full lifecycle loop: compaction planning AND execution,
        // stale-lease expiry, retention enforcement (partition drops +
        // snapshot expiration), and orphan cleanup. The previous wiring ran
        // only the planner, so monolithic deployments never enforced
        // retention (issue #959).
        compactor_tasks.push(tokio::spawn(compactor_service.run_lifecycle_loop()));

        Some((compactor_tasks, compactor_bootstrap))
    } else {
        tracing::info!("Compactor disabled in configuration");
        None
    };

    // Initialize shared acceptor resources for both gRPC and HTTP servers.
    // ACCEPTOR_WAL_DIR override wins, otherwise [wal].wal_dir + "/acceptor".
    let wal_dir = config.wal.wal_dir_for_service(
        "acceptor",
        std::env::var("ACCEPTOR_WAL_DIR").ok().map(Into::into),
    );
    let grpc_addr = SocketAddr::from(([0, 0, 0, 0], 4317));
    let http_addr = SocketAddr::from(([0, 0, 0, 0], 4318));
    let advertise_addr =
        std::env::var("ACCEPTOR_ADVERTISE_ADDR").unwrap_or_else(|_| grpc_addr.to_string());

    let acceptor_resources = init_acceptor_resources(config.clone(), advertise_addr, wal_dir)
        .await
        .context("Failed to initialize acceptor resources")?;

    // Clone resources for both servers (all fields are Arcs, cheap to clone)
    let grpc_resources = acceptor_resources.clone();
    let http_resources = acceptor_resources.clone();

    // Start OTLP/gRPC server
    let grpc_config = GrpcAcceptorConfig {
        addr: grpc_addr,
        resources: grpc_resources,
        max_decoding_message_size: config.acceptor.max_request_body_bytes as usize,
    };
    let grpc_handle = tokio::spawn(async move {
        serve_otlp_grpc(
            grpc_config,
            otlp_grpc_init_tx,
            otlp_grpc_shutdown_rx,
            otlp_grpc_stopped_tx,
        )
        .await
        .expect("Failed to start OTLP/gRPC server");
    });

    // Start OTLP/HTTP server (with Prometheus remote_write support)
    let http_config = HttpAcceptorConfig {
        addr: http_addr,
        flight_transport: http_resources.flight_transport,
        wal_manager: http_resources.wal_manager,
        authenticator: http_resources.authenticator,
        rate_limiter: http_resources.rate_limiter,
        storage_usage: http_resources.storage_usage,
        cors_allowed_origins: config
            .self_monitoring
            .frontend
            .enabled
            .then(|| config.self_monitoring.frontend.allowed_origins.clone()),
        max_request_body_bytes: config.acceptor.max_request_body_bytes as usize,
    };
    let http_handle = tokio::spawn(async move {
        serve_otlp_http(
            http_config,
            otlp_http_init_tx,
            otlp_http_shutdown_rx,
            otlp_http_stopped_tx,
        )
        .await
        .expect("Failed to start OTLP/HTTP server");
    });

    // Start HTTP router
    let app = create_router(state.clone());
    let http_router_addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let http_router_handle = tokio::spawn(async move {
        tracing::info!("Starting HTTP router on {http_router_addr}");
        let listener = tokio::net::TcpListener::bind(http_router_addr)
            .await
            .expect("Failed to bind HTTP router");
        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(async {
                http_router_shutdown_rx.await.ok();
                tracing::info!("HTTP router shutting down gracefully");
            })
            .await
            .expect("HTTP router error");
    });

    // Flight authentication (shared across the router/querier/writer
    // Flight servers when [auth].internal_service_key is configured)
    let internal_service_key = config.auth.internal_service_key.clone();
    if internal_service_key.is_none() {
        tracing::warn!(
            "Flight ports are UNAUTHENTICATED ([auth].internal_service_key is not set); \
             they must be restricted to a trusted network"
        );
    }
    let tenant_flight_auth = internal_service_key.clone().map(|key| {
        common::flight::auth::FlightAuthInterceptor::new(
            Arc::clone(&acceptor_resources.authenticator),
            key,
        )
    });
    let internal_flight_auth =
        internal_service_key.map(common::flight::auth::FlightAuthInterceptor::internal_only);

    // Start Router Flight service
    let flight_service = create_flight_service(state);
    let router_flight_auth = tenant_flight_auth.clone();
    let flight_handle = tokio::spawn(async move {
        tracing::info!("Starting Router Flight service on {flight_addr}");

        let shutdown = async {
            router_flight_shutdown_rx.await.ok();
            tracing::info!("Router Flight service shutting down gracefully");
        };
        let serve = match router_flight_auth {
            Some(interceptor) => {
                Server::builder()
                    .add_service(common::flight::flight_service_server_with_interceptor(
                        flight_service,
                        move |req| interceptor.intercept(req),
                    ))
                    .serve_with_shutdown(flight_addr, shutdown)
                    .await
            }
            None => {
                Server::builder()
                    .add_service(common::flight::flight_service_server(flight_service))
                    .serve_with_shutdown(flight_addr, shutdown)
                    .await
            }
        };
        match serve {
            Ok(_) => tracing::info!("Router Flight service stopped"),
            Err(e) => tracing::error!("Router Flight service error: {e}"),
        }
    });

    // Start Writer Flight service (internal callers only)
    let writer_flight_handle = tokio::spawn(async move {
        tracing::info!("Starting Writer Flight service on {writer_flight_addr}");

        let shutdown = async {
            writer_flight_shutdown_rx.await.ok();
            tracing::info!("Writer Flight service shutting down gracefully");
        };
        let serve = match internal_flight_auth {
            Some(interceptor) => {
                Server::builder()
                    .add_service(common::flight::flight_service_server_with_interceptor(
                        writer_flight_service,
                        move |req| interceptor.intercept(req),
                    ))
                    .serve_with_shutdown(writer_flight_addr, shutdown)
                    .await
            }
            None => {
                Server::builder()
                    .add_service(common::flight::flight_service_server(writer_flight_service))
                    .serve_with_shutdown(writer_flight_addr, shutdown)
                    .await
            }
        };
        match serve {
            Ok(_) => tracing::info!("Writer Flight service stopped"),
            Err(e) => tracing::error!("Writer Flight service error: {e}"),
        }
    });

    // Start Querier Flight service
    let querier_flight_handle = tokio::spawn(async move {
        tracing::info!("Starting Querier Flight service on {querier_flight_addr}");

        let shutdown = async {
            querier_flight_shutdown_rx.await.ok();
            tracing::info!("Querier Flight service shutting down gracefully");
        };
        let serve = match tenant_flight_auth {
            Some(interceptor) => {
                Server::builder()
                    .add_service(common::flight::flight_service_server_with_interceptor(
                        querier_flight_service,
                        move |req| interceptor.intercept(req),
                    ))
                    .serve_with_shutdown(querier_flight_addr, shutdown)
                    .await
            }
            None => {
                Server::builder()
                    .add_service(common::flight::flight_service_server(
                        querier_flight_service,
                    ))
                    .serve_with_shutdown(querier_flight_addr, shutdown)
                    .await
            }
        };
        match serve {
            Ok(_) => tracing::info!("Querier Flight service stopped"),
            Err(e) => tracing::error!("Querier Flight service error: {e}"),
        }
    });

    // Wait for OTLP servers to initialize
    otlp_grpc_init_rx
        .await
        .context("Failed to receive init signal from OTLP/gRPC server")?;
    otlp_http_init_rx
        .await
        .context("Failed to receive init signal from OTLP/HTTP server")?;

    tracing::info!("All services started successfully");

    // Wait for ctrl+c
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for ctrl+c signal")?;
    tracing::info!("Shutting down service discovery and other services");

    // Shutdown compactor first (if it was running)
    if let Some((compactor_tasks, compactor_bootstrap)) = compactor_handle {
        tracing::info!("Stopping compactor tasks");
        for task in compactor_tasks {
            task.abort();
            let _ = task.await;
        }

        if let Err(e) = compactor_bootstrap.shutdown().await {
            tracing::error!("Failed to shutdown compactor service bootstrap: {e}");
        }
    }

    // Graceful deregistration using service bootstrap
    if let Err(e) = router_bootstrap.shutdown().await {
        tracing::error!("Failed to shutdown router service bootstrap: {e}");
    }
    if let Err(e) = writer_bootstrap.shutdown().await {
        tracing::error!("Failed to shutdown writer service bootstrap: {e}");
    }

    // Unregister querier Flight service and shutdown bootstrap
    if let Err(e) = querier_flight_transport
        .unregister_service(querier_service_id)
        .await
    {
        tracing::error!("Failed to unregister querier Flight service: {e}");
    }

    // Signal servers to shutdown gracefully
    let _ = http_router_shutdown_tx.send(());
    let _ = router_flight_shutdown_tx.send(());
    let _ = writer_flight_shutdown_tx.send(());
    let _ = querier_flight_shutdown_tx.send(());

    // Wait for servers to stop
    let _ = grpc_handle.await;
    let _ = http_handle.await;
    let _ = http_router_handle.await;
    let _ = flight_handle.await;
    let _ = writer_flight_handle.await;
    let _ = querier_flight_handle.await;

    // Stop background WAL processing task to release Arc<Wal> reference
    tracing::info!("Stopping background WAL processing task");
    writer_bg_handle.abort();
    let _ = writer_bg_handle.await;
    writer_reconciler_handle.abort();
    let _ = writer_reconciler_handle.await;

    writer_wal_manager
        .flush_all()
        .await
        .context("Failed to flush Writer WALs during shutdown")?;

    if let Some(telemetry) = _telemetry {
        telemetry.shutdown();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).unwrap_or_else(|e| panic!("parse {args:?}: {e}"))
    }

    #[test]
    fn no_subcommand_is_the_monolith() {
        let cli = parse(&["signaldb"]);
        assert!(cli.command.is_none());
        assert!(matches!(
            cli.command.unwrap_or_default(),
            SignalDbCommands::Common(CommonCommands::Start)
        ));
    }

    #[test]
    fn monolith_common_commands_still_parse() {
        assert!(matches!(
            parse(&["signaldb", "config", "--json"]).command,
            Some(SignalDbCommands::Common(CommonCommands::Config {
                json: true
            }))
        ));
        assert!(matches!(
            parse(&["signaldb", "validate"]).command,
            Some(SignalDbCommands::Common(CommonCommands::Validate))
        ));
    }

    #[test]
    fn service_subcommand_selects_that_service() {
        assert!(matches!(
            parse(&["signaldb", "router", "--config", "x.toml"]).command,
            Some(SignalDbCommands::Router(_))
        ));
        assert!(matches!(
            parse(&["signaldb", "writer"]).command,
            Some(SignalDbCommands::Writer(_))
        ));
        assert!(matches!(
            parse(&["signaldb", "querier"]).command,
            Some(SignalDbCommands::Querier(_))
        ));
        assert!(matches!(
            parse(&["signaldb", "compactor"]).command,
            Some(SignalDbCommands::Compactor(_))
        ));
        assert!(matches!(
            parse(&["signaldb", "mcp", "--stdio"]).command,
            Some(SignalDbCommands::Mcp(_))
        ));
    }

    #[test]
    fn shared_options_work_before_and_after_the_service_name() {
        let before = parse(&["signaldb", "--config", "x.toml", "-v", "router"]);
        let after = parse(&["signaldb", "router", "--config", "x.toml", "-v"]);
        for cli in [before, after] {
            assert_eq!(
                cli.common.config.as_deref(),
                Some(std::path::Path::new("x.toml"))
            );
            assert!(cli.common.verbose);
            assert!(matches!(cli.command, Some(SignalDbCommands::Router(_))));
        }
    }

    #[test]
    fn service_flags_and_common_commands_nest_under_the_service() {
        let cli = parse(&["signaldb", "acceptor", "--grpc-port", "4319", "validate"]);
        let Some(SignalDbCommands::Acceptor(args)) = cli.command else {
            panic!("expected acceptor");
        };
        assert_eq!(args.grpc_port, 4319);
        assert!(matches!(
            args.command,
            Some(acceptor::cli::AcceptorCommands::Common(
                CommonCommands::Validate
            ))
        ));
    }

    #[test]
    fn unknown_subcommand_is_a_usage_error() {
        let err = Cli::try_parse_from(["signaldb", "frobnicate"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn help_lists_every_service_and_service_help_is_its_own() {
        let top = Cli::command().render_long_help().to_string();
        for svc in [
            "acceptor",
            "router",
            "writer",
            "querier",
            "compactor",
            "mcp",
        ] {
            assert!(
                top.contains(&format!("\n  {svc}")),
                "top-level help lacks {svc}:\n{top}"
            );
        }
        // Global options reach the subcommands only once the command is
        // built (which the parser does implicitly).
        let mut cmd = Cli::command();
        cmd.build();
        let acceptor = cmd
            .find_subcommand_mut("acceptor")
            .expect("acceptor subcommand")
            .render_long_help()
            .to_string();
        for flag in [
            "--grpc-port",
            "--http-port",
            "--bind",
            "--wal-dir",
            "--config",
        ] {
            assert!(
                acceptor.contains(flag),
                "acceptor help lacks {flag}:\n{acceptor}"
            );
        }
        assert!(
            !acceptor.contains("--flight-port"),
            "acceptor help leaks router flags"
        );
    }
}
