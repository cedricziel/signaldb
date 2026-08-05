use acceptor::{
    AcceptorResources, GrpcAcceptorConfig, HttpAcceptorConfig, init_acceptor_resources,
    serve_otlp_grpc, serve_otlp_http,
};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use common::CatalogManager;
use common::cli::{CommonArgs, CommonCommands, utils};
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{Wal, WalConfig};
use compactor::service::CompactorService;
use querier::QuerierFlightService;
use router::{RouterAppState, RouterState, create_flight_service, create_router};
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

    // Load application configuration
    let config = utils::load_config(cli.common.config.as_ref())?;

    // Handle common commands that don't require starting the service
    let command = cli.command.unwrap_or_default();
    let SignalDbCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
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

    let mut writer_wal = Wal::new(writer_wal_config)
        .await
        .context("Failed to initialize Writer WAL")?;
    writer_wal.start_background_flush();
    let writer_wal = Arc::new(writer_wal);

    // Create Iceberg-based Flight ingestion service with CatalogManager
    let writer_flight_service = IcebergWriterFlightService::new(
        catalog_manager.clone(),
        object_store.clone(),
        writer_wal.clone(),
        &config.writer,
    );

    // Start background WAL processing for Iceberg writes
    let writer_bg_handle = writer_flight_service.start_background_processing();

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

    // Clone resources for HTTP server
    let grpc_resources = AcceptorResources {
        flight_transport: Arc::clone(&acceptor_resources.flight_transport),
        wal_manager: Arc::clone(&acceptor_resources.wal_manager),
        authenticator: Arc::clone(&acceptor_resources.authenticator),
        rate_limiter: Arc::clone(&acceptor_resources.rate_limiter),
        storage_usage: Arc::clone(&acceptor_resources.storage_usage),
    };

    let http_resources = AcceptorResources {
        flight_transport: Arc::clone(&acceptor_resources.flight_transport),
        wal_manager: Arc::clone(&acceptor_resources.wal_manager),
        authenticator: Arc::clone(&acceptor_resources.authenticator),
        rate_limiter: Arc::clone(&acceptor_resources.rate_limiter),
        storage_usage: Arc::clone(&acceptor_resources.storage_usage),
    };

    // Start OTLP/gRPC server
    let grpc_config = GrpcAcceptorConfig {
        addr: grpc_addr,
        resources: grpc_resources,
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
    let flight_addr = SocketAddr::from(([0, 0, 0, 0], 50053));
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

    if let Ok(wal) = Arc::try_unwrap(writer_wal) {
        wal.shutdown()
            .await
            .context("Failed to shutdown Writer WAL")?;
    } else {
        tracing::warn!("Could not get exclusive access to Writer WAL for shutdown - forcing flush");
    }

    if let Some(telemetry) = _telemetry {
        telemetry.shutdown();
    }

    Ok(())
}
