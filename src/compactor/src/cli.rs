//! `signaldb compactor` — the compactor service's command-line entry point.
//!
//! Full execution service that identifies compaction candidates, executes
//! compaction with Parquet rewriting, commits changes atomically, enforces
//! data retention / lifecycle policies, coordinates across multiple instances
//! using distributed leases and round-robin scheduling, and serves
//! observability endpoints (Prometheus metrics, status, health).

use crate::service::CompactorService;
use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use common::cli::CommonArgs;
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use std::sync::Arc;
use tonic::transport::Server;

/// Command-line arguments of the `compactor` subcommand (`signaldb compactor`).
///
/// The compactor has no service-specific flags: its configuration comes from
/// the shared `--config` option (default `signaldb.toml`), listen addresses
/// from `COMPACTOR_FLIGHT_ADDR`, and everything else from the `[compactor]`
/// section of the configuration.
#[derive(clap::Args, Debug, Default)]
pub struct Args {}

/// Waits for a shutdown signal (SIGINT or SIGTERM)
async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigint =
            signal(SignalKind::interrupt()).context("Failed to install SIGINT handler")?;
        let mut sigterm =
            signal(SignalKind::terminate()).context("Failed to install SIGTERM handler")?;

        tokio::select! {
            _ = sigint.recv() => tracing::info!("Received SIGINT"),
            _ = sigterm.recv() => tracing::info!("Received SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("Failed to listen for shutdown signal")?;
        tracing::info!("Received Ctrl+C");
    }

    Ok(())
}

/// Run the `compactor` service with the shared options and its own arguments.
pub async fn run(common: &CommonArgs, _args: Args) -> Result<()> {
    // Initialize structured logging (RUST_LOG-compatible env filter)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Load configuration: the shared --config option, defaulting to
    // signaldb.toml; a missing file falls back to defaults (env overrides
    // still apply), as the standalone compactor always did.
    let config_path = common
        .config
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("signaldb.toml"));
    let config = if config_path.exists() {
        Configuration::load_from_path(&config_path).context("Failed to load configuration")?
    } else {
        tracing::info!("Configuration file not found, using defaults");
        Configuration::default()
    };
    // Publish to the process-global for readers without a config handle.
    let _ = common::config::CONFIG.set(config.clone());

    // Standalone services need shared discovery/catalog backends; the
    // in-memory defaults only make sense in monolithic mode (issue #554).
    config.validate_for_distributed("compactor")?;

    // Check if compactor is enabled
    if !config.compactor.enabled {
        tracing::info!("Compactor is disabled in configuration (compactor.enabled = false)");
        tracing::info!(
            "Set SIGNALDB_COMPACTOR_ENABLED=true or enable in config file to run compactor"
        );
        return Ok(());
    }

    tracing::info!(
        "Starting SignalDB Compactor Service (Phase 3/4: Retention, Lifecycle & Multi-Instance Safety)"
    );
    tracing::info!(
        "Running with full compaction execution, atomic commits, distributed leases, and retention enforcement"
    );

    // Determine Flight listen address from environment or default
    let flight_addr_str =
        std::env::var("COMPACTOR_FLIGHT_ADDR").unwrap_or_else(|_| "0.0.0.0:50055".to_string());
    let flight_addr: std::net::SocketAddr = flight_addr_str
        .parse()
        .with_context(|| format!("Invalid COMPACTOR_FLIGHT_ADDR: {flight_addr_str}"))?;

    // Initialize service bootstrap — register with the real Flight address so that
    // other services and operators can discover this compactor instance.
    let bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Compactor,
        flight_addr_str.clone(),
    )
    .await
    .context("Failed to initialize compactor service bootstrap")?;

    tracing::info!(
        "Compactor service registered with ID: {} (Flight: {flight_addr_str})",
        bootstrap.service_id()
    );

    // Initialize catalog manager, attaching the SQL catalog as the tenant
    // source so lifecycle management (compaction, retention, orphan cleanup)
    // covers admin-API (database) tenants alongside config-defined ones.
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .context("Failed to initialize catalog manager")?
            .with_tenant_source(Arc::new(bootstrap.catalog().clone())),
    );

    // Assemble every compactor component (planner, executor, leases,
    // retention, orphan cleanup) — shared with monolithic mode.
    let service = CompactorService::new(
        &config,
        catalog_manager,
        Arc::new(bootstrap.catalog().clone()),
        bootstrap.service_id(),
    )
    .context("Failed to assemble compactor service")?;

    // Start Flight service (Phase 4d) — shares the same planner/executor/
    // lease_manager; leases protect against duplicate work between the
    // background task and on-demand Flight requests.
    let flight_service = service.flight_service();
    // The compactor's Flight surface is operator/service-to-service only
    // (do_action can trigger compaction cycles and exposes tenant lease
    // metadata): when the internal key is configured, tenant API keys are
    // rejected outright.
    let flight_auth = config
        .auth
        .internal_service_key
        .clone()
        .map(common::flight::auth::FlightAuthInterceptor::internal_only);
    if flight_auth.is_none() {
        tracing::warn!(
            "Flight port is UNAUTHENTICATED ([auth].internal_service_key is not set); \
             it must be restricted to a trusted network"
        );
    }
    let flight_task = {
        tokio::spawn(async move {
            tracing::info!("Starting Compactor Flight service on {flight_addr}");
            let serve = match flight_auth {
                Some(interceptor) => {
                    Server::builder()
                        .add_service(common::flight::flight_service_server_with_interceptor(
                            flight_service,
                            move |req| interceptor.intercept(req),
                        ))
                        .serve(flight_addr)
                        .await
                }
                None => {
                    Server::builder()
                        .add_service(common::flight::flight_service_server(flight_service))
                        .serve(flight_addr)
                        .await
                }
            };
            match serve {
                Ok(()) => tracing::info!("Compactor Flight service stopped"),
                Err(e) => tracing::error!("Compactor Flight service error: {e}"),
            }
        })
    };

    // Start observability HTTP endpoint (Phase 6): Prometheus /metrics,
    // JSON /status, and /health. Disabled when metrics_addr is empty.
    let metrics_addr_str = std::env::var("COMPACTOR_METRICS_ADDR")
        .unwrap_or_else(|_| config.compactor.metrics_addr.clone());
    let observability_task = if metrics_addr_str.is_empty() {
        tracing::info!("Compactor observability HTTP endpoint disabled (metrics_addr is empty)");
        None
    } else {
        let metrics_addr: std::net::SocketAddr = metrics_addr_str
            .parse()
            .with_context(|| format!("Invalid compactor metrics_addr: {metrics_addr_str}"))?;
        let observability_state = service.observability_state(bootstrap.service_id());
        Some(tokio::spawn(async move {
            if let Err(e) = crate::http::serve(metrics_addr, observability_state).await {
                tracing::error!("Compactor observability HTTP endpoint error: {e:#}");
            }
        }))
    };

    // Start planning, execution, retention enforcement, and orphan cleanup
    let planning_task = tokio::spawn(service.run_lifecycle_loop());

    // Wait for shutdown signal (SIGINT or SIGTERM)
    tracing::info!("Compactor service running, waiting for shutdown signal");
    wait_for_shutdown_signal().await?;

    tracing::info!("Received shutdown signal, stopping compactor service");

    // Stop background tasks
    planning_task.abort();
    flight_task.abort();
    if let Some(task) = observability_task {
        task.abort();
    }

    // Graceful shutdown
    bootstrap
        .shutdown()
        .await
        .context("Failed to shutdown service bootstrap")?;

    tracing::info!("Compactor service stopped");

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::orphan::OrphanCleanupConfig;
    use crate::retention::RetentionConfig;
    use std::time::Duration;

    #[test]
    fn test_retention_config_defaults() {
        let config = RetentionConfig::default();
        assert!(config.enabled, "Retention should be enabled by default");
        assert!(
            !config.dry_run,
            "Retention should enforce (not dry-run) by default"
        );
        assert_eq!(
            config.retention_check_interval,
            Duration::from_secs(3600),
            "Default check interval should be 1 hour"
        );
    }

    #[test]
    fn test_orphan_cleanup_config_defaults() {
        let config = OrphanCleanupConfig::default();
        assert!(
            config.enabled,
            "Orphan cleanup should be enabled by default so storage is reclaimed (#935)"
        );
        assert!(
            !config.dry_run,
            "Orphan cleanup should delete (not dry-run) by default, like retention"
        );
        assert_eq!(
            config.cleanup_interval(),
            Duration::from_secs(24 * 3600),
            "Default cleanup interval should be 24 hours"
        );
    }
}
