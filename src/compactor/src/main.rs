//! SignalDB Compactor Service
//!
//! Phase 1: Dry-run planning service that identifies compaction candidates
//! without executing actual compaction. Logs what would be compacted.

use anyhow::{Context, Result};
use clap::Parser;
use common::catalog_manager::CatalogManager;
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use compactor::executor::{CompactionExecutor, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionPlanner, PlannerConfig};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "signaldb.toml")]
    config: String,
}

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
            _ = sigint.recv() => log::info!("Received SIGINT"),
            _ = sigterm.recv() => log::info!("Received SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("Failed to listen for shutdown signal")?;
        log::info!("Received Ctrl+C");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    // Load configuration
    let config = if std::path::Path::new(&args.config).exists() {
        Configuration::load_from_path(std::path::Path::new(&args.config))
            .context("Failed to load configuration")?
    } else {
        log::info!("Configuration file not found, using defaults");
        Configuration::default()
    };

    // Check if compactor is enabled
    if !config.compactor.enabled {
        log::info!("Compactor is disabled in configuration (compactor.enabled = false)");
        log::info!("Set SIGNALDB_COMPACTOR_ENABLED=true or enable in config file to run compactor");
        return Ok(());
    }

    log::info!("Starting SignalDB Compactor Service (Phase 2: Full execution)");
    log::info!("Running with full compaction execution and atomic commits");

    // Initialize service bootstrap with sentinel address
    // The compactor is a background worker that doesn't expose any Flight endpoints,
    // so it uses "compactor:none" as a sentinel address to indicate it's not
    // connectable. This ensures discovery logic won't treat it as a valid endpoint.
    let bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Compactor,
        "compactor:none".to_string(),
    )
    .await
    .context("Failed to initialize compactor service bootstrap")?;

    log::info!(
        "Compactor service registered with ID: {}",
        bootstrap.service_id()
    );

    // Initialize catalog manager
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .context("Failed to initialize catalog manager")?,
    );

    // Create compaction planner
    let planner_config = PlannerConfig::from(&config.compactor);
    let planner = Arc::new(CompactionPlanner::new(
        catalog_manager.clone(),
        planner_config.clone(),
    ));

    // Create compaction executor
    let executor_config = ExecutorConfig::from(&planner_config);
    let metrics = CompactionMetrics::new();
    let executor = Arc::new(CompactionExecutor::new(
        catalog_manager.clone(),
        executor_config,
        metrics.clone(),
    ));

    log::info!(
        "Compaction planner and executor initialized with tick interval: {:?}",
        config.compactor.tick_interval
    );

    // Start planning and execution loop
    let tick_interval = config.compactor.tick_interval;
    let planning_task = {
        let planner = planner.clone();
        let executor = executor.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            use tokio::time::interval;
            let mut ticker = interval(tick_interval);

            loop {
                ticker.tick().await;

                log::debug!("Running compaction planning cycle");

                match planner.plan().await {
                    Ok(candidates) => {
                        if candidates.is_empty() {
                            log::info!("No compaction candidates found in this cycle");
                        } else {
                            log::info!("Found {} compaction candidates:", candidates.len());

                            // Execute compaction for each candidate
                            for candidate in candidates {
                                candidate.log();

                                log::info!(
                                    "Executing compaction for {}/{}/{} partition {}",
                                    candidate.tenant_id,
                                    candidate.dataset_id,
                                    candidate.table_name,
                                    candidate.partition_id
                                );

                                match executor.execute_candidate(candidate).await {
                                    Ok(result) => {
                                        log::info!(
                                            "Compaction job {} completed with status: {:?}",
                                            result.job_id,
                                            result.status
                                        );

                                        if let Some(error) = result.error {
                                            log::error!("Job {} error: {}", result.job_id, error);
                                        } else {
                                            log::info!(
                                                "Job {}: {} files → {} files, {} bytes → {} bytes, duration={:?}",
                                                result.job_id,
                                                result.input_files_count,
                                                result.output_files_count,
                                                result.bytes_before,
                                                result.bytes_after,
                                                result.duration
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Failed to execute compaction: {e:?}");
                                    }
                                }
                            }

                            // Log metrics summary after cycle
                            let summary = metrics.summary();
                            summary.log();
                        }
                    }
                    Err(e) => {
                        log::error!("Compaction planning cycle failed: {e:?}");
                    }
                }
            }
        })
    };

    // Wait for shutdown signal (SIGINT or SIGTERM)
    log::info!("Compactor service running, waiting for shutdown signal");
    wait_for_shutdown_signal().await?;

    log::info!("Received shutdown signal, stopping compactor service");

    // Stop planning task
    planning_task.abort();

    // Graceful shutdown
    bootstrap
        .shutdown()
        .await
        .context("Failed to shutdown service bootstrap")?;

    log::info!("Compactor service stopped");

    Ok(())
}
