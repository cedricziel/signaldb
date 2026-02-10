//! SignalDB Compactor Service
//!
//! Phase 2: Full execution service that identifies compaction candidates,
//! executes compaction with Parquet rewriting, and commits changes atomically.

use anyhow::{Context, Result};
use clap::Parser;
use common::catalog_manager::CatalogManager;
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use compactor::executor::{CompactionExecutor, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::orphan::OrphanCleanupConfig;
use compactor::planner::{CompactionPlanner, PlannerConfig};
use compactor::retention::metrics::RetentionMetrics;
use compactor::retention::{RetentionConfig, RetentionEnforcer};
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
    let compaction_metrics = CompactionMetrics::new();
    let executor = Arc::new(CompactionExecutor::new(
        catalog_manager.clone(),
        executor_config,
        compaction_metrics.clone(),
    ));

    log::info!(
        "Compaction planner and executor initialized with tick interval: {:?}",
        config.compactor.tick_interval
    );

    // Initialize retention enforcement (Phase 3)
    let retention_config = RetentionConfig::from(config.compactor.retention.clone());
    let retention_metrics = RetentionMetrics::new();
    let retention_enforcer = Arc::new(
        RetentionEnforcer::new(
            catalog_manager.clone(),
            retention_config.clone(),
            retention_metrics,
        )
        .context("Failed to initialize retention enforcer")?,
    );

    log::info!(
        "Retention enforcer initialized (enabled: {}, check_interval: {:?}, dry_run: {})",
        retention_config.enabled,
        retention_config.retention_check_interval,
        retention_config.dry_run
    );

    // Initialize orphan cleanup (Phase 3)
    let orphan_cleanup_config = OrphanCleanupConfig::from(config.compactor.orphan_cleanup.clone());

    log::info!(
        "Orphan cleanup configured (enabled: {}, cleanup_interval: {:?}, dry_run: {})",
        orphan_cleanup_config.enabled,
        orphan_cleanup_config.cleanup_interval(),
        orphan_cleanup_config.dry_run
    );

    // Start planning, execution, and retention enforcement loop
    let compaction_interval = config.compactor.tick_interval;
    let planning_task = {
        let planner = planner.clone();
        let executor = executor.clone();
        let metrics = compaction_metrics.clone();
        let retention_enforcer = retention_enforcer.clone();
        let retention_config = retention_config.clone();
        let orphan_cleanup_config = orphan_cleanup_config.clone();

        tokio::spawn(async move {
            use tokio::time::interval;

            let mut compaction_ticker = interval(compaction_interval);
            let mut retention_ticker = interval(retention_config.retention_check_interval);
            let mut orphan_cleanup_ticker = interval(orphan_cleanup_config.cleanup_interval());

            loop {
                tokio::select! {
                    _ = compaction_ticker.tick() => {
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

                    _ = retention_ticker.tick() => {
                        if retention_config.enabled {
                            log::debug!("Running retention enforcement cycle");

                            // TODO: Get list of all tenants/datasets from catalog
                            // For now, we'll use a placeholder list
                            log::warn!("Using placeholder tenant list - catalog enumeration not implemented. Retention will not run on real tenants.");
                            let tenant_dataset_pairs = vec![
                                ("default".to_string(), "default".to_string()),
                            ];

                            for (tenant_id, dataset_id) in tenant_dataset_pairs {
                                match retention_enforcer.enforce_retention(&tenant_id, &dataset_id).await {
                                    Ok(result) => {
                                        log::info!(
                                            "Retention enforcement completed for {}/{}: {} tables processed, {} partitions dropped, {} snapshots expired, {} bytes reclaimed",
                                            tenant_id,
                                            dataset_id,
                                            result.tables_processed,
                                            result.total_partitions_dropped,
                                            result.total_snapshots_expired,
                                            result.total_bytes_reclaimed
                                        );

                                        if !result.errors.is_empty() {
                                            log::warn!(
                                                "Retention enforcement had {} errors for {}/{}",
                                                result.errors.len(),
                                                tenant_id,
                                                dataset_id
                                            );
                                            for error in &result.errors {
                                                log::warn!("Retention error: {}", error);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Retention enforcement failed for {}/{}: {e:?}", tenant_id, dataset_id);
                                    }
                                }
                            }
                        }
                    }

                    _ = orphan_cleanup_ticker.tick() => {
                        if orphan_cleanup_config.enabled {
                            log::debug!("Running orphan cleanup cycle");

                            // Orphan cleanup would be implemented here
                            // For now, we just log that it would run
                            log::info!(
                                "[{}] Orphan cleanup would run here",
                                if orphan_cleanup_config.dry_run { "DRY RUN" } else { "LIVE" }
                            );
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_retention_config_defaults() {
        let config = RetentionConfig::default();
        assert!(!config.enabled, "Retention should be disabled by default");
        assert!(config.dry_run, "Dry-run should be enabled by default");
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
            !config.enabled,
            "Orphan cleanup should be disabled by default"
        );
        assert!(config.dry_run, "Dry-run should be enabled by default");
        assert_eq!(
            config.cleanup_interval(),
            Duration::from_secs(24 * 3600),
            "Default cleanup interval should be 24 hours"
        );
    }

    #[tokio::test]
    async fn test_retention_enforcer_initialization() {
        let config = RetentionConfig::default();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let metrics = RetentionMetrics::new();

        let enforcer = RetentionEnforcer::new(catalog_manager, config, metrics);
        assert!(
            enforcer.is_ok(),
            "Retention enforcer should initialize successfully"
        );
    }
}
