//! # Compactor service assembly and lifecycle loop
//!
//! Builds the full set of compactor components (planner, executor, scheduler,
//! distributed leases, retention enforcer, orphan detector/cleaner) from a
//! [`Configuration`] and runs the background lifecycle loop that ticks
//! compaction, stale-lease expiry, retention enforcement, and orphan cleanup.
//!
//! Both the standalone `signaldb-compactor` binary and the monolithic
//! `signaldb` binary drive the compactor through this module, so every
//! deployment shape gets the same lifecycle behavior — historically the
//! monolith wired only the planner and silently never enforced retention
//! (issue #959).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use common::catalog::Catalog;
use common::catalog_manager::CatalogManager;
use common::config::Configuration;
use common::storage::create_object_store;
use tracing::Instrument;
use uuid::Uuid;

use crate::executor::{CompactionExecutor, ExecutorConfig};
use crate::flight::CompactorFlightService;
use crate::http::ObservabilityState;
use crate::lease::LeaseManager;
use crate::metrics::CompactionMetrics;
use crate::orphan::{OrphanCleaner, OrphanCleanupConfig, OrphanDetector};
use crate::planner::{CompactionPlanner, PlannerConfig};
use crate::retention::metrics::RetentionMetrics;
use crate::retention::{RetentionConfig, RetentionEnforcer};
use crate::scheduler::RoundRobinScheduler;

/// Enumerate the active tenants through the source-agnostic registry, logging
/// and returning empty on failure so a lifecycle cycle degrades rather than
/// aborting. `purpose` names the cycle in the error log.
async fn active_tenants_or_empty(
    catalog_manager: &CatalogManager,
    purpose: &str,
) -> Vec<common::catalog_manager::ResolvedTenant> {
    match catalog_manager.list_active_tenants().await {
        Ok(tenants) => tenants,
        Err(e) => {
            tracing::error!("Failed to enumerate tenants for {purpose}: {e:#}");
            Vec::new()
        }
    }
}

/// List the signal tables that actually exist in a dataset's namespace,
/// so lifecycle jobs neither chase phantom tables nor skip real ones.
async fn list_signal_tables(
    catalog_manager: &CatalogManager,
    tenant_id: &str,
    dataset_id: &str,
) -> Result<Vec<String>> {
    let namespace = catalog_manager
        .build_namespace(tenant_id, dataset_id)
        .context("Failed to build namespace")?;
    let identifiers = catalog_manager
        .catalog()
        .list_tabulars(&namespace)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list tables in {namespace:?}: {e}"))?;

    let mut tables: Vec<String> = identifiers
        .iter()
        .map(|identifier| identifier.name().to_string())
        .filter(|name| name == "traces" || name == "logs" || name.starts_with("metrics"))
        .collect();
    tables.sort();
    Ok(tables)
}

/// Fully assembled compactor: every component the lifecycle loop and the
/// Flight/observability surfaces need, built once from configuration.
pub struct CompactorService {
    planner: Arc<CompactionPlanner>,
    executor: Arc<CompactionExecutor>,
    compaction_metrics: CompactionMetrics,
    lease_manager: LeaseManager,
    scheduler: RoundRobinScheduler,
    retention_config: RetentionConfig,
    retention_metrics: RetentionMetrics,
    retention_enforcer: Arc<RetentionEnforcer>,
    orphan_cleanup_config: OrphanCleanupConfig,
    orphan_detector: Arc<OrphanDetector>,
    orphan_cleaner: Arc<OrphanCleaner>,
    catalog_manager: Arc<CatalogManager>,
    compaction_interval: Duration,
}

impl CompactorService {
    /// Assemble all compactor components.
    ///
    /// `catalog_manager` must already have its tenant source attached so
    /// lifecycle management covers admin-API tenants alongside config-defined
    /// ones. `service_catalog` backs distributed leases and advisory attribute
    /// statistics; `instance_id` identifies this instance for lease ownership.
    pub fn new(
        config: &Configuration,
        catalog_manager: Arc<CatalogManager>,
        service_catalog: Arc<Catalog>,
        instance_id: Uuid,
    ) -> Result<Self> {
        let planner_config = PlannerConfig::from(&config.compactor);
        let planner = Arc::new(CompactionPlanner::new(
            catalog_manager.clone(),
            planner_config.clone(),
        ));

        let executor_config = ExecutorConfig::from(&planner_config);
        let compaction_metrics = CompactionMetrics::new();
        let executor = Arc::new(
            CompactionExecutor::new(
                catalog_manager.clone(),
                executor_config,
                compaction_metrics.clone(),
            )
            // Persist advisory attribute statistics (epic #737, #733).
            .with_service_catalog(service_catalog.clone()),
        );

        tracing::info!(
            "Compaction planner and executor initialized with tick interval: {:?}",
            config.compactor.tick_interval
        );

        let lease_ttl = Duration::from_secs(config.compactor.lease_ttl_seconds);
        let lease_manager = LeaseManager::new(service_catalog, instance_id, lease_ttl);

        tracing::info!(
            "Lease manager initialized (instance_id: {instance_id}, ttl: {lease_ttl:?})"
        );

        let scheduler = RoundRobinScheduler::new(
            planner.clone(),
            config.compactor.max_candidates_per_cycle,
            config.compactor.max_per_tenant,
        );

        tracing::info!(
            "Round-robin scheduler initialized (max_per_cycle: {}, max_per_tenant: {})",
            config.compactor.max_candidates_per_cycle,
            config.compactor.max_per_tenant
        );

        let retention_config = RetentionConfig::from(config.compactor.retention.clone());
        let retention_metrics = RetentionMetrics::new();
        let retention_enforcer = Arc::new(
            RetentionEnforcer::new(
                catalog_manager.clone(),
                retention_config.clone(),
                retention_metrics.clone(),
            )
            .context("Failed to initialize retention enforcer")?,
        );

        tracing::info!(
            "Retention enforcer initialized (enabled: {}, check_interval: {:?}, dry_run: {})",
            retention_config.enabled,
            retention_config.retention_check_interval,
            retention_config.dry_run
        );

        let orphan_cleanup_config =
            OrphanCleanupConfig::from(config.compactor.orphan_cleanup.clone());

        tracing::info!(
            "Orphan cleanup configured (enabled: {}, cleanup_interval: {:?}, dry_run: {})",
            orphan_cleanup_config.enabled,
            orphan_cleanup_config.cleanup_interval(),
            orphan_cleanup_config.dry_run
        );

        let object_store = create_object_store(&config.storage)
            .context("Failed to create object store for orphan cleanup")?;

        // Detector and cleaner are created once — reused across cleanup ticks
        // so that metrics accumulate over the lifetime of the service.
        let orphan_detector = Arc::new(OrphanDetector::new(
            orphan_cleanup_config.clone(),
            catalog_manager.clone(),
            object_store.clone(),
        ));
        let orphan_cleaner = Arc::new(OrphanCleaner::with_detector(
            orphan_cleanup_config.clone(),
            object_store,
            orphan_detector.clone(),
        ));

        Ok(Self {
            planner,
            executor,
            compaction_metrics,
            lease_manager,
            scheduler,
            retention_config,
            retention_metrics,
            retention_enforcer,
            orphan_cleanup_config,
            orphan_detector,
            orphan_cleaner,
            catalog_manager,
            compaction_interval: config.compactor.tick_interval,
        })
    }

    /// Flight `do_action` surface sharing this service's planner, executor,
    /// and lease manager; leases protect against duplicate work between the
    /// background loop and on-demand Flight requests.
    pub fn flight_service(&self) -> CompactorFlightService {
        CompactorFlightService::new(
            self.planner.clone(),
            self.executor.clone(),
            self.lease_manager.clone(),
            self.compaction_metrics.clone(),
        )
    }

    /// State for the observability HTTP endpoint (Prometheus `/metrics`,
    /// JSON `/status`, `/health`).
    pub fn observability_state(&self, service_id: Uuid) -> ObservabilityState {
        ObservabilityState::new(
            service_id,
            self.compaction_metrics.clone(),
            self.retention_metrics.clone(),
            self.orphan_detector.metrics().clone(),
        )
    }

    /// Run the background lifecycle loop until the task is aborted: ticks
    /// compaction planning/execution, stale-lease expiry (30s), retention
    /// enforcement, and orphan cleanup on their configured intervals.
    ///
    /// Callers `tokio::spawn` this and abort the handle on shutdown.
    pub async fn run_lifecycle_loop(mut self) {
        use tokio::time::{MissedTickBehavior, interval};

        let mut compaction_ticker = interval(self.compaction_interval);
        let mut retention_ticker = interval(self.retention_config.retention_check_interval);
        let mut orphan_cleanup_ticker = interval(self.orphan_cleanup_config.cleanup_interval());
        // Clean up stale leases from crashed instances every 30 seconds
        let mut lease_expiry_ticker = interval(Duration::from_secs(30));
        // Cycles run serially in this task, so a long compaction cycle can
        // make other tickers miss; resume on cadence instead of bursting
        // through every missed tick.
        for ticker in [
            &mut compaction_ticker,
            &mut retention_ticker,
            &mut orphan_cleanup_ticker,
            &mut lease_expiry_ticker,
        ] {
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        }

        loop {
            tokio::select! {
                _ = compaction_ticker.tick() => self.run_compaction_cycle().await,
                _ = lease_expiry_ticker.tick() => self.expire_stale_leases().await,
                _ = retention_ticker.tick() => self.run_retention_cycle().await,
                _ = orphan_cleanup_ticker.tick() => self.run_orphan_cleanup_cycle().await,
            }
        }
    }

    /// One compaction cycle: schedule candidates fairly, then execute each
    /// under a distributed lease.
    async fn run_compaction_cycle(&mut self) {
        tracing::debug!("Running compaction planning cycle");

        let candidates = match self.scheduler.schedule().await {
            Ok(candidates) => candidates,
            Err(e) => {
                tracing::error!("Compaction scheduling cycle failed: {e:?}");
                return;
            }
        };

        if candidates.is_empty() {
            tracing::info!("No compaction candidates found in this cycle");
            return;
        }

        tracing::info!(
            "Found {} compaction candidates (scheduler: round-robin):",
            candidates.len()
        );

        for candidate in candidates {
            candidate.log();

            // Attempt to acquire a lease; skip if another instance holds it
            match self.lease_manager.try_acquire_default(&candidate).await {
                Ok(None) => {
                    tracing::debug!(
                        "Skipping {}/{}/{} partition {} — lease held by another instance",
                        candidate.tenant_id,
                        candidate.dataset_id,
                        candidate.table_name,
                        candidate.partition_id
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Lease acquisition failed for {}/{}/{} partition {}: {e:#}",
                        candidate.tenant_id,
                        candidate.dataset_id,
                        candidate.table_name,
                        candidate.partition_id
                    );
                }
                Ok(Some(lease)) => {
                    tracing::info!(
                        "Executing compaction for {}/{}/{} partition {}",
                        candidate.tenant_id,
                        candidate.dataset_id,
                        candidate.table_name,
                        candidate.partition_id
                    );

                    // Keep the lease alive for jobs longer than the TTL.
                    let renewal = self.lease_manager.spawn_renewal(lease.clone());

                    match self.executor.execute_candidate(candidate).await {
                        Ok(result) => {
                            tracing::info!(
                                "Compaction job {} completed with status: {:?}",
                                result.job_id,
                                result.status
                            );

                            if let Some(error) = result.error {
                                tracing::error!("Job {} error: {}", result.job_id, error);
                            } else {
                                tracing::info!(
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
                            tracing::error!("Failed to execute compaction: {e:?}");
                        }
                    }

                    // Release the lease regardless of job outcome
                    drop(renewal);
                    if let Err(e) = self.lease_manager.release(&lease).await {
                        tracing::warn!("Failed to release lease: {e:#}");
                    }
                }
            }
        }

        // Log metrics summary after cycle
        self.compaction_metrics.summary().log();
    }

    /// Expire leases abandoned by crashed instances.
    async fn expire_stale_leases(&self) {
        match self.lease_manager.expire_stale().await {
            Ok(count) if count > 0 => {
                tracing::info!("Expired {count} stale compaction lease(s) from crashed instances");
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("Stale lease cleanup failed: {e:#}");
            }
        }
    }

    /// One retention cycle across every active tenant/dataset: partition
    /// drops and snapshot expiration per the resolved retention policy.
    async fn run_retention_cycle(&self) {
        if !self.retention_config.enabled {
            return;
        }
        tracing::debug!("Running retention enforcement cycle");

        let active_tenants = active_tenants_or_empty(&self.catalog_manager, "retention").await;
        for tenant_config in &active_tenants {
            for dataset_config in &tenant_config.datasets {
                let tenant_id = &tenant_config.id;
                let dataset_id = &dataset_config.id;
                match self
                    .retention_enforcer
                    .enforce_retention(tenant_id, dataset_id)
                    .await
                {
                    Ok(result) => {
                        tracing::info!(
                            "Retention enforcement completed for {}/{}: {} tables processed, {} partitions dropped, {} snapshots expired, {} bytes reclaimed",
                            tenant_id,
                            dataset_id,
                            result.tables_processed,
                            result.total_partitions_dropped,
                            result.total_snapshots_expired,
                            result.total_bytes_reclaimed
                        );

                        if !result.errors.is_empty() {
                            tracing::warn!(
                                "Retention enforcement had {} errors for {}/{}",
                                result.errors.len(),
                                tenant_id,
                                dataset_id
                            );
                            for error in &result.errors {
                                tracing::warn!("Retention error: {}", error);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Retention enforcement failed for {}/{}: {e:?}",
                            tenant_id,
                            dataset_id
                        );
                    }
                }
            }
        }
    }

    /// One orphan-cleanup cycle: pre-expire snapshots via retention (issue
    /// #475 ordering fix), then detect and delete unreferenced files per
    /// table.
    async fn run_orphan_cleanup_cycle(&self) {
        if !self.orphan_cleanup_config.enabled {
            return;
        }
        tracing::debug!("Running orphan cleanup cycle");

        let active_tenants = active_tenants_or_empty(&self.catalog_manager, "orphan cleanup").await;

        // Run retention enforcement first to expire old snapshots, which
        // reduces the live file set size before orphan detection. This is
        // the ordering fix for issue #475 (P3).
        if self.retention_config.enabled {
            tracing::debug!("Running pre-orphan retention enforcement to reduce live file set");
            for tenant_config in &active_tenants {
                for dataset_config in &tenant_config.datasets {
                    let tid = &tenant_config.id;
                    let did = &dataset_config.id;
                    if let Err(e) = self.retention_enforcer.enforce_retention(tid, did).await {
                        tracing::warn!(
                            "Pre-orphan retention enforcement failed for {tid}/{did}: {e:#}"
                        );
                    }
                }
            }
        }

        for tenant_config in &active_tenants {
            for dataset_config in &tenant_config.datasets {
                // List the tables that actually exist in this dataset's
                // namespace. The previous hardcoded list chased the
                // nonexistent metrics_counter every cycle and silently
                // skipped metrics_sum / metrics_exponential_histogram /
                // metrics_summary forever (issue #561).
                let signal_tables = match list_signal_tables(
                    &self.catalog_manager,
                    &tenant_config.id,
                    &dataset_config.id,
                )
                .await
                {
                    Ok(tables) => tables,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to list tables for {}/{}: {e:#}",
                            tenant_config.id,
                            dataset_config.id
                        );
                        continue;
                    }
                };
                for table_name in &signal_tables {
                    let tid = &tenant_config.id;
                    let did = &dataset_config.id;
                    let job_span = common::self_monitoring::spans::job_span(
                        "orphan_cleanup",
                        tid,
                        did,
                        Some(table_name),
                    );
                    match self
                        .orphan_detector
                        .identify_orphan_candidates(tid, did, table_name)
                        .instrument(job_span.clone())
                        .await
                    {
                        Ok(candidates) if !candidates.is_empty() => {
                            match self
                                .orphan_cleaner
                                .delete_orphans_batch(candidates)
                                .instrument(job_span.clone())
                                .await
                            {
                                Ok(result) => {
                                    self.orphan_detector
                                        .metrics()
                                        .record_deletion_failures(result.failed_count);
                                    tracing::info!(
                                        "Orphan cleanup {}/{}/{}: deleted={}, \
                                         would_delete={}, bytes_freed={}, failed={}",
                                        tid,
                                        did,
                                        table_name,
                                        result.deleted_count,
                                        result.would_delete_count,
                                        result.total_bytes_freed,
                                        result.failed_count,
                                    )
                                }
                                Err(e) => tracing::error!(
                                    "Orphan cleanup failed for {}/{}/{}: {e:?}",
                                    tid,
                                    did,
                                    table_name
                                ),
                            }
                        }
                        Ok(_) => {} // no orphans
                        Err(e) => tracing::warn!(
                            "Orphan detection failed for {}/{}/{}: {e:#}",
                            tid,
                            did,
                            table_name
                        ),
                    }

                    // Reclaim unreferenced metadata files (old metadata.json
                    // versions, expired snapshots' manifest lists/manifests)
                    // that delete-after-commit pruning no longer tracks
                    // (#935, #959).
                    match self
                        .orphan_detector
                        .identify_orphan_metadata_candidates(tid, did, table_name)
                        .instrument(job_span.clone())
                        .await
                    {
                        Ok(candidates) if !candidates.is_empty() => {
                            match self
                                .orphan_cleaner
                                .delete_orphans_batch(candidates)
                                .instrument(job_span.clone())
                                .await
                            {
                                Ok(result) => tracing::info!(
                                    "Metadata orphan cleanup {}/{}/{}: deleted={}, \
                                     would_delete={}, bytes_freed={}, failed={}",
                                    tid,
                                    did,
                                    table_name,
                                    result.deleted_count,
                                    result.would_delete_count,
                                    result.total_bytes_freed,
                                    result.failed_count,
                                ),
                                Err(e) => tracing::error!(
                                    "Metadata orphan cleanup failed for {}/{}/{}: {e:?}",
                                    tid,
                                    did,
                                    table_name
                                ),
                            }
                        }
                        Ok(_) => {} // no orphaned metadata
                        Err(e) => tracing::warn!(
                            "Metadata orphan detection skipped for {}/{}/{}: {e:#}",
                            tid,
                            did,
                            table_name
                        ),
                    }
                }
            }
        }

        // Log accumulated metrics periodically
        self.orphan_detector.metrics().log_summary();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn in_memory_service() -> CompactorService {
        let config = Configuration::default();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let service_catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        CompactorService::new(&config, catalog_manager, service_catalog, Uuid::new_v4())
            .expect("service assembles from default config")
    }

    #[tokio::test]
    async fn assembles_all_components_from_default_config() {
        let service = in_memory_service().await;

        // The Flight and observability surfaces share the assembled
        // components — constructing them must not require extra state.
        let _flight = service.flight_service();
        let _obs = service.observability_state(Uuid::new_v4());
    }

    #[tokio::test]
    async fn cycles_run_cleanly_on_empty_backends() {
        let mut service = in_memory_service().await;

        // Every cycle the lifecycle loop drives must degrade gracefully on
        // an empty catalog rather than panic or error the loop away.
        service.run_compaction_cycle().await;
        service.expire_stale_leases().await;
        service.run_retention_cycle().await;
        service.run_orphan_cleanup_cycle().await;
    }

    #[tokio::test]
    async fn list_signal_tables_reflects_catalog_contents() {
        let catalog_manager = CatalogManager::new_in_memory().await.unwrap();

        // Empty namespace: no phantom tables to iterate.
        let tables = list_signal_tables(&catalog_manager, "t", "d")
            .await
            .unwrap();
        assert!(tables.is_empty(), "empty catalog must list no tables");

        // Only the tables that exist come back — including the metrics
        // subtypes the old hardcoded list skipped.
        for table in ["traces", "metrics_sum", "metrics_summary"] {
            catalog_manager.ensure_table("t", "d", table).await.unwrap();
        }
        let tables = list_signal_tables(&catalog_manager, "t", "d")
            .await
            .unwrap();
        assert_eq!(tables, vec!["metrics_sum", "metrics_summary", "traces"]);

        // The phantom table from the old list cannot even be created.
        assert!(
            catalog_manager
                .ensure_table("t", "d", "metrics_counter")
                .await
                .is_err(),
            "metrics_counter is not a real signal table"
        );
    }
}
