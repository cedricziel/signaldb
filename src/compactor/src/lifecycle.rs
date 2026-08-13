//! # Compactor lifecycle cycles
//!
//! The compactor's four background jobs — compaction, stale-lease expiry,
//! retention enforcement, and orphan cleanup — each as a self-contained unit
//! owning exactly the components it needs.
//!
//! Splitting them apart is what lets [`crate::service::CompactorService::run_lifecycle_loop`]
//! drive each on its own task. They used to share a single `select!` loop, so
//! a long compaction cycle (every candidate is a full partition rewrite with
//! retries and backoff) postponed stale-lease expiry — the recovery mechanism
//! for crashed instances — for its entire duration (issue #1011).
//!
//! ## Concurrency
//!
//! Independent tasks mean compaction and retention can now commit against the
//! same table concurrently. Both paths use Iceberg's optimistic concurrency
//! and retry on conflict, so the cost is CAS pressure rather than lost work
//! (see #996). Retention is the one job that must not overlap *itself*: the
//! orphan-cleanup cycle pre-expires snapshots through the same enforcer (the
//! #475 ordering fix), so [`RetentionCycle`] serializes every enforcement pass
//! behind an internal gate.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use common::catalog_manager::{CatalogManager, ResolvedTenant};
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::executor::{CompactionExecutor, CompactionStatus};
use crate::lease::LeaseManager;
use crate::metrics::CompactionMetrics;
use crate::orphan::{OrphanCleaner, OrphanCleanupConfig, OrphanDetector};
use crate::retention::metrics::RetentionMetrics;
use crate::retention::{RetentionConfig, RetentionEnforcer, SignalType};
use crate::scheduler::RoundRobinScheduler;

/// Stale leases are the recovery path for crashed instances, so they are
/// checked on a fixed short cadence independent of the compaction interval.
pub(crate) const LEASE_EXPIRY_INTERVAL: Duration = Duration::from_secs(30);

/// Tick cadences for the lifecycle cycles.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LifecycleIntervals {
    pub(crate) compaction: Duration,
    pub(crate) lease_expiry: Duration,
    pub(crate) retention: Duration,
    pub(crate) orphan_cleanup: Duration,
}

/// Enumerate the active tenants through the source-agnostic registry, logging
/// and returning empty on failure so a lifecycle cycle degrades rather than
/// aborting. `purpose` names the cycle in the error log.
async fn active_tenants_or_empty(
    catalog_manager: &CatalogManager,
    purpose: &str,
) -> Vec<ResolvedTenant> {
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
///
/// Membership is decided by [`SignalType::from_table_name`], shared with the
/// retention enforcer, so every signal the schema registry can create is
/// covered — a local name allowlist here left `profiles` with no orphan
/// cleanup at all (#1014).
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
        .filter(|name| SignalType::from_table_name(name).is_ok())
        .collect();
    tables.sort();
    Ok(tables)
}

/// Schedules compaction candidates fairly and executes each one under a
/// distributed lease.
pub(crate) struct CompactionCycle {
    scheduler: RoundRobinScheduler,
    executor: Arc<CompactionExecutor>,
    lease_manager: LeaseManager,
    metrics: CompactionMetrics,
}

impl CompactionCycle {
    pub(crate) fn new(
        scheduler: RoundRobinScheduler,
        executor: Arc<CompactionExecutor>,
        lease_manager: LeaseManager,
        metrics: CompactionMetrics,
    ) -> Self {
        Self {
            scheduler,
            executor,
            lease_manager,
            metrics,
        }
    }

    /// Swap in a different scheduler so tests can drive the cycle from a
    /// stub planner instead of a real Iceberg-backed catalog scan.
    #[cfg(test)]
    pub(crate) fn replace_scheduler(&mut self, scheduler: RoundRobinScheduler) {
        self.scheduler = scheduler;
    }

    /// One compaction cycle: schedule candidates fairly, then execute each
    /// under a distributed lease.
    pub(crate) async fn run(&mut self) {
        tracing::debug!("Running compaction planning cycle");

        // The declined-file counters are cumulative for the process lifetime,
        // so snapshot them before planning and report only this cycle's delta
        // below. Logging the raw totals would attribute every earlier cycle's
        // declined files to this one, which is exactly backwards for the
        // question the log exists to answer.
        let deferred_before = self.metrics.deferred_open_partition_files();
        let unclassifiable_before = self.metrics.unclassifiable_files();

        let candidates = match self.scheduler.schedule(Instant::now()).await {
            Ok(candidates) => candidates,
            Err(e) => {
                tracing::error!("Compaction scheduling cycle failed: {e:?}");
                return;
            }
        };

        if candidates.is_empty() {
            // Report what *this* planning pass declined, so an empty cycle is
            // distinguishable from one where every file was deferred to a
            // still-open partition or excluded as unclassifiable.
            let deferred = self
                .metrics
                .deferred_open_partition_files()
                .saturating_sub(deferred_before);
            let unclassifiable = self
                .metrics
                .unclassifiable_files()
                .saturating_sub(unclassifiable_before);
            tracing::info!(
                deferred_open_partition_files = deferred,
                unclassifiable_files = unclassifiable,
                "No compaction candidates found in this cycle"
            );
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

                    // `execute_candidate` consumes the candidate, but the
                    // outcome has to be attributed back to its partition.
                    let outcome_key = candidate.clone();

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

                            match result.status {
                                CompactionStatus::Success => {
                                    self.scheduler.record_success(&outcome_key)
                                }
                                // A conflict means another actor committed
                                // first. That is contention to retry, not a
                                // partition that cannot succeed — cooling it
                                // down would suppress the very retry the
                                // conflict path exists to perform.
                                CompactionStatus::Conflict => {}
                                CompactionStatus::Failed => {
                                    self.scheduler.record_failure(&outcome_key, Instant::now())
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to execute compaction: {e:?}");
                            self.scheduler.record_failure(&outcome_key, Instant::now());
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
        self.metrics.summary().log();
    }
}

/// Garbage-collects leases abandoned by crashed instances.
///
/// This is the compactor's recovery path, which is why it runs on its own
/// task: partitions whose owner died stay unclaimable until their lease is
/// expired here.
pub(crate) struct LeaseExpiryCycle {
    lease_manager: LeaseManager,
    metrics: CompactionMetrics,
}

impl LeaseExpiryCycle {
    pub(crate) fn new(lease_manager: LeaseManager, metrics: CompactionMetrics) -> Self {
        Self {
            lease_manager,
            metrics,
        }
    }

    /// Expire leases abandoned by crashed instances.
    pub(crate) async fn run(&self) {
        match self.lease_manager.expire_stale().await {
            Ok(count) => {
                self.metrics.record_stale_leases_expired(count);
                if count > 0 {
                    tracing::info!(
                        "Expired {count} stale compaction lease(s) from crashed instances"
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Stale lease cleanup failed: {e:#}");
            }
        }
    }
}

/// Enforces retention policies (partition drops and snapshot expiration)
/// across every active tenant/dataset.
pub(crate) struct RetentionCycle {
    config: RetentionConfig,
    enforcer: Arc<RetentionEnforcer>,
    metrics: RetentionMetrics,
    catalog_manager: Arc<CatalogManager>,
    /// Serializes enforcement passes. The retention cycle and the orphan
    /// cleanup cycle run on separate tasks but share this enforcer, and two
    /// passes over the same table would race each other's commits.
    gate: Mutex<()>,
}

impl RetentionCycle {
    pub(crate) fn new(
        config: RetentionConfig,
        enforcer: Arc<RetentionEnforcer>,
        metrics: RetentionMetrics,
        catalog_manager: Arc<CatalogManager>,
    ) -> Self {
        Self {
            config,
            enforcer,
            metrics,
            catalog_manager,
            gate: Mutex::new(()),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// One retention cycle across every active tenant/dataset.
    pub(crate) async fn run(&self) {
        if !self.enabled() {
            return;
        }
        tracing::debug!("Running retention enforcement cycle");

        let tenants = active_tenants_or_empty(&self.catalog_manager, "retention").await;
        self.enforce(&tenants, "retention cycle").await;
    }

    /// Enforce retention across `tenants`, serialized against any other
    /// in-flight pass. No-op when retention is disabled. `purpose` names the
    /// caller in logs, since orphan cleanup drives this too.
    pub(crate) async fn enforce(&self, tenants: &[ResolvedTenant], purpose: &str) {
        if !self.enabled() {
            return;
        }

        let _pass = self.gate.lock().await;
        for tenant_config in tenants {
            for dataset_config in &tenant_config.datasets {
                let tenant_id = &tenant_config.id;
                let dataset_id = &dataset_config.id;
                match self.enforcer.enforce_retention(tenant_id, dataset_id).await {
                    Ok(result) => {
                        tracing::info!(
                            "Retention enforcement completed for {}/{} ({}): {} tables processed, {} partitions dropped, {} snapshots expired, {} bytes reclaimed",
                            tenant_id,
                            dataset_id,
                            purpose,
                            result.tables_processed,
                            result.total_partitions_dropped,
                            result.total_snapshots_expired,
                            result.total_bytes_reclaimed
                        );

                        if !result.errors.is_empty() {
                            self.metrics
                                .record_enforcement_failures(result.errors.len());
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
                        self.metrics.record_enforcement_failures(1);
                        tracing::error!(
                            "Retention enforcement failed for {}/{} ({}): {e:?}",
                            tenant_id,
                            dataset_id,
                            purpose
                        );
                    }
                }
            }
        }
    }
}

/// Detects and deletes files no retained snapshot references.
pub(crate) struct OrphanCleanupCycle {
    config: OrphanCleanupConfig,
    retention: Arc<RetentionCycle>,
    detector: Arc<OrphanDetector>,
    cleaner: Arc<OrphanCleaner>,
    catalog_manager: Arc<CatalogManager>,
}

impl OrphanCleanupCycle {
    pub(crate) fn new(
        config: OrphanCleanupConfig,
        retention: Arc<RetentionCycle>,
        detector: Arc<OrphanDetector>,
        cleaner: Arc<OrphanCleaner>,
        catalog_manager: Arc<CatalogManager>,
    ) -> Self {
        Self {
            config,
            retention,
            detector,
            cleaner,
            catalog_manager,
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// One orphan-cleanup cycle: pre-expire snapshots via retention (issue
    /// #475 ordering fix), then detect and delete unreferenced files per
    /// table.
    pub(crate) async fn run(&self) {
        if !self.enabled() {
            return;
        }
        tracing::debug!("Running orphan cleanup cycle");

        let active_tenants = active_tenants_or_empty(&self.catalog_manager, "orphan cleanup").await;

        // Run retention enforcement first to expire old snapshots, which
        // reduces the live file set size before orphan detection. This is
        // the ordering fix for issue #475 (P3).
        self.retention
            .enforce(&active_tenants, "pre-orphan cleanup")
            .await;

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
                        .detector
                        .identify_orphan_candidates(tid, did, table_name)
                        .instrument(job_span.clone())
                        .await
                    {
                        Ok(candidates) if !candidates.is_empty() => {
                            match self
                                .cleaner
                                .delete_orphans_batch(candidates)
                                .instrument(job_span.clone())
                                .await
                            {
                                Ok(result) => {
                                    self.detector
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
                        .detector
                        .identify_orphan_metadata_candidates(tid, did, table_name)
                        .instrument(job_span.clone())
                        .await
                    {
                        Ok(candidates) if !candidates.is_empty() => {
                            match self
                                .cleaner
                                .delete_orphans_batch(candidates)
                                .instrument(job_span.clone())
                                .await
                            {
                                Ok(result) => {
                                    self.detector
                                        .metrics()
                                        .record_deletion_failures(result.failed_count);
                                    tracing::info!(
                                        "Metadata orphan cleanup {}/{}/{}: deleted={}, \
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
        self.detector.metrics().log_summary();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration as TokioDuration, timeout};

    fn retention_cycle(catalog_manager: Arc<CatalogManager>) -> Arc<RetentionCycle> {
        let config = RetentionConfig::default();
        let metrics = RetentionMetrics::new();
        let enforcer = Arc::new(
            RetentionEnforcer::new(catalog_manager.clone(), config.clone(), metrics.clone())
                .expect("retention enforcer builds from default config"),
        );
        Arc::new(RetentionCycle::new(
            config,
            enforcer,
            metrics,
            catalog_manager,
        ))
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
        // subtypes the old hardcoded list skipped, and `profiles`, which
        // the same list excluded from orphan cleanup entirely (#1014).
        for table in ["traces", "metrics_sum", "metrics_summary", "profiles"] {
            catalog_manager.ensure_table("t", "d", table).await.unwrap();
        }
        let tables = list_signal_tables(&catalog_manager, "t", "d")
            .await
            .unwrap();
        assert_eq!(
            tables,
            vec!["metrics_sum", "metrics_summary", "profiles", "traces"]
        );

        // The phantom table from the old list cannot even be created.
        assert!(
            catalog_manager
                .ensure_table("t", "d", "metrics_counter")
                .await
                .is_err(),
            "metrics_counter is not a real signal table"
        );
    }

    /// The retention cycle and the orphan cycle drive the same enforcer from
    /// separate tasks; the gate must keep their passes from interleaving.
    #[tokio::test]
    async fn retention_passes_are_serialized_against_each_other() {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let retention = retention_cycle(catalog_manager);
        assert!(retention.enabled(), "retention is enabled by default");

        // Stand in for a pass already running on the other task.
        let in_flight = retention.gate.lock().await;

        let mut waiting = tokio::spawn({
            let retention = retention.clone();
            async move { retention.run().await }
        });

        // A second pass must wait rather than run concurrently, even though
        // the catalog is empty and the pass itself is trivial.
        assert!(
            timeout(TokioDuration::from_millis(200), &mut waiting)
                .await
                .is_err(),
            "retention pass ran while another pass held the gate"
        );

        drop(in_flight);
        timeout(TokioDuration::from_secs(5), waiting)
            .await
            .expect("retention pass proceeds once the gate is released")
            .expect("retention pass does not panic");
    }
}
