//! # Compactor service assembly and lifecycle loop
//!
//! Builds the full set of compactor components (planner, executor, scheduler,
//! distributed leases, retention enforcer, orphan detector/cleaner) from a
//! [`Configuration`] and runs the background lifecycle loop that ticks
//! compaction, stale-lease expiry, retention enforcement, and orphan cleanup.
//!
//! Each cycle runs on its own task (see [`crate::lifecycle`]), so a long
//! compaction pass cannot postpone stale-lease expiry, retention, or orphan
//! cleanup (issue #1011).
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
use tokio::task::JoinSet;
use tokio::time::{Interval, MissedTickBehavior, interval};
use uuid::Uuid;

use crate::executor::{CompactionExecutor, ExecutorConfig};
use crate::flight::CompactorFlightService;
use crate::http::ObservabilityState;
use crate::lease::LeaseManager;
use crate::lifecycle::{
    CompactionCycle, LEASE_EXPIRY_INTERVAL, LeaseExpiryCycle, LifecycleIntervals,
    OrphanCleanupCycle, RetentionCycle,
};
use crate::metrics::CompactionMetrics;
use crate::orphan::{OrphanCleaner, OrphanCleanupConfig, OrphanDetector};
use crate::planner::{CompactionPlanner, PlannerConfig};
use crate::retention::metrics::RetentionMetrics;
use crate::retention::{RetentionConfig, RetentionEnforcer};
use crate::scheduler::RoundRobinScheduler;

/// Ticker for one lifecycle cycle.
///
/// `Delay` keeps a cycle that overran its period from bursting through every
/// missed tick afterwards; it resumes on cadence instead.
fn lifecycle_ticker(period: Duration) -> Interval {
    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker
}

/// Fully assembled compactor: every component the lifecycle loop and the
/// Flight/observability surfaces need, built once from configuration.
pub struct CompactorService {
    planner: Arc<CompactionPlanner>,
    executor: Arc<CompactionExecutor>,
    compaction_metrics: CompactionMetrics,
    lease_manager: LeaseManager,
    retention_metrics: RetentionMetrics,
    orphan_detector: Arc<OrphanDetector>,
    compaction: CompactionCycle,
    lease_expiry: LeaseExpiryCycle,
    retention: Arc<RetentionCycle>,
    orphan_cleanup: OrphanCleanupCycle,
    intervals: LifecycleIntervals,
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
        let executor_config = ExecutorConfig::from(&planner_config);
        let compaction_metrics = CompactionMetrics::new();

        // The planner shares the executor's metrics so the files it declines
        // to compact — still-open partitions, unclassifiable files — are
        // visible next to the jobs it does produce.
        let planner = Arc::new(
            CompactionPlanner::new(catalog_manager.clone(), planner_config.clone())
                .with_metrics(compaction_metrics.clone()),
        );
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

        let intervals = LifecycleIntervals {
            compaction: config.compactor.tick_interval,
            lease_expiry: LEASE_EXPIRY_INTERVAL,
            retention: retention_config.retention_check_interval,
            orphan_cleanup: orphan_cleanup_config.cleanup_interval(),
        };

        let compaction = CompactionCycle::new(
            scheduler,
            executor.clone(),
            lease_manager.clone(),
            compaction_metrics.clone(),
        );
        let lease_expiry = LeaseExpiryCycle::new(lease_manager.clone(), compaction_metrics.clone());
        let retention = Arc::new(RetentionCycle::new(
            retention_config,
            retention_enforcer,
            catalog_manager.clone(),
        ));
        let orphan_cleanup = OrphanCleanupCycle::new(
            orphan_cleanup_config,
            retention.clone(),
            orphan_detector.clone(),
            orphan_cleaner,
            catalog_manager,
        );

        Ok(Self {
            planner,
            executor,
            compaction_metrics,
            lease_manager,
            retention_metrics,
            orphan_detector,
            compaction,
            lease_expiry,
            retention,
            orphan_cleanup,
            intervals,
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

    /// Run the background lifecycle loop until the task is aborted.
    ///
    /// Each cycle — compaction planning/execution, stale-lease expiry,
    /// retention enforcement, orphan cleanup — runs on its own task at its own
    /// cadence. They used to share one `select!` loop, which meant a long
    /// compaction pass (each candidate is a full partition rewrite with
    /// retries and backoff) blocked stale-lease expiry, delaying recovery from
    /// crashed instances by the length of that pass (issue #1011).
    ///
    /// Callers `tokio::spawn` this and abort the handle on shutdown: the
    /// `JoinSet` aborts every cycle task when this future is dropped.
    pub async fn run_lifecycle_loop(self) {
        let Self {
            mut compaction,
            lease_expiry,
            retention,
            orphan_cleanup,
            intervals,
            ..
        } = self;

        let mut tasks = JoinSet::new();

        let compaction_interval = intervals.compaction;
        tasks.spawn(async move {
            let mut ticker = lifecycle_ticker(compaction_interval);
            loop {
                ticker.tick().await;
                compaction.run().await;
            }
        });

        let lease_expiry_interval = intervals.lease_expiry;
        tasks.spawn(async move {
            let mut ticker = lifecycle_ticker(lease_expiry_interval);
            loop {
                ticker.tick().await;
                lease_expiry.run().await;
            }
        });

        if retention.enabled() {
            let retention_interval = intervals.retention;
            tasks.spawn(async move {
                let mut ticker = lifecycle_ticker(retention_interval);
                loop {
                    ticker.tick().await;
                    retention.run().await;
                }
            });
        } else {
            tracing::info!("Retention enforcement disabled — not starting its lifecycle task");
        }

        if orphan_cleanup.enabled() {
            let orphan_cleanup_interval = intervals.orphan_cleanup;
            tasks.spawn(async move {
                let mut ticker = lifecycle_ticker(orphan_cleanup_interval);
                loop {
                    ticker.tick().await;
                    orphan_cleanup.run().await;
                }
            });
        } else {
            tracing::info!("Orphan cleanup disabled — not starting its lifecycle task");
        }

        // The cycle tasks never return on their own, so this only yields when
        // one panics: report it rather than losing the cycle silently.
        while let Some(joined) = tasks.join_next().await {
            if let Err(e) = joined
                && !e.is_cancelled()
            {
                tracing::error!("Compactor lifecycle task terminated unexpectedly: {e}");
            }
        }
    }

    /// Override the lifecycle tick cadences. Test-only: production cadences
    /// come from configuration, and tests need sub-second cycles.
    #[cfg(test)]
    fn with_intervals(mut self, intervals: LifecycleIntervals) -> Self {
        self.intervals = intervals;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{CompactionCandidate, PartitionStats};
    use crate::scheduler::Planner;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn service_from(
        config: &Configuration,
        service_catalog: Arc<Catalog>,
    ) -> CompactorService {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        CompactorService::new(config, catalog_manager, service_catalog, Uuid::new_v4())
            .expect("service assembles from config")
    }

    async fn in_memory_service() -> CompactorService {
        let service_catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        service_from(&Configuration::default(), service_catalog).await
    }

    /// Stands in for a compaction cycle that outlives every other cadence:
    /// it records that planning started, then never finishes.
    struct BlockedPlanner {
        entered: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl Planner for BlockedPlanner {
        async fn plan(&self) -> Result<Vec<CompactionCandidate>> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
    }

    /// Counts completed planning passes, so a test can tell whether the
    /// compaction cycle is still ticking.
    struct CountingPlanner {
        entered: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl Planner for CountingPlanner {
        async fn plan(&self) -> Result<Vec<CompactionCandidate>> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        }
    }

    fn candidate(partition_id: &str) -> CompactionCandidate {
        CompactionCandidate {
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            table_name: "traces".to_string(),
            partition_id: partition_id.to_string(),
            stats: PartitionStats {
                file_count: 5,
                total_size_bytes: 1024 * 1024,
                avg_file_size_bytes: 204_800,
            },
        }
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
        service.compaction.run().await;
        service.lease_expiry.run().await;
        service.retention.run().await;
        service.orphan_cleanup.run().await;
    }

    /// The regression this refactor exists for (issue #1011): stale-lease
    /// expiry is how a crashed instance's partitions become claimable again,
    /// so it must keep ticking while a compaction cycle is still running.
    #[tokio::test(flavor = "multi_thread")]
    async fn stale_lease_expiry_runs_while_a_compaction_cycle_is_still_going() {
        let mut config = Configuration::default();
        // Keep the test to the two cycles under examination.
        config.compactor.retention.enabled = false;
        config.compactor.orphan_cleanup.enabled = false;

        let service_catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        let mut service = service_from(&config, service_catalog.clone()).await;

        // A lease from an instance that has since "crashed", already past its
        // TTL and therefore due for expiry.
        let crashed = LeaseManager::new(
            service_catalog.clone(),
            Uuid::new_v4(),
            Duration::from_millis(1),
        );
        crashed
            .try_acquire_default(&candidate("crashed-instance"))
            .await
            .expect("lease acquisition succeeds")
            .expect("uncontended lease is granted");

        let entered = Arc::new(AtomicUsize::new(0));
        service
            .compaction
            .replace_scheduler(RoundRobinScheduler::new(
                Arc::new(BlockedPlanner {
                    entered: entered.clone(),
                }),
                0,
                0,
            ));

        let metrics = service.compaction_metrics.clone();
        let lifecycle = tokio::spawn(
            service
                .with_intervals(LifecycleIntervals {
                    compaction: Duration::from_millis(10),
                    lease_expiry: Duration::from_millis(10),
                    retention: Duration::from_secs(3600),
                    orphan_cleanup: Duration::from_secs(3600),
                })
                .run_lifecycle_loop(),
        );

        // Expiry must happen even though compaction never returns. Poll
        // rather than sleep once, so a slow CI machine costs time, not a
        // false failure.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while metrics.stale_leases_expired() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "stale lease was never expired while a compaction cycle was running"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // ...and the compaction cycle really is still in flight: planning was
        // entered exactly once and has not come back.
        assert_eq!(
            entered.load(Ordering::SeqCst),
            1,
            "compaction cycle should still be blocked inside its first planning pass"
        );

        lifecycle.abort();
    }

    /// Aborting the handle returned by `tokio::spawn(run_lifecycle_loop())`
    /// — what both binaries do on shutdown — must stop the per-cycle tasks
    /// too, not leave them ticking against a torn-down catalog.
    #[tokio::test(flavor = "multi_thread")]
    async fn aborting_the_loop_stops_every_cycle_task() {
        let mut service = in_memory_service().await;

        let entered = Arc::new(AtomicUsize::new(0));
        service
            .compaction
            .replace_scheduler(RoundRobinScheduler::new(
                Arc::new(CountingPlanner {
                    entered: entered.clone(),
                }),
                0,
                0,
            ));

        let lifecycle = tokio::spawn(
            service
                .with_intervals(LifecycleIntervals {
                    compaction: Duration::from_millis(10),
                    lease_expiry: Duration::from_millis(10),
                    retention: Duration::from_millis(10),
                    orphan_cleanup: Duration::from_millis(10),
                })
                .run_lifecycle_loop(),
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while entered.load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "compaction cycle never ticked"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        lifecycle.abort();
        assert!(
            lifecycle.await.unwrap_err().is_cancelled(),
            "lifecycle loop should end on abort"
        );

        // The spawned cycle tasks die with the JoinSet, so nothing re-enters
        // planning after the abort.
        let after_abort = entered.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            entered.load(Ordering::SeqCst),
            after_abort,
            "cycle tasks kept running after the lifecycle handle was aborted"
        );
    }
}
