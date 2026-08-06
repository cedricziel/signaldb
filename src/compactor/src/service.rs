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

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use common::catalog::Catalog;
use common::catalog_manager::CatalogManager;
use common::config::Configuration;
use common::storage::create_object_store;
use futures::FutureExt;
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
use crate::metrics::{CompactionMetrics, Cycle, CycleHealth};
use crate::orphan::{OrphanCleaner, OrphanCleanupConfig, OrphanDetector};
use crate::planner::{CompactionPlanner, PlannerConfig};
use crate::retention::metrics::RetentionMetrics;
use crate::retention::{RetentionConfig, RetentionEnforcer};
use crate::scheduler::RoundRobinScheduler;

/// Smallest cadence a lifecycle cycle may tick at.
///
/// `tokio::time::interval` panics on a zero period, and every cadence except
/// lease expiry comes from configuration (`tick_interval = "0s"`,
/// `cleanup_interval_hours = 0`). A cycle that busy-loops is not a useful
/// reading of that config either, so a zero is clamped and reported rather
/// than taking the process down.
const MIN_CYCLE_PERIOD: Duration = Duration::from_secs(1);

/// Ticker for one lifecycle cycle.
///
/// `Delay` keeps a cycle that overran its period from bursting through every
/// missed tick afterwards; it resumes on cadence instead.
fn lifecycle_ticker(cycle: Cycle, period: Duration) -> Interval {
    let period = if period < MIN_CYCLE_PERIOD {
        tracing::warn!(
            "Configured {} interval {period:?} is below the {MIN_CYCLE_PERIOD:?} minimum; using the minimum",
            cycle.label()
        );
        MIN_CYCLE_PERIOD
    } else {
        period
    };
    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker
}

/// Smallest and largest backoff sleep after a cycle iteration panics.
///
/// The next scheduled tick already provides pacing between attempts; this is
/// an *additional* sleep layered on top, so a cycle that panics every single
/// time backs off exponentially instead of retrying at its raw tick cadence
/// forever (which for the 30s lease-expiry cadence would otherwise mean a
/// panic loop retrying every 30s indefinitely).
const PANIC_BACKOFF_MIN: Duration = Duration::from_secs(1);
const PANIC_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// Backoff for the `n`th consecutive panic (`n >= 1`): doubles per panic,
/// capped at [`PANIC_BACKOFF_MAX`].
fn backoff_for(consecutive_panics: u32) -> Duration {
    let exponent = consecutive_panics.saturating_sub(1).min(8);
    PANIC_BACKOFF_MIN
        .saturating_mul(1u32 << exponent)
        .min(PANIC_BACKOFF_MAX)
}

/// Best-effort human-readable message from a caught panic payload.
fn panic_message(panic: &(dyn Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Run one lifecycle-cycle iteration, catching a panic instead of letting it
/// unwind the task the ticking loop lives on.
///
/// Left unguarded, a panic inside `fut` (e.g. `compaction.run()`) would end
/// that cycle's task for the rest of the process's life — for lease expiry
/// that is exactly the silent, permanent loss issue #1011 is about, just
/// moved from "one long compaction cycle" to "one panic". The cycle's owned
/// state (`compaction`, `lease_expiry`, ...) lives one stack frame up from
/// the panicking call and is untouched by the unwind, so the same instance
/// keeps running on the next tick — no reconstruction needed. RAII guards
/// inside a cycle (e.g. the lease renewal task) still run their `Drop` during
/// the unwind, so a panic mid-job does not leak a lease.
///
/// `consecutive_panics` tracks the run of panics on this specific cycle so
/// repeated failures back off exponentially ([`backoff_for`]); a successful
/// iteration resets it to zero.
async fn run_cycle_guarded<F>(
    cycle: Cycle,
    health: &CycleHealth,
    consecutive_panics: &mut u32,
    fut: F,
) where
    F: Future<Output = ()>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(()) => {
            *consecutive_panics = 0;
        }
        Err(panic) => {
            *consecutive_panics = consecutive_panics.saturating_add(1);
            health.record_panic(cycle);
            let message = panic_message(&*panic);
            let backoff = backoff_for(*consecutive_panics);
            tracing::error!(
                "{} lifecycle cycle panicked ({} consecutive): {message}; retrying after {backoff:?}",
                cycle.label(),
                consecutive_panics,
            );
            tokio::time::sleep(backoff).await;
            health.record_retrying(cycle);
        }
    }
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
    cycle_health: CycleHealth,
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
            retention_metrics.clone(),
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
            cycle_health: CycleHealth::new(),
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
            self.cycle_health.clone(),
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
    /// Every cycle iteration is also guarded against panics
    /// ([`run_cycle_guarded`]): a panic is caught, counted in
    /// [`CycleHealth`](crate::metrics::CycleHealth) (visible on `/health` and
    /// `/metrics`), and retried after a backoff — a cycle task ending
    /// permanently on an unhandled panic would reopen the exact failure mode
    /// this split exists to close, just triggered by a bug instead of a slow
    /// tick.
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
            cycle_health,
            ..
        } = self;

        let mut tasks = JoinSet::new();

        let compaction_interval = intervals.compaction;
        let health = cycle_health.clone();
        tasks.spawn(async move {
            let mut ticker = lifecycle_ticker(Cycle::Compaction, compaction_interval);
            let mut consecutive_panics = 0;
            loop {
                ticker.tick().await;
                run_cycle_guarded(
                    Cycle::Compaction,
                    &health,
                    &mut consecutive_panics,
                    compaction.run(),
                )
                .await;
            }
        });

        let lease_expiry_interval = intervals.lease_expiry;
        let health = cycle_health.clone();
        tasks.spawn(async move {
            let mut ticker = lifecycle_ticker(Cycle::LeaseExpiry, lease_expiry_interval);
            let mut consecutive_panics = 0;
            loop {
                ticker.tick().await;
                run_cycle_guarded(
                    Cycle::LeaseExpiry,
                    &health,
                    &mut consecutive_panics,
                    lease_expiry.run(),
                )
                .await;
            }
        });

        if retention.enabled() {
            let retention_interval = intervals.retention;
            let health = cycle_health.clone();
            tasks.spawn(async move {
                let mut ticker = lifecycle_ticker(Cycle::Retention, retention_interval);
                let mut consecutive_panics = 0;
                loop {
                    ticker.tick().await;
                    run_cycle_guarded(
                        Cycle::Retention,
                        &health,
                        &mut consecutive_panics,
                        retention.run(),
                    )
                    .await;
                }
            });
        } else {
            tracing::info!("Retention enforcement disabled — not starting its lifecycle task");
        }

        if orphan_cleanup.enabled() {
            let orphan_cleanup_interval = intervals.orphan_cleanup;
            let health = cycle_health.clone();
            tasks.spawn(async move {
                let mut ticker = lifecycle_ticker(Cycle::OrphanCleanup, orphan_cleanup_interval);
                let mut consecutive_panics = 0;
                loop {
                    ticker.tick().await;
                    run_cycle_guarded(
                        Cycle::OrphanCleanup,
                        &health,
                        &mut consecutive_panics,
                        orphan_cleanup.run(),
                    )
                    .await;
                }
            });
        } else {
            tracing::info!("Orphan cleanup disabled — not starting its lifecycle task");
        }

        // The cycle tasks never return on their own (panics inside them are
        // caught by run_cycle_guarded, not propagated), so this only yields
        // on a genuine task-level failure: report it rather than losing the
        // cycle silently.
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

    /// Panics on its first call, then behaves like [`CountingPlanner`].
    /// Stands in for a cycle that hits a bug once (e.g. an unexpected catalog
    /// response) and must keep running afterward rather than going dark.
    struct PanicOnFirstCallPlanner {
        entered: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl Planner for PanicOnFirstCallPlanner {
        async fn plan(&self) -> Result<Vec<CompactionCandidate>> {
            let call = self.entered.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                panic!("simulated planner panic on the first call");
            }
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

    /// A zero interval reaches `tokio::time::interval`, which panics on it —
    /// and every cadence but lease expiry comes from user configuration.
    #[tokio::test]
    async fn zero_cycle_intervals_are_clamped_instead_of_panicking() {
        let ticker = lifecycle_ticker(Cycle::Compaction, Duration::ZERO);
        assert_eq!(ticker.period(), MIN_CYCLE_PERIOD);

        // A configured cadence above the floor is left exactly as configured.
        let ticker = lifecycle_ticker(Cycle::Retention, Duration::from_secs(3600));
        assert_eq!(ticker.period(), Duration::from_secs(3600));
    }

    /// A cycle that panics must not end its task permanently — that would
    /// silently reopen the exact failure mode this refactor exists to close
    /// (issue #1011), just triggered by a bug instead of a slow tick.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_cycle_recovers_and_keeps_ticking() {
        let mut config = Configuration::default();
        config.compactor.retention.enabled = false;
        config.compactor.orphan_cleanup.enabled = false;

        let service_catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        let mut service = service_from(&config, service_catalog).await;

        let entered = Arc::new(AtomicUsize::new(0));
        service
            .compaction
            .replace_scheduler(RoundRobinScheduler::new(
                Arc::new(PanicOnFirstCallPlanner {
                    entered: entered.clone(),
                }),
                0,
                0,
            ));

        let health = service.cycle_health.clone();
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

        // The first tick panics inside `plan()`. The cycle must recover and
        // reach a second, successful entry rather than the task quietly
        // ending after the first.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while entered.load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "compaction cycle never recovered from its panic"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert_eq!(
            health.panics(Cycle::Compaction),
            1,
            "the caught panic must be recorded"
        );
        assert!(
            !health.is_down(Cycle::Compaction),
            "the cycle must be marked retrying again once backoff completes"
        );

        lifecycle.abort();
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
        // The catalog is empty, so retention/orphan cleanup are no-ops
        // regardless — dry_run is set anyway so this test never depends on
        // that emptiness to stay non-destructive.
        let mut config = Configuration::default();
        config.compactor.retention.dry_run = true;
        config.compactor.orphan_cleanup.dry_run = true;
        let service_catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        let mut service = service_from(&config, service_catalog).await;

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
        // Only the compaction counter is asserted on, and the cycles tick at
        // 10ms here — keep the two destructive cycles out of the loop rather
        // than pointing them at a catalog at that cadence.
        let mut config = Configuration::default();
        config.compactor.retention.enabled = false;
        config.compactor.orphan_cleanup.enabled = false;
        let service_catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        let mut service = service_from(&config, service_catalog).await;

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
