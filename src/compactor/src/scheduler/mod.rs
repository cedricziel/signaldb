//! Fair round-robin scheduling of compaction work across tenants.
//!
//! The scheduler wraps [`CompactionPlanner`] and ensures that no single tenant
//! monopolises a compaction tick. It tracks the last-served tenant position and
//! rotates through tenants in round-robin order on each call to [`schedule`].
//!
//! ## Design
//!
//! - `plan()` is called once per tick to get all candidates across all tenants.
//! - Candidates are grouped by `tenant_id`.
//! - Tenants are iterated starting from the position **after** the last-served tenant,
//!   wrapping around to ensure every tenant eventually gets work done.
//! - At most `max_per_tenant` candidates are taken from each tenant per cycle.
//! - The total returned is capped at `max_candidates_per_cycle`.
//!
//! ## Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Instant;
//! use compactor::planner::CompactionPlanner;
//! use compactor::scheduler::RoundRobinScheduler;
//!
//! # async fn example() -> anyhow::Result<()> {
//! # let catalog_manager = todo!();
//! # let planner_config = todo!();
//! let planner = Arc::new(CompactionPlanner::new(catalog_manager, planner_config));
//! let mut scheduler = RoundRobinScheduler::new(planner, 20, 5);
//!
//! let candidates = scheduler.schedule(Instant::now()).await?;
//! // candidates contains at most 20 entries, at most 5 from any one tenant
//! # Ok(())
//! # }
//! ```

pub mod cooldown;

use crate::metrics::CompactionMetrics;
use crate::planner::{CompactionCandidate, CompactionPlanner};
use anyhow::{Context, Result};
use cooldown::{CooldownTracker, PartitionKey};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Seam over compaction planning.
///
/// `RoundRobinScheduler` depends on this trait rather than the concrete
/// `CompactionPlanner` so tests can inject a fixed candidate list without
/// driving a real Iceberg-backed catalog scan, while production code still
/// exercises the exact `schedule()` implementation.
#[tonic::async_trait]
pub trait Planner: Send + Sync {
    /// Run a planning cycle and return all compaction candidates.
    async fn plan(&self) -> Result<Vec<CompactionCandidate>>;
}

#[tonic::async_trait]
impl Planner for CompactionPlanner {
    async fn plan(&self) -> Result<Vec<CompactionCandidate>> {
        CompactionPlanner::plan(self).await
    }
}

/// Fair scheduler that distributes compaction work across tenants using
/// a round-robin policy with per-tenant and total cycle limits.
pub struct RoundRobinScheduler {
    planner: Arc<dyn Planner>,
    /// Maximum total candidates returned per `schedule()` call.
    /// Set to 0 to disable the total cap.
    max_candidates_per_cycle: usize,
    /// Maximum candidates from any single tenant per `schedule()` call.
    /// Set to 0 to disable the per-tenant cap.
    max_per_tenant: usize,
    /// The `tenant_id` served last (used to advance round-robin position).
    last_tenant: Option<String>,
    /// Partitions suppressed after a failed compaction attempt (#1053).
    cooldowns: CooldownTracker,
    /// Optional counter for partitions withheld by a cooldown. `None` keeps
    /// scheduling log-only, which is what the unit tests default to.
    metrics: Option<CompactionMetrics>,
}

impl RoundRobinScheduler {
    /// Create a new `RoundRobinScheduler`.
    ///
    /// # Arguments
    ///
    /// * `planner`                 – The underlying compaction planner
    /// * `max_candidates_per_cycle` – Cap on total candidates per tick (0 = unlimited)
    /// * `max_per_tenant`           – Cap per tenant per tick (0 = unlimited)
    pub fn new(
        planner: Arc<dyn Planner>,
        max_candidates_per_cycle: usize,
        max_per_tenant: usize,
    ) -> Self {
        Self {
            planner,
            max_candidates_per_cycle,
            max_per_tenant,
            last_tenant: None,
            cooldowns: CooldownTracker::new(),
            metrics: None,
        }
    }

    /// Count partitions withheld by a cooldown into the shared compaction
    /// metrics.
    pub fn with_metrics(mut self, metrics: CompactionMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Run a scheduling cycle: plan all candidates and distribute fairly.
    ///
    /// Returns at most `max_candidates_per_cycle` candidates with at most
    /// `max_per_tenant` from any single tenant. Tenants are served in
    /// round-robin order starting from after the last-served tenant.
    ///
    /// If `max_candidates_per_cycle == 0`, no total cap is applied.
    /// If `max_per_tenant == 0`, no per-tenant cap is applied.
    ///
    /// Partitions still inside a failure cooldown are dropped before any cap
    /// is applied, so a partition that cannot succeed does not consume a
    /// tenant's slot (#1053). `now` is passed in rather than read from the
    /// clock so tests can drive the cooldown windows directly.
    pub async fn schedule(&mut self, now: Instant) -> Result<Vec<CompactionCandidate>> {
        let all = self
            .planner
            .plan()
            .await
            .context("Compaction planner failed")?;

        if all.is_empty() {
            return Ok(vec![]);
        }

        // Drop suppressed partitions before the caps below see them.
        // Filtering after grouping would let a poisoned partition occupy a
        // per-tenant slot it is then skipped from, which is the capacity
        // waste this cooldown exists to stop.
        let eligible = self.filter_cooling_down(all, now);
        self.cooldowns.prune(now);

        if eligible.is_empty() {
            return Ok(vec![]);
        }

        // Group candidates by tenant_id, preserving planner order within each group
        let mut by_tenant: HashMap<String, Vec<CompactionCandidate>> = HashMap::new();
        for candidate in eligible {
            by_tenant
                .entry(candidate.tenant_id.clone())
                .or_default()
                .push(candidate);
        }

        // Sort tenant keys for deterministic round-robin ordering
        let mut tenant_keys: Vec<String> = by_tenant.keys().cloned().collect();
        tenant_keys.sort();

        // Find start index: one after the last-served tenant
        let start = if let Some(last) = &self.last_tenant {
            tenant_keys
                .iter()
                .position(|k| k == last)
                .map(|pos| (pos + 1) % tenant_keys.len())
                .unwrap_or(0)
        } else {
            0
        };

        let mut result: Vec<CompactionCandidate> = Vec::new();
        let total_cap = if self.max_candidates_per_cycle == 0 {
            usize::MAX
        } else {
            self.max_candidates_per_cycle
        };
        let per_tenant_cap = if self.max_per_tenant == 0 {
            usize::MAX
        } else {
            self.max_per_tenant
        };

        let n = tenant_keys.len();
        let mut last_served: Option<String> = None;

        for i in 0..n {
            if result.len() >= total_cap {
                break;
            }

            let tenant = &tenant_keys[(start + i) % n];
            if let Some(candidates) = by_tenant.get(tenant) {
                let take = candidates
                    .len()
                    .min(per_tenant_cap)
                    .min(total_cap - result.len());
                if take > 0 {
                    result.extend_from_slice(&candidates[..take]);
                    last_served = Some(tenant.clone());
                }
            }
        }

        // Advance position so the next call continues from where we left off
        if let Some(last) = last_served {
            self.last_tenant = Some(last);
        }

        Ok(result)
    }

    /// Drop candidates whose partition is still inside a failure cooldown,
    /// counting and logging each one so withheld capacity is never silent.
    fn filter_cooling_down(
        &self,
        candidates: Vec<CompactionCandidate>,
        now: Instant,
    ) -> Vec<CompactionCandidate> {
        candidates
            .into_iter()
            .filter(|candidate| {
                let key = PartitionKey::from_candidate(candidate);
                match self.cooldowns.cooling(&key, now) {
                    Some(cooling) => {
                        if let Some(metrics) = &self.metrics {
                            metrics.record_cooldown_partition_skipped();
                        }
                        tracing::debug!(
                            tenant_id = %candidate.tenant_id,
                            dataset_id = %candidate.dataset_id,
                            table = %candidate.table_name,
                            partition_id = %candidate.partition_id,
                            consecutive_failures = cooling.consecutive_failures,
                            cooldown_remaining_secs = cooling.remaining.as_secs(),
                            "Skipping partition — compaction is cooling down after repeated failures"
                        );
                        false
                    }
                    None => true,
                }
            })
            .collect()
    }

    /// Record that compaction of `candidate`'s partition failed, arming (or
    /// escalating) its cooldown.
    ///
    /// Commit conflicts must not be reported here: a conflict means another
    /// actor committed first, which is contention to retry rather than a
    /// partition that cannot succeed.
    pub fn record_failure(&mut self, candidate: &CompactionCandidate, now: Instant) {
        let cooling = self
            .cooldowns
            .record_failure(PartitionKey::from_candidate(candidate), now);

        tracing::warn!(
            tenant_id = %candidate.tenant_id,
            dataset_id = %candidate.dataset_id,
            table = %candidate.table_name,
            partition_id = %candidate.partition_id,
            consecutive_failures = cooling.consecutive_failures,
            cooldown_secs = cooling.remaining.as_secs(),
            "Compaction failed — partition will be skipped until its cooldown expires"
        );
    }

    /// Record that compaction of `candidate`'s partition succeeded, clearing
    /// any cooldown and resetting its failure escalation.
    pub fn record_success(&mut self, candidate: &CompactionCandidate) {
        self.cooldowns
            .record_success(&PartitionKey::from_candidate(candidate));
    }

    /// Number of partitions currently tracked for cooldown (diagnostics).
    pub fn tracked_cooldowns(&self) -> usize {
        self.cooldowns.len()
    }

    /// Reset round-robin position tracking.
    ///
    /// Useful after configuration changes or tenant list changes to avoid
    /// stale position state.
    pub fn reset_position(&mut self) {
        self.last_tenant = None;
    }

    /// Return the current caps (useful for diagnostics).
    pub fn limits(&self) -> (usize, usize) {
        (self.max_candidates_per_cycle, self.max_per_tenant)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{CompactionCandidate, PartitionStats};
    use crate::scheduler::cooldown::BASE_COOLDOWN;
    use std::time::Duration;

    fn make_candidate(tenant: &str, partition: &str) -> CompactionCandidate {
        make_candidate_in(tenant, "traces", partition)
    }

    fn make_candidate_in(tenant: &str, table: &str, partition: &str) -> CompactionCandidate {
        CompactionCandidate {
            tenant_id: tenant.to_string(),
            dataset_id: "ds".to_string(),
            table_name: table.to_string(),
            partition_id: partition.to_string(),
            stats: PartitionStats {
                file_count: 5,
                total_size_bytes: 1024 * 1024,
                avg_file_size_bytes: 204_800,
            },
        }
    }

    /// A `Planner` that returns a fixed candidate list instead of scanning a
    /// real Iceberg catalog. This is the injectable seam the real
    /// `RoundRobinScheduler` depends on, so tests below exercise the actual
    /// `schedule()` implementation end to end.
    struct FakePlanner {
        candidates: Vec<CompactionCandidate>,
    }

    #[tonic::async_trait]
    impl Planner for FakePlanner {
        async fn plan(&self) -> Result<Vec<CompactionCandidate>> {
            Ok(self.candidates.clone())
        }
    }

    fn scheduler_with(
        candidates: Vec<CompactionCandidate>,
        max_candidates_per_cycle: usize,
        max_per_tenant: usize,
    ) -> RoundRobinScheduler {
        let planner: Arc<dyn Planner> = Arc::new(FakePlanner { candidates });
        RoundRobinScheduler::new(planner, max_candidates_per_cycle, max_per_tenant)
    }

    #[tokio::test]
    async fn per_tenant_cap_limits_candidates_taken_from_each_tenant() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("alice", "p2"),
            make_candidate("alice", "p3"),
            make_candidate("bob", "p1"),
            make_candidate("bob", "p2"),
        ];

        let mut scheduler = scheduler_with(candidates, 10, 2);
        let result = scheduler.schedule(Instant::now()).await.unwrap();

        let alice_count = result.iter().filter(|c| c.tenant_id == "alice").count();
        let bob_count = result.iter().filter(|c| c.tenant_id == "bob").count();

        assert_eq!(alice_count, 2, "alice capped at 2, got {alice_count}");
        assert_eq!(bob_count, 2, "bob capped at 2, got {bob_count}");
        assert_eq!(result.len(), 4, "4 total (2 alice + 2 bob)");
    }

    #[tokio::test]
    async fn total_cap_limits_overall_output_size() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("alice", "p2"),
            make_candidate("bob", "p1"),
            make_candidate("bob", "p2"),
            make_candidate("carol", "p1"),
        ];

        let mut scheduler = scheduler_with(candidates, 3, 5);
        let result = scheduler.schedule(Instant::now()).await.unwrap();

        assert_eq!(result.len(), 3, "Total capped at 3, got {}", result.len());
    }

    #[tokio::test]
    async fn round_robin_advances_to_a_different_tenant_on_the_next_cycle() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("bob", "p1"),
            make_candidate("carol", "p1"),
        ];

        // Cycle 1 — should start from alice (first alphabetically).
        let mut scheduler = scheduler_with(candidates, 1, 1);
        let first = scheduler.schedule(Instant::now()).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].tenant_id, "alice");

        // Cycle 2 — should start from the tenant after the one served in cycle 1.
        let second = scheduler.schedule(Instant::now()).await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].tenant_id, "bob");
    }

    #[tokio::test]
    async fn reset_position_restarts_round_robin_from_the_first_tenant() {
        let candidates = vec![make_candidate("alice", "p1"), make_candidate("bob", "p1")];

        let mut scheduler = scheduler_with(candidates, 1, 1);

        let first = scheduler.schedule(Instant::now()).await.unwrap();
        assert_eq!(first[0].tenant_id, "alice");

        scheduler.reset_position();

        let after_reset = scheduler.schedule(Instant::now()).await.unwrap();
        assert_eq!(
            after_reset[0].tenant_id, "alice",
            "After reset, should serve the same first tenant again"
        );
    }

    #[tokio::test]
    async fn empty_candidates_returns_empty_result() {
        let mut scheduler = scheduler_with(vec![], 20, 5);
        let result = scheduler.schedule(Instant::now()).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn single_tenant_gets_all_candidates_up_to_per_tenant_cap() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("alice", "p2"),
            make_candidate("alice", "p3"),
            make_candidate("alice", "p4"),
        ];

        let mut scheduler = scheduler_with(candidates, 10, 3);
        let result = scheduler.schedule(Instant::now()).await.unwrap();

        assert_eq!(result.len(), 3, "Single tenant gets up to max_per_tenant=3");
    }

    #[tokio::test]
    async fn zero_caps_are_treated_as_unlimited_and_return_all_candidates() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("alice", "p2"),
            make_candidate("bob", "p1"),
            make_candidate("bob", "p2"),
            make_candidate("carol", "p1"),
        ];

        let mut scheduler = scheduler_with(candidates, 0, 0); // 0 = unlimited
        let result = scheduler.schedule(Instant::now()).await.unwrap();
        assert_eq!(result.len(), 5, "Unlimited caps return all candidates");
    }

    #[tokio::test]
    async fn every_tenant_is_served_at_least_once_across_multiple_cycles() {
        // 3 tenants, 2 partitions each, cap 1 per tenant per cycle, 3 total per cycle.
        // Since the fake planner returns the same full candidate list every
        // cycle (like the real planner re-scanning unfinished work), two
        // cycles of round-robin rotation must reach every tenant.
        let candidates: Vec<_> = ["alice", "bob", "carol"]
            .iter()
            .flat_map(|t| vec![make_candidate(t, "p1"), make_candidate(t, "p2")])
            .collect();

        let planner: Arc<dyn Planner> = Arc::new(FakePlanner { candidates });
        let mut scheduler = RoundRobinScheduler::new(planner, 3, 1);
        let mut seen: HashMap<String, usize> = HashMap::new();

        for _ in 0..2 {
            let batch = scheduler.schedule(Instant::now()).await.unwrap();
            for c in batch {
                *seen.entry(c.tenant_id).or_insert(0) += 1;
            }
        }

        for tenant in &["alice", "bob", "carol"] {
            assert!(
                *seen.get(*tenant).unwrap_or(&0) >= 1,
                "Tenant {tenant} should have been served at least once"
            );
        }
    }

    // --- Cooldown of recently-failed partitions (#1053) ---

    #[tokio::test]
    async fn a_failed_partition_is_not_rescheduled_while_it_cools_down() {
        let candidate = make_candidate("alice", "p1");
        let mut scheduler = scheduler_with(vec![candidate.clone()], 10, 5);
        let now = Instant::now();

        assert_eq!(scheduler.schedule(now).await.unwrap().len(), 1);

        scheduler.record_failure(&candidate, now);

        assert!(
            scheduler.schedule(now).await.unwrap().is_empty(),
            "a partition that just failed must not be re-selected"
        );
    }

    #[tokio::test]
    async fn a_cooled_down_partition_becomes_eligible_again_once_the_window_elapses() {
        let candidate = make_candidate("alice", "p1");
        let mut scheduler = scheduler_with(vec![candidate.clone()], 10, 5);
        let now = Instant::now();

        scheduler.record_failure(&candidate, now);
        assert!(
            scheduler
                .schedule(now + BASE_COOLDOWN - Duration::from_secs(1))
                .await
                .unwrap()
                .is_empty()
        );

        assert_eq!(
            scheduler.schedule(now + BASE_COOLDOWN).await.unwrap().len(),
            1,
            "the partition is retried after its window expires"
        );
    }

    #[tokio::test]
    async fn a_repeat_failure_doubles_the_suppression_window() {
        let candidate = make_candidate("alice", "p1");
        let mut scheduler = scheduler_with(vec![candidate.clone()], 10, 5);
        let now = Instant::now();

        scheduler.record_failure(&candidate, now);
        let retry_at = now + BASE_COOLDOWN;
        scheduler.record_failure(&candidate, retry_at);

        assert!(
            scheduler
                .schedule(retry_at + BASE_COOLDOWN)
                .await
                .unwrap()
                .is_empty(),
            "the second failure suppresses for longer than the base window"
        );
        assert_eq!(
            scheduler
                .schedule(retry_at + BASE_COOLDOWN * 2)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn a_success_clears_the_cooldown_immediately() {
        let candidate = make_candidate("alice", "p1");
        let mut scheduler = scheduler_with(vec![candidate.clone()], 10, 5);
        let now = Instant::now();

        scheduler.record_failure(&candidate, now);
        scheduler.record_success(&candidate);

        assert_eq!(
            scheduler.schedule(now).await.unwrap().len(),
            1,
            "a recovered partition is eligible again at once"
        );
    }

    /// The capacity complaint in #1053 is that poisoned partitions crowd out
    /// healthy ones. Filtering has to happen before the per-tenant cap, or a
    /// suppressed partition still consumes one of the tenant's slots.
    #[tokio::test]
    async fn a_cooling_partition_does_not_consume_a_per_tenant_slot() {
        let poisoned = make_candidate("alice", "p1");
        let candidates = vec![
            poisoned.clone(),
            make_candidate("alice", "p2"),
            make_candidate("alice", "p3"),
        ];
        let mut scheduler = scheduler_with(candidates, 10, 2);
        let now = Instant::now();

        scheduler.record_failure(&poisoned, now);
        let result = scheduler.schedule(now).await.unwrap();

        assert_eq!(
            result.len(),
            2,
            "the tenant's two slots go to healthy partitions, not the poisoned one"
        );
        assert!(
            result.iter().all(|c| c.partition_id != "p1"),
            "the suppressed partition must be absent"
        );
    }

    #[tokio::test]
    async fn suppression_is_isolated_to_the_failing_partition() {
        let poisoned = make_candidate("alice", "p1");
        let candidates = vec![poisoned.clone(), make_candidate("alice", "p2")];
        let mut scheduler = scheduler_with(candidates, 10, 5);
        let now = Instant::now();

        scheduler.record_failure(&poisoned, now);
        let result = scheduler.schedule(now).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].partition_id, "p2");
    }

    /// `partition_id` is an hours-since-epoch bucket shared across tables, so
    /// a failure in one table must not suppress the same hour elsewhere.
    #[tokio::test]
    async fn suppression_is_isolated_to_the_failing_table() {
        let poisoned = make_candidate_in("alice", "traces", "p1");
        let candidates = vec![poisoned.clone(), make_candidate_in("alice", "logs", "p1")];
        let mut scheduler = scheduler_with(candidates, 10, 5);
        let now = Instant::now();

        scheduler.record_failure(&poisoned, now);
        let result = scheduler.schedule(now).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].table_name, "logs");
    }

    #[tokio::test]
    async fn skipping_a_cooling_partition_is_counted() {
        let candidate = make_candidate("alice", "p1");
        let metrics = CompactionMetrics::new();
        let planner: Arc<dyn Planner> = Arc::new(FakePlanner {
            candidates: vec![candidate.clone()],
        });
        let mut scheduler = RoundRobinScheduler::new(planner, 10, 5).with_metrics(metrics.clone());
        let now = Instant::now();

        scheduler.record_failure(&candidate, now);
        scheduler.schedule(now).await.unwrap();

        assert_eq!(
            metrics.cooldown_partitions_skipped(),
            1,
            "operators need a signal that capacity is being withheld"
        );
    }

    #[tokio::test]
    async fn every_candidate_cooling_down_yields_an_empty_cycle() {
        let a = make_candidate("alice", "p1");
        let b = make_candidate("bob", "p1");
        let mut scheduler = scheduler_with(vec![a.clone(), b.clone()], 10, 5);
        let now = Instant::now();

        scheduler.record_failure(&a, now);
        scheduler.record_failure(&b, now);

        assert!(scheduler.schedule(now).await.unwrap().is_empty());
    }

    /// Hourly partitions age out of retention, so entries for partitions that
    /// no longer exist must not accumulate for the process lifetime.
    #[tokio::test]
    async fn long_expired_cooldown_entries_are_pruned() {
        let candidate = make_candidate("alice", "p1");
        let mut scheduler = scheduler_with(vec![candidate.clone()], 10, 5);
        let now = Instant::now();

        scheduler.record_failure(&candidate, now);
        assert_eq!(scheduler.tracked_cooldowns(), 1);

        // A much later cycle: the entry is far past its window.
        scheduler
            .schedule(now + Duration::from_secs(24 * 60 * 60))
            .await
            .unwrap();

        assert_eq!(
            scheduler.tracked_cooldowns(),
            0,
            "stale entries are pruned during scheduling"
        );
    }
}
