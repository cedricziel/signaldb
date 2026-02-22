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
//! use compactor::planner::CompactionPlanner;
//! use compactor::scheduler::RoundRobinScheduler;
//!
//! # async fn example() -> anyhow::Result<()> {
//! # let catalog_manager = todo!();
//! # let planner_config = todo!();
//! let planner = Arc::new(CompactionPlanner::new(catalog_manager, planner_config));
//! let mut scheduler = RoundRobinScheduler::new(planner, 20, 5);
//!
//! let candidates = scheduler.schedule().await?;
//! // candidates contains at most 20 entries, at most 5 from any one tenant
//! # Ok(())
//! # }
//! ```

use crate::planner::{CompactionCandidate, CompactionPlanner};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Fair scheduler that distributes compaction work across tenants using
/// a round-robin policy with per-tenant and total cycle limits.
pub struct RoundRobinScheduler {
    planner: Arc<CompactionPlanner>,
    /// Maximum total candidates returned per `schedule()` call.
    /// Set to 0 to disable the total cap.
    max_candidates_per_cycle: usize,
    /// Maximum candidates from any single tenant per `schedule()` call.
    /// Set to 0 to disable the per-tenant cap.
    max_per_tenant: usize,
    /// The `tenant_id` served last (used to advance round-robin position).
    last_tenant: Option<String>,
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
        planner: Arc<CompactionPlanner>,
        max_candidates_per_cycle: usize,
        max_per_tenant: usize,
    ) -> Self {
        Self {
            planner,
            max_candidates_per_cycle,
            max_per_tenant,
            last_tenant: None,
        }
    }

    /// Run a scheduling cycle: plan all candidates and distribute fairly.
    ///
    /// Returns at most `max_candidates_per_cycle` candidates with at most
    /// `max_per_tenant` from any single tenant. Tenants are served in
    /// round-robin order starting from after the last-served tenant.
    ///
    /// If `max_candidates_per_cycle == 0`, no total cap is applied.
    /// If `max_per_tenant == 0`, no per-tenant cap is applied.
    pub async fn schedule(&mut self) -> Result<Vec<CompactionCandidate>> {
        let all = self
            .planner
            .plan()
            .await
            .context("Compaction planner failed")?;

        if all.is_empty() {
            return Ok(vec![]);
        }

        // Group candidates by tenant_id, preserving planner order within each group
        let mut by_tenant: HashMap<String, Vec<CompactionCandidate>> = HashMap::new();
        for candidate in all {
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

    fn make_candidate(tenant: &str, partition: &str) -> CompactionCandidate {
        CompactionCandidate {
            tenant_id: tenant.to_string(),
            dataset_id: "ds".to_string(),
            table_name: "traces".to_string(),
            partition_id: partition.to_string(),
            stats: PartitionStats {
                file_count: 5,
                total_size_bytes: 1024 * 1024,
                avg_file_size_bytes: 204_800,
            },
        }
    }

    /// A mock-like scheduler for testing that seeds a fixed candidate list
    /// rather than calling the real planner.
    struct TestScheduler {
        candidates: Vec<CompactionCandidate>,
        max_per_cycle: usize,
        max_per_tenant: usize,
        last_tenant: Option<String>,
    }

    impl TestScheduler {
        fn new(
            candidates: Vec<CompactionCandidate>,
            max_per_cycle: usize,
            max_per_tenant: usize,
        ) -> Self {
            Self {
                candidates,
                max_per_cycle,
                max_per_tenant,
                last_tenant: None,
            }
        }

        fn schedule(&mut self) -> Vec<CompactionCandidate> {
            if self.candidates.is_empty() {
                return vec![];
            }

            let mut by_tenant: HashMap<String, Vec<CompactionCandidate>> = HashMap::new();
            for c in &self.candidates {
                by_tenant
                    .entry(c.tenant_id.clone())
                    .or_default()
                    .push(c.clone());
            }

            let mut tenant_keys: Vec<String> = by_tenant.keys().cloned().collect();
            tenant_keys.sort();

            let start = if let Some(last) = &self.last_tenant {
                tenant_keys
                    .iter()
                    .position(|k| k == last)
                    .map(|pos| (pos + 1) % tenant_keys.len())
                    .unwrap_or(0)
            } else {
                0
            };

            let total_cap = if self.max_per_cycle == 0 {
                usize::MAX
            } else {
                self.max_per_cycle
            };
            let per_tenant_cap = if self.max_per_tenant == 0 {
                usize::MAX
            } else {
                self.max_per_tenant
            };

            let mut result = Vec::new();
            let n = tenant_keys.len();
            let mut last_served: Option<String> = None;

            for i in 0..n {
                if result.len() >= total_cap {
                    break;
                }
                let tenant = &tenant_keys[(start + i) % n];
                if let Some(cands) = by_tenant.get(tenant) {
                    let take = cands
                        .len()
                        .min(per_tenant_cap)
                        .min(total_cap - result.len());
                    if take > 0 {
                        result.extend_from_slice(&cands[..take]);
                        last_served = Some(tenant.clone());
                    }
                }
            }
            if let Some(last) = last_served {
                self.last_tenant = Some(last);
            }
            result
        }

        fn reset(&mut self) {
            self.last_tenant = None;
        }
    }

    #[test]
    fn test_per_tenant_cap_limits_candidates() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("alice", "p2"),
            make_candidate("alice", "p3"),
            make_candidate("bob", "p1"),
            make_candidate("bob", "p2"),
        ];

        let mut sched = TestScheduler::new(candidates, 10, 2);
        let result = sched.schedule();

        // alice gets at most 2, bob gets at most 2 → total 4
        let alice_count = result.iter().filter(|c| c.tenant_id == "alice").count();
        let bob_count = result.iter().filter(|c| c.tenant_id == "bob").count();

        assert!(alice_count <= 2, "alice capped at 2, got {alice_count}");
        assert!(bob_count <= 2, "bob capped at 2, got {bob_count}");
        assert_eq!(result.len(), 4, "4 total (2 alice + 2 bob)");
    }

    #[test]
    fn test_total_cap_limits_output() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("alice", "p2"),
            make_candidate("bob", "p1"),
            make_candidate("bob", "p2"),
            make_candidate("carol", "p1"),
        ];

        let mut sched = TestScheduler::new(candidates, 3, 5);
        let result = sched.schedule();
        assert!(result.len() <= 3, "Total capped at 3, got {}", result.len());
    }

    #[test]
    fn test_round_robin_advances_position() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("bob", "p1"),
            make_candidate("carol", "p1"),
        ];

        // Cycle 1 — should start from alice (first alphabetically)
        let mut sched = TestScheduler::new(candidates.clone(), 1, 1);
        let first = sched.schedule();
        assert_eq!(first.len(), 1);
        let first_tenant = first[0].tenant_id.clone();

        // Cycle 2 — should start from tenant AFTER the one served in cycle 1
        let second = sched.schedule();
        assert_eq!(second.len(), 1);
        let second_tenant = second[0].tenant_id.clone();

        assert_ne!(
            first_tenant, second_tenant,
            "Round-robin should advance to a different tenant"
        );
    }

    #[test]
    fn test_reset_position_restarts_from_beginning() {
        let candidates = vec![make_candidate("alice", "p1"), make_candidate("bob", "p1")];

        let mut sched = TestScheduler::new(candidates, 1, 1);

        let first = sched.schedule();
        let first_tenant = first[0].tenant_id.clone();

        sched.reset();

        let after_reset = sched.schedule();
        let after_tenant = after_reset[0].tenant_id.clone();

        // After reset, should start from the same point as the very first call
        assert_eq!(
            first_tenant, after_tenant,
            "After reset, should serve the same first tenant again"
        );
    }

    #[test]
    fn test_empty_candidates_returns_empty() {
        let mut sched = TestScheduler::new(vec![], 20, 5);
        let result = sched.schedule();
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_tenant_gets_all_up_to_per_tenant_cap() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("alice", "p2"),
            make_candidate("alice", "p3"),
            make_candidate("alice", "p4"),
        ];

        let mut sched = TestScheduler::new(candidates, 10, 3);
        let result = sched.schedule();

        assert_eq!(result.len(), 3, "Single tenant gets up to max_per_tenant=3");
    }

    #[test]
    fn test_unlimited_caps_returns_all() {
        let candidates = vec![
            make_candidate("alice", "p1"),
            make_candidate("alice", "p2"),
            make_candidate("bob", "p1"),
            make_candidate("bob", "p2"),
            make_candidate("carol", "p1"),
        ];

        let mut sched = TestScheduler::new(candidates, 0, 0); // 0 = unlimited
        let result = sched.schedule();
        assert_eq!(result.len(), 5, "Unlimited caps return all candidates");
    }

    #[test]
    fn test_fairness_over_multiple_cycles() {
        // 3 tenants, 2 partitions each, cap 1 per tenant per cycle, 3 total per cycle
        let candidates: Vec<_> = ["alice", "bob", "carol"]
            .iter()
            .flat_map(|t| vec![make_candidate(t, "p1"), make_candidate(t, "p2")])
            .collect();

        let mut sched = TestScheduler::new(candidates, 3, 1);
        let mut seen: HashMap<String, usize> = HashMap::new();

        // Run 2 cycles (limited candidates since test scheduler doesn't re-plan)
        for _ in 0..2 {
            let batch = sched.schedule();
            for c in batch {
                *seen.entry(c.tenant_id).or_insert(0) += 1;
            }
        }

        // Each tenant should have been served at least once
        for tenant in &["alice", "bob", "carol"] {
            assert!(
                *seen.get(*tenant).unwrap_or(&0) >= 1,
                "Tenant {tenant} should have been served at least once"
            );
        }
    }
}
