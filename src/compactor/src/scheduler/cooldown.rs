//! Cooldown tracking for partitions whose compaction keeps failing.
//!
//! The planner is stateless: it re-evaluates every partition from file count
//! and size on each tick, so a partition that cannot commit — a schema error,
//! a corrupt input file, a rewrite that always runs out of memory — stays
//! eligible forever. It is re-selected every `tick_interval`, fails again, and
//! repeats, burning compaction capacity that genuinely compactable partitions
//! are queued behind (#1053).
//!
//! This tracker is the scheduler's memory of those failures. A failed
//! partition is suppressed for [`BASE_COOLDOWN`], doubling on each consecutive
//! failure up to [`MAX_COOLDOWN`], and a success clears the entry outright so
//! a partition that recovers is not penalised for its history.
//!
//! Conflicts are deliberately *not* failures here: an Iceberg CAS conflict
//! means another actor committed first, which is contention to retry, not a
//! partition that is stuck.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::planner::CompactionCandidate;

/// Suppression window applied after a partition's first failure.
pub const BASE_COOLDOWN: Duration = Duration::from_secs(15 * 60);

/// Upper bound on the suppression window after repeated failures.
pub const MAX_COOLDOWN: Duration = Duration::from_secs(6 * 60 * 60);

/// How long an expired entry is kept before pruning.
///
/// The entry has to outlive its own window, otherwise the escalation count is
/// lost the moment the partition becomes eligible again and a permanently
/// broken partition would never get past the base window.
const RETAIN_AFTER_EXPIRY: Duration = MAX_COOLDOWN;

/// Identity of a compaction partition.
///
/// `partition_id` alone is an hours-since-epoch bucket, which repeats across
/// every table of every tenant, so the full tuple is the only correct key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub tenant_id: String,
    pub dataset_id: String,
    pub table_name: String,
    pub partition_id: String,
}

impl PartitionKey {
    /// Build the key identifying `candidate`'s partition.
    pub fn from_candidate(candidate: &CompactionCandidate) -> Self {
        Self {
            tenant_id: candidate.tenant_id.clone(),
            dataset_id: candidate.dataset_id.clone(),
            table_name: candidate.table_name.clone(),
            partition_id: candidate.partition_id.clone(),
        }
    }
}

/// An active suppression, as observed at a given instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cooling {
    /// Time left before the partition becomes eligible again.
    pub remaining: Duration,
    /// Consecutive failures recorded for this partition.
    pub consecutive_failures: u32,
}

#[derive(Debug, Clone, Copy)]
struct CooldownEntry {
    until: Instant,
    consecutive_failures: u32,
}

/// Tracks which partitions are currently suppressed and for how long.
#[derive(Debug, Default)]
pub struct CooldownTracker {
    entries: HashMap<PartitionKey, CooldownEntry>,
}

impl CooldownTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the active suppression for `key`, if any.
    pub fn cooling(&self, key: &PartitionKey, now: Instant) -> Option<Cooling> {
        self.entries.get(key).and_then(|entry| {
            let remaining = entry.until.checked_duration_since(now)?;
            (!remaining.is_zero()).then_some(Cooling {
                remaining,
                consecutive_failures: entry.consecutive_failures,
            })
        })
    }

    /// Record a failed compaction attempt and arm (or escalate) the window.
    ///
    /// Returns the suppression now in force.
    pub fn record_failure(&mut self, key: PartitionKey, now: Instant) -> Cooling {
        let entry = self.entries.entry(key).or_insert(CooldownEntry {
            until: now,
            consecutive_failures: 0,
        });
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);

        let window = backoff_window(entry.consecutive_failures);
        entry.until = now + window;

        Cooling {
            remaining: window,
            consecutive_failures: entry.consecutive_failures,
        }
    }

    /// Clear any suppression for `key` after a successful compaction, so the
    /// next failure starts again from [`BASE_COOLDOWN`].
    pub fn record_success(&mut self, key: &PartitionKey) {
        self.entries.remove(key);
    }

    /// Drop entries whose window expired long enough ago that their
    /// escalation history is no longer worth keeping.
    ///
    /// Partitions are hourly and age out of retention, so without this the
    /// tracker would grow without bound over the process lifetime.
    pub fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|_, entry| now < entry.until + RETAIN_AFTER_EXPIRY);
    }

    /// Number of tracked partitions, expired entries included.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether any partition is tracked.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Exponential backoff window for the n-th consecutive failure, capped at
/// [`MAX_COOLDOWN`].
fn backoff_window(consecutive_failures: u32) -> Duration {
    let doublings = consecutive_failures.saturating_sub(1).min(u32::BITS - 1);
    BASE_COOLDOWN
        .checked_mul(1u32 << doublings)
        .unwrap_or(MAX_COOLDOWN)
        .min(MAX_COOLDOWN)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(table: &str, partition: &str) -> PartitionKey {
        PartitionKey {
            tenant_id: "acme".to_string(),
            dataset_id: "prod".to_string(),
            table_name: table.to_string(),
            partition_id: partition.to_string(),
        }
    }

    #[test]
    fn a_failure_suppresses_the_partition_for_the_base_window() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();

        let cooling = tracker.record_failure(key("traces", "480000"), now);
        assert_eq!(cooling.consecutive_failures, 1);
        assert_eq!(cooling.remaining, BASE_COOLDOWN);

        assert!(tracker.cooling(&key("traces", "480000"), now).is_some());
    }

    #[test]
    fn an_untracked_partition_is_never_cooling_down() {
        let tracker = CooldownTracker::new();
        assert!(
            tracker
                .cooling(&key("traces", "480000"), Instant::now())
                .is_none()
        );
    }

    #[test]
    fn suppression_ends_exactly_when_the_window_elapses() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();
        tracker.record_failure(key("traces", "480000"), now);

        let just_before = now + BASE_COOLDOWN - Duration::from_secs(1);
        assert!(
            tracker
                .cooling(&key("traces", "480000"), just_before)
                .is_some(),
            "still suppressed one second before the window ends"
        );

        assert!(
            tracker
                .cooling(&key("traces", "480000"), now + BASE_COOLDOWN)
                .is_none(),
            "eligible again at the boundary"
        );
    }

    #[test]
    fn consecutive_failures_double_the_window() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();

        assert_eq!(
            tracker
                .record_failure(key("traces", "480000"), now)
                .remaining,
            BASE_COOLDOWN
        );
        assert_eq!(
            tracker
                .record_failure(key("traces", "480000"), now)
                .remaining,
            BASE_COOLDOWN * 2
        );
        assert_eq!(
            tracker
                .record_failure(key("traces", "480000"), now)
                .remaining,
            BASE_COOLDOWN * 4
        );
    }

    #[test]
    fn the_window_never_exceeds_the_cap() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();

        let mut last = Duration::ZERO;
        for _ in 0..64 {
            last = tracker
                .record_failure(key("traces", "480000"), now)
                .remaining;
        }
        assert_eq!(last, MAX_COOLDOWN, "escalation saturates at the cap");
    }

    #[test]
    fn success_clears_the_suppression_immediately() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();
        tracker.record_failure(key("traces", "480000"), now);

        tracker.record_success(&key("traces", "480000"));

        assert!(tracker.cooling(&key("traces", "480000"), now).is_none());
        assert!(tracker.is_empty(), "the entry is dropped, not just expired");
    }

    #[test]
    fn success_resets_escalation_so_the_next_failure_starts_from_the_base_window() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();

        tracker.record_failure(key("traces", "480000"), now);
        tracker.record_failure(key("traces", "480000"), now);
        tracker.record_success(&key("traces", "480000"));

        let cooling = tracker.record_failure(key("traces", "480000"), now);
        assert_eq!(cooling.consecutive_failures, 1);
        assert_eq!(cooling.remaining, BASE_COOLDOWN);
    }

    #[test]
    fn partitions_of_the_same_table_are_tracked_independently() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();

        tracker.record_failure(key("traces", "480000"), now);

        assert!(tracker.cooling(&key("traces", "480001"), now).is_none());
    }

    /// `partition_id` is an hours-since-epoch bucket and repeats across every
    /// table, so a table-blind key would suppress unrelated healthy work.
    #[test]
    fn the_same_partition_id_in_another_table_is_not_suppressed() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();

        tracker.record_failure(key("traces", "480000"), now);

        assert!(tracker.cooling(&key("logs", "480000"), now).is_none());
    }

    #[test]
    fn pruning_drops_long_expired_entries_but_keeps_recent_history() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();
        tracker.record_failure(key("traces", "480000"), now);
        let expiry = now + BASE_COOLDOWN;

        tracker.prune(expiry + RETAIN_AFTER_EXPIRY - Duration::from_secs(1));
        assert_eq!(tracker.len(), 1, "escalation history survives the window");

        tracker.prune(expiry + RETAIN_AFTER_EXPIRY);
        assert!(tracker.is_empty(), "stale entries do not accumulate");
    }

    #[test]
    fn pruning_never_drops_an_active_suppression() {
        let mut tracker = CooldownTracker::new();
        let now = Instant::now();
        tracker.record_failure(key("traces", "480000"), now);

        tracker.prune(now + Duration::from_secs(60));

        assert!(
            tracker
                .cooling(&key("traces", "480000"), now + Duration::from_secs(60))
                .is_some()
        );
    }
}
