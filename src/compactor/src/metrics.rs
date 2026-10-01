//! Compaction metrics tracking
//!
//! Provides thread-safe metrics collection for compaction operations using atomic counters.

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Upper bound on the distinct `(tenant, dataset, table)` label sets tracked.
///
/// Tenants and datasets are operator-created, so the label space is not
/// closed and an unbounded map here would be a memory leak with a metrics
/// endpoint attached. Work beyond the cap is still counted, under
/// [`OVERFLOW_LABEL`], so the totals stay honest even when the breakdown
/// stops being specific.
pub const MAX_TRACKED_SCOPES: usize = 512;

/// Tenant id standing in for every scope past [`MAX_TRACKED_SCOPES`].
pub const OVERFLOW_LABEL: &str = "__overflow__";

/// The table a compaction job acted on: the label set for the per-job
/// counters.
///
/// Owned strings rather than borrowed: these live in the metrics map for the
/// process's lifetime, and the cardinality cap keeps the total bounded.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobScope {
    pub tenant_id: String,
    pub dataset_id: String,
    pub table_name: String,
}

impl JobScope {
    pub fn new(tenant_id: &str, dataset_id: &str, table_name: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_name: table_name.to_string(),
        }
    }

    /// The bucket every scope past the cap collapses into.
    fn overflow() -> Self {
        Self::new(OVERFLOW_LABEL, OVERFLOW_LABEL, OVERFLOW_LABEL)
    }
}

/// Job outcomes for one label set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScopeCounts {
    pub started: usize,
    pub succeeded: usize,
}

/// Thread-safe metrics for tracking compaction operations
#[derive(Debug, Clone)]
pub struct CompactionMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    jobs_started: AtomicUsize,
    jobs_succeeded: AtomicUsize,
    jobs_failed: AtomicUsize,
    conflicts_detected: AtomicUsize,
    retries_attempted: AtomicUsize,
    total_input_files: AtomicUsize,
    total_output_files: AtomicUsize,
    bytes_before_compaction: AtomicU64,
    bytes_after_compaction: AtomicU64,
    total_duration_ms: AtomicU64,
    deferred_open_partitions: AtomicUsize,
    unclassifiable_files: AtomicUsize,
    oversized_partitions_skipped: AtomicUsize,
    cooldown_partitions_skipped: AtomicUsize,
    stale_leases_expired: AtomicU64,
    /// Per-table job outcomes. The aggregates above stay authoritative for
    /// totals; this only says how they are distributed.
    by_scope: DashMap<JobScope, ScopeCountsInner>,
    /// Failures split further by error type, which is what separates a
    /// self-resolving conflict from a job that will never succeed.
    failures: DashMap<(JobScope, String), AtomicUsize>,
}

/// Interior-mutable counterpart of [`ScopeCounts`].
#[derive(Debug, Default)]
struct ScopeCountsInner {
    started: AtomicUsize,
    succeeded: AtomicUsize,
}

impl Default for CompactionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionMetrics {
    /// Create a new metrics tracker
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                jobs_started: AtomicUsize::new(0),
                jobs_succeeded: AtomicUsize::new(0),
                jobs_failed: AtomicUsize::new(0),
                conflicts_detected: AtomicUsize::new(0),
                retries_attempted: AtomicUsize::new(0),
                total_input_files: AtomicUsize::new(0),
                total_output_files: AtomicUsize::new(0),
                bytes_before_compaction: AtomicU64::new(0),
                bytes_after_compaction: AtomicU64::new(0),
                total_duration_ms: AtomicU64::new(0),
                deferred_open_partitions: AtomicUsize::new(0),
                unclassifiable_files: AtomicUsize::new(0),
                oversized_partitions_skipped: AtomicUsize::new(0),
                cooldown_partitions_skipped: AtomicUsize::new(0),
                stale_leases_expired: AtomicU64::new(0),
                by_scope: DashMap::new(),
                failures: DashMap::new(),
            }),
        }
    }

    /// Record data files skipped because their partition is still open.
    ///
    /// Deferral is expected steady-state behavior, not an error: the current
    /// hour is always deferred. The counter exists so an operator can tell
    /// "nothing to compact" apart from "everything is waiting on
    /// `partition_lateness`".
    pub fn record_deferred_open_partition_files(&self, count: usize) {
        self.inner
            .deferred_open_partitions
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Files skipped because their partition is still open.
    pub fn deferred_open_partition_files(&self) -> usize {
        self.inner.deferred_open_partitions.load(Ordering::Relaxed)
    }

    /// Record data files whose `timestamp_hour` partition could not be
    /// determined, and which are therefore never compacted.
    pub fn record_unclassifiable_files(&self, count: usize) {
        self.inner
            .unclassifiable_files
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Data files excluded from compaction as unclassifiable.
    pub fn unclassifiable_files(&self) -> usize {
        self.inner.unclassifiable_files.load(Ordering::Relaxed)
    }

    /// Record a partition the planner declined because its eligible inputs
    /// exceed `max_partition_input_mb`.
    ///
    /// Unlike a deferred-open partition, this one will not become a
    /// candidate on its own: it stays uncompacted until the cap is raised
    /// or the rewrite can handle it. The counter is the operator's only
    /// signal that compaction is declining work, so it must not be silent.
    pub fn record_oversized_partition_skipped(&self) {
        self.inner
            .oversized_partitions_skipped
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Partitions declined for exceeding the input-size cap.
    pub fn oversized_partitions_skipped(&self) -> usize {
        self.inner
            .oversized_partitions_skipped
            .load(Ordering::Relaxed)
    }

    /// Record a partition the scheduler withheld because its compaction
    /// recently failed and it is still inside its cooldown window.
    ///
    /// Unlike a conflict, this is capacity deliberately not spent. A steadily
    /// climbing value means some partition cannot be compacted at all and is
    /// being backed off rather than retried every tick (#1053).
    pub fn record_cooldown_partition_skipped(&self) {
        self.inner
            .cooldown_partitions_skipped
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Partitions skipped because they are cooling down after a failure.
    pub fn cooldown_partitions_skipped(&self) -> usize {
        self.inner
            .cooldown_partitions_skipped
            .load(Ordering::Relaxed)
    }

    /// The label set a *new* scope is counted under: itself, or the overflow
    /// bucket once the cap is reached.
    ///
    /// Only called on a miss, so the first [`MAX_TRACKED_SCOPES`] label sets
    /// seen keep their own series and later ones share the bucket. Which
    /// tables win that race does not matter operationally: a deployment with
    /// that many tables has a dashboard problem rather than a compaction
    /// problem, and the totals are unaffected either way.
    fn label_set_for_new_scope(&self, scope: &JobScope) -> JobScope {
        if self.inner.by_scope.len() >= MAX_TRACKED_SCOPES {
            tracing::warn!(
                tenant_id = %scope.tenant_id,
                dataset_id = %scope.dataset_id,
                table = %scope.table_name,
                max_tracked_scopes = MAX_TRACKED_SCOPES,
                "Compaction metrics label cardinality cap reached; counting this table under \
                 the overflow bucket"
            );
            return JobScope::overflow();
        }
        scope.clone()
    }

    /// Increment one of a scope's counters, allocating a label set only when
    /// the scope is seen for the first time.
    fn bump(&self, scope: &JobScope, pick: fn(&ScopeCountsInner) -> &AtomicUsize) {
        if let Some(counts) = self.inner.by_scope.get(scope) {
            pick(counts.value()).fetch_add(1, Ordering::Relaxed);
            return;
        }
        let label_set = self.label_set_for_new_scope(scope);
        pick(self.inner.by_scope.entry(label_set).or_default().value())
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Give `scope` a series if it has none, and return the label set it is
    /// counted under.
    fn ensure_scope(&self, scope: &JobScope) -> JobScope {
        if self.inner.by_scope.contains_key(scope) {
            return scope.clone();
        }
        let label_set = self.label_set_for_new_scope(scope);
        self.inner.by_scope.entry(label_set.clone()).or_default();
        label_set
    }

    /// Record the start of a compaction job on `scope`'s table
    pub fn record_job_start(&self, scope: &JobScope) {
        self.inner.jobs_started.fetch_add(1, Ordering::Relaxed);
        self.bump(scope, |counts| &counts.started);
    }

    /// Record a successful compaction job on `scope`'s table
    pub fn record_job_success(
        &self,
        scope: &JobScope,
        input_files: usize,
        output_files: usize,
        bytes_before: u64,
        bytes_after: u64,
        duration: Duration,
    ) {
        self.inner.jobs_succeeded.fetch_add(1, Ordering::Relaxed);
        self.bump(scope, |counts| &counts.succeeded);
        self.inner
            .total_input_files
            .fetch_add(input_files, Ordering::Relaxed);
        self.inner
            .total_output_files
            .fetch_add(output_files, Ordering::Relaxed);
        self.inner
            .bytes_before_compaction
            .fetch_add(bytes_before, Ordering::Relaxed);
        self.inner
            .bytes_after_compaction
            .fetch_add(bytes_after, Ordering::Relaxed);
        self.inner
            .total_duration_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    /// Record a failed compaction job on `scope`'s table.
    ///
    /// `error_type` is the retry classification (`conflict`,
    /// `resource_exhausted`, ...), which is what tells a self-resolving
    /// failure apart from one that will never succeed on its own.
    pub fn record_job_failure(&self, scope: &JobScope, error_type: &str) {
        self.inner.jobs_failed.fetch_add(1, Ordering::Relaxed);
        // Registered even when the failure is the scope's first event, so a
        // table that only ever fails still appears in the breakdown.
        let label_set = self.ensure_scope(scope);
        self.inner
            .failures
            .entry((label_set, error_type.to_string()))
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Job outcomes per table, for the labeled metric series.
    pub fn jobs_by_scope(&self) -> HashMap<JobScope, ScopeCounts> {
        self.inner
            .by_scope
            .iter()
            .map(|entry| {
                let counts = entry.value();
                (
                    entry.key().clone(),
                    ScopeCounts {
                        started: counts.started.load(Ordering::Relaxed),
                        succeeded: counts.succeeded.load(Ordering::Relaxed),
                    },
                )
            })
            .collect()
    }

    /// Failure counts per table and error type. A table's total failures
    /// are the sum over its error types.
    pub fn failures_by_scope(&self) -> HashMap<(JobScope, String), usize> {
        self.inner
            .failures
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect()
    }

    /// Record leases reclaimed from crashed instances by one expiry pass.
    ///
    /// This is the compactor's recovery path: a partition whose owner died
    /// stays unclaimable until its lease is expired here, so operators need
    /// to see the pass running (issue #1011).
    pub fn record_stale_leases_expired(&self, count: u64) {
        self.inner
            .stale_leases_expired
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record a detected conflict
    pub fn record_conflict(&self) {
        self.inner
            .conflicts_detected
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a retry attempt
    pub fn record_retry(&self) {
        self.inner.retries_attempted.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the number of jobs started
    pub fn jobs_started(&self) -> usize {
        self.inner.jobs_started.load(Ordering::Relaxed)
    }

    /// Get the number of jobs succeeded
    pub fn jobs_succeeded(&self) -> usize {
        self.inner.jobs_succeeded.load(Ordering::Relaxed)
    }

    /// Get the number of jobs failed
    pub fn jobs_failed(&self) -> usize {
        self.inner.jobs_failed.load(Ordering::Relaxed)
    }

    /// Get the number of conflicts detected
    pub fn conflicts_detected(&self) -> usize {
        self.inner.conflicts_detected.load(Ordering::Relaxed)
    }

    /// Get the number of retries attempted
    pub fn retries_attempted(&self) -> usize {
        self.inner.retries_attempted.load(Ordering::Relaxed)
    }

    /// Get the number of stale leases reclaimed from crashed instances
    pub fn stale_leases_expired(&self) -> u64 {
        self.inner.stale_leases_expired.load(Ordering::Relaxed)
    }

    /// Get the total input files processed
    pub fn total_input_files(&self) -> usize {
        self.inner.total_input_files.load(Ordering::Relaxed)
    }

    /// Get the total output files created
    pub fn total_output_files(&self) -> usize {
        self.inner.total_output_files.load(Ordering::Relaxed)
    }

    /// Get the total bytes before compaction
    pub fn bytes_before_compaction(&self) -> u64 {
        self.inner.bytes_before_compaction.load(Ordering::Relaxed)
    }

    /// Get the total bytes after compaction
    pub fn bytes_after_compaction(&self) -> u64 {
        self.inner.bytes_after_compaction.load(Ordering::Relaxed)
    }

    /// Get the cumulative duration of successful jobs in milliseconds
    pub fn total_duration_ms(&self) -> u64 {
        self.inner.total_duration_ms.load(Ordering::Relaxed)
    }

    /// Calculate the overall compression ratio
    pub fn compression_ratio(&self) -> f64 {
        let before = self.bytes_before_compaction() as f64;
        let after = self.bytes_after_compaction() as f64;

        if after > 0.0 { before / after } else { 0.0 }
    }

    /// Calculate the average job duration
    pub fn avg_duration_ms(&self) -> f64 {
        let total_ms = self.inner.total_duration_ms.load(Ordering::Relaxed) as f64;
        let jobs = self.jobs_succeeded() as f64;

        if jobs > 0.0 { total_ms / jobs } else { 0.0 }
    }

    /// Get a summary of all metrics
    pub fn summary(&self) -> MetricsSummary {
        MetricsSummary {
            jobs_started: self.jobs_started(),
            jobs_succeeded: self.jobs_succeeded(),
            jobs_failed: self.jobs_failed(),
            conflicts_detected: self.conflicts_detected(),
            retries_attempted: self.retries_attempted(),
            stale_leases_expired: self.stale_leases_expired(),
            total_input_files: self.total_input_files(),
            total_output_files: self.total_output_files(),
            bytes_before_compaction: self.bytes_before_compaction(),
            bytes_after_compaction: self.bytes_after_compaction(),
            compression_ratio: self.compression_ratio(),
            avg_duration_ms: self.avg_duration_ms(),
            deferred_open_partition_files: self.deferred_open_partition_files(),
            unclassifiable_files: self.unclassifiable_files(),
        }
    }

    /// Reset all metrics to zero
    #[cfg(test)]
    pub fn reset(&self) {
        self.inner.jobs_started.store(0, Ordering::Relaxed);
        self.inner.jobs_succeeded.store(0, Ordering::Relaxed);
        self.inner.jobs_failed.store(0, Ordering::Relaxed);
        self.inner.by_scope.clear();
        self.inner.failures.clear();
        self.inner.conflicts_detected.store(0, Ordering::Relaxed);
        self.inner.retries_attempted.store(0, Ordering::Relaxed);
        self.inner.stale_leases_expired.store(0, Ordering::Relaxed);
        self.inner.total_input_files.store(0, Ordering::Relaxed);
        self.inner.total_output_files.store(0, Ordering::Relaxed);
        self.inner
            .bytes_before_compaction
            .store(0, Ordering::Relaxed);
        self.inner
            .bytes_after_compaction
            .store(0, Ordering::Relaxed);
        self.inner.total_duration_ms.store(0, Ordering::Relaxed);
        self.inner
            .deferred_open_partitions
            .store(0, Ordering::Relaxed);
        self.inner.unclassifiable_files.store(0, Ordering::Relaxed);
        self.inner
            .cooldown_partitions_skipped
            .store(0, Ordering::Relaxed);
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub jobs_started: usize,
    pub jobs_succeeded: usize,
    pub jobs_failed: usize,
    pub conflicts_detected: usize,
    pub retries_attempted: usize,
    pub stale_leases_expired: u64,
    pub total_input_files: usize,
    pub total_output_files: usize,
    pub bytes_before_compaction: u64,
    pub bytes_after_compaction: u64,
    pub compression_ratio: f64,
    pub avg_duration_ms: f64,
    /// Files skipped because their partition is still open.
    pub deferred_open_partition_files: usize,
    /// Files excluded from compaction as unclassifiable.
    pub unclassifiable_files: usize,
}

impl MetricsSummary {
    /// Log the metrics summary
    pub fn log(&self) {
        tracing::info!("=== Compaction Metrics Summary ===");
        tracing::info!(
            "Jobs: {} started, {} succeeded, {} failed",
            self.jobs_started,
            self.jobs_succeeded,
            self.jobs_failed
        );
        tracing::info!(
            "Conflicts: {} detected, {} retries attempted",
            self.conflicts_detected,
            self.retries_attempted
        );
        tracing::info!("Stale leases expired: {}", self.stale_leases_expired);
        tracing::info!(
            "Files: {} input → {} output",
            self.total_input_files,
            self.total_output_files
        );
        tracing::info!(
            "Storage: {} MB → {} MB ({:.2}x compression)",
            crate::planner::format_mb(self.bytes_before_compaction),
            crate::planner::format_mb(self.bytes_after_compaction),
            self.compression_ratio
        );
        tracing::info!("Average job duration: {:.2}ms", self.avg_duration_ms);
        // Declined files are what separates "nothing to compact" from
        // "everything is waiting on partition_lateness" or "everything is
        // unclassifiable", so they belong in the same summary.
        tracing::info!(
            "Declined: {} files in still-open partitions, {} unclassifiable",
            self.deferred_open_partition_files,
            self.unclassifiable_files
        );
    }
}

/// Identifies one of the four lifecycle-cycle tasks `CompactorService` runs.
///
/// Exists so [`CycleHealth`] and the supervising loop in
/// [`crate::service::CompactorService::run_lifecycle_loop`] key panic
/// tracking by an exhaustively-matched value rather than a bare `&str`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cycle {
    Compaction,
    LeaseExpiry,
    Retention,
    OrphanCleanup,
}

impl Cycle {
    /// Human-readable name used in logs and metric labels.
    pub fn label(self) -> &'static str {
        match self {
            Cycle::Compaction => "compaction",
            Cycle::LeaseExpiry => "lease_expiry",
            Cycle::Retention => "retention",
            Cycle::OrphanCleanup => "orphan_cleanup",
        }
    }

    /// All cycles, for iterating when rendering a metric per cycle.
    pub fn all() -> [Cycle; 4] {
        [
            Cycle::Compaction,
            Cycle::LeaseExpiry,
            Cycle::Retention,
            Cycle::OrphanCleanup,
        ]
    }
}

#[derive(Debug, Default)]
struct CycleStatus {
    /// Cumulative panics recovered from this cycle's task over the process
    /// lifetime.
    panics: AtomicU64,
    /// Set while the cycle is in its post-panic backoff sleep; cleared as
    /// soon as it resumes retrying. A liveness probe reading this only sees
    /// a cycle as down for the bounded backoff window, not forever — a
    /// cycle that keeps panicking keeps re-entering this state instead of
    /// latching it, which is what makes it a meaningful "is it happening
    /// right now" signal rather than "did it ever happen".
    down: AtomicBool,
}

/// Health of the four lifecycle-cycle tasks that [`CompactorService`] runs.
///
/// [`crate::service::CompactorService::run_lifecycle_loop`] guards every
/// `Cycle::run()` call against panics: a panic is caught there instead of
/// ending that cycle's task for the rest of the process's life (issue
/// #1011 is precisely about a cycle silently going away permanently — a
/// caught-and-recovered panic must not become a second way for the same
/// thing to happen). This type is what makes a recovered panic visible on
/// `/health` and `/metrics` instead of only in the log.
///
/// [`CompactorService`]: crate::service::CompactorService
#[derive(Debug, Clone, Default)]
pub struct CycleHealth {
    inner: Arc<CycleHealthInner>,
}

#[derive(Debug, Default)]
struct CycleHealthInner {
    compaction: CycleStatus,
    lease_expiry: CycleStatus,
    retention: CycleStatus,
    orphan_cleanup: CycleStatus,
}

impl CycleHealth {
    pub fn new() -> Self {
        Self::default()
    }

    fn status(&self, cycle: Cycle) -> &CycleStatus {
        match cycle {
            Cycle::Compaction => &self.inner.compaction,
            Cycle::LeaseExpiry => &self.inner.lease_expiry,
            Cycle::Retention => &self.inner.retention,
            Cycle::OrphanCleanup => &self.inner.orphan_cleanup,
        }
    }

    /// Record a caught panic and mark the cycle down for the backoff window
    /// the caller is about to sleep through.
    pub fn record_panic(&self, cycle: Cycle) {
        let status = self.status(cycle);
        status.panics.fetch_add(1, Ordering::Relaxed);
        status.down.store(true, Ordering::Relaxed);
    }

    /// Clear the down flag once the cycle resumes retrying after backoff.
    pub fn record_retrying(&self, cycle: Cycle) {
        self.status(cycle).down.store(false, Ordering::Relaxed);
    }

    /// Panics recovered from this cycle over the process lifetime.
    pub fn panics(&self, cycle: Cycle) -> u64 {
        self.status(cycle).panics.load(Ordering::Relaxed)
    }

    /// Whether this cycle is currently in its post-panic backoff sleep.
    pub fn is_down(&self, cycle: Cycle) -> bool {
        self.status(cycle).down.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        let metrics = CompactionMetrics::new();

        assert_eq!(metrics.jobs_started(), 0);
        assert_eq!(metrics.jobs_succeeded(), 0);
        assert_eq!(metrics.jobs_failed(), 0);
        assert_eq!(metrics.conflicts_detected(), 0);
        assert_eq!(metrics.retries_attempted(), 0);
        assert_eq!(metrics.total_input_files(), 0);
        assert_eq!(metrics.total_output_files(), 0);
        assert_eq!(metrics.bytes_before_compaction(), 0);
        assert_eq!(metrics.bytes_after_compaction(), 0);
    }

    /// Label set for tests that only care about the aggregate counters.
    fn any_scope() -> JobScope {
        JobScope::new("test-tenant", "test-dataset", "traces")
    }

    /// A single global failure counter cannot answer the first question an
    /// operator asks — *what* is failing. On hive 12 of 140 jobs failed and
    /// finding out they were all one table took a log dig.
    #[test]
    fn job_outcomes_are_counted_per_table() {
        let metrics = CompactionMetrics::new();
        let profiles = JobScope::new("_system", "_monitoring", "profiles");
        let traces = JobScope::new("_system", "_monitoring", "traces");

        metrics.record_job_start(&profiles);
        metrics.record_job_failure(&profiles, "resource_exhausted");
        metrics.record_job_start(&traces);
        metrics.record_job_success(
            &traces,
            10,
            1,
            2048,
            1024,
            std::time::Duration::from_millis(5),
        );

        let by_scope = metrics.jobs_by_scope();
        assert_eq!(by_scope.get(&profiles).map(|c| c.succeeded), Some(0));
        assert_eq!(by_scope.get(&traces).map(|c| c.succeeded), Some(1));
        assert_eq!(
            metrics
                .failures_by_scope()
                .get(&(profiles, "resource_exhausted".to_string())),
            Some(&1),
            "the failing table must be identifiable from the metric alone"
        );
        assert!(
            !metrics
                .failures_by_scope()
                .keys()
                .any(|(scope, _)| scope == &traces),
            "a table that never failed must not appear in the failure breakdown"
        );

        // The aggregate stays intact: /status and the compact_status tool
        // read it, and it is what makes a per-table count legible as a share.
        assert_eq!(metrics.jobs_failed(), 1);
        assert_eq!(metrics.jobs_succeeded(), 1);
        assert_eq!(metrics.jobs_started(), 2);
    }

    /// The table says what is failing; the error type says whether it is
    /// worth paging over. A commit conflict resolves itself, a terminal
    /// resource error does not.
    #[test]
    fn failures_are_counted_per_error_type() {
        let metrics = CompactionMetrics::new();
        let profiles = JobScope::new("_system", "_monitoring", "profiles");

        metrics.record_job_failure(&profiles, "resource_exhausted");
        metrics.record_job_failure(&profiles, "resource_exhausted");
        metrics.record_job_failure(&profiles, "conflict");

        let failures = metrics.failures_by_scope();
        assert_eq!(
            failures.get(&(profiles.clone(), "resource_exhausted".to_string())),
            Some(&2)
        );
        assert_eq!(failures.get(&(profiles, "conflict".to_string())), Some(&1));
    }

    /// Label sets are operator-created (tenants and datasets), so the series
    /// count has to be bounded — an unbounded metric map is a memory leak
    /// with a metrics endpoint attached.
    #[test]
    fn scope_cardinality_is_capped() {
        let metrics = CompactionMetrics::new();
        for i in 0..(MAX_TRACKED_SCOPES + 50) {
            metrics.record_job_failure(&JobScope::new(&format!("tenant-{i}"), "d", "traces"), "x");
        }

        let by_scope = metrics.jobs_by_scope();
        assert!(
            by_scope.len() <= MAX_TRACKED_SCOPES + 1,
            "tracked scopes ({}) must stay within the cap plus the overflow bucket",
            by_scope.len()
        );
        assert!(
            by_scope
                .keys()
                .any(|scope| scope.tenant_id == OVERFLOW_LABEL),
            "work beyond the cap must land in the overflow bucket, not vanish"
        );
        // Nothing is lost from the aggregate, whatever the cap does.
        assert_eq!(metrics.jobs_failed(), MAX_TRACKED_SCOPES + 50);
    }

    #[test]
    fn test_record_job_start() {
        let metrics = CompactionMetrics::new();

        metrics.record_job_start(&any_scope());
        assert_eq!(metrics.jobs_started(), 1);

        metrics.record_job_start(&any_scope());
        assert_eq!(metrics.jobs_started(), 2);
    }

    #[test]
    fn test_record_job_success() {
        let metrics = CompactionMetrics::new();

        metrics.record_job_success(
            &any_scope(),
            15,                      // input files
            2,                       // output files
            30 * 1024 * 1024,        // 30 MB before
            128 * 1024 * 1024,       // 128 MB after
            Duration::from_secs(10), // 10 seconds
        );

        assert_eq!(metrics.jobs_succeeded(), 1);
        assert_eq!(metrics.total_input_files(), 15);
        assert_eq!(metrics.total_output_files(), 2);
        assert_eq!(metrics.bytes_before_compaction(), 30 * 1024 * 1024);
        assert_eq!(metrics.bytes_after_compaction(), 128 * 1024 * 1024);
    }

    #[test]
    fn test_record_job_failure() {
        let metrics = CompactionMetrics::new();

        metrics.record_job_failure(&any_scope(), "conflict");
        assert_eq!(metrics.jobs_failed(), 1);

        metrics.record_job_failure(&any_scope(), "conflict");
        assert_eq!(metrics.jobs_failed(), 2);
    }

    #[test]
    fn test_record_conflict() {
        let metrics = CompactionMetrics::new();

        metrics.record_conflict();
        assert_eq!(metrics.conflicts_detected(), 1);
    }

    #[test]
    fn test_record_retry() {
        let metrics = CompactionMetrics::new();

        metrics.record_retry();
        assert_eq!(metrics.retries_attempted(), 1);
    }

    #[test]
    fn test_compression_ratio() {
        let metrics = CompactionMetrics::new();

        // No data yet
        assert_eq!(metrics.compression_ratio(), 0.0);

        // Add some data: 30MB -> 15MB = 2x compression
        metrics.record_job_success(
            &any_scope(),
            10,
            2,
            30 * 1024 * 1024,
            15 * 1024 * 1024,
            Duration::from_secs(5),
        );

        assert!((metrics.compression_ratio() - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_avg_duration() {
        let metrics = CompactionMetrics::new();

        // No jobs yet
        assert_eq!(metrics.avg_duration_ms(), 0.0);

        // Add two jobs with 10s and 20s durations
        metrics.record_job_success(&any_scope(), 10, 2, 1024, 512, Duration::from_secs(10));
        metrics.record_job_success(&any_scope(), 10, 2, 1024, 512, Duration::from_secs(20));

        // Average should be 15 seconds = 15000 ms
        assert!((metrics.avg_duration_ms() - 15000.0).abs() < 1.0);
    }

    #[test]
    fn test_metrics_summary() {
        let metrics = CompactionMetrics::new();

        metrics.record_job_start(&any_scope());
        metrics.record_job_success(
            &any_scope(),
            15,
            2,
            30 * 1024 * 1024,
            15 * 1024 * 1024,
            Duration::from_secs(10),
        );
        metrics.record_conflict();
        metrics.record_retry();

        let summary = metrics.summary();

        assert_eq!(summary.jobs_started, 1);
        assert_eq!(summary.jobs_succeeded, 1);
        assert_eq!(summary.jobs_failed, 0);
        assert_eq!(summary.conflicts_detected, 1);
        assert_eq!(summary.retries_attempted, 1);
        assert_eq!(summary.total_input_files, 15);
        assert_eq!(summary.total_output_files, 2);
        assert!((summary.compression_ratio - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_metrics_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(CompactionMetrics::new());
        let mut handles = vec![];

        // Spawn 10 threads, each recording 100 job starts
        for _ in 0..10 {
            let metrics_clone = Arc::clone(&metrics);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    metrics_clone.record_job_start(&any_scope());
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 1000 total jobs started
        assert_eq!(metrics.jobs_started(), 1000);
    }
}
