//! Orphan Cleanup Metrics
//!
//! Thread-safe metrics for monitoring orphan file cleanup operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Reason why orphan cleanup was skipped for a table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Live file count estimate exceeded `max_live_files_threshold`
    LiveFilesThresholdExceeded,
}

impl SkipReason {
    /// Label used in metric output
    pub fn as_str(&self) -> &'static str {
        match self {
            SkipReason::LiveFilesThresholdExceeded => "live_files_threshold_exceeded",
        }
    }
}

/// Thread-safe metrics for tracking orphan cleanup operations.
///
/// Counters are cumulative across cleanup cycles. Create one instance per
/// `OrphanDetector` and keep it alive for the lifetime of the service.
#[derive(Debug, Clone)]
pub struct OrphanMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    /// Tables skipped because estimated live file count exceeded threshold
    cleanup_skipped_threshold: AtomicUsize,
    /// Total orphan candidates identified across all cycles
    candidates_identified: AtomicUsize,
    /// Total files deleted
    files_deleted: AtomicUsize,
    /// Total bytes freed
    bytes_freed: AtomicU64,
    /// Total deletion failures
    deletion_failures: AtomicUsize,
}

impl Default for OrphanMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl OrphanMetrics {
    /// Create a new metrics tracker
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                cleanup_skipped_threshold: AtomicUsize::new(0),
                candidates_identified: AtomicUsize::new(0),
                files_deleted: AtomicUsize::new(0),
                bytes_freed: AtomicU64::new(0),
                deletion_failures: AtomicUsize::new(0),
            }),
        }
    }

    /// Record a cleanup run skipped for the given reason
    pub fn record_cleanup_skipped(&self, reason: SkipReason) {
        match reason {
            SkipReason::LiveFilesThresholdExceeded => {
                self.inner
                    .cleanup_skipped_threshold
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record orphan candidates identified
    pub fn record_candidates_identified(&self, count: usize) {
        self.inner
            .candidates_identified
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record files deleted
    pub fn record_files_deleted(&self, count: usize) {
        self.inner.files_deleted.fetch_add(count, Ordering::Relaxed);
    }

    /// Record bytes freed
    pub fn record_bytes_freed(&self, bytes: u64) {
        self.inner.bytes_freed.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record deletion failures
    pub fn record_deletion_failures(&self, count: usize) {
        self.inner
            .deletion_failures
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Total tables skipped due to live files threshold
    ///
    /// Corresponds to metric: `compactor_orphan_cleanup_skipped_total{reason="live_files_threshold_exceeded"}`
    pub fn cleanup_skipped_threshold(&self) -> usize {
        self.inner.cleanup_skipped_threshold.load(Ordering::Relaxed)
    }

    /// Total orphan candidates identified
    pub fn candidates_identified(&self) -> usize {
        self.inner.candidates_identified.load(Ordering::Relaxed)
    }

    /// Total files deleted
    pub fn files_deleted(&self) -> usize {
        self.inner.files_deleted.load(Ordering::Relaxed)
    }

    /// Total bytes freed
    pub fn bytes_freed(&self) -> u64 {
        self.inner.bytes_freed.load(Ordering::Relaxed)
    }

    /// Total deletion failures
    pub fn deletion_failures(&self) -> usize {
        self.inner.deletion_failures.load(Ordering::Relaxed)
    }

    /// Log a summary of current metrics at info level
    pub fn log_summary(&self) {
        tracing::info!(
            cleanup_skipped_threshold = self.cleanup_skipped_threshold(),
            candidates_identified = self.candidates_identified(),
            files_deleted = self.files_deleted(),
            bytes_freed = self.bytes_freed(),
            deletion_failures = self.deletion_failures(),
            "Orphan cleanup metrics summary"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let m = OrphanMetrics::new();
        assert_eq!(m.cleanup_skipped_threshold(), 0);
        assert_eq!(m.candidates_identified(), 0);
        assert_eq!(m.files_deleted(), 0);
        assert_eq!(m.bytes_freed(), 0);
        assert_eq!(m.deletion_failures(), 0);
    }

    #[test]
    fn test_metrics_increment() {
        let m = OrphanMetrics::new();

        m.record_cleanup_skipped(SkipReason::LiveFilesThresholdExceeded);
        assert_eq!(m.cleanup_skipped_threshold(), 1);

        m.record_candidates_identified(42);
        assert_eq!(m.candidates_identified(), 42);

        m.record_files_deleted(10);
        assert_eq!(m.files_deleted(), 10);

        m.record_bytes_freed(1024 * 1024);
        assert_eq!(m.bytes_freed(), 1024 * 1024);

        m.record_deletion_failures(2);
        assert_eq!(m.deletion_failures(), 2);
    }

    #[test]
    fn test_skip_reason_label() {
        assert_eq!(
            SkipReason::LiveFilesThresholdExceeded.as_str(),
            "live_files_threshold_exceeded"
        );
    }

    #[test]
    fn test_metrics_are_cloneable_and_shared() {
        let m1 = OrphanMetrics::new();
        let m2 = m1.clone();

        m1.record_files_deleted(5);
        // Clone shares the same Arc inner — both should see the update
        assert_eq!(m2.files_deleted(), 5);
    }
}
