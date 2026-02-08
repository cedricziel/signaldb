//! Retention Enforcement Metrics
//!
//! Thread-safe metrics for monitoring retention enforcement operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Thread-safe metrics for tracking retention enforcement operations
#[derive(Debug, Clone)]
pub struct RetentionMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    /// Number of retention cutoffs computed
    cutoffs_computed: AtomicUsize,
    /// Total number of partitions evaluated for retention
    partitions_evaluated: AtomicUsize,
    /// Total number of partitions dropped
    partitions_dropped: AtomicUsize,
    /// Total number of snapshots expired
    snapshots_expired: AtomicUsize,
    /// Total bytes reclaimed
    bytes_reclaimed: AtomicU64,
    /// Total duration in milliseconds
    total_duration_ms: AtomicU64,
}

impl Default for RetentionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl RetentionMetrics {
    /// Create a new metrics tracker
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                cutoffs_computed: AtomicUsize::new(0),
                partitions_evaluated: AtomicUsize::new(0),
                partitions_dropped: AtomicUsize::new(0),
                snapshots_expired: AtomicUsize::new(0),
                bytes_reclaimed: AtomicU64::new(0),
                total_duration_ms: AtomicU64::new(0),
            }),
        }
    }

    /// Record a retention cutoff computation
    pub fn record_cutoff_computed(&self) {
        self.inner.cutoffs_computed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record partitions evaluated
    pub fn record_partitions_evaluated(&self, count: usize) {
        self.inner
            .partitions_evaluated
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record partitions dropped
    pub fn record_partitions_dropped(&self, count: usize) {
        self.inner
            .partitions_dropped
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record snapshots expired
    pub fn record_snapshots_expired(&self, count: usize) {
        self.inner
            .snapshots_expired
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record bytes reclaimed
    pub fn record_bytes_reclaimed(&self, bytes: u64) {
        self.inner
            .bytes_reclaimed
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record operation duration
    pub fn record_duration_ms(&self, duration_ms: u64) {
        self.inner
            .total_duration_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    /// Get total cutoffs computed
    pub fn cutoffs_computed(&self) -> usize {
        self.inner.cutoffs_computed.load(Ordering::Relaxed)
    }

    /// Get total partitions evaluated
    pub fn partitions_evaluated(&self) -> usize {
        self.inner.partitions_evaluated.load(Ordering::Relaxed)
    }

    /// Get total partitions dropped
    pub fn partitions_dropped(&self) -> usize {
        self.inner.partitions_dropped.load(Ordering::Relaxed)
    }

    /// Get total snapshots expired
    pub fn snapshots_expired(&self) -> usize {
        self.inner.snapshots_expired.load(Ordering::Relaxed)
    }

    /// Get total bytes reclaimed
    pub fn bytes_reclaimed(&self) -> u64 {
        self.inner.bytes_reclaimed.load(Ordering::Relaxed)
    }

    /// Get total duration in milliseconds
    pub fn total_duration_ms(&self) -> u64 {
        self.inner.total_duration_ms.load(Ordering::Relaxed)
    }

    /// Create mock metrics for testing
    #[cfg(test)]
    pub fn new_mock() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = RetentionMetrics::new();
        assert_eq!(metrics.cutoffs_computed(), 0);
        assert_eq!(metrics.partitions_evaluated(), 0);
        assert_eq!(metrics.partitions_dropped(), 0);
    }

    #[test]
    fn test_metrics_increment() {
        let metrics = RetentionMetrics::new();

        metrics.record_cutoff_computed();
        assert_eq!(metrics.cutoffs_computed(), 1);

        metrics.record_partitions_evaluated(10);
        assert_eq!(metrics.partitions_evaluated(), 10);

        metrics.record_partitions_dropped(3);
        assert_eq!(metrics.partitions_dropped(), 3);

        metrics.record_snapshots_expired(5);
        assert_eq!(metrics.snapshots_expired(), 5);

        metrics.record_bytes_reclaimed(1024);
        assert_eq!(metrics.bytes_reclaimed(), 1024);

        metrics.record_duration_ms(1500);
        assert_eq!(metrics.total_duration_ms(), 1500);
    }

    #[test]
    fn test_mock_metrics() {
        let metrics = RetentionMetrics::new_mock();
        metrics.record_cutoff_computed();
        assert_eq!(metrics.cutoffs_computed(), 1);
    }
}
