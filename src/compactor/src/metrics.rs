//! Compaction metrics tracking
//!
//! Provides thread-safe metrics collection for compaction operations using atomic counters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

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
            }),
        }
    }

    /// Record the start of a compaction job
    pub fn record_job_start(&self) {
        self.inner.jobs_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful compaction job
    pub fn record_job_success(
        &self,
        input_files: usize,
        output_files: usize,
        bytes_before: u64,
        bytes_after: u64,
        duration: Duration,
    ) {
        self.inner.jobs_succeeded.fetch_add(1, Ordering::Relaxed);
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

    /// Record a failed compaction job
    pub fn record_job_failure(&self) {
        self.inner.jobs_failed.fetch_add(1, Ordering::Relaxed);
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
            total_input_files: self.total_input_files(),
            total_output_files: self.total_output_files(),
            bytes_before_compaction: self.bytes_before_compaction(),
            bytes_after_compaction: self.bytes_after_compaction(),
            compression_ratio: self.compression_ratio(),
            avg_duration_ms: self.avg_duration_ms(),
        }
    }

    /// Reset all metrics to zero
    #[cfg(test)]
    pub fn reset(&self) {
        self.inner.jobs_started.store(0, Ordering::Relaxed);
        self.inner.jobs_succeeded.store(0, Ordering::Relaxed);
        self.inner.jobs_failed.store(0, Ordering::Relaxed);
        self.inner.conflicts_detected.store(0, Ordering::Relaxed);
        self.inner.retries_attempted.store(0, Ordering::Relaxed);
        self.inner.total_input_files.store(0, Ordering::Relaxed);
        self.inner.total_output_files.store(0, Ordering::Relaxed);
        self.inner
            .bytes_before_compaction
            .store(0, Ordering::Relaxed);
        self.inner
            .bytes_after_compaction
            .store(0, Ordering::Relaxed);
        self.inner.total_duration_ms.store(0, Ordering::Relaxed);
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
    pub total_input_files: usize,
    pub total_output_files: usize,
    pub bytes_before_compaction: u64,
    pub bytes_after_compaction: u64,
    pub compression_ratio: f64,
    pub avg_duration_ms: f64,
}

impl MetricsSummary {
    /// Format bytes as MB with 2 decimal places
    fn format_mb(bytes: u64) -> String {
        format!("{:.2}", bytes as f64 / (1024.0 * 1024.0))
    }

    /// Log the metrics summary
    pub fn log(&self) {
        log::info!("=== Compaction Metrics Summary ===");
        log::info!(
            "Jobs: {} started, {} succeeded, {} failed",
            self.jobs_started,
            self.jobs_succeeded,
            self.jobs_failed
        );
        log::info!(
            "Conflicts: {} detected, {} retries attempted",
            self.conflicts_detected,
            self.retries_attempted
        );
        log::info!(
            "Files: {} input → {} output",
            self.total_input_files,
            self.total_output_files
        );
        log::info!(
            "Storage: {} MB → {} MB ({:.2}x compression)",
            Self::format_mb(self.bytes_before_compaction),
            Self::format_mb(self.bytes_after_compaction),
            self.compression_ratio
        );
        log::info!("Average job duration: {:.2}ms", self.avg_duration_ms);
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

    #[test]
    fn test_record_job_start() {
        let metrics = CompactionMetrics::new();

        metrics.record_job_start();
        assert_eq!(metrics.jobs_started(), 1);

        metrics.record_job_start();
        assert_eq!(metrics.jobs_started(), 2);
    }

    #[test]
    fn test_record_job_success() {
        let metrics = CompactionMetrics::new();

        metrics.record_job_success(
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

        metrics.record_job_failure();
        assert_eq!(metrics.jobs_failed(), 1);

        metrics.record_job_failure();
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
        metrics.record_job_success(10, 2, 1024, 512, Duration::from_secs(10));
        metrics.record_job_success(10, 2, 1024, 512, Duration::from_secs(20));

        // Average should be 15 seconds = 15000 ms
        assert!((metrics.avg_duration_ms() - 15000.0).abs() < 1.0);
    }

    #[test]
    fn test_metrics_summary() {
        let metrics = CompactionMetrics::new();

        metrics.record_job_start();
        metrics.record_job_success(
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
                    metrics_clone.record_job_start();
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
