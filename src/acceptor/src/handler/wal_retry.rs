//! # WAL Retry Consumer
//!
//! Background consumer that replays unprocessed acceptor WAL entries.
//!
//! The hot path (OTLP/Prometheus handlers) appends to the WAL, forwards the
//! batch to a writer via Flight, and marks the entry processed on success.
//! When the forward fails (writer down, slow, restarting, catalog
//! unreachable), the entry stays unprocessed in the WAL. Without a retry
//! consumer those entries were never re-forwarded and their segments never
//! reclaimed.
//!
//! `WalRetryConsumer` closes that gap: on an interval it scans every WAL
//! managed by the [`WalManager`], re-forwards unprocessed entries older than
//! a minimum age (so it does not race in-flight hot-path forwards), and
//! marks them processed on success so segment cleanup can reclaim disk.
//!
//! Entries whose payload cannot be read or deserialized are poison: they can
//! never be forwarded, so retrying them forever just burns each pass and
//! keeps their segments pinned. After [`MAX_ENTRY_FAILURES`] consecutive
//! poison failures an entry is dead-lettered — its raw payload is preserved
//! under `<wal_dir>/dead-letter/` and the entry is marked processed. Forward
//! failures never count toward dead-lettering: they mean the writer is
//! unavailable, not that the entry is bad.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::flight::transport::InMemoryFlightTransport;
use common::wal::{Wal, WalOperation, bytes_to_record_batch};
use uuid::Uuid;

use super::WalManager;
use super::forward::forward_batch_to_writer;

/// How often the retry consumer scans for unprocessed entries.
pub const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Minimum age an unprocessed entry must reach before it is retried.
///
/// This keeps the consumer from racing the hot path, which forwards the
/// entry inline right after appending it.
pub const DEFAULT_MIN_ENTRY_AGE: Duration = Duration::from_secs(30);

/// Consecutive poison failures (unreadable or undeserializable payload)
/// after which an entry is dead-lettered instead of retried again.
///
/// Mirrors the writer-side WAL processor's policy: the raw payload is
/// preserved under `<wal_dir>/dead-letter/` and the entry is marked
/// processed so it stops being rescanned every pass and its segment can
/// be reclaimed.
pub const MAX_ENTRY_FAILURES: u32 = 10;

/// Outcome of a single retry pass, for logging and tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RetryStats {
    /// Entries successfully re-forwarded and marked processed
    pub retried: usize,
    /// Entries that could not be re-forwarded this pass
    pub failed: usize,
    /// Poison entries moved to the dead-letter directory this pass
    pub dead_lettered: usize,
}

/// Background consumer that re-forwards unprocessed WAL entries to a writer.
pub struct WalRetryConsumer {
    wal_manager: Arc<WalManager>,
    flight_transport: Arc<InMemoryFlightTransport>,
    interval: Duration,
    min_entry_age: Duration,
    /// Consecutive poison-failure counts per pending entry. Entries are
    /// removed when they forward successfully or get dead-lettered, so the
    /// map stays bounded by the number of pending poison entries.
    entry_failures: HashMap<Uuid, u32>,
}

impl WalRetryConsumer {
    pub fn new(
        wal_manager: Arc<WalManager>,
        flight_transport: Arc<InMemoryFlightTransport>,
    ) -> Self {
        Self {
            wal_manager,
            flight_transport,
            interval: DEFAULT_RETRY_INTERVAL,
            min_entry_age: DEFAULT_MIN_ENTRY_AGE,
            entry_failures: HashMap::new(),
        }
    }

    /// Override scan interval and minimum entry age (used by tests).
    pub fn with_timing(mut self, interval: Duration, min_entry_age: Duration) -> Self {
        self.interval = interval;
        self.min_entry_age = min_entry_age;
        self
    }

    /// Spawn the retry loop as a background task.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut consumer = self;
            let mut ticker = tokio::time::interval(consumer.interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                ticker.tick().await;

                match consumer.run_once().await {
                    Ok(stats)
                        if stats.retried > 0 || stats.failed > 0 || stats.dead_lettered > 0 =>
                    {
                        tracing::info!(
                            retried = stats.retried,
                            failed = stats.failed,
                            dead_lettered = stats.dead_lettered,
                            "WAL retry pass completed"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "WAL retry pass failed");
                    }
                }
            }
        })
    }

    /// Run a single retry pass over all managed WALs.
    ///
    /// Forward failures for one WAL abort the rest of that WAL's entries for
    /// this pass (the writer is likely unavailable) but other WALs are still
    /// attempted.
    pub async fn run_once(&mut self) -> anyhow::Result<RetryStats> {
        let mut stats = RetryStats::default();
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        for ((tenant, dataset, signal), wal) in self.wal_manager.all_wals().await {
            let entries = match wal.get_unprocessed_entries().await {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::warn!(
                        tenant_id = %tenant,
                        dataset_id = %dataset,
                        signal = %signal,
                        error = %e,
                        "Failed to list unprocessed WAL entries"
                    );
                    continue;
                }
            };

            for entry in entries {
                if matches!(entry.operation, WalOperation::Flush) {
                    continue;
                }
                if now.saturating_sub(entry.timestamp) < self.min_entry_age.as_secs() {
                    continue;
                }

                let batch = match wal.read_entry_data(&entry).await {
                    Ok(data) => match bytes_to_record_batch(&data) {
                        Ok(batch) => batch,
                        Err(e) => {
                            tracing::warn!(
                                entry_id = %entry.id,
                                error = %e,
                                "Failed to deserialize WAL entry data; skipping"
                            );
                            self.record_poison_failure(&wal, entry.id, &mut stats).await;
                            continue;
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            entry_id = %entry.id,
                            error = %e,
                            "Failed to read WAL entry data; skipping"
                        );
                        self.record_poison_failure(&wal, entry.id, &mut stats).await;
                        continue;
                    }
                };

                match forward_batch_to_writer(
                    &self.flight_transport,
                    batch,
                    entry.metadata.as_deref(),
                )
                .await
                {
                    Ok(()) => {
                        if let Err(e) = wal.mark_processed(entry.id).await {
                            tracing::warn!(
                                entry_id = %entry.id,
                                error = %e,
                                "Re-forwarded WAL entry but failed to mark it processed"
                            );
                        }
                        self.entry_failures.remove(&entry.id);
                        stats.retried += 1;
                    }
                    Err(e) => {
                        tracing::warn!(
                            tenant_id = %tenant,
                            dataset_id = %dataset,
                            signal = %signal,
                            entry_id = %entry.id,
                            error = %e,
                            "Failed to re-forward WAL entry; writer may be unavailable"
                        );
                        stats.failed += 1;
                        // Writer is likely down — don't hammer it with the
                        // remaining entries of this WAL in the same pass.
                        break;
                    }
                }
            }
        }

        Ok(stats)
    }

    /// Record a poison failure (unreadable or undeserializable payload) for
    /// an entry, dead-lettering it once it exhausts its attempts.
    ///
    /// The count is in-memory only and resets on restart, so a poison entry
    /// may get another full round of attempts after a restart — that's fine;
    /// dead-lettering only needs to happen eventually, not exactly at N.
    async fn record_poison_failure(&mut self, wal: &Wal, entry_id: Uuid, stats: &mut RetryStats) {
        let failures = self.entry_failures.entry(entry_id).or_insert(0);
        *failures += 1;
        if *failures < MAX_ENTRY_FAILURES {
            stats.failed += 1;
            return;
        }
        match wal.dead_letter(entry_id).await {
            Ok(path) => {
                tracing::error!(
                    entry_id = %entry_id,
                    path = %path.display(),
                    "WAL entry exhausted its retries; payload preserved in the dead-letter directory and entry marked processed"
                );
                self.entry_failures.remove(&entry_id);
                stats.dead_lettered += 1;
            }
            Err(e) => {
                tracing::error!(
                    entry_id = %entry_id,
                    error = %e,
                    "Failed to dead-letter WAL entry; it will be retried"
                );
                stats.failed += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::service_bootstrap::{ServiceBootstrap, ServiceType};
    use common::wal::WalConfig;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(base_dir: &Path) -> WalConfig {
        let mut config = WalConfig::with_defaults(base_dir.to_path_buf());
        config.max_segment_size = 1024 * 1024;
        config.max_buffer_entries = 100;
        config.flush_interval_secs = 3600;
        config
    }

    fn test_manager(base_dir: &Path) -> WalManager {
        WalManager::new(
            test_config(base_dir),
            test_config(base_dir),
            test_config(base_dir),
            test_config(base_dir),
        )
    }

    async fn test_transport() -> Arc<InMemoryFlightTransport> {
        let bootstrap = ServiceBootstrap::new_for_test(ServiceType::Acceptor, "127.0.0.1:0")
            .await
            .unwrap();
        Arc::new(InMemoryFlightTransport::new(bootstrap))
    }

    async fn append_entry(manager: &WalManager) {
        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        wal.append(WalOperation::WriteTraces, b"not-a-batch".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
    }

    #[tokio::test]
    async fn run_once_skips_entries_younger_than_min_age() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(test_manager(temp_dir.path()));
        append_entry(&manager).await;

        let mut consumer = WalRetryConsumer::new(manager.clone(), test_transport().await)
            .with_timing(Duration::from_secs(1), Duration::from_secs(3600));

        let stats = consumer.run_once().await.unwrap();
        assert_eq!(stats, RetryStats::default());

        // Entry must still be pending
        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert_eq!(wal.get_unprocessed_entries().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn run_once_counts_failures_and_keeps_entries_pending() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(test_manager(temp_dir.path()));
        append_entry(&manager).await;

        // The payload is not a valid RecordBatch, so the pass records a
        // poison failure; below MAX_ENTRY_FAILURES the entry must remain
        // unprocessed for a later pass.
        let mut consumer = WalRetryConsumer::new(manager.clone(), test_transport().await)
            .with_timing(Duration::from_secs(1), Duration::ZERO);

        let stats = consumer.run_once().await.unwrap();
        assert_eq!(stats.retried, 0);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.dead_lettered, 0);

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert_eq!(wal.get_unprocessed_entries().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn poison_entry_is_dead_lettered_after_exhausting_retries() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(test_manager(temp_dir.path()));
        // Payload is not valid Arrow IPC, so every deserialize attempt fails
        append_entry(&manager).await;

        let mut consumer = WalRetryConsumer::new(manager.clone(), test_transport().await)
            .with_timing(Duration::from_secs(1), Duration::ZERO);

        for _ in 0..MAX_ENTRY_FAILURES - 1 {
            let stats = consumer.run_once().await.unwrap();
            assert_eq!(stats.failed, 1);
            assert_eq!(stats.dead_lettered, 0);
        }

        // The pass that exhausts the attempts dead-letters the entry
        let stats = consumer.run_once().await.unwrap();
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.dead_lettered, 1);

        // Entry no longer pending, so later passes are clean
        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());
        let stats = consumer.run_once().await.unwrap();
        assert_eq!(stats, RetryStats::default());

        // Raw payload is preserved in the WAL's dead-letter directory
        let dead_letter_dir = temp_dir
            .path()
            .join("acme")
            .join("production")
            .join("traces")
            .join("dead-letter");
        let mut files = tokio::fs::read_dir(&dead_letter_dir).await.unwrap();
        let file = files.next_entry().await.unwrap().unwrap();
        let preserved = tokio::fs::read(file.path()).await.unwrap();
        assert_eq!(preserved, b"not-a-batch");
    }

    #[tokio::test]
    async fn discovery_surfaces_wals_from_previous_run() {
        let temp_dir = TempDir::new().unwrap();

        // Simulate a previous acceptor run that left an unprocessed entry
        {
            let manager = test_manager(temp_dir.path());
            append_entry(&manager).await;
        }

        // Fresh manager (fresh process): cache is empty until discovery runs
        let manager = Arc::new(test_manager(temp_dir.path()));
        assert_eq!(manager.wal_count().await, 0);

        let discovered = manager.discover_existing_wals().await.unwrap();
        assert_eq!(discovered, 1);
        assert_eq!(manager.wal_count().await, 1);

        let wals = manager.all_wals().await;
        assert_eq!(wals.len(), 1);
        let ((tenant, dataset, signal), wal) = &wals[0];
        assert_eq!(tenant, "acme");
        assert_eq!(dataset, "production");
        assert_eq!(signal, "traces");
        assert_eq!(wal.get_unprocessed_entries().await.unwrap().len(), 1);
    }
}
