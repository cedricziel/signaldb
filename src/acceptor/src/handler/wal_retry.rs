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
//! never be forwarded. Deserialization is deterministic, so a poison entry is
//! dead-lettered the first time it fails — its raw payload is preserved under
//! `<wal_dir>/dead-letter/` and the entry is marked processed, freeing its
//! segment. Forward failures are treated separately: they mean the writer is
//! unavailable, not that the entry is bad.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::error::format_error_chain;
use common::flight::transport::InMemoryFlightTransport;
use common::wal::{Wal, WalOperation, bytes_to_record_batch};
use uuid::Uuid;

use super::WalManager;
use super::forward::{ForwardFailureKind, classify_forward_failure, forward_batch_to_writer};

/// How often the retry consumer scans for unprocessed entries.
pub const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Minimum age an unprocessed entry must reach before it is retried.
///
/// This keeps the consumer from racing the hot path, which forwards the
/// entry inline right after appending it.
pub const DEFAULT_MIN_ENTRY_AGE: Duration = Duration::from_secs(30);

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
                        self.record_unreadable_entry(&wal, entry.id, &e.to_string(), &mut stats)
                            .await;
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
                        stats.retried += 1;
                    }
                    Err(e) => {
                        // The cause chain is the whole diagnostic value here:
                        // "Flight do_put failed" alone cannot distinguish a
                        // dead writer from a batch the writer rejects, and the
                        // two demand opposite responses.
                        let reason = format_error_chain(&e);
                        let kind = classify_forward_failure(&e);
                        tracing::warn!(
                            tenant_id = %tenant,
                            dataset_id = %dataset,
                            signal = %signal,
                            entry_id = %entry.id,
                            failure = ?kind,
                            error = %reason,
                            "Failed to re-forward WAL entry"
                        );

                        match kind {
                            // The writer judged the batch and refused it.
                            // Retrying replays the same bytes into the same
                            // verdict, and until this entry is retired every
                            // later entry in this WAL is unreachable — one
                            // rejected batch shadowed ~101 500 entries on hive
                            // (#1060). Retire it and keep going.
                            ForwardFailureKind::Permanent => {
                                self.record_rejected_entry(&wal, entry.id, &reason, &mut stats)
                                    .await;
                                continue;
                            }
                            // Writer may be down — don't hammer it with the
                            // remaining entries of this WAL in the same pass.
                            ForwardFailureKind::Transient => {
                                stats.failed += 1;
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Reclaim WAL disk now that the pass is over and no listed entries are
        // in flight. Cleanup compacts sealed segments, which moves surviving
        // entries to new offsets, so it must not run *inside* a pass (#1305).
        // This consumer is the acceptor's only WAL reader, so the end of a
        // pass is the safe point. The manager throttles the actual sweep and
        // logs any failures.
        self.wal_manager.cleanup_all_if_due().await;

        Ok(stats)
    }

    /// Retire an entry whose payload could be read but is not a valid batch.
    ///
    /// The failure is deterministic — misframed bytes do not become a valid
    /// Arrow IPC frame on a later attempt — so there is nothing to gain from
    /// counting attempts. Retrying only keeps the entry in the pending set,
    /// which forces every subsequent pass to walk it again and pins its
    /// segment against reclamation. Dead-letter it immediately: the raw bytes
    /// are preserved under `<wal_dir>/dead-letter/` and the entry is marked
    /// processed.
    async fn record_poison_failure(&mut self, wal: &Wal, entry_id: Uuid, stats: &mut RetryStats) {
        match wal.dead_letter(entry_id).await {
            Ok(path) => {
                tracing::error!(
                    entry_id = %entry_id,
                    path = %path.display(),
                    "WAL entry payload could not be deserialized; raw bytes preserved in the dead-letter directory and entry marked processed"
                );
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

    /// Retire an entry the writer refuses to accept.
    ///
    /// This is the third kind of poison entry, and the only one that reaches
    /// the writer: the payload reads and deserializes cleanly, but cannot be
    /// shaped into its target table. The verdict is a property of the bytes,
    /// so it does not change on a later pass.
    ///
    /// The payload is preserved rather than discarded — unlike a truncated
    /// record it is complete, and becomes replayable once the rejection cause
    /// is fixed — with the writer's own reason recorded alongside it.
    async fn record_rejected_entry(
        &mut self,
        wal: &Wal,
        entry_id: Uuid,
        reason: &str,
        stats: &mut RetryStats,
    ) {
        match wal.dead_letter_rejected(entry_id, reason).await {
            Ok(path) => {
                tracing::error!(
                    entry_id = %entry_id,
                    path = %path.display(),
                    error = %reason,
                    "Writer rejected WAL entry; payload preserved in the dead-letter directory and entry marked processed"
                );
                stats.dead_lettered += 1;
            }
            Err(e) => {
                tracing::error!(
                    entry_id = %entry_id,
                    error = %e,
                    "Failed to dead-letter rejected WAL entry; it will be retried"
                );
                stats.failed += 1;
            }
        }
    }

    /// Retire an entry whose payload cannot be read at all.
    ///
    /// [`Wal::dead_letter`] cannot be used here: it re-reads the payload before
    /// preserving it, so the same read that already failed fails again and the
    /// entry would stay pending, pinning its segment forever. Record the
    /// entry's identity, byte range, and the read error as a marker instead,
    /// then mark it processed. No bytes are recoverable, and the message says
    /// so rather than promising a replayable payload.
    async fn record_unreadable_entry(
        &mut self,
        wal: &Wal,
        entry_id: Uuid,
        reason: &str,
        stats: &mut RetryStats,
    ) {
        match wal.dead_letter_unreadable(entry_id, reason).await {
            Ok(path) => {
                tracing::error!(
                    entry_id = %entry_id,
                    path = %path.display(),
                    "WAL entry payload is unreadable and cannot be recovered; metadata recorded in the dead-letter directory and entry marked processed"
                );
                stats.dead_lettered += 1;
            }
            Err(e) => {
                tracing::error!(
                    entry_id = %entry_id,
                    error = %e,
                    "Failed to retire unreadable WAL entry; it will be retried"
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
    async fn poison_entry_is_dead_lettered_on_first_failure() {
        // A deserialize failure is deterministic: a truncated or misframed
        // Arrow payload does not become readable on the tenth attempt. Holding
        // such an entry pending only keeps its segment open and forces every
        // later pass to walk it again — on the hive deployment that left
        // 25k unreadable entries pinning a 3.1 GB segment indefinitely
        // (issue #1058). Retire it the moment it fails to deserialize.
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(test_manager(temp_dir.path()));
        // Payload is not valid Arrow IPC, so deserialization always fails.
        append_entry(&manager).await;

        let mut consumer = WalRetryConsumer::new(manager.clone(), test_transport().await)
            .with_timing(Duration::from_secs(1), Duration::ZERO);

        let stats = consumer.run_once().await.unwrap();
        assert_eq!(stats.retried, 0);
        assert_eq!(
            stats.failed, 0,
            "a poison entry is retired, not counted as a retryable failure"
        );
        assert_eq!(stats.dead_lettered, 1);

        // Entry is no longer pending, so the segment can be reclaimed and
        // later passes are clean.
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
    async fn unreadable_entry_is_retired_without_re_reading_its_payload() {
        // `dead_letter` re-reads the payload before preserving it, which is
        // precisely what fails when the recorded byte range is unreadable. An
        // entry in that state must still leave the pending set, or it pins its
        // segment forever — the failure mode this whole change exists to end.
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(test_manager(temp_dir.path()));
        append_entry(&manager).await;

        // Truncate the segment's data file so the entry's recorded
        // [data_offset, data_offset + data_size) range is out of bounds.
        let data_path = temp_dir
            .path()
            .join("acme")
            .join("production")
            .join("traces")
            .join("wal-0000000000.data");
        tokio::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .await
            .unwrap()
            .set_len(0)
            .await
            .unwrap();

        let mut consumer = WalRetryConsumer::new(manager.clone(), test_transport().await)
            .with_timing(Duration::from_secs(1), Duration::ZERO);

        let stats = consumer.run_once().await.unwrap();
        assert_eq!(
            stats.dead_lettered, 1,
            "an unreadable entry must still be retired"
        );
        assert_eq!(stats.failed, 0);

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "unreadable entry must not stay pending and pin its segment"
        );

        // Nothing to preserve, so a marker recording what was known about the
        // entry stands in for the payload.
        let dead_letter_dir = temp_dir
            .path()
            .join("acme")
            .join("production")
            .join("traces")
            .join("dead-letter");
        let mut files = tokio::fs::read_dir(&dead_letter_dir).await.unwrap();
        let file = files.next_entry().await.unwrap().unwrap();
        let name = file.file_name().to_string_lossy().to_string();
        assert!(
            name.ends_with(".unreadable.json"),
            "expected an unreadable marker, got {name}"
        );
        let marker = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert!(
            marker.contains("out of bounds"),
            "marker must record why the payload could not be read: {marker}"
        );
    }

    #[tokio::test]
    async fn poison_entry_does_not_block_the_rest_of_its_wal() {
        // Retiring poison on sight must also let the pass continue past it, so
        // a corrupt entry cannot starve the intact entries queued behind it.
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(test_manager(temp_dir.path()));
        append_entry(&manager).await;
        append_entry(&manager).await;
        append_entry(&manager).await;

        let mut consumer = WalRetryConsumer::new(manager.clone(), test_transport().await)
            .with_timing(Duration::from_secs(1), Duration::ZERO);

        let stats = consumer.run_once().await.unwrap();
        assert_eq!(
            stats.dead_lettered, 3,
            "every poison entry in the WAL is retired in a single pass"
        );

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());
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

    #[tokio::test]
    async fn a_retry_pass_reclaims_processed_wal_segments() {
        // `Wal::cleanup` had no caller in any service (#1305), so processed
        // segments accumulated and were re-read at every start. The retry pass
        // is the acceptor's only WAL consumer, so its end is the one point
        // where compaction cannot move entries under a reader. This guards the
        // wiring, not the sweep itself.
        let temp_dir = TempDir::new().unwrap();
        let mut config = test_config(temp_dir.path());
        config.max_segment_size = 1024; // small: a few appends seal a segment
        config.max_buffer_entries = 1;
        let manager = Arc::new(WalManager::new(
            config.clone(),
            config.clone(),
            config.clone(),
            config,
        ));

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        for i in 0..12u64 {
            let id = wal
                .append(
                    WalOperation::WriteTraces,
                    vec![b'x'; 200 + i as usize],
                    None,
                )
                .await
                .unwrap();
            wal.flush().await.unwrap();
            wal.mark_processed(id).await.unwrap();
        }
        assert!(
            wal.segment_count().await > 1,
            "test needs sealed segments to reclaim"
        );

        let mut consumer = WalRetryConsumer::new(manager.clone(), test_transport().await)
            .with_timing(Duration::from_secs(1), Duration::from_secs(0));
        consumer.run_once().await.unwrap();

        assert_eq!(
            wal.segment_count().await,
            1,
            "a retry pass must reclaim sealed segments whose entries are all processed"
        );
    }

    /// A `FlightService` whose `do_put` always rejects the batch with
    /// `INVALID_ARGUMENT` — the "writer inspected it and refused it" case
    /// [`classify_forward_failure`] maps to [`ForwardFailureKind::Permanent`].
    /// Every other RPC is unimplemented; the retry consumer only exercises
    /// `do_put`.
    struct RejectingFlightService;

    #[tonic::async_trait]
    impl arrow_flight::flight_service_server::FlightService for RejectingFlightService {
        type HandshakeStream = futures::stream::BoxStream<
            'static,
            Result<arrow_flight::HandshakeResponse, tonic::Status>,
        >;
        type ListFlightsStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::FlightInfo, tonic::Status>>;
        type DoGetStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::FlightData, tonic::Status>>;
        type DoPutStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::PutResult, tonic::Status>>;
        type DoExchangeStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::FlightData, tonic::Status>>;
        type DoActionStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::Result, tonic::Status>>;
        type ListActionsStream =
            futures::stream::BoxStream<'static, Result<arrow_flight::ActionType, tonic::Status>>;

        async fn handshake(
            &self,
            _request: tonic::Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
        ) -> Result<tonic::Response<Self::HandshakeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("handshake"))
        }
        async fn list_flights(
            &self,
            _request: tonic::Request<arrow_flight::Criteria>,
        ) -> Result<tonic::Response<Self::ListFlightsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("list_flights"))
        }
        async fn get_flight_info(
            &self,
            _request: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::FlightInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented("get_flight_info"))
        }
        async fn poll_flight_info(
            &self,
            _request: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::PollInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented("poll_flight_info"))
        }
        async fn get_schema(
            &self,
            _request: tonic::Request<arrow_flight::FlightDescriptor>,
        ) -> Result<tonic::Response<arrow_flight::SchemaResult>, tonic::Status> {
            Err(tonic::Status::unimplemented("get_schema"))
        }
        async fn do_get(
            &self,
            _request: tonic::Request<arrow_flight::Ticket>,
        ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_get"))
        }
        async fn do_put(
            &self,
            _request: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
        ) -> Result<tonic::Response<Self::DoPutStream>, tonic::Status> {
            Err(tonic::Status::invalid_argument(
                "batch does not match the target table's schema",
            ))
        }
        async fn do_exchange(
            &self,
            _request: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
        ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_exchange"))
        }
        async fn do_action(
            &self,
            _request: tonic::Request<arrow_flight::Action>,
        ) -> Result<tonic::Response<Self::DoActionStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("do_action"))
        }
        async fn list_actions(
            &self,
            _request: tonic::Request<arrow_flight::Empty>,
        ) -> Result<tonic::Response<Self::ListActionsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("list_actions"))
        }
    }

    /// A minimal but valid Arrow `RecordBatch`, round-trippable through
    /// [`common::wal::record_batch_to_bytes`]/[`bytes_to_record_batch`].
    fn sample_record_batch() -> datafusion::arrow::record_batch::RecordBatch {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;

        let schema = StdArc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn writer_rejected_entry_is_dead_lettered_with_the_writer_s_reason() {
        // record_rejected_entry is the third dead-letter path (the other two
        // are covered above): the payload deserializes fine, but the writer
        // itself refuses it. Unlike the other two, this one requires a real
        // Flight round-trip to exercise, since the rejection is a property
        // of what the *writer* does with a syntactically valid batch.
        let catalog = common::catalog::Catalog::new_in_memory().await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let writer_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(common::flight::flight_service_server(
                    RejectingFlightService,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        ServiceBootstrap::new_for_test_with_catalog(
            catalog.clone(),
            ServiceType::Writer,
            &writer_addr.to_string(),
        )
        .await
        .unwrap();

        let acceptor_bootstrap = ServiceBootstrap::new_for_test_with_catalog(
            catalog,
            ServiceType::Acceptor,
            "127.0.0.1:0",
        )
        .await
        .unwrap();
        let flight_transport = Arc::new(InMemoryFlightTransport::new(acceptor_bootstrap));

        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(test_manager(temp_dir.path()));
        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        let batch_bytes = common::wal::record_batch_to_bytes(&sample_record_batch()).unwrap();
        wal.append(WalOperation::WriteTraces, batch_bytes, None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        let mut consumer = WalRetryConsumer::new(manager.clone(), flight_transport)
            .with_timing(Duration::from_secs(1), Duration::ZERO);
        let stats = consumer.run_once().await.unwrap();

        assert_eq!(
            stats.dead_lettered, 1,
            "a writer rejection must be dead-lettered, not left pending"
        );
        assert_eq!(stats.failed, 0);
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());

        let dead_letter_dir = temp_dir
            .path()
            .join("acme")
            .join("production")
            .join("traces")
            .join("dead-letter");
        let mut saw_marker = false;
        let mut files = tokio::fs::read_dir(&dead_letter_dir).await.unwrap();
        while let Some(entry) = files.next_entry().await.unwrap() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let marker = tokio::fs::read_to_string(&path).await.unwrap();
                assert!(
                    marker.contains("schema"),
                    "rejection marker must carry the writer's own reason: {marker}"
                );
                saw_marker = true;
            }
        }
        assert!(saw_marker, "expected a .rejected.json marker file");
    }
}
