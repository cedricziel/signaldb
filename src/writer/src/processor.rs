use crate::storage::IcebergTableWriter;
use anyhow::{Context, Result};
use common::CatalogManager;
use common::config::WriterConfig;
use common::wal::manager::WalManager;
use common::wal::{Wal, WalEntry, bytes_to_record_batch};
use datafusion::arrow::array::RecordBatch;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{Duration, interval};
use uuid::Uuid;

/// Maximum WAL entries committed per Iceberg transaction. Bounds the size
/// of the idempotency marker written with each commit (one uuid per entry).
const MAX_ENTRIES_PER_COMMIT: usize = 1024;

/// Consecutive failures after which an entry is dead-lettered: its raw
/// payload is preserved under `<wal_dir>/dead-letter/` and the entry is
/// marked processed so it stops blocking ingestion.
const MAX_ENTRY_FAILURES: u32 = 10;

/// A WAL entry's W3C trace context (`traceparent`, `tracestate`) as carried
/// from the originating ingest request.
type EntryTraceContext = (Option<String>, Option<String>);

/// Entries grouped for one table's batch write: the WAL they came from, the
/// `(id, batch)` payloads to commit, and the trace context each entry arrived
/// with.
struct TableBatch {
    wal: Arc<Wal>,
    entries: Vec<(Uuid, RecordBatch)>,
    trace_contexts: Vec<EntryTraceContext>,
}

/// Grouping key for one processing cycle: which WAL the entries live in, then
/// tenant, dataset, and table. Including the WAL keeps every group
/// single-sourced, so marking and the per-WAL idempotency marker stay exact
/// even when two WALs feed the same table (a legacy root WAL being drained
/// alongside the tenant's own, see [`WalManager::adopt_root_segments`]). The
/// WAL is identified by the address of the cached `Arc<Wal>`, which the
/// manager keeps alive for the whole cycle — cheaper and more direct than
/// re-deriving its `writer_id` string for every entry.
type GroupKey = (usize, String, String, String);

/// Why a WAL entry's payload could not be turned into a batch. The two are
/// retired differently: an unreadable payload (bounds/CRC failure) is
/// deterministic and has no bytes worth preserving, so it is retired at once
/// via [`Wal::dead_letter_unreadable`]; an undecodable one is retried a few
/// times and then preserved via [`Wal::dead_letter`].
#[derive(Debug)]
enum EntryDecodeError {
    /// The WAL refused to hand out the bytes (out of bounds, CRC mismatch).
    Unreadable(anyhow::Error),
    /// The bytes read fine but are not a valid Arrow IPC stream.
    Undecodable(anyhow::Error),
}

impl std::fmt::Display for EntryDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntryDecodeError::Unreadable(e) => write!(f, "unreadable payload: {e}"),
            EntryDecodeError::Undecodable(e) => write!(f, "undecodable payload: {e}"),
        }
    }
}

/// Per-`(tenant, dataset, table)` commit-coalescing gate.
///
/// Decides whether a group's pending rows should be committed now, so a
/// high-frequency producer does not force one Iceberg snapshot (and one catalog
/// metadata write) per request. A group commits when its rows reach
/// `max_uncommitted_rows` (a burst safety valve) OR `commit_interval` has
/// elapsed since its last commit (liveness for low-volume groups). A
/// never-committed group is eligible immediately so first data is not delayed.
///
/// State is in-memory only: losing it on restart merely lets the first
/// post-restart tick commit slightly early, which is harmless.
struct CommitCoalescer {
    commit_interval: Duration,
    max_uncommitted_rows: usize,
    last_commit: HashMap<String, Instant>,
}

impl CommitCoalescer {
    fn new(config: &WriterConfig) -> Self {
        Self {
            commit_interval: config.commit_interval,
            max_uncommitted_rows: config.max_uncommitted_rows,
            last_commit: HashMap::new(),
        }
    }

    /// Whether the group `key` holding `pending_rows` rows should commit at
    /// `now`. The row bound triggers an *earlier* commit; it never delays a
    /// low-volume group past `commit_interval`.
    fn should_commit(&self, key: &str, pending_rows: usize, now: Instant) -> bool {
        if pending_rows >= self.max_uncommitted_rows {
            return true;
        }
        match self.last_commit.get(key) {
            // Never committed: commit first data promptly rather than waiting a
            // full interval for a group that has never been flushed.
            None => true,
            Some(&last) => now.duration_since(last) >= self.commit_interval,
        }
    }

    /// Record that `key` committed at `now`, restarting its interval.
    fn record_commit(&mut self, key: &str, now: Instant) {
        self.last_commit.insert(key.to_string(), now);
    }
}

/// Scope of a forced commit (read-your-writes flush).
///
/// A flush forces an immediate commit only for groups matching this scope; all
/// other `(tenant, dataset, table)` groups keep normal coalescing. This stops
/// one tenant's flush from bypassing the floor for unrelated tenants and
/// reintroducing catalog write amplification.
#[derive(Clone, Debug)]
pub struct FlushScope {
    /// Tenant whose pending groups are force-committed.
    pub tenant_id: String,
    /// Restrict to a single dataset within the tenant; `None` flushes every
    /// dataset for the tenant.
    pub dataset_id: Option<String>,
}

impl FlushScope {
    /// Whether a group for `(tenant, dataset)` is covered by this scope.
    fn matches(&self, tenant: &str, dataset: &str) -> bool {
        self.tenant_id == tenant && self.dataset_id.as_deref().is_none_or(|d| d == dataset)
    }
}

/// Extract the W3C trace context (`traceparent`/`tracestate`) that the ingest
/// request stored in a WAL entry's metadata. Lets the asynchronous processor
/// link its batch span back to the originating ingest trace instead of
/// appearing as a detached root. Returns `(None, None)` when the metadata is
/// absent, not JSON, or carries no trace context.
fn trace_context_from_metadata(metadata: &Option<String>) -> EntryTraceContext {
    let Some(raw) = metadata else {
        return (None, None);
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(raw) else {
        return (None, None);
    };
    let get = |key: &str| json.get(key).and_then(|v| v.as_str()).map(str::to_string);
    (get("traceparent"), get("tracestate"))
}

/// WAL processor that reads entries and writes them to Iceberg tables
/// Replaces the direct Parquet writing approach with transaction-based Iceberg writes
pub struct WalProcessor {
    /// One WAL per tenant/dataset/signal (#932): a poisoned or slow WAL only
    /// ever affects its own tenant, and each cycle drains every cached WAL.
    wal_manager: Arc<WalManager>,
    catalog_manager: Arc<CatalogManager>,
    object_store: Arc<dyn ObjectStore>,
    // Cache of table writers per tenant/table combination
    table_writers: HashMap<String, IcebergTableWriter>,
    /// Consecutive processing failures per entry. Entries that keep
    /// failing are dead-lettered so one poison entry cannot wedge the
    /// processing loop forever (in-memory: a restart grants a fresh set
    /// of attempts, which is fine — dead-lettering only needs to happen
    /// eventually).
    entry_failures: HashMap<Uuid, u32>,
    /// Commit-coalescing gate per `(tenant, dataset, table)`.
    coalescer: CommitCoalescer,
}

impl WalProcessor {
    /// Create a new WAL processor with shared CatalogManager and the default
    /// writer commit-coalescing policy.
    pub fn new(
        wal_manager: Arc<WalManager>,
        catalog_manager: Arc<CatalogManager>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::with_config(
            wal_manager,
            catalog_manager,
            object_store,
            &WriterConfig::default(),
        )
    }

    /// Create a new WAL processor with an explicit commit-coalescing policy.
    pub fn with_config(
        wal_manager: Arc<WalManager>,
        catalog_manager: Arc<CatalogManager>,
        object_store: Arc<dyn ObjectStore>,
        writer_config: &WriterConfig,
    ) -> Self {
        Self {
            wal_manager,
            catalog_manager,
            object_store,
            table_writers: HashMap::new(),
            entry_failures: HashMap::new(),
            coalescer: CommitCoalescer::new(writer_config),
        }
    }

    /// Start the WAL processing loop
    /// This will continuously process unprocessed WAL entries
    pub async fn start_processing_loop(&mut self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(1)); // Process every second

        loop {
            interval.tick().await;

            if let Err(e) = self.process_pending_entries().await {
                tracing::error!(error = %e, "Error processing WAL entries");
                // Continue processing despite errors
            }
        }
    }

    /// Process pending WAL entries subject to the commit-coalescing floor: a
    /// group is committed only once its rows reach `max_uncommitted_rows` or
    /// `commit_interval` has elapsed since its last commit. Sub-floor groups
    /// are left unprocessed for a later tick.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn process_pending_entries(&mut self) -> Result<()> {
        self.drain_pending(Vec::new()).await
    }

    /// Immediately commit the pending groups covered by `scope`, ignoring the
    /// coalescing floor for them only. Groups outside the scope keep normal
    /// coalescing. The read-your-writes drain used by the force-commit primitive.
    #[tracing::instrument(level = "debug", skip_all, fields(signaldb.tenant.id = %scope.tenant_id))]
    pub async fn force_commit_pending(&mut self, scope: FlushScope) -> Result<()> {
        self.drain_pending(vec![scope]).await
    }

    /// Shared drain implementation. Groups matching any entry in `flush_scopes`
    /// are force-committed (floor bypassed); all others follow the coalescing
    /// floor. The background loop passes an empty `flush_scopes` (pure
    /// coalescing); `Flush` WAL markers contribute their own tenant/dataset
    /// scope.
    async fn drain_pending(&mut self, mut flush_scopes: Vec<FlushScope>) -> Result<()> {
        // Every cached WAL is drained each cycle. Listing a WAL's entries is
        // an in-memory read that cannot fail today; the error arm is kept
        // deliberately so that if it ever becomes fallible (a reload from
        // disk, say) one tenant's failure skips only that tenant's WAL for
        // the cycle instead of aborting every other tenant's drain (#932).
        // It reports through a counter and one aggregate line, never a log
        // line per WAL: at a few hundred tenants a failing disk would
        // otherwise emit a log flood that self-monitoring re-ingests, which
        // is the export-churn loop of the #865 incident.
        let mut pending_entries: Vec<(Arc<Wal>, WalEntry)> = Vec::new();
        let mut list_failures = 0usize;
        for ((tenant, dataset, signal), wal) in self.wal_manager.all_wals().await {
            match wal.get_unprocessed_entries().await {
                Ok(entries) => {
                    pending_entries.extend(entries.into_iter().map(|e| (wal.clone(), e)));
                }
                Err(e) => {
                    list_failures += 1;
                    common::self_monitoring::app_metrics()
                        .wal_list_failures
                        .add(
                            1,
                            &[
                                opentelemetry::KeyValue::new("tenant", tenant.clone()),
                                opentelemetry::KeyValue::new("signal", signal.clone()),
                            ],
                        );
                    tracing::debug!(
                        tenant_id = %tenant,
                        dataset_id = %dataset,
                        signal = %signal,
                        error = %e,
                        "Failed to list unprocessed WAL entries; skipping this WAL for the cycle"
                    );
                }
            }
        }
        if list_failures > 0 {
            tracing::warn!(
                list_failures,
                "WALs skipped this cycle because their entries could not be listed"
            );
        }

        if pending_entries.is_empty() {
            // Keep the backlog gauge honest on an idle WAL: without this it
            // would stick at the last non-zero reading and read as a false
            // stall (there is nothing deferred when nothing is pending).
            common::self_monitoring::app_metrics()
                .writer_groups_deferred
                .record(0, &[]);
            return Ok(());
        }

        tracing::debug!(
            entry_count = pending_entries.len(),
            "Processing pending WAL entries"
        );

        // Group entries by tenant, dataset, and table for batch processing.
        // Alongside each group's (id, batch) payloads we keep the per-entry
        // trace context so the batch span can link back to every ingest trace
        // it commits (fan-in).
        //
        // NOTE: entries deferred by the coalescing floor are re-deserialized on
        // the next tick (they remain unprocessed and are re-read here). At the
        // default `commit_interval ≈ tick` this is ~1 redundant decode; if
        // `commit_interval` is configured much larger than the tick, gate the
        // floor on entry metadata (`data_size`) before deserializing instead.
        let mut grouped_entries: HashMap<GroupKey, TableBatch> = HashMap::new();

        // A `Flush` marker is a force-commit request scoped to its own
        // tenant/dataset: it carries no data, but force-commits that scope's
        // pending groups this cycle (bypassing the floor), and is marked
        // processed once drained.
        let mut flush_marker_ids: Vec<(Arc<Wal>, Uuid)> = Vec::new();

        for (wal, entry) in pending_entries {
            if matches!(entry.operation, common::wal::WalOperation::Flush) {
                flush_marker_ids.push((wal.clone(), entry.id));
                flush_scopes.push(FlushScope {
                    tenant_id: entry.tenant_id.clone(),
                    dataset_id: Some(entry.dataset_id.clone()),
                });
                continue;
            }

            // A poison entry (unroutable or undeserializable) must not
            // abort the whole cycle: record the failure, skip it this
            // round, and dead-letter it once it exhausts its attempts.
            let routed = match self.determine_target_table(&entry) {
                Ok(routed) => routed,
                Err(e) => {
                    tracing::warn!(
                        entry_id = %entry.id,
                        tenant_id = %entry.tenant_id,
                        dataset_id = %entry.dataset_id,
                        signal = entry.operation.signal(),
                        data_offset = entry.data_offset,
                        data_size = entry.data_size,
                        error = %e,
                        "Failed to route WAL entry"
                    );
                    self.record_entry_failure(
                        &wal,
                        entry.id,
                        &entry.tenant_id,
                        &entry.dataset_id,
                        entry.operation.signal(),
                    )
                    .await;
                    continue;
                }
            };
            let (tenant_id, dataset_id, table_name) = routed;
            // Anti-loop guard (#760): processing the _system tenant's own
            // telemetry must not emit logs/spans that get exported and
            // re-ingested as _system telemetry.
            let suppress = common::self_monitoring::is_self_monitoring_tenant(&tenant_id);
            let batch = match common::self_monitoring::maybe_suppress_self_telemetry(
                suppress,
                Self::deserialize_entry_data(&wal, &entry),
            )
            .await
            {
                Ok(batch) => batch,
                Err(e) => {
                    common::self_monitoring::maybe_suppress_self_telemetry(suppress, async {
                        tracing::warn!(
                            entry_id = %entry.id,
                            tenant_id = %entry.tenant_id,
                            dataset_id = %entry.dataset_id,
                            signal = entry.operation.signal(),
                            data_offset = entry.data_offset,
                            data_size = entry.data_size,
                            error = %e,
                            "Failed to deserialize WAL entry"
                        );
                        match e {
                            EntryDecodeError::Unreadable(cause) => {
                                self.record_unreadable_entry(&wal, &entry, &cause.to_string())
                                    .await;
                            }
                            EntryDecodeError::Undecodable(_) => {
                                self.record_entry_failure(
                                    &wal,
                                    entry.id,
                                    &entry.tenant_id,
                                    &entry.dataset_id,
                                    entry.operation.signal(),
                                )
                                .await;
                            }
                        }
                    })
                    .await;
                    continue;
                }
            };

            let trace_ctx = trace_context_from_metadata(&entry.metadata);
            let group = grouped_entries
                .entry((
                    Arc::as_ptr(&wal) as usize,
                    tenant_id,
                    dataset_id,
                    table_name,
                ))
                .or_insert_with(|| TableBatch {
                    wal: wal.clone(),
                    entries: Vec::new(),
                    trace_contexts: Vec::new(),
                });
            group.entries.push((entry.id, batch));
            group.trace_contexts.push(trace_ctx);
        }

        // Process each group using batch writes. Marking happens inside
        // process_batch_for_table, interleaved with commits to preserve
        // the idempotency-marker invariant.
        //
        // `now` is captured once for the whole cycle: a group committed late in
        // a slow cycle records a commit time slightly earlier than it actually
        // finished, which only biases toward committing marginally sooner next
        // cycle (never later) — harmless.
        let now = Instant::now();
        let mut deferred_groups: u64 = 0;
        // When this drain is forced (explicit force-commit or a Flush marker),
        // a group that fails to commit must surface as an error so the caller
        // does not believe its read-your-writes drain succeeded.
        let mut forced_commit_failed = false;
        for (
            (_, tenant_id, dataset_id, table_name),
            TableBatch {
                wal,
                entries,
                trace_contexts,
            },
        ) in grouped_entries
        {
            let writer_key = format!("{tenant_id}:{dataset_id}:{table_name}");

            // A group is force-committed only when a flush scope (explicit
            // request or a Flush marker) covers its tenant/dataset; unrelated
            // tenants keep normal coalescing so one flush can't amplify their
            // commits.
            let forced = flush_scopes
                .iter()
                .any(|s| s.matches(&tenant_id, &dataset_id));

            // Coalescing floor: defer this group's commit unless forced, its
            // rows have reached the ceiling, or its interval has elapsed. The
            // entries stay unprocessed (durable in the WAL) and are revisited
            // on a later tick — capping commit rate at ~1 per interval per
            // table regardless of ingest rate (#888).
            let pending_rows: usize = entries.iter().map(|(_, b)| b.num_rows()).sum();
            if !forced && !self.coalescer.should_commit(&writer_key, pending_rows, now) {
                deferred_groups += 1;
                tracing::debug!(
                    tenant_id = %tenant_id,
                    table_name = %table_name,
                    pending_rows,
                    "Deferring commit: below coalescing floor"
                );
                continue;
            }

            let group_ids: Vec<Uuid> = entries.iter().map(|(id, _)| *id).collect();
            // Anti-loop guard (#760): suppression is per group because the
            // loop interleaves tenants — only the _system tenant's batches
            // must not be re-instrumented.
            let suppress = common::self_monitoring::is_self_monitoring_tenant(&tenant_id);
            let committed = common::self_monitoring::maybe_suppress_self_telemetry(suppress, async {
                match self
                    .process_batch_for_table(
                        &wal,
                        &tenant_id,
                        &dataset_id,
                        &table_name,
                        entries,
                        trace_contexts,
                    )
                    .await
                {
                    Ok(processed_ids) => {
                        // Restart this group's coalescing interval only on a
                        // real commit.
                        self.coalescer.record_commit(&writer_key, now);
                        for entry_id in &processed_ids {
                            self.entry_failures.remove(entry_id);
                        }
                        tracing::debug!(
                            entry_count = processed_ids.len(),
                            tenant_id = %tenant_id,
                            table_name = %table_name,
                            "Processed and marked entries for table"
                        );
                        true
                    }
                    Err(e) => {
                        tracing::error!(tenant_id = %tenant_id, table_name = %table_name, error = %e, "Failed to process batch for table");
                        for entry_id in group_ids {
                            self.record_entry_failure(
                                &wal,
                                entry_id,
                                &tenant_id,
                                &dataset_id,
                                &table_name,
                            )
                            .await;
                        }
                        false
                    }
                }
            })
            .await;
            // Only a *forced* group's failure fails the drain; a background
            // group failing is best-effort (already dead-lettered/retried).
            if forced && !committed {
                forced_commit_failed = true;
            }
        }

        // Retire the Flush markers only once their requested drain fully
        // succeeded — otherwise leave them so the next cycle retries the
        // force-drain rather than silently dropping the read-your-writes request.
        if !forced_commit_failed {
            for (wal, flush_id) in flush_marker_ids {
                if let Err(e) = wal.mark_processed(flush_id).await {
                    tracing::warn!(entry_id = %flush_id, error = %e, "Failed to mark Flush marker processed");
                }
            }
        }

        // Publish the coalescing backlog for stall observability. Read
        // alongside `signaldb.wal.entries_pending`: a sustained non-zero
        // deferred-group count with rising pending entries means commits are
        // not keeping up.
        common::self_monitoring::app_metrics()
            .writer_groups_deferred
            .record(deferred_groups, &[]);

        // A forced drain that could not commit every group is a failed
        // read-your-writes request: surface it so `do_action("flush")` returns
        // an error and the caller retries. The background (non-forced) loop
        // keeps its best-effort semantics — failures there are already
        // dead-lettered and retried without failing the tick.
        if !flush_scopes.is_empty() && forced_commit_failed {
            return Err(anyhow::anyhow!(
                "force-commit drain failed: one or more groups did not commit"
            ));
        }

        Ok(())
    }

    /// Process a batch of entries for a specific table
    #[tracing::instrument(
        skip_all,
        fields(
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.table = %table_name,
            signaldb.wal.entry_count = entries.len() as i64
        )
    )]
    async fn process_batch_for_table(
        &mut self,
        wal: &Arc<Wal>,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        entries: Vec<(Uuid, RecordBatch)>,
        trace_contexts: Vec<EntryTraceContext>,
    ) -> Result<Vec<Uuid>> {
        // Link this batch span back to every distinct ingest trace whose
        // entries it commits. The processor fans work in from many ingest
        // requests, so a single parent can't represent them all — links keep
        // each source trace reachable. No-op when self-monitoring is disabled.
        link_batch_origins(&tracing::Span::current(), &trace_contexts);

        let writer_key = format!("{tenant_id}:{dataset_id}:{table_name}");

        // Get or create table writer
        if !self.table_writers.contains_key(&writer_key) {
            let writer = IcebergTableWriter::new(
                &self.catalog_manager,
                self.object_store.clone(),
                tenant_id.to_string(),
                dataset_id.to_string(),
                table_name.to_string(),
            )
            .await?;
            self.table_writers.insert(writer_key.clone(), writer);
        }

        let wal_writer_id = wal.writer_id().to_string();
        let writer = self
            .table_writers
            .get_mut(&writer_key)
            .ok_or_else(|| anyhow::anyhow!("Failed to get table writer for {}", writer_key))?;

        // Step 1: dedupe against the idempotency marker. Ids recorded
        // there were committed to Iceberg but never marked processed
        // (crash between commit and index write, or a failed mark).
        // Re-inserting them would duplicate rows — mark them durably
        // instead, and do it BEFORE any new commit replaces the marker.
        // A mark failure aborts the whole group for the same reason.
        let committed = writer.load_committed_marker(&wal_writer_id).await?;
        let mut already_committed_ids = Vec::new();
        let mut fresh = Vec::new();
        for (entry_id, batch) in entries {
            if committed.contains(&entry_id) {
                tracing::info!(
                    entry_id = %entry_id,
                    table_name = %table_name,
                    "Skipping re-insert of WAL entry already committed to Iceberg"
                );
                already_committed_ids.push(entry_id);
            } else {
                fresh.push((entry_id, batch));
            }
        }

        // Mark every already-committed id in one batch call so the WAL
        // persists the affected segment index(es) once instead of once
        // per entry (same rationale as the chunked mark below, #943).
        let mut processed_ids = Vec::new();
        if !already_committed_ids.is_empty() {
            wal.mark_processed_many(&already_committed_ids)
                .await
                .with_context(|| {
                    format!(
                        "Failed to mark {} already-committed WAL entries as processed",
                        already_committed_ids.len()
                    )
                })?;
            processed_ids.extend(already_committed_ids);
        }

        // Step 2: commit fresh entries in bounded chunks, marking each
        // chunk before the next commit so at most one commit's ids are
        // ever unmarked — which is exactly what the marker covers.
        let mut fresh_iter = fresh.into_iter().peekable();
        while fresh_iter.peek().is_some() {
            let chunk: Vec<(Uuid, RecordBatch)> =
                fresh_iter.by_ref().take(MAX_ENTRIES_PER_COMMIT).collect();
            let chunk_ids: Vec<Uuid> = chunk.iter().map(|(id, _)| *id).collect();

            writer
                .append_batches_with_marker(&wal_writer_id, chunk)
                .await?;

            // One batch call per committed chunk: the WAL persists each
            // affected segment's index once instead of rewriting and
            // fsyncing it per entry (issue #943). On failure the entries
            // stay unprocessed but their ids are in the marker, so the
            // next tick re-marks instead of re-inserting.
            wal.mark_processed_many(&chunk_ids).await.with_context(|| {
                format!(
                    "Failed to mark {} WAL entries as processed after commit",
                    chunk_ids.len()
                )
            })?;
            processed_ids.extend(chunk_ids);
        }

        Ok(processed_ids)
    }

    /// Record a processing failure for an entry, dead-lettering it once
    /// it exhausts its attempts.
    async fn record_entry_failure(
        &mut self,
        wal: &Wal,
        entry_id: Uuid,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
    ) {
        let failures = self.entry_failures.entry(entry_id).or_insert(0);
        *failures += 1;
        if *failures < MAX_ENTRY_FAILURES {
            return;
        }
        match wal.dead_letter(entry_id).await {
            Ok(path) => {
                tracing::error!(
                    entry_id = %entry_id,
                    tenant_id = %tenant_id,
                    dataset_id = %dataset_id,
                    signal = %signal,
                    failures = *failures,
                    path = %path.display(),
                    "WAL entry exhausted its retries; payload preserved in the dead-letter directory and entry marked processed"
                );
                self.entry_failures.remove(&entry_id);
            }
            Err(e) => {
                tracing::error!(
                    entry_id = %entry_id,
                    tenant_id = %tenant_id,
                    dataset_id = %dataset_id,
                    signal = %signal,
                    error = %e,
                    "Failed to dead-letter WAL entry; it will be retried"
                );
            }
        }
    }

    /// Retire an entry whose payload the WAL cannot read at all (byte range
    /// past the data file, or a record that failed its integrity check).
    ///
    /// The failure is a property of the bytes on disk, so retrying only keeps
    /// the entry pending and pins its segment. [`Wal::dead_letter`] cannot be
    /// used: it re-reads the payload, which is the very read that failed. The
    /// WAL has already quarantined whatever bytes it could and logged the
    /// tenant/dataset/signal/offset; this records the marker and marks the
    /// entry processed so the neighbours keep flowing.
    async fn record_unreadable_entry(&mut self, wal: &Wal, entry: &WalEntry, reason: &str) {
        match wal.dead_letter_unreadable(entry.id, reason).await {
            Ok(path) => {
                tracing::error!(
                    entry_id = %entry.id,
                    tenant_id = %entry.tenant_id,
                    dataset_id = %entry.dataset_id,
                    signal = entry.operation.signal(),
                    data_offset = entry.data_offset,
                    data_size = entry.data_size,
                    path = %path.display(),
                    "WAL entry payload is unreadable; marker recorded in the dead-letter directory and entry marked processed"
                );
                self.entry_failures.remove(&entry.id);
            }
            Err(e) => {
                tracing::error!(
                    entry_id = %entry.id,
                    tenant_id = %entry.tenant_id,
                    dataset_id = %entry.dataset_id,
                    signal = entry.operation.signal(),
                    error = %e,
                    "Failed to retire unreadable WAL entry; it will be retried"
                );
            }
        }
    }

    /// Determine which tenant, dataset, and table an entry should go to
    /// Extracts tenant_id and dataset_id from the WalEntry, but prefers metadata-provided
    /// values (from Flight metadata) when available for proper tenant isolation.
    /// For metrics, uses target_table from metadata if available, enabling routing to
    /// metrics_exponential_histogram, metrics_summary, etc.
    fn determine_target_table(&self, entry: &WalEntry) -> Result<(String, String, String)> {
        let mut tenant_id = entry.tenant_id.clone();
        let mut dataset_id = entry.dataset_id.clone();

        // Parse metadata JSON once and reuse for both tenant/dataset and target_table
        let parsed_metadata = entry
            .metadata
            .as_deref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());

        // Override with metadata-provided tenant/dataset (from Flight metadata)
        if let Some(ref metadata) = parsed_metadata {
            if let Some(tid) = metadata.get("tenant_id").and_then(|v| v.as_str()) {
                tenant_id = tid.to_string();
            }
            if let Some(did) = metadata.get("dataset_id").and_then(|v| v.as_str()) {
                dataset_id = did.to_string();
            }
        }

        // Map operation types to appropriate table
        let table_name = match entry.operation {
            common::wal::WalOperation::WriteTraces => "traces".to_string(),
            common::wal::WalOperation::WriteLogs => "logs".to_string(),
            common::wal::WalOperation::WriteMetrics => {
                // Try to extract target_table from the already-parsed metadata
                parsed_metadata
                    .as_ref()
                    .and_then(|m| m.get("target_table"))
                    .map(|target_table| {
                        if let Some(table_str) = target_table.as_str() {
                            tracing::debug!(
                                target_table = %table_str,
                                entry_id = %entry.id,
                                "Using target_table from metadata"
                            );
                            table_str.to_string()
                        } else {
                            tracing::warn!(
                                "target_table in metadata is not a string, defaulting to metrics_gauge"
                            );
                            "metrics_gauge".to_string()
                        }
                    })
                    .unwrap_or_else(|| {
                        tracing::debug!("No target_table in metadata, defaulting to metrics_gauge");
                        "metrics_gauge".to_string()
                    })
            }
            common::wal::WalOperation::WriteProfiles => "profiles".to_string(),
            common::wal::WalOperation::Flush => {
                return Err(anyhow::anyhow!(
                    "Flush operations should not be processed as table writes"
                ));
            }
        };

        Ok((tenant_id, dataset_id, table_name))
    }

    /// Deserialize WAL entry data back to RecordBatch
    async fn deserialize_entry_data(
        wal: &Wal,
        entry: &WalEntry,
    ) -> std::result::Result<RecordBatch, EntryDecodeError> {
        let data = wal
            .read_entry_data(entry)
            .await
            .map_err(EntryDecodeError::Unreadable)?;
        bytes_to_record_batch(&data).map_err(|e| {
            EntryDecodeError::Undecodable(anyhow::anyhow!(
                "Failed to deserialize WAL entry data: {e}"
            ))
        })
    }

    /// Get statistics about the processor
    pub fn get_stats(&self) -> ProcessorStats {
        ProcessorStats {
            active_writers: self.table_writers.len(),
            writer_keys: self.table_writers.keys().cloned().collect(),
        }
    }

    /// Close all table writers and clean up resources
    pub async fn shutdown(&mut self) -> Result<()> {
        tracing::info!(
            writer_count = self.table_writers.len(),
            "Shutting down WAL processor"
        );

        // Clear all writers (they should handle cleanup automatically when dropped)
        self.table_writers.clear();

        Ok(())
    }
}

/// Statistics about the WAL processor
#[derive(Debug)]
pub struct ProcessorStats {
    pub active_writers: usize,
    pub writer_keys: Vec<String>,
}

/// Add one span link per distinct ingest trace context. The WAL batch fans
/// in entries from many independent requests, so the batch span links to
/// each origin instead of electing a parent (the semconv batch model).
pub(crate) fn link_batch_origins(span: &tracing::Span, trace_contexts: &[EntryTraceContext]) {
    let mut linked = std::collections::HashSet::new();
    for (traceparent, tracestate) in trace_contexts {
        if let Some(traceparent) = traceparent
            && linked.insert(traceparent.clone())
        {
            common::flight::trace_context::add_link_from_fields(
                span,
                Some(traceparent),
                tracestate.as_deref(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::wal::{Wal, WalConfig, WalOperation};
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    /// A manager holding exactly `wal`, so tests that drive one WAL by hand
    /// can feed it to the processor.
    async fn manager_for(wal: &Arc<Wal>) -> Arc<WalManager> {
        let manager = WalManager::uniform(WalConfig::default());
        let displaced = manager
            .register(
                (
                    "test-tenant".to_string(),
                    "test-dataset".to_string(),
                    "traces".to_string(),
                ),
                wal.clone(),
            )
            .await;
        assert!(displaced.is_none(), "fresh manager holds no WAL under key");
        Arc::new(manager)
    }

    #[tokio::test]
    async fn poisoned_tenant_wal_does_not_block_another_tenants_wal() {
        // #932: with one WAL per tenant, a WAL that cannot even be listed (or
        // whose every entry is poison) must not stall the drain of another
        // tenant's WAL in the same cycle.
        let temp_dir = tempdir().unwrap();
        let mut base = WalConfig::with_defaults(temp_dir.path().to_path_buf());
        base.max_buffer_entries = 1000;
        base.flush_interval_secs = 60;
        let manager = Arc::new(WalManager::uniform(base));

        // Tenant A: a poison entry (undecodable payload) sits at the head of
        // its WAL and stays pending for MAX_ENTRY_FAILURES cycles.
        let acme = manager
            .get_wal("acme", "production", "metrics")
            .await
            .unwrap();
        acme.append(
            WalOperation::WriteMetrics,
            b"not arrow ipc".to_vec(),
            Some(r#"{"tenant_id":"acme","dataset_id":"production"}"#.to_string()),
        )
        .await
        .unwrap();
        acme.flush().await.unwrap();

        // Tenant B: a healthy entry.
        let globex = manager
            .get_wal("globex", "production", "metrics")
            .await
            .unwrap();
        globex
            .append(
                WalOperation::WriteMetrics,
                metrics_gauge_bytes(3),
                Some(r#"{"tenant_id":"globex","dataset_id":"production"}"#.to_string()),
            )
            .await
            .unwrap();
        globex.flush().await.unwrap();

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let mut processor = WalProcessor::new(manager.clone(), catalog_manager, object_store);
        processor
            .process_pending_entries()
            .await
            .expect("one tenant's poison must not fail the cycle");

        assert!(
            globex.get_unprocessed_entries().await.unwrap().is_empty(),
            "tenant B's entry must be committed in the same cycle tenant A is poisoned"
        );
        assert_eq!(
            acme.get_unprocessed_entries().await.unwrap().len(),
            1,
            "tenant A's poison entry is still pending (retried, not yet dead-lettered)"
        );

        // Keep cycling until tenant A's poison is retired. The dead-letter
        // marker must land in A's own directory and B's must never appear:
        // the blast radius of a poisoned segment is one tenant/dataset/signal
        // directory.
        for _ in 1..super::MAX_ENTRY_FAILURES {
            processor.process_pending_entries().await.unwrap();
        }
        assert!(
            temp_dir
                .path()
                .join("acme/production/metrics/dead-letter")
                .is_dir(),
            "tenant A's poison must be quarantined under tenant A's own directory"
        );
        assert!(
            !temp_dir
                .path()
                .join("globex/production/metrics/dead-letter")
                .exists()
        );
    }

    /// #932's other half: the writer used to hold one WAL for every tenant,
    /// so every `do_put` serialized on that WAL's segment mutex. With a WAL
    /// per tenant, a tenant hammering its own WAL must not delay another
    /// tenant's append + flush. Needs a multi-threaded runtime: on the
    /// default current-thread runtime the two tasks interleave at await
    /// points and the contention this asserts on cannot be observed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_tenants_busy_wal_does_not_delay_another_tenants_append() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let temp_dir = tempdir().unwrap();
        let mut base = WalConfig::with_defaults(temp_dir.path().to_path_buf());
        // Every append hits the disk, so tenant A's loop holds its segment
        // lock across real writes rather than buffering in memory.
        base.max_buffer_entries = 1;
        base.flush_interval_secs = 3600;
        let manager = Arc::new(WalManager::uniform(base));

        let acme = manager
            .get_wal("acme", "production", "metrics")
            .await
            .unwrap();
        let globex = manager
            .get_wal("globex", "production", "metrics")
            .await
            .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let (first_write_tx, first_write_rx) = tokio::sync::oneshot::channel();
        let hammer = tokio::spawn({
            let acme = acme.clone();
            let stop = stop.clone();
            async move {
                let payload = vec![7u8; 1024 * 1024];
                let mut first_write_tx = Some(first_write_tx);
                while !stop.load(Ordering::Relaxed) {
                    acme.append(WalOperation::WriteMetrics, payload.clone(), None)
                        .await
                        .unwrap();
                    acme.flush().await.unwrap();
                    if let Some(tx) = first_write_tx.take() {
                        let _ = tx.send(());
                    }
                }
            }
        });

        // Only start timing once tenant A is demonstrably mid-flight.
        first_write_rx.await.unwrap();

        let started = std::time::Instant::now();
        globex
            .append(WalOperation::WriteMetrics, metrics_gauge_bytes(1), None)
            .await
            .unwrap();
        globex.flush().await.unwrap();
        let elapsed = started.elapsed();

        stop.store(true, Ordering::Relaxed);
        hammer.await.unwrap();

        assert!(
            elapsed < Duration::from_secs(5),
            "tenant B's append+flush took {elapsed:?} while tenant A was hammering its own WAL; \
             the two must not share a segment lock"
        );
        assert_eq!(
            globex.get_unprocessed_entries().await.unwrap().len(),
            1,
            "tenant B's entry landed in tenant B's WAL"
        );
    }

    fn coalescer(interval_secs: u64, max_rows: usize) -> CommitCoalescer {
        CommitCoalescer::new(&WriterConfig {
            commit_interval: Duration::from_secs(interval_secs),
            max_uncommitted_rows: max_rows,
            ..Default::default()
        })
    }

    #[test]
    fn coalescer_commits_first_data_immediately() {
        // A never-committed group is eligible at once, so first data is not
        // delayed a full interval.
        let c = coalescer(5, 100_000);
        let now = Instant::now();
        assert!(c.should_commit("t:d:traces", 1, now));
    }

    #[test]
    fn coalescer_defers_low_volume_group_within_interval() {
        // After an initial commit, a low-volume group must wait out the
        // interval — many small batches within it yield no further commit.
        let mut c = coalescer(5, 100_000);
        let t0 = Instant::now();
        c.record_commit("t:d:traces", t0);

        for offset_ms in [10u64, 1_000, 4_999] {
            let now = t0 + Duration::from_millis(offset_ms);
            assert!(
                !c.should_commit("t:d:traces", 50, now),
                "should defer at {offset_ms}ms (< interval)"
            );
        }
    }

    #[test]
    fn coalescer_commits_low_volume_group_after_interval() {
        // Liveness: once the interval elapses, even a single row commits.
        let mut c = coalescer(5, 100_000);
        let t0 = Instant::now();
        c.record_commit("t:d:traces", t0);
        let now = t0 + Duration::from_secs(5);
        assert!(c.should_commit("t:d:traces", 1, now));
    }

    #[test]
    fn coalescer_commits_burst_early_on_row_ceiling() {
        // The row ceiling triggers an *earlier* commit even mid-interval.
        let mut c = coalescer(5, 100_000);
        let t0 = Instant::now();
        c.record_commit("t:d:traces", t0);
        let now = t0 + Duration::from_millis(100);
        assert!(c.should_commit("t:d:traces", 100_000, now));
        assert!(c.should_commit("t:d:traces", 250_000, now));
    }

    #[test]
    fn coalescer_tracks_groups_independently() {
        // One group's commit must not restart another group's interval.
        let mut c = coalescer(5, 100_000);
        let t0 = Instant::now();
        c.record_commit("t:d:traces", t0);
        let now = t0 + Duration::from_millis(100);
        // traces is mid-interval (deferred); logs was never committed (eligible).
        assert!(!c.should_commit("t:d:traces", 10, now));
        assert!(c.should_commit("t:d:logs", 10, now));
    }

    #[test]
    fn trace_context_from_metadata_reads_w3c_fields() {
        let meta = Some(
            r#"{"schema_version":"v1","traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","tracestate":"vendor=abc"}"#
                .to_string(),
        );
        let (traceparent, tracestate) = trace_context_from_metadata(&meta);
        assert_eq!(
            traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        assert_eq!(tracestate.as_deref(), Some("vendor=abc"));
    }

    #[test]
    fn trace_context_from_metadata_tolerates_missing_absent_and_garbage() {
        // No metadata at all.
        assert_eq!(trace_context_from_metadata(&None), (None, None));
        // Valid JSON without a trace context (e.g. routing-only metadata).
        assert_eq!(
            trace_context_from_metadata(&Some(r#"{"schema_version":"v1"}"#.to_string())),
            (None, None)
        );
        // Not JSON.
        assert_eq!(
            trace_context_from_metadata(&Some("not json".to_string())),
            (None, None)
        );
        // traceparent absent but tracestate present — traceparent gates linking.
        let (traceparent, _) =
            trace_context_from_metadata(&Some(r#"{"tracestate":"vendor=abc"}"#.to_string()));
        assert!(traceparent.is_none());
    }

    #[tokio::test]
    async fn test_processor_creation() {
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024, // 1MB
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());

        let processor = WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        let stats = processor.get_stats();
        assert_eq!(stats.active_writers, 0);
    }

    #[tokio::test]
    async fn test_determine_target_table() {
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024, // 1MB
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());

        let processor = WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        // Test different operation types
        let entry = WalEntry {
            id: uuid::Uuid::new_v4(),
            operation: WalOperation::WriteTraces,
            data_size: 0,
            data_offset: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            processed: false,
            tenant_id: "acme".to_string(),
            dataset_id: "production".to_string(),
            metadata: None,
        };

        let (tenant, dataset, table) = processor.determine_target_table(&entry).unwrap();
        assert_eq!(tenant, "acme");
        assert_eq!(dataset, "production");
        assert_eq!(table, "traces");

        let entry = WalEntry {
            id: uuid::Uuid::new_v4(),
            operation: WalOperation::WriteLogs,
            data_size: 0,
            data_offset: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            processed: false,
            tenant_id: "globex".to_string(),
            dataset_id: "staging".to_string(),
            metadata: None,
        };

        let (tenant, dataset, table) = processor.determine_target_table(&entry).unwrap();
        assert_eq!(tenant, "globex");
        assert_eq!(dataset, "staging");
        assert_eq!(table, "logs");
    }

    #[tokio::test]
    async fn poison_entry_is_dead_lettered_after_exhausting_retries() {
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024,
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());

        // Garbage bytes: deserialization fails on every attempt. Before
        // the dead-letter path this aborted every processing cycle
        // forever.
        let entry_id = wal
            .append(WalOperation::WriteTraces, b"not arrow ipc".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        let mut processor =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);
        for _ in 0..super::MAX_ENTRY_FAILURES {
            processor
                .process_pending_entries()
                .await
                .expect("a poison entry must not abort the cycle");
        }

        // The entry is out of the way and its payload is preserved.
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "poison entry must be dead-lettered and marked processed"
        );
        let dead_letter_path = temp_dir
            .path()
            .join("dead-letter")
            .join(format!("{}.bin", entry_id.simple()));
        let preserved = tokio::fs::read(&dead_letter_path).await.unwrap();
        assert_eq!(preserved, b"not arrow ipc");
    }

    #[tokio::test]
    async fn corrupt_payload_is_retired_immediately_and_neighbours_still_process() {
        // #946: a payload that fails its record CRC is unreadable for good.
        // The processor must retire it on the first sighting (not after
        // MAX_ENTRY_FAILURES cycles) and keep processing the entries around it.
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024,
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());

        let meta = Some(r#"{"tenant_id":"acme","dataset_id":"production"}"#.to_string());
        let good_before = wal
            .append(
                WalOperation::WriteMetrics,
                metrics_gauge_bytes(1),
                meta.clone(),
            )
            .await
            .unwrap();
        let victim = wal
            .append(
                WalOperation::WriteMetrics,
                metrics_gauge_bytes(2),
                meta.clone(),
            )
            .await
            .unwrap();
        let good_after = wal
            .append(WalOperation::WriteMetrics, metrics_gauge_bytes(3), meta)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        // Flip one payload byte of the middle entry on disk.
        let victim_entry = wal
            .get_entries()
            .await
            .unwrap()
            .into_iter()
            .find(|e| e.id == victim)
            .unwrap();
        let data_path = temp_dir.path().join("wal-0000000000.data");
        let mut bytes = tokio::fs::read(&data_path).await.unwrap();
        let idx =
            victim_entry.data_offset as usize + common::wal::framing::DATA_RECORD_HEADER_LEN + 4;
        bytes[idx] ^= 0x01;
        tokio::fs::write(&data_path, &bytes).await.unwrap();

        let mut processor =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);
        processor
            .process_pending_entries()
            .await
            .expect("a corrupt payload must not abort the cycle");

        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "good neighbours must be committed and the corrupt entry retired in one cycle"
        );
        let dead_letter = temp_dir.path().join("dead-letter");
        assert!(
            dead_letter
                .join(format!("{}.unreadable.json", victim.simple()))
                .exists(),
            "unreadable marker must be recorded"
        );
        assert!(
            dead_letter
                .join(format!("{}.corrupt.bin", victim.simple()))
                .exists(),
            "corrupt record bytes must be quarantined by the WAL"
        );
        for good in [good_before, good_after] {
            assert!(
                !dead_letter.join(format!("{}.bin", good.simple())).exists(),
                "healthy neighbours must not be dead-lettered"
            );
        }
    }

    #[tokio::test]
    async fn system_tenant_wal_processing_is_suppressed_from_otel_export() {
        // Regression test for issue #760: the background WAL-processing
        // loop must not emit log records that pass the OTel export filter
        // while processing _system-tenant entries — they would be exported
        // and re-ingested as _system telemetry (feedback loop). A normal
        // tenant's processing logs must still export.
        //
        // Garbage payloads route fine but fail deserialization, which
        // makes the loop emit warn-level logs — exactly the kind of
        // telemetry that fed the loop.
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024,
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "default".to_string(),
            dataset_id: "default".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let mut processor =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        // _system-tenant entry: nothing may pass the export filter.
        wal.append(
            WalOperation::WriteTraces,
            b"not arrow ipc".to_vec(),
            Some(r#"{"tenant_id":"_system","dataset_id":"_monitoring"}"#.to_string()),
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();

        let probe = common::testing::OtelExportProbe::new();
        {
            let _guard = probe.install();
            processor.process_pending_entries().await.unwrap();
        }
        assert_eq!(
            probe.exported_events(),
            0,
            "_system WAL entry processing must not export log records"
        );

        // Normal-tenant entry: processing logs still export.
        wal.append(
            WalOperation::WriteTraces,
            b"not arrow ipc".to_vec(),
            Some(r#"{"tenant_id":"acme","dataset_id":"production"}"#.to_string()),
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();

        let probe = common::testing::OtelExportProbe::new();
        {
            let _guard = probe.install();
            processor.process_pending_entries().await.unwrap();
        }
        assert!(
            probe.exported_events() > 0,
            "normal tenant WAL entry processing must still export telemetry"
        );
    }

    #[tokio::test]
    async fn test_process_pending_entries_empty() {
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024, // 1MB
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());

        let mut processor =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        // Should handle empty entries gracefully
        let result = processor.process_pending_entries().await;
        assert!(result.is_ok());
    }

    use crate::test_support::metrics_gauge_bytes;

    fn coalescing_wal_config(dir: &std::path::Path) -> WalConfig {
        WalConfig {
            wal_dir: dir.to_path_buf(),
            max_segment_size: 8 * 1024 * 1024,
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "acme".to_string(),
            dataset_id: "production".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        }
    }

    #[tokio::test]
    async fn force_commit_pending_is_noop_when_nothing_pending() {
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let mut processor =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        // No pending entries: force-commit must succeed and commit nothing.
        processor
            .force_commit_pending(FlushScope {
                tenant_id: "acme".to_string(),
                dataset_id: Some("production".to_string()),
            })
            .await
            .unwrap();
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn force_commit_drains_a_group_the_floor_would_defer() {
        // A very large interval means any group committed once is deferred for
        // the rest of the test; force-commit must bypass that and drain it.
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let config = WriterConfig {
            commit_interval: Duration::from_secs(3600),
            max_uncommitted_rows: 1_000_000,
            ..Default::default()
        };
        let mut processor = WalProcessor::with_config(
            manager_for(&wal).await,
            catalog_manager,
            object_store,
            &config,
        );

        let meta = Some(r#"{"target_table":"metrics_gauge"}"#.to_string());

        // First data for the group commits immediately (never-committed → eligible),
        // which records the group's commit time and starts its interval.
        wal.append(
            WalOperation::WriteMetrics,
            metrics_gauge_bytes(5),
            meta.clone(),
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();
        processor.process_pending_entries().await.unwrap();
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "first group commit should have processed the entry"
        );

        // A second small batch for the same group is now within the (huge)
        // interval, so the floor defers it.
        wal.append(WalOperation::WriteMetrics, metrics_gauge_bytes(5), meta)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        processor.process_pending_entries().await.unwrap();
        assert_eq!(
            wal.get_unprocessed_entries().await.unwrap().len(),
            1,
            "second batch must be deferred by the coalescing floor"
        );

        // Force-commit ignores the floor and drains it.
        processor
            .force_commit_pending(FlushScope {
                tenant_id: "acme".to_string(),
                dataset_id: Some("production".to_string()),
            })
            .await
            .unwrap();
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "force-commit must drain the deferred group"
        );
    }

    #[tokio::test]
    async fn flush_marker_drains_deferred_group_and_is_retired() {
        // A `Flush` WAL marker triggers the same drain as force-commit, and the
        // marker itself is marked processed so it does not linger.
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let config = WriterConfig {
            commit_interval: Duration::from_secs(3600),
            max_uncommitted_rows: 1_000_000,
            ..Default::default()
        };
        let mut processor = WalProcessor::with_config(
            manager_for(&wal).await,
            catalog_manager,
            object_store,
            &config,
        );

        let meta = Some(r#"{"target_table":"metrics_gauge"}"#.to_string());

        wal.append(
            WalOperation::WriteMetrics,
            metrics_gauge_bytes(5),
            meta.clone(),
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();
        processor.process_pending_entries().await.unwrap();

        // Deferred second batch.
        wal.append(WalOperation::WriteMetrics, metrics_gauge_bytes(5), meta)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        processor.process_pending_entries().await.unwrap();
        assert_eq!(wal.get_unprocessed_entries().await.unwrap().len(), 1);

        // A Flush marker forces the drain via the normal processing loop.
        wal.append(WalOperation::Flush, Vec::new(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        processor.process_pending_entries().await.unwrap();
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "Flush marker must drain the deferred group and retire itself"
        );
    }

    #[tokio::test]
    async fn scoped_flush_commits_only_the_requested_tenant() {
        // A scoped flush must force-commit only its tenant's groups; other
        // tenants keep coalescing, so one flush can't amplify their commits.
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let config = WriterConfig {
            commit_interval: Duration::from_secs(3600),
            max_uncommitted_rows: 1_000_000,
            ..Default::default()
        };
        let mut processor = WalProcessor::with_config(
            manager_for(&wal).await,
            catalog_manager,
            object_store,
            &config,
        );

        let meta_a = Some(
            r#"{"tenant_id":"acme","dataset_id":"production","target_table":"metrics_gauge"}"#
                .to_string(),
        );
        let meta_b = Some(
            r#"{"tenant_id":"globex","dataset_id":"staging","target_table":"metrics_gauge"}"#
                .to_string(),
        );

        // First data for both tenants commits immediately (never-committed
        // groups), which records their commit time and starts their intervals.
        wal.append(
            WalOperation::WriteMetrics,
            metrics_gauge_bytes(5),
            meta_a.clone(),
        )
        .await
        .unwrap();
        wal.append(
            WalOperation::WriteMetrics,
            metrics_gauge_bytes(5),
            meta_b.clone(),
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();
        processor.process_pending_entries().await.unwrap();
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());

        // Second batch for both — now both are within the (huge) interval, so
        // the floor would defer them.
        wal.append(WalOperation::WriteMetrics, metrics_gauge_bytes(5), meta_a)
            .await
            .unwrap();
        let globex_id = wal
            .append(WalOperation::WriteMetrics, metrics_gauge_bytes(5), meta_b)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        // Flush scoped to acme: acme's batch commits, globex's stays deferred.
        processor
            .force_commit_pending(FlushScope {
                tenant_id: "acme".to_string(),
                dataset_id: None,
            })
            .await
            .unwrap();

        let unprocessed = wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(
            unprocessed.len(),
            1,
            "only globex's entry should remain deferred after an acme-scoped flush"
        );
        assert_eq!(unprocessed[0].id, globex_id);
    }

    #[tokio::test]
    async fn deferred_entries_survive_a_processor_restart() {
        // do_put now acks after WAL flush without committing; the commit is the
        // background loop's job. An entry acked but not yet committed must be
        // committed after a restart (a fresh processor over the same WAL),
        // preserving at-least-once delivery.
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let meta = Some(r#"{"target_table":"metrics_gauge"}"#.to_string());

        // Acked-but-uncommitted: appended and flushed to the WAL, no processing.
        wal.append(WalOperation::WriteMetrics, metrics_gauge_bytes(5), meta)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        assert_eq!(wal.get_unprocessed_entries().await.unwrap().len(), 1);

        // "Restart": a brand-new processor (fresh coalescer state, same catalog
        // + object store) over the same WAL commits the pending entry.
        let mut restarted =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);
        restarted.process_pending_entries().await.unwrap();
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "a restarted processor must commit entries acked before the crash"
        );
    }

    use crate::test_support::schema_mismatched_bytes;

    #[tokio::test]
    async fn force_commit_reports_error_when_a_group_fails_to_commit() {
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let mut processor =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        // Routes to metrics_gauge but the batch schema does not match, so the
        // commit fails. A read-your-writes drain must not report success.
        wal.append(
            WalOperation::WriteMetrics,
            schema_mismatched_bytes(),
            Some(r#"{"target_table":"metrics_gauge"}"#.to_string()),
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();

        assert!(
            processor
                .force_commit_pending(FlushScope {
                    tenant_id: "acme".to_string(),
                    dataset_id: Some("production".to_string()),
                })
                .await
                .is_err(),
            "force-commit must surface a group commit failure"
        );
        assert_eq!(
            wal.get_unprocessed_entries().await.unwrap().len(),
            1,
            "the uncommitted entry must remain for retry, not be dropped"
        );
    }

    #[tokio::test]
    async fn flush_marker_is_retained_when_forced_drain_fails() {
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let mut processor =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        wal.append(
            WalOperation::WriteMetrics,
            schema_mismatched_bytes(),
            Some(r#"{"target_table":"metrics_gauge"}"#.to_string()),
        )
        .await
        .unwrap();
        wal.append(WalOperation::Flush, Vec::new(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        // The bad group fails, so the forced drain (triggered by the marker)
        // returns an error and the Flush marker must not retire — the drain it
        // requested did not complete.
        assert!(processor.process_pending_entries().await.is_err());
        let unprocessed = wal.get_unprocessed_entries().await.unwrap();
        assert!(
            unprocessed
                .iter()
                .any(|e| matches!(e.operation, WalOperation::Flush)),
            "Flush marker must be retained when its forced drain fails"
        );
    }

    /// Pins the WAL fan-in model (`otel-compliant-self-tracing` group 9): a
    /// batch span links to each distinct source ingest trace — one link per
    /// origin, deduplicated, never a parent.
    #[tokio::test]
    async fn batch_span_links_each_distinct_origin() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::prelude::*;

        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        let contexts: Vec<super::EntryTraceContext> = vec![
            (
                Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
                None,
            ),
            (
                Some("00-1bf7651916cd43dd8448eb211c80319d-c7ad6b7169203332-01".into()),
                None,
            ),
            // Duplicate of the first — must dedupe.
            (
                Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
                None,
            ),
            (
                Some("00-2cf7651916cd43dd8448eb211c80319e-d7ad6b7169203333-01".into()),
                None,
            ),
            // No context recorded for this entry.
            (None, None),
        ];

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("batch");
            super::link_batch_origins(&span, &contexts);
            let _guard = span.enter();
        });

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|s| s.name == "batch")
            .expect("batch span");
        assert_eq!(span.links.len(), 3, "one link per distinct origin trace");
        assert_eq!(
            span.parent_span_id,
            opentelemetry::trace::SpanId::INVALID,
            "batch span must not adopt any origin as parent"
        );
    }
}
