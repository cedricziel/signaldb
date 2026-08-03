use crate::storage::IcebergTableWriter;
use anyhow::{Context, Result};
use common::CatalogManager;
use common::config::WriterConfig;
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

/// Entries grouped for one table's batch write: the `(id, batch)` payloads to
/// commit, paired with the trace context each entry arrived with.
type TableBatch = (Vec<(Uuid, RecordBatch)>, Vec<EntryTraceContext>);

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
fn trace_context_from_metadata(metadata: &Option<String>) -> (Option<String>, Option<String>) {
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
    wal: Arc<Wal>,
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
        wal: Arc<Wal>,
        catalog_manager: Arc<CatalogManager>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::with_config(wal, catalog_manager, object_store, &WriterConfig::default())
    }

    /// Create a new WAL processor with an explicit commit-coalescing policy.
    pub fn with_config(
        wal: Arc<Wal>,
        catalog_manager: Arc<CatalogManager>,
        object_store: Arc<dyn ObjectStore>,
        writer_config: &WriterConfig,
    ) -> Self {
        Self {
            wal,
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
    #[tracing::instrument(level = "debug", skip_all, fields(tenant_id = %scope.tenant_id))]
    pub async fn force_commit_pending(&mut self, scope: FlushScope) -> Result<()> {
        self.drain_pending(vec![scope]).await
    }

    /// Shared drain implementation. Groups matching any entry in `flush_scopes`
    /// are force-committed (floor bypassed); all others follow the coalescing
    /// floor. The background loop passes an empty `flush_scopes` (pure
    /// coalescing); `Flush` WAL markers contribute their own tenant/dataset
    /// scope.
    async fn drain_pending(&mut self, mut flush_scopes: Vec<FlushScope>) -> Result<()> {
        let pending_entries = self.wal.get_unprocessed_entries().await?;

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
        let mut grouped_entries: HashMap<(String, String, String), TableBatch> = HashMap::new();

        // A `Flush` marker is a force-commit request scoped to its own
        // tenant/dataset: it carries no data, but force-commits that scope's
        // pending groups this cycle (bypassing the floor), and is marked
        // processed once drained.
        let mut flush_marker_ids: Vec<Uuid> = Vec::new();

        for entry in pending_entries {
            if matches!(entry.operation, common::wal::WalOperation::Flush) {
                flush_marker_ids.push(entry.id);
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
                self.deserialize_entry_data(&entry),
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
                        self.record_entry_failure(
                            entry.id,
                            &entry.tenant_id,
                            &entry.dataset_id,
                            entry.operation.signal(),
                        )
                        .await;
                    })
                    .await;
                    continue;
                }
            };

            let trace_ctx = trace_context_from_metadata(&entry.metadata);
            let group = grouped_entries
                .entry((tenant_id, dataset_id, table_name))
                .or_default();
            group.0.push((entry.id, batch));
            group.1.push(trace_ctx);
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
        for ((tenant_id, dataset_id, table_name), (entries, trace_contexts)) in grouped_entries {
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
            for flush_id in flush_marker_ids {
                if let Err(e) = self.wal.mark_processed(flush_id).await {
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
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            entry_count = entries.len()
        )
    )]
    async fn process_batch_for_table(
        &mut self,
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
        let span = tracing::Span::current();
        let mut linked = std::collections::HashSet::new();
        for (traceparent, tracestate) in &trace_contexts {
            if let Some(traceparent) = traceparent
                && linked.insert(traceparent.clone())
            {
                common::flight::trace_context::add_link_from_fields(
                    &span,
                    Some(traceparent),
                    tracestate.as_deref(),
                );
            }
        }

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

        let wal = self.wal.clone();
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
        let mut processed_ids = Vec::new();
        let mut fresh = Vec::new();
        for (entry_id, batch) in entries {
            if committed.contains(&entry_id) {
                wal.mark_processed(entry_id).await.with_context(|| {
                    format!("Failed to mark already-committed WAL entry {entry_id} as processed")
                })?;
                tracing::info!(
                    entry_id = %entry_id,
                    table_name = %table_name,
                    "Skipping re-insert of WAL entry already committed to Iceberg"
                );
                processed_ids.push(entry_id);
            } else {
                fresh.push((entry_id, batch));
            }
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

            for entry_id in &chunk_ids {
                // On failure the entry stays unprocessed but its id is in
                // the marker, so the next tick re-marks instead of
                // re-inserting.
                wal.mark_processed(*entry_id).await.with_context(|| {
                    format!("Failed to mark WAL entry {entry_id} as processed after commit")
                })?;
            }
            processed_ids.extend(chunk_ids);
        }

        Ok(processed_ids)
    }

    /// Record a processing failure for an entry, dead-lettering it once
    /// it exhausts its attempts.
    async fn record_entry_failure(
        &mut self,
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
        match self.wal.dead_letter(entry_id).await {
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
    async fn deserialize_entry_data(&self, entry: &WalEntry) -> Result<RecordBatch> {
        let data = self.wal.read_entry_data(entry).await?;
        bytes_to_record_batch(&data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize WAL entry data: {}", e))
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::wal::{Wal, WalConfig, WalOperation};
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    fn coalescer(interval_secs: u64, max_rows: usize) -> CommitCoalescer {
        CommitCoalescer::new(&WriterConfig {
            commit_interval: Duration::from_secs(interval_secs),
            max_uncommitted_rows: max_rows,
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

        let processor = WalProcessor::new(wal, catalog_manager, object_store);
        assert_eq!(processor.table_writers.len(), 0);

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

        let processor = WalProcessor::new(wal, catalog_manager, object_store);

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

        let mut processor = WalProcessor::new(wal.clone(), catalog_manager, object_store);
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
        let mut processor = WalProcessor::new(wal.clone(), catalog_manager, object_store);

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

        let mut processor = WalProcessor::new(wal, catalog_manager, object_store);

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
        let mut processor = WalProcessor::new(wal.clone(), catalog_manager, object_store);

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
        };
        let mut processor =
            WalProcessor::with_config(wal.clone(), catalog_manager, object_store, &config);

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
        };
        let mut processor =
            WalProcessor::with_config(wal.clone(), catalog_manager, object_store, &config);

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
        };
        let mut processor =
            WalProcessor::with_config(wal.clone(), catalog_manager, object_store, &config);

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
        let mut restarted = WalProcessor::new(wal.clone(), catalog_manager, object_store);
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
        let mut processor = WalProcessor::new(wal.clone(), catalog_manager, object_store);

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
        let mut processor = WalProcessor::new(wal.clone(), catalog_manager, object_store);

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
}
