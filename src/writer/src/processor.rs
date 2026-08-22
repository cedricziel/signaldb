use crate::routing::{self, RouteMetadata, RouteTarget};
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
use tokio::time::Duration;
use uuid::Uuid;

/// Maximum WAL entries committed per Iceberg transaction. Bounds the size
/// of the idempotency marker written with each commit (one uuid per entry).
const MAX_ENTRIES_PER_COMMIT: usize = 1024;

/// Consecutive failures after which an entry is dead-lettered: its raw
/// payload is preserved under `<wal_dir>/dead-letter/` and the entry is
/// marked processed so it stops blocking ingestion.
const MAX_ENTRY_FAILURES: u32 = 10;

/// How many groups commit at once in one drain cycle.
///
/// Each in-flight commit holds a Parquet write plus an Iceberg/catalog round
/// trip, so this bounds concurrent object-store and catalog load rather than
/// CPU. High enough that a few slow tenants cannot serialize the rest,
/// low enough not to stampede the catalog — whose contention is what made
/// commits slow in the first place (#959/#895).
const COMMIT_CONCURRENCY: usize = 8;

/// Hard cap on non-`Flush` entries taken from one WAL in one drain cycle,
/// independent of `[writer].max_drain_bytes_per_cycle`. The byte budget alone
/// does not bound cycle work for a WAL with a very large number of small
/// entries (near-zero `data_size` each); this backstops that case.
const MAX_DRAIN_ENTRIES_PER_CYCLE: usize = 65_536;

/// The `signaldb.tenant.id` attribute for a processor metric, per the
/// registry's `registry.signaldb.tenancy` group (`otel/registry/signaldb.yaml`)
/// — mirrors `reconcile::provisioning_attrs`, which exists for the same
/// reason: a bare `tenant` key drifts from the reconciler's metrics and trips
/// `weaver registry live-check`.
fn tenant_attr(tenant_id: &str) -> opentelemetry::KeyValue {
    opentelemetry::KeyValue::new("signaldb.tenant.id", tenant_id.to_string())
}

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

/// What one group's commit attempt did, collected from the concurrent fan-out
/// so the cycle can report its aggregate without shared counters.
#[derive(Debug)]
enum GroupOutcome {
    /// Committed, or failed in a way the background loop absorbs.
    Committed,
    /// Left for a later tick by the coalescing floor.
    Deferred,
    /// A group covered by a flush scope did not commit, so a read-your-writes
    /// caller must be told its drain failed.
    ForcedCommitFailed,
}

/// Grouping key for one processing cycle: which WAL the entries live in, then
/// tenant, dataset, and table. Including the WAL keeps every group
/// single-sourced, so marking and the per-WAL idempotency marker stay exact
/// even when two WALs feed the same table (a legacy root WAL being drained
/// alongside the tenant's own, see [`WalManager::adopt_root_segments`]). The
/// WAL is identified by the address of the cached `Arc<Wal>`, which the
/// manager keeps alive for the whole cycle — cheaper and more direct than
/// re-deriving its `writer_id` string for every entry.
///
/// That pointer identity is sound **only** for a value confined to one cycle.
/// A key that outlives the cycle (a resident memtable group, a cache entry)
/// must identify the WAL by [`Wal::writer_id`] instead: the manager may evict
/// a WAL and create another, and the allocator is free to hand the new one the
/// freed address (ABA). Two WALs' entries would then merge into one group,
/// which writes one WAL's idempotency marker over another's entries — visible
/// as duplicated rows on the retry path, never as an error.
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

/// Bound one WAL's contribution to a drain cycle, oldest first: `entries` is
/// consumed in its incoming order (the order [`Wal::get_unprocessed_entries`]
/// returns, i.e. write order) and split into what this cycle takes and what
/// stays for a later one.
///
/// `max_bytes` is the running cap on `Σ data_size` of *data* entries taken
/// (`0` disables it — take everything, subject only to
/// [`MAX_DRAIN_ENTRIES_PER_CYCLE`]). A data entry is deferred rather than
/// taken once including it would push the running total over the budget, so
/// the cap is never exceeded within a cycle.
///
/// `Flush` markers are always taken regardless of the budget or the entry
/// count: they carry no data to decode, and a scoped flush request must not
/// be starved by an unrelated backlog sitting ahead of it in the same WAL.
///
/// Returns `(taken, deferred_count)`; `taken` preserves the original order.
fn apply_drain_budget(entries: Vec<WalEntry>, max_bytes: u64) -> (Vec<WalEntry>, usize) {
    let mut taken = Vec::with_capacity(entries.len());
    let mut deferred = 0usize;
    let mut bytes_taken = 0u64;
    let mut data_entries_taken = 0usize;

    for entry in entries {
        if matches!(entry.operation, common::wal::WalOperation::Flush) {
            taken.push(entry);
            continue;
        }

        let would_exceed_bytes = max_bytes != 0 && bytes_taken + entry.data_size > max_bytes;
        let would_exceed_count = data_entries_taken >= MAX_DRAIN_ENTRIES_PER_CYCLE;
        if would_exceed_bytes || would_exceed_count {
            deferred += 1;
            continue;
        }

        bytes_taken += entry.data_size;
        data_entries_taken += 1;
        taken.push(entry);
    }

    (taken, deferred)
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
    /// Cache of table writers per tenant/table combination.
    ///
    /// Two levels of locking, because groups commit concurrently (#1306): the
    /// outer lock is held only to look a writer up or insert one, the inner
    /// one for the length of that table's commit. Different tables therefore
    /// commit in parallel while one table's commits stay serialized — which
    /// they must be, since a commit reloads the table and rewrites the
    /// idempotency marker.
    table_writers: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<IcebergTableWriter>>>>,
    /// Consecutive processing failures per entry. Entries that keep
    /// failing are dead-lettered so one poison entry cannot wedge the
    /// processing loop forever (in-memory: a restart grants a fresh set
    /// of attempts, which is fine — dead-lettering only needs to happen
    /// eventually).
    entry_failures: tokio::sync::Mutex<HashMap<Uuid, u32>>,
    /// Commit-coalescing gate per `(tenant, dataset, table)`.
    coalescer: tokio::sync::Mutex<CommitCoalescer>,
    /// How long another writer's idempotency marker is kept before it is
    /// retired from a table. Zero disables retirement.
    wal_marker_retention: Duration,
    /// Byte budget for how much backlog one drain cycle decodes from a
    /// single WAL, oldest entries first. Zero disables the budget. See
    /// [`apply_drain_budget`].
    max_drain_bytes_per_cycle: u64,
    /// When this processor started, and when it last swept markers. Both are
    /// needed: the sweep is throttled, and a marker with no recorded commit
    /// time (written before markers were dated) may only be retired once this
    /// process has itself outlived the retention window.
    started_at: Instant,
    last_marker_sweep: tokio::sync::Mutex<Option<Instant>>,
    /// Group commits currently in flight, and the high-water mark across the
    /// processor's life. Test-only: the point of the concurrent drain is that
    /// groups overlap, and overlap is a property of control flow that wall
    /// clock can only hint at.
    #[cfg(test)]
    in_flight_commits: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    peak_concurrent_commits: std::sync::atomic::AtomicUsize,
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
            table_writers: tokio::sync::Mutex::new(HashMap::new()),
            entry_failures: tokio::sync::Mutex::new(HashMap::new()),
            coalescer: tokio::sync::Mutex::new(CommitCoalescer::new(writer_config)),
            wal_marker_retention: writer_config.wal_marker_retention,
            max_drain_bytes_per_cycle: writer_config.max_drain_bytes_per_cycle,
            started_at: Instant::now(),
            last_marker_sweep: tokio::sync::Mutex::new(None),
            #[cfg(test)]
            in_flight_commits: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            peak_concurrent_commits: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Every writer id this process owns — one per cached WAL directory.
    ///
    /// Markers are keyed per WAL directory, so all of them are live evidence
    /// for commits this process is still making and none may be retired.
    ///
    /// This is the *cached* set, not every WAL directory on disk, and that is
    /// only sound because of two invariants elsewhere:
    /// `WalManager::discover_existing_wals` opens every on-disk WAL that still
    /// has segments at startup, and `WalManager::evict_idle` never evicts a
    /// WAL that holds unprocessed entries. An uncached WAL is therefore fully
    /// drained, so its marker protects nothing. **If either invariant
    /// changes, this exclusion set silently starts under-reporting and a live
    /// writer's marker becomes retirable.**
    async fn own_marker_writer_ids(&self) -> std::collections::HashSet<String> {
        self.wal_manager
            .all_wals()
            .await
            .iter()
            .map(|(_, wal)| wal.writer_id().to_string())
            .collect()
    }

    /// Process pending WAL entries subject to the commit-coalescing floor: a
    /// group is committed only once its rows reach `max_uncommitted_rows` or
    /// `commit_interval` has elapsed since its last commit. Sub-floor groups
    /// are left unprocessed for a later tick.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn process_pending_entries(&mut self) -> Result<()> {
        let drained = self.drain_pending(Vec::new()).await;

        // Reclaim WAL disk here and NOT in `drain_pending`, so the sweep never
        // lands inside `do_action("flush")`: that RPC is bounded by
        // `FLUSH_TIMEOUT` precisely so a slow catalog cannot hang a client, and
        // a compaction sweep over every tenant's WAL would spend that budget on
        // work the caller did not ask for. This background pass has no deadline.
        //
        // Cleanup runs *between* drains, never during one — compaction moves
        // surviving entries to new offsets — and under the processor lock this
        // pass already holds, so no other drain can be mid-pass. The manager
        // throttles how often a sweep actually runs.
        self.wal_manager.cleanup_all_if_due().await;
        self.retire_stale_markers_if_due().await;

        drained
    }

    /// How often the marker sweep is allowed to run. Retirement is bounded by
    /// a retention window measured in days, so checking hourly converges just
    /// as fast while keeping the metadata-only commits rare.
    const MARKER_SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

    /// Retire other writers' stale idempotency markers from the tables this
    /// processor commits to, at most once per [`Self::MARKER_SWEEP_INTERVAL`].
    ///
    /// Only cached writers are swept: those are exactly the tables this writer
    /// commits to, which is where its own markers live and where foreign ones
    /// accumulate. Failures are logged, never fatal — an unretired marker
    /// costs metadata size, and losing the guarded commit to a concurrent
    /// writer is the guard working.
    ///
    /// Known scope limit: a table that no live writer commits to any more —
    /// a tenant that stopped reporting entirely — is never swept by anyone,
    /// so its markers stay. Growth is bounded for active tables, which is
    /// where markers actually accumulate; sweeping the dormant remainder
    /// needs a pass that iterates the whole table universe (the signal-table
    /// reconciler already does), tracked separately.
    async fn retire_stale_markers_if_due(&self) {
        if self.wal_marker_retention.is_zero() {
            return;
        }
        {
            let mut last = self.last_marker_sweep.lock().await;
            let due = last.is_none_or(|at| at.elapsed() >= Self::MARKER_SWEEP_INTERVAL);
            if !due {
                return;
            }
            *last = Some(Instant::now());
        }

        // An undated marker predates dated markers entirely, so it could
        // belong to a writer that is healthy but has not committed since the
        // deploy. Outliving the window proves otherwise: a live writer would
        // have rewritten it with a dated one by now.
        let process_outlived_retention = self.started_at.elapsed() >= self.wal_marker_retention;

        let writers: Vec<(String, Arc<tokio::sync::Mutex<IcebergTableWriter>>)> = self
            .table_writers
            .lock()
            .await
            .iter()
            .map(|(key, writer)| (key.clone(), writer.clone()))
            .collect();

        let own_writer_ids = self.own_marker_writer_ids().await;
        let mut retired = 0usize;
        for (writer_key, writer) in writers {
            // The writer id is per WAL directory, and a table's markers are
            // keyed by it; ours is whichever WAL last committed to this table.
            let mut writer = writer.lock().await;
            match writer
                .retire_stale_markers(
                    &own_writer_ids,
                    self.wal_marker_retention,
                    process_outlived_retention,
                )
                .await
            {
                Ok(count) => retired += count,
                Err(e) => tracing::debug!(
                    writer_key = %writer_key,
                    error = %e,
                    "Could not retire stale WAL markers this pass; they stay until the next one"
                ),
            }
        }

        if retired > 0 {
            tracing::info!(
                retired,
                "Retired WAL idempotency markers left by writer ids past their retention window"
            );
        }
    }

    /// Immediately commit the pending groups covered by `scope`, ignoring the
    /// coalescing floor for them only. Groups outside the scope keep normal
    /// coalescing. The read-your-writes drain used by the force-commit primitive.
    ///
    /// One [`Self::drain_pending`] call only commits what fits under
    /// `max_drain_bytes_per_cycle`, so this loops until `scope`'s backlog is
    /// actually empty rather than returning after one partial cycle — a
    /// caller asking to flush its own just-written data expects it to be
    /// queryable when this returns, not merely "less pending than before".
    /// Unbounded here on purpose: the caller (`do_action("flush")`) wraps
    /// this in `FLUSH_TIMEOUT`, which cancels the whole call if the scope's
    /// backlog cannot drain in time.
    #[tracing::instrument(level = "debug", skip_all, fields(signaldb.tenant.id = %scope.tenant_id))]
    pub async fn force_commit_pending(&mut self, scope: FlushScope) -> Result<()> {
        loop {
            self.drain_pending(vec![scope.clone()]).await?;
            if !self.scope_has_unprocessed(&scope).await? {
                return Ok(());
            }
        }
    }

    /// Whether any WAL still holds an unprocessed entry within `scope`.
    /// Drives [`Self::force_commit_pending`]'s drain loop: a cycle bounded by
    /// the drain budget can leave `scope`'s backlog partially drained, and
    /// this is how the loop tells "partially" from "done".
    async fn scope_has_unprocessed(&self, scope: &FlushScope) -> Result<bool> {
        for ((tenant, dataset, _signal), wal) in self.wal_manager.all_wals().await {
            if !scope.matches(&tenant, &dataset) {
                continue;
            }
            if !wal.get_unprocessed_entries().await?.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Shared drain implementation. Groups matching any entry in `flush_scopes`
    /// are force-committed (floor bypassed); all others follow the coalescing
    /// floor. The background loop passes an empty `flush_scopes` (pure
    /// coalescing); `Flush` WAL markers contribute their own tenant/dataset
    /// scope.
    ///
    /// Each WAL contributes at most `max_drain_bytes_per_cycle` bytes of data
    /// entries to this cycle, oldest first (see [`apply_drain_budget`]);
    /// anything past that stays unprocessed for a later cycle. This bounds
    /// how much Arrow one tick decodes, so a large post-outage backlog drains
    /// over several ticks instead of being decoded to memory in one.
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
        // Entries a WAL's backlog holds beyond this cycle's byte budget
        // (`apply_drain_budget`): still durable and unprocessed, just left
        // for a later cycle. Tracked separately from `list_failures`, which
        // is entries this cycle never even saw.
        let mut budget_deferred_entries = 0usize;
        for ((tenant, dataset, signal), wal) in self.wal_manager.all_wals().await {
            match wal.get_unprocessed_entries().await {
                Ok(entries) => {
                    let (taken, deferred) =
                        apply_drain_budget(entries, self.max_drain_bytes_per_cycle);
                    budget_deferred_entries += deferred;
                    pending_entries.extend(taken.into_iter().map(|e| (wal.clone(), e)));
                }
                Err(e) => {
                    list_failures += 1;
                    common::self_monitoring::app_metrics()
                        .wal_list_failures
                        .add(
                            1,
                            &[
                                tenant_attr(&tenant),
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
        if budget_deferred_entries > 0 {
            tracing::warn!(
                budget_deferred_entries,
                max_drain_bytes_per_cycle = self.max_drain_bytes_per_cycle,
                "WAL entries left unprocessed this cycle by the per-cycle drain budget; they remain durable and will be retried"
            );
        }
        common::self_monitoring::app_metrics()
            .writer_entries_deferred_by_budget
            .record(budget_deferred_entries as u64, &[]);

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
            let RouteTarget {
                tenant_id,
                dataset_id,
                table_name,
            } = routed;
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

        // Groups commit concurrently. The per-tenant WAL fanout (#932)
        // isolated failures but not latency: committing one group at a time
        // meant a tenant whose Iceberg round trip took tens of seconds delayed
        // every other tenant's commit in the same cycle (#1306). Groups are
        // independent — separate WAL entries, separate tables, per-table
        // writer locks — so the cycle now costs the slowest single group
        // rather than the sum of all of them.
        use futures::StreamExt as _;
        // Shared reborrow: the closure is FnMut, so it cannot capture the
        // `&mut self` this method holds. Every method it calls now takes
        // `&self` and reaches its state through interior mutability.
        let this = &*self;
        let outcomes: Vec<GroupOutcome> = futures::stream::iter(grouped_entries)
            .map(
                |(
                    (_, tenant_id, dataset_id, table_name),
                    TableBatch {
                        wal,
                        entries,
                        trace_contexts,
                    },
                )| {
                    let flush_scopes = &flush_scopes;
                    async move {
                        let writer_key = format!("{tenant_id}:{dataset_id}:{table_name}");

                        // A group is force-committed only when a flush scope
                        // (explicit request or a Flush marker) covers its
                        // tenant/dataset; unrelated tenants keep normal
                        // coalescing so one flush can't amplify their commits.
                        let forced = flush_scopes
                            .iter()
                            .any(|s| s.matches(&tenant_id, &dataset_id));

                        // Coalescing floor: defer this group's commit unless
                        // forced, its rows have reached the ceiling, or its
                        // interval has elapsed. The entries stay unprocessed
                        // (durable in the WAL) and are revisited on a later
                        // tick — capping commit rate at ~1 per interval per
                        // table regardless of ingest rate (#888).
                        let pending_rows: usize = entries.iter().map(|(_, b)| b.num_rows()).sum();
                        if !forced
                            && !this
                                .coalescer
                                .lock()
                                .await
                                .should_commit(&writer_key, pending_rows, now)
                        {
                            tracing::debug!(
                                tenant_id = %tenant_id,
                                table_name = %table_name,
                                pending_rows,
                                "Deferring commit: below coalescing floor"
                            );
                            return GroupOutcome::Deferred;
                        }

                        let group_ids: Vec<Uuid> = entries.iter().map(|(id, _)| *id).collect();
                        // Anti-loop guard (#760): suppression is per group
                        // because groups interleave tenants — only the _system
                        // tenant's batches must not be re-instrumented.
                        let suppress =
                            common::self_monitoring::is_self_monitoring_tenant(&tenant_id);
                        let started = Instant::now();
                        #[cfg(test)]
                        {
                            use std::sync::atomic::Ordering;
                            let now = this.in_flight_commits.fetch_add(1, Ordering::SeqCst) + 1;
                            this.peak_concurrent_commits.fetch_max(now, Ordering::SeqCst);
                        }
                        let committed = common::self_monitoring::maybe_suppress_self_telemetry(
                            suppress,
                            async {
                                match this
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
                                        // Restart this group's coalescing
                                        // interval only on a real commit.
                                        this.coalescer
                                            .lock()
                                            .await
                                            .record_commit(&writer_key, now);
                                        {
                                            let mut entry_failures =
                                                this.entry_failures.lock().await;
                                            for entry_id in &processed_ids {
                                                entry_failures.remove(entry_id);
                                            }
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
                                            this.record_entry_failure(
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
                            },
                        )
                        .await;
                        #[cfg(test)]
                        this.in_flight_commits
                            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

                        // Per-tenant commit latency, so the coupling this
                        // change removes stays measurable: a tenant whose
                        // commits are slow is now visible as that tenant's
                        // latency instead of as everyone's. `outcome`
                        // separates failed commits (fast catalog rejections)
                        // from successful ones, so a p99 spike during an
                        // incident isn't diluted by the failures alongside it.
                        common::self_monitoring::app_metrics()
                            .writer_commit_duration
                            .record(
                                started.elapsed().as_secs_f64(),
                                &[
                                    tenant_attr(&tenant_id),
                                    opentelemetry::KeyValue::new(
                                        "outcome",
                                        if committed { "success" } else { "failure" },
                                    ),
                                ],
                            );

                        // Only a *forced* group's failure fails the drain; a
                        // background group failing is best-effort (already
                        // dead-lettered/retried).
                        if forced && !committed {
                            GroupOutcome::ForcedCommitFailed
                        } else {
                            GroupOutcome::Committed
                        }
                    }
                },
            )
            .buffer_unordered(COMMIT_CONCURRENCY)
            .collect()
            .await;

        let deferred_groups = outcomes
            .iter()
            .filter(|o| matches!(o, GroupOutcome::Deferred))
            .count() as u64;
        let forced_commit_failed = outcomes
            .iter()
            .any(|o| matches!(o, GroupOutcome::ForcedCommitFailed));

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
        &self,
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

        // Get or create this table's writer. The cache lock is released
        // before the commit so other tables proceed in parallel; the writer's
        // own lock then serializes this table's commits, which is required —
        // a commit reloads the table and replaces its idempotency marker.
        let cached = self.table_writers.lock().await.get(&writer_key).cloned();
        let shared_writer = match cached {
            Some(writer) => writer,
            None => {
                // Created with the cache lock RELEASED. Creation is a catalog
                // round trip, and the outer lock covers every table — holding
                // it here would make a burst of first-time tenants serialize
                // behind each other, and block even groups whose writer is
                // already cached, which is exactly the coupling this fan-out
                // removes. `or_insert` settles a race by keeping the first
                // writer in; a table appears in only one group per cycle, so
                // that race is not reachable today anyway.
                let writer = Arc::new(tokio::sync::Mutex::new(
                    IcebergTableWriter::new(
                        &self.catalog_manager,
                        self.object_store.clone(),
                        tenant_id.to_string(),
                        dataset_id.to_string(),
                        table_name.to_string(),
                    )
                    .await?,
                ));
                self.table_writers
                    .lock()
                    .await
                    .entry(writer_key.clone())
                    .or_insert(writer)
                    .clone()
            }
        };

        let wal_writer_id = wal.writer_id().to_string();
        let writer = &mut *shared_writer.lock().await;

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
                // Per-entry detail stays at debug: replaying a large group
                // (up to `MAX_ENTRIES_PER_COMMIT`) after a crash used to log
                // one `info!` line per already-committed entry, which
                // self-monitoring re-ingests — the export-churn class of
                // #865. The count below is the one line that matters.
                tracing::debug!(
                    entry_id = %entry_id,
                    table_name = %table_name,
                    "Skipping re-insert of WAL entry already committed to Iceberg"
                );
                already_committed_ids.push(entry_id);
            } else {
                fresh.push((entry_id, batch));
            }
        }
        if !already_committed_ids.is_empty() {
            tracing::info!(
                skipped = already_committed_ids.len(),
                table_name = %table_name,
                "Skipped re-insert of WAL entries already committed to Iceberg"
            );
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
        &self,
        wal: &Wal,
        entry_id: Uuid,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
    ) {
        let failures = {
            let mut entry_failures = self.entry_failures.lock().await;
            let failures = entry_failures.entry(entry_id).or_insert(0);
            *failures += 1;
            *failures
        };
        if failures < MAX_ENTRY_FAILURES {
            return;
        }
        match wal.dead_letter(entry_id).await {
            Ok(path) => {
                tracing::error!(
                    entry_id = %entry_id,
                    tenant_id = %tenant_id,
                    dataset_id = %dataset_id,
                    signal = %signal,
                    failures,
                    path = %path.display(),
                    "WAL entry exhausted its retries; payload preserved in the dead-letter directory and entry marked processed"
                );
                self.entry_failures.lock().await.remove(&entry_id);
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
    async fn record_unreadable_entry(&self, wal: &Wal, entry: &WalEntry, reason: &str) {
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
                self.entry_failures.lock().await.remove(&entry.id);
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

    /// Determine which tenant, dataset, and table an entry should go to.
    ///
    /// The entry's own tenant/dataset (those of the WAL it lives in) are the
    /// fallback; the metadata the sender supplied takes precedence, exactly as
    /// on the ingest path. Both paths share [`routing::route`] so a batch
    /// committed live and the same batch replayed after a restart cannot reach
    /// different tables (#1319).
    fn determine_target_table(&self, entry: &WalEntry) -> Result<RouteTarget> {
        let metadata = entry
            .metadata
            .as_deref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
        let field = |key: &str| -> Option<&str> { metadata.as_ref()?.get(key)?.as_str() };

        Ok(routing::route(
            &entry.operation,
            RouteMetadata {
                tenant_id: field("tenant_id"),
                dataset_id: field("dataset_id"),
                target_table: field("target_table"),
            },
            &entry.tenant_id,
            &entry.dataset_id,
        )?)
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
    pub async fn get_stats(&self) -> ProcessorStats {
        let writers = self.table_writers.lock().await;
        ProcessorStats {
            active_writers: writers.len(),
            writer_keys: writers.keys().cloned().collect(),
        }
    }

    /// Close all table writers and clean up resources
    pub async fn shutdown(&mut self) -> Result<()> {
        // Clear all writers (they should handle cleanup automatically when dropped)
        let mut writers = self.table_writers.lock().await;
        tracing::info!(writer_count = writers.len(), "Shutting down WAL processor");
        writers.clear();

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
    async fn tenants_commit_concurrently_rather_than_one_after_another() {
        // The per-tenant WAL fanout (#932) isolates *failures* — one tenant's
        // poison is skipped and the others still drain — but not *latency*:
        // the drain committed one group at a time, so a tenant whose Iceberg
        // round trip takes tens of seconds (the condition seen on hive in
        // #959/#895) delayed every other tenant's commit in the same cycle
        // (#1306).
        //
        // Asserted against the drain's own control flow rather than wall
        // clock: the processor records how many group commits were ever in
        // flight at once. A sequential drain can never exceed one.
        let temp_dir = tempdir().unwrap();
        let mut base = WalConfig::with_defaults(temp_dir.path().to_path_buf());
        base.max_buffer_entries = 1000;
        base.flush_interval_secs = 60;
        let manager = Arc::new(WalManager::uniform(base));

        // Four tenants, each with one healthy entry, so the cycle has four
        // independent groups to commit.
        let tenants = ["acme", "globex", "initech", "umbrella"];
        for tenant in tenants {
            let wal = manager
                .get_wal(tenant, "production", "metrics")
                .await
                .unwrap();
            wal.append(
                WalOperation::WriteMetrics,
                metrics_gauge_bytes(3),
                Some(format!(
                    r#"{{"tenant_id":"{tenant}","dataset_id":"production"}}"#
                )),
            )
            .await
            .unwrap();
            wal.flush().await.unwrap();
        }

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let mut processor = WalProcessor::new(manager.clone(), catalog_manager, object_store);

        processor.process_pending_entries().await.unwrap();

        for tenant in tenants {
            let wal = manager
                .get_wal(tenant, "production", "metrics")
                .await
                .unwrap();
            assert!(
                wal.get_unprocessed_entries().await.unwrap().is_empty(),
                "{tenant}'s entry must be committed"
            );
        }

        let peak = processor
            .peak_concurrent_commits
            .load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            peak > 1,
            "group commits never overlapped (peak concurrent commits = {peak}), so one slow \
             tenant still delays every other tenant in the cycle"
        );
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
    async fn draining_reclaims_processed_wal_segments() {
        // `Wal::cleanup` had no caller in any service (#1305), so processed
        // segments were never reclaimed. The writer's drain is one of the two
        // places that now sweeps — at the pass boundary, where no listed
        // entries are in flight. This guards the wiring, not the sweep.
        let temp_dir = tempdir().unwrap();
        let mut wal_config = WalConfig::with_defaults(temp_dir.path().to_path_buf());
        wal_config.max_segment_size = 1024; // small: a few appends seal a segment
        wal_config.max_buffer_entries = 1;
        wal_config.flush_interval_secs = 3600;
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
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

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let mut processor =
            WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        processor.process_pending_entries().await.unwrap();

        assert_eq!(
            wal.segment_count().await,
            1,
            "a drain pass must reclaim sealed segments whose entries are all processed"
        );
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

        let stats = processor.get_stats().await;
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

        let routed = processor.determine_target_table(&entry).unwrap();
        assert_eq!(routed.tenant_id, "acme");
        assert_eq!(routed.dataset_id, "production");
        assert_eq!(routed.table_name, "traces");

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

        let routed = processor.determine_target_table(&entry).unwrap();
        assert_eq!(routed.tenant_id, "globex");
        assert_eq!(routed.dataset_id, "staging");
        assert_eq!(routed.table_name, "logs");
    }

    /// #1319: the ingest path normalizes the metadata tenant/dataset (trim,
    /// and fall back to the default id when blank) before choosing a WAL, so
    /// replay must apply the same rule to the same metadata. Otherwise a batch
    /// lands in the `acme` WAL but commits to Iceberg tenant `" acme "`, and
    /// where a row ends up depends on whether it was committed live or
    /// replayed after a restart.
    #[tokio::test]
    async fn replay_normalizes_metadata_ids_the_way_ingest_does() {
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(WalConfig::with_defaults(temp_dir.path().to_path_buf()))
                .await
                .unwrap(),
        );
        let processor = WalProcessor::new(
            manager_for(&wal).await,
            Arc::new(CatalogManager::new_in_memory().await.unwrap()),
            Arc::new(InMemory::new()),
        );

        // The entry ids are what the ingest path derived: `" acme "` trimmed,
        // and an empty dataset replaced by the default.
        let entry = WalEntry {
            id: uuid::Uuid::new_v4(),
            operation: WalOperation::WriteTraces,
            data_size: 0,
            data_offset: 0,
            timestamp: 0,
            processed: false,
            tenant_id: "acme".to_string(),
            dataset_id: common::bootstrap::DEFAULT_DATASET_ID.to_string(),
            // The metadata is what the sender supplied, verbatim.
            metadata: Some(r#"{"tenant_id":" acme ","dataset_id":""}"#.to_string()),
        };

        let routed = processor.determine_target_table(&entry).unwrap();
        assert_eq!(
            routed.tenant_id, "acme",
            "replay must trim the metadata tenant id"
        );
        assert_eq!(
            routed.dataset_id,
            common::bootstrap::DEFAULT_DATASET_ID,
            "replay must fall back to the default dataset for a blank id"
        );
        assert_eq!(routed.table_name, "traces");
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

    #[tokio::test]
    async fn drain_pending_respects_max_drain_bytes_per_cycle_oldest_first() {
        // A replay backlog must not be decoded past a per-cycle byte budget:
        // without one, a multi-GB WAL after an outage is decoded to Arrow in
        // a single tick and OOM-kills the writer (W5). A budget of ~2.5x one
        // entry's size must commit exactly the oldest two of six per cycle.
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());

        let entry_bytes = metrics_gauge_bytes(1);
        let entry_size = entry_bytes.len() as u64;
        let config = WriterConfig {
            commit_interval: Duration::from_secs(0),
            max_uncommitted_rows: 1_000_000,
            max_drain_bytes_per_cycle: entry_size * 2 + entry_size / 2,
            ..Default::default()
        };
        let mut processor = WalProcessor::with_config(
            manager_for(&wal).await,
            catalog_manager,
            object_store,
            &config,
        );

        let meta = Some(r#"{"target_table":"metrics_gauge"}"#.to_string());
        let mut ids = Vec::new();
        for _ in 0..6 {
            ids.push(
                wal.append(
                    WalOperation::WriteMetrics,
                    entry_bytes.clone(),
                    meta.clone(),
                )
                .await
                .unwrap(),
            );
        }
        wal.flush().await.unwrap();

        processor.process_pending_entries().await.unwrap();
        let remaining: Vec<Uuid> = wal
            .get_unprocessed_entries()
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect();
        assert_eq!(
            remaining,
            ids[2..].to_vec(),
            "budget must cap the first cycle to the oldest two entries"
        );

        processor.process_pending_entries().await.unwrap();
        let remaining: Vec<Uuid> = wal
            .get_unprocessed_entries()
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect();
        assert_eq!(
            remaining,
            ids[4..].to_vec(),
            "second cycle commits the next two, oldest first"
        );

        processor.process_pending_entries().await.unwrap();
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "third cycle drains the rest"
        );
    }

    #[tokio::test]
    async fn drain_pending_with_budget_unset_drains_everything_in_one_cycle() {
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let config = WriterConfig {
            commit_interval: Duration::from_secs(0),
            max_uncommitted_rows: 1_000_000,
            max_drain_bytes_per_cycle: 0,
            ..Default::default()
        };
        let mut processor = WalProcessor::with_config(
            manager_for(&wal).await,
            catalog_manager,
            object_store,
            &config,
        );

        let meta = Some(r#"{"target_table":"metrics_gauge"}"#.to_string());
        for _ in 0..6 {
            wal.append(
                WalOperation::WriteMetrics,
                metrics_gauge_bytes(1),
                meta.clone(),
            )
            .await
            .unwrap();
        }
        wal.flush().await.unwrap();

        processor.process_pending_entries().await.unwrap();
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "0 (unset) must not bound the drain"
        );
    }

    #[tokio::test]
    async fn flush_marker_beyond_drain_budget_is_still_included() {
        // The byte budget bounds *data* entries; a `Flush` marker must never
        // be starved by it, even when it sits chronologically after entries
        // the budget already deferred.
        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(coalescing_wal_config(temp_dir.path()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());

        let entry_bytes = metrics_gauge_bytes(1);
        let entry_size = entry_bytes.len() as u64;
        let config = WriterConfig {
            commit_interval: Duration::from_secs(0),
            max_uncommitted_rows: 1_000_000,
            max_drain_bytes_per_cycle: entry_size * 2 + entry_size / 2,
            ..Default::default()
        };
        let mut processor = WalProcessor::with_config(
            manager_for(&wal).await,
            catalog_manager,
            object_store,
            &config,
        );

        let meta = Some(r#"{"target_table":"metrics_gauge"}"#.to_string());
        for _ in 0..6 {
            wal.append(
                WalOperation::WriteMetrics,
                entry_bytes.clone(),
                meta.clone(),
            )
            .await
            .unwrap();
        }
        // Arrives after the data, past the byte budget's cutoff.
        wal.append(WalOperation::Flush, Vec::new(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        processor.process_pending_entries().await.unwrap();

        let remaining = wal.get_unprocessed_entries().await.unwrap();
        assert!(
            !remaining
                .iter()
                .any(|e| matches!(e.operation, WalOperation::Flush)),
            "a Flush marker must never be starved by the drain budget"
        );
        assert_eq!(
            remaining.len(),
            4,
            "budget-deferred data entries stay pending; only the forced group committed"
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

    // W6: mirrors `reconcile::provisioning_attrs_uses_registry_attribute_names`
    // — the processor's metrics must key tenant scoping the same way the
    // reconciler's do, not the bare `tenant` key weaver live-check flags.
    #[test]
    fn tenant_attr_uses_registry_attribute_name() {
        let attr = tenant_attr("acme");

        assert_eq!(attr.key.as_str(), "signaldb.tenant.id");
        assert_eq!(attr.value.to_string(), "acme");
    }

    /// W7: replaying a group whose entries are already committed to Iceberg
    /// (e.g. a crash between commit and `mark_processed`) used to log one
    /// `info!` line per entry — up to `MAX_ENTRIES_PER_COMMIT` per group per
    /// tick, which self-monitoring re-ingests (the #865 export-churn class).
    /// It must log a single aggregate line regardless of group size.
    #[tokio::test]
    async fn replay_of_already_committed_entries_logs_one_aggregate_line() {
        const ENTRY_COUNT: usize = 50;

        let temp_dir = tempdir().unwrap();
        let wal = Arc::new(
            Wal::new(WalConfig::with_defaults(temp_dir.path().to_path_buf()))
                .await
                .unwrap(),
        );
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store = Arc::new(InMemory::new());
        let processor = WalProcessor::new(manager_for(&wal).await, catalog_manager, object_store);

        for _ in 0..ENTRY_COUNT {
            wal.append(
                WalOperation::WriteMetrics,
                crate::test_support::metrics_gauge_bytes(1),
                None,
            )
            .await
            .unwrap();
        }
        wal.flush().await.unwrap();

        let mut entries = Vec::new();
        for entry in wal.get_unprocessed_entries().await.unwrap() {
            let batch = WalProcessor::deserialize_entry_data(&wal, &entry)
                .await
                .unwrap();
            entries.push((entry.id, batch));
        }
        assert_eq!(entries.len(), ENTRY_COUNT);

        // First pass: a real commit, so the Iceberg idempotency marker
        // records every id and the WAL marks every entry processed.
        processor
            .process_batch_for_table(
                &wal,
                "acme",
                "production",
                "metrics_gauge",
                entries.clone(),
                Vec::new(),
            )
            .await
            .unwrap();

        // Replay: the same ids/batches are processed again. Every one of
        // them hits the "already committed" branch.
        let probe = common::testing::OtelExportProbe::new();
        {
            let _guard = probe.install();
            processor
                .process_batch_for_table(
                    &wal,
                    "acme",
                    "production",
                    "metrics_gauge",
                    entries,
                    Vec::new(),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            probe.exported_events(),
            1,
            "replaying {ENTRY_COUNT} already-committed entries must log one \
             aggregate line, not one per entry"
        );
    }
}
