use crate::schema_transform::{
    transform_logs_v1_to_iceberg, transform_metrics_exponential_histogram_v1_to_iceberg,
    transform_metrics_gauge_v1_to_iceberg, transform_metrics_histogram_v1_to_iceberg,
    transform_metrics_sum_v1_to_iceberg, transform_metrics_summary_v1_to_iceberg,
    transform_profiles_v1_to_iceberg, transform_trace_v1_to_v2, warm_trace_v1_to_v2_plan,
};
use anyhow::{Context, Result};
use common::CatalogManager;

use common::iceberg::sort::{
    DeclaredSortColumn, UndeclaredFallback, is_sorted_by, sort_batch_by, write_sort_key,
};
use datafusion::arrow::array::{RecordBatch, new_null_array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use iceberg_rust::arrow::write::{write_parquet_partitioned, write_sorted_parquet_partitioned};
use iceberg_rust::catalog::Catalog as IcebergRustCatalog;
use iceberg_rust::catalog::commit::{CommitTable, TableRequirement, TableUpdate};
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::table_metadata::MAIN_BRANCH;
use iceberg_rust::table::Table;
use object_store::ObjectStore;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use uuid;

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
        }
    }
}

/// Outcome of one call to [`IcebergTableWriter::append_batches_with_marker`]:
/// which entries are safe to mark processed, and which were rejected while
/// being prepared, with why.
///
/// A schema/transform failure is a property of the one entry that carries
/// it, not of the group — see W2 in the writer review: before this type
/// existed, that failure aborted the whole call via `?`, so one poison
/// entry in a commit group dragged every healthy same-cycle neighbour into
/// the failure path with it (and, per W1, counted them all toward their own
/// dead-lettering budget). Preparing every entry independently and
/// collecting rejections here instead keeps a poison entry's blast radius
/// to itself.
pub struct CommitOutcome {
    /// Ids safe to mark processed: committed to Iceberg, or a zero-row batch
    /// with nothing to commit.
    pub committed: Vec<uuid::Uuid>,
    /// Ids whose batch could not be prepared for this table, paired with
    /// why. The caller must retire these immediately
    /// (`Wal::dead_letter_rejected`) rather than count them toward a retry
    /// budget — the fault is in the entry's bytes/shape, so retrying cannot
    /// change the outcome.
    pub rejected: Vec<(uuid::Uuid, anyhow::Error)>,
}

/// Writes signal batches to an Iceberg table.
///
/// The only write entry point is [`Self::append_batches_with_marker`],
/// which verifies every commit against the catalog before reporting
/// success. The SQL catalog's compare-and-swap does not report a lost
/// race (issue #538), so any commit path that trusts `commit()`'s return
/// value can silently lose data -- do not add one.
pub struct IcebergTableWriter {
    catalog: Arc<dyn IcebergRustCatalog>,
    table: Table,
    #[allow(dead_code)] // Will be used for data writing
    object_store: Arc<dyn ObjectStore>,
    tenant_id: String,
    dataset_id: String,
    /// Tenant-resolved materialized-label allowlists (a tenant schema
    /// override replaces the global set), used by the transforms.
    materialized: common::config::MaterializedLabels,
    /// Retry configuration for failed operations
    retry_config: RetryConfig,
}

impl IcebergTableWriter {
    /// Create a new IcebergTableWriter for a specific table
    pub async fn new(
        catalog_manager: &CatalogManager,
        object_store: Arc<dyn ObjectStore>,
        tenant_id: String,
        dataset_id: String,
        table_name: String,
    ) -> Result<Self> {
        // Resolve the trace v1->v2 materialization plan now, not on the
        // first ingested batch: a bad extraction-rule reference fails
        // table-writer construction with a clear error instead of
        // panicking (and, under `panic = "abort"`, killing the process)
        // mid-ingest.
        if table_name == "traces" {
            warm_trace_v1_to_v2_plan()
                .context("Failed to build trace v1->v2 materialization plan")?;
        }

        let table = catalog_manager
            .ensure_table(&tenant_id, &dataset_id, &table_name)
            .await?;
        let catalog = catalog_manager.catalog();

        tracing::info!(
            "Successfully created/loaded Iceberg table: {} for tenant '{tenant_id}' dataset '{dataset_id}'",
            table.identifier()
        );

        let table_metadata = table.metadata();
        let current_schema = table.current_schema()?;
        tracing::debug!("Table location: {}", table_metadata.location);
        tracing::debug!("Schema has {} fields", current_schema.fields().len());

        let materialized = catalog_manager
            .config()
            .get_tenant_schema_config(&tenant_id)
            .materialized_labels;

        Ok(Self {
            catalog,
            table,
            object_store,
            tenant_id,
            dataset_id,
            materialized,
            retry_config: RetryConfig::default(),
        })
    }

    /// Apply schema transformation if the batch has v1 (wire) schema but the
    /// table expects the Iceberg storage schema.
    ///
    /// Dispatches per table so that logs and metrics batches are handled, not
    /// just traces. Wire-format batches are recognized by their raw OTLP
    /// marker columns (`time_unix_nano` for logs, `data_json` for metrics);
    /// batches already in storage format lack those columns and pass through
    /// unchanged, keeping the method idempotent for callers that already
    /// transformed (e.g. the Flight ingestion path).
    fn apply_schema_transformation_if_needed(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let num_columns = batch.num_columns();
        let schema = batch.schema();
        let has_field = |name: &str| schema.index_of(name).is_ok();

        match self.table.identifier().name() {
            "traces" => {
                // v1 schema uses "name" (renamed to "span_name" in v2) and lacks
                // computed fields; v2 uses "span_name" plus "timestamp"/"date_day"/"hour".
                if has_field("name") && !has_field("span_name") {
                    tracing::debug!("Detected v1 traces batch, applying v1->v2 transformation");
                    transform_trace_v1_to_v2(batch, &self.materialized.traces)
                } else if has_field("span_name") {
                    tracing::debug!("Detected v2 traces batch, no transformation needed");
                    Ok(batch)
                } else {
                    let field_names: Vec<&str> =
                        schema.fields().iter().map(|f| f.name().as_str()).collect();
                    tracing::warn!(
                        "Unknown traces schema: {num_columns} columns with fields: {field_names:?}. Assuming no transformation needed."
                    );
                    Ok(batch)
                }
            }
            // Wire-format logs carry raw OTLP "time_unix_nano"; the storage
            // schema uses computed "timestamp"/"date_day"/"hour" columns.
            "logs" if has_field("time_unix_nano") => {
                tracing::debug!("Detected v1 logs batch, applying logs->iceberg transformation");
                transform_logs_v1_to_iceberg(batch, &self.materialized.logs)
            }
            // Wire-format metrics carry the raw "data_json" payload column.
            "metrics_gauge" if has_field("data_json") => {
                transform_metrics_gauge_v1_to_iceberg(batch, &self.materialized.metrics)
            }
            "metrics_sum" if has_field("data_json") => {
                transform_metrics_sum_v1_to_iceberg(batch, &self.materialized.metrics)
            }
            "metrics_histogram" if has_field("data_json") => {
                transform_metrics_histogram_v1_to_iceberg(batch, &self.materialized.metrics)
            }
            "metrics_exponential_histogram" if has_field("data_json") => {
                transform_metrics_exponential_histogram_v1_to_iceberg(
                    batch,
                    &self.materialized.metrics,
                )
            }
            "metrics_summary" if has_field("data_json") => {
                transform_metrics_summary_v1_to_iceberg(batch, &self.materialized.metrics)
            }
            // Wire-format profiles carry raw OTLP "time_unix_nano"; the
            // storage schema uses computed "timestamp"/"date_day"/"hour".
            "profiles" if has_field("time_unix_nano") => {
                tracing::debug!(
                    "Detected v1 profiles batch, applying profiles->iceberg transformation"
                );
                transform_profiles_v1_to_iceberg(batch, &self.materialized.profiles)
            }
            _ => Ok(batch),
        }
    }

    /// Get table identifier
    pub fn table_identifier(&self) -> &Identifier {
        self.table.identifier()
    }

    /// Get table metadata
    pub fn table_metadata(&self) -> &iceberg_rust::spec::table_metadata::TableMetadata {
        self.table.metadata()
    }

    /// Update retry configuration
    pub fn set_retry_config(&mut self, retry_config: RetryConfig) {
        self.retry_config = retry_config;
    }

    /// Get current retry configuration
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Reload the table from the catalog, bypassing any cached handle,
    /// so marker reads and the next commit are based on current metadata.
    async fn reload_table(&mut self) -> Result<()> {
        let ident = self.table.identifier().clone();
        let tabular = self
            .catalog
            .clone()
            .load_tabular(&ident)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to reload Iceberg table {ident}: {e}"))?;
        match tabular {
            Tabular::Table(table) => {
                self.table = table;
                Ok(())
            }
            _ => Err(anyhow::anyhow!(
                "Expected table but found different tabular type for {ident}"
            )),
        }
    }

    /// Parse this WAL writer's idempotency marker from the (in-memory)
    /// table properties.
    fn read_marker(&self, wal_writer_id: &str) -> HashSet<uuid::Uuid> {
        self.table
            .metadata()
            .properties
            .get(&wal_marker_key(wal_writer_id))
            .map(|value| decode_marker_ids(value))
            .unwrap_or_default()
    }

    /// Delete idempotency markers left by writer ids that have not committed
    /// to this table within `retention`. Returns how many were retired.
    ///
    /// Every marker is a permanent table property, and a new writer id appears
    /// whenever a WAL directory is created or wiped — a redeploy on ephemeral
    /// storage, a WAL quarantined and recreated after corruption (#883), an
    /// operator clearing `.data/wal`. Multiplied by the per-tenant WAL fanout
    /// (#932), the property set grew without bound, and every property is paid
    /// for in `metadata.json` on every read and every commit — the file #959
    /// fought down from 11.9 MB to 28.5 KB (#1307).
    ///
    /// `process_outlived_retention` says whether this process has itself been
    /// running longer than `retention`; see [`stale_marker_keys`] for why
    /// undated markers wait for that.
    ///
    /// The delete is guarded by an assertion on the branch's current snapshot,
    /// so a marker written between the read and the delete makes this commit
    /// fail rather than discard fresh idempotency evidence. A failure here is
    /// never fatal: the markers simply stay until the next pass.
    pub async fn retire_stale_markers(
        &mut self,
        own_writer_ids: &HashSet<String>,
        retention: Duration,
        process_outlived_retention: bool,
    ) -> Result<usize> {
        let now_secs = common::wal::unix_now_secs();
        let removals = stale_marker_keys(
            &self.table.metadata().properties,
            own_writer_ids,
            now_secs,
            retention,
            process_outlived_retention,
        );
        if removals.is_empty() {
            return Ok(0);
        }

        let retired = removals.len();
        let requirements = match self.table.metadata().current_snapshot_id {
            Some(snapshot_id) => vec![TableRequirement::AssertRefSnapshotId {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id,
            }],
            // A table with no snapshot has never been committed to, so no
            // marker can be racing us.
            None => Vec::new(),
        };

        self.catalog
            .clone()
            .update_table(CommitTable {
                identifier: self.table.identifier().clone(),
                requirements,
                updates: vec![TableUpdate::RemoveProperties { removals }],
            })
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to retire stale WAL markers on {}: {e}",
                    self.table.identifier()
                )
            })?;
        self.reload_table().await?;

        tracing::info!(
            table = %self.table.identifier(),
            retired,
            "Retired WAL idempotency markers from writers that have not committed within retention"
        );
        Ok(retired)
    }

    /// Reload the table and return the WAL entry ids recorded by the most
    /// recent marker commit for `wal_writer_id`. Ids present here are
    /// durably committed to Iceberg even if the WAL never marked them
    /// processed (crash between commit and index write).
    pub async fn load_committed_marker(
        &mut self,
        wal_writer_id: &str,
    ) -> Result<HashSet<uuid::Uuid>> {
        self.reload_table().await?;
        Ok(self.read_marker(wal_writer_id))
    }

    /// Put a commit group's rows in the table's declared sort order, so the
    /// files written from them can honestly attest that order.
    ///
    /// The whole group is concatenated and sorted once rather than sorted per
    /// batch: files are rolled from the stream in order, so a file can only be
    /// sorted if the stream as a whole is. The group is one commit interval of
    /// ingest — seconds of data, already held in memory here — and the sort is
    /// columnar (one `lexsort_to_indices` pass plus a `take`), so the cost is
    /// bounded by the group rather than by the table.
    ///
    /// Every reason the sort cannot be done writes the files unattested: a
    /// table that declares no order (nothing to honor), a key column absent
    /// from the batches, or a failed concat. Ingest does not sort an
    /// undeclared table at all ([`UndeclaredFallback::LeaveUnsorted`]) —
    /// nothing could attest the result, so the reader would gain nothing for
    /// the write path's trouble. Unattested files are read as unsorted and
    /// queried correctly; falsely attested ones would not be.
    ///
    /// Returns the batches to write and whether they may attest the order.
    fn order_for_write(
        &self,
        batches: Vec<RecordBatch>,
        target_schema: &ArrowSchemaRef,
    ) -> (Vec<RecordBatch>, bool) {
        let key = write_sort_key(
            self.table.metadata(),
            self.table.identifier().name(),
            UndeclaredFallback::LeaveUnsorted,
        );
        if key.columns.is_empty() {
            return (batches, false);
        }

        match Self::sort_group(&batches, target_schema, &key.columns) {
            Ok(sorted) => (vec![sorted], key.attest),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    table = %self.table.identifier(),
                    "Failed to sort rows by the declared key; writing files unattested"
                );
                (batches, false)
            }
        }
    }

    /// Concatenate a commit group and sort it by `sort_columns`.
    fn sort_group(
        batches: &[RecordBatch],
        target_schema: &ArrowSchemaRef,
        sort_columns: &[DeclaredSortColumn],
    ) -> Result<RecordBatch> {
        let merged = match batches {
            [single] => single.clone(),
            many => concat_batches(target_schema, many)
                .context("Failed to concatenate a commit group for sorting")?,
        };
        let sorted = sort_batch_by(&merged, sort_columns)?;

        // The honesty invariant is worth a self-check where it is cheap to
        // run. Debug builds (tests included) verify the sort actually held
        // before the file claims it; release builds trust the sort kernel.
        debug_assert!(
            is_sorted_by(&sorted, sort_columns).unwrap_or(false),
            "rows about to be attested are not sorted by the declared key"
        );

        Ok(sorted)
    }

    /// Atomically append `entries`' batches and record their WAL entry ids
    /// as this writer's idempotency marker — data files and marker ride in
    /// ONE Iceberg commit (a single catalog CAS), so replay after a crash
    /// can always tell whether the data landed.
    ///
    /// Contract for callers: dedupe `entries` against
    /// [`Self::load_committed_marker`] and durably mark any previously
    /// committed ids processed BEFORE calling this — the commit REPLACES
    /// the marker, discarding the evidence for earlier commits.
    ///
    /// Commit outcomes are verified against the catalog rather than
    /// trusted: an `Err` can follow a commit that actually landed
    /// (ambiguous failure), and the sql catalog can lose a CAS silently.
    /// After every attempt the table is reloaded and the marker checked —
    /// only the marker decides success, so retries can never double-append.
    ///
    /// Returns a [`CommitOutcome`] rather than failing the whole call for a
    /// batch that cannot be shaped into the table: preparation happens per
    /// entry, so one poison entry cannot take its healthy neighbours down
    /// with it (W2). The remaining failure modes here (catalog/object-store
    /// I/O) are whole-call: they apply equally to every survivor, so an
    /// `Err` still aborts everything, exactly as before.
    #[tracing::instrument(
        skip_all,
        fields(
            signaldb.tenant.id = %self.tenant_id,
            signaldb.dataset.id = %self.dataset_id,
            signaldb.wal.entry_count = entries.len() as i64
        )
    )]
    pub async fn append_batches_with_marker(
        &mut self,
        wal_writer_id: &str,
        entries: Vec<(uuid::Uuid, RecordBatch)>,
    ) -> Result<CommitOutcome> {
        // The Parquet writer requires batches in the table's exact Arrow
        // schema (derived from the Iceberg schema, e.g. microsecond
        // timestamps), so coerce after the wire→storage transformation.
        let target_schema: ArrowSchemaRef = Arc::new(
            self.table
                .current_schema()
                .map_err(|e| anyhow::anyhow!("Failed to get current Iceberg schema: {e}"))?
                .fields()
                .try_into()
                .map_err(|e: iceberg_rust::spec::error::Error| {
                    anyhow::anyhow!("Failed to convert Iceberg schema to Arrow: {e}")
                })?,
        );

        // Step 1: prepare every entry independently. A transform/coercion
        // failure is a property of that entry's bytes, not of the group —
        // collecting it as a rejection here (rather than aborting the whole
        // call via `?`) is what keeps a poison entry from taking its
        // healthy neighbours down with it.
        let mut committed_ids = Vec::new();
        let mut rejected = Vec::new();
        let mut transformed = Vec::new();
        for (id, batch) in entries {
            if batch.num_rows() == 0 {
                // Nothing to commit for this id, but there is also nothing
                // wrong with it — it is as processed as an empty batch can
                // be.
                committed_ids.push(id);
                continue;
            }
            let prepared = self
                .apply_schema_transformation_if_needed(batch)
                .and_then(|batch| coerce_batch_to_schema(batch, &target_schema));
            match prepared {
                Ok(batch) => {
                    committed_ids.push(id);
                    transformed.push(batch);
                }
                Err(e) => rejected.push((id, e)),
            }
        }
        if transformed.is_empty() {
            // Every survivor (if any) was zero-row, and/or every non-empty
            // entry was rejected: nothing left to commit, so no Iceberg
            // transaction is needed either way.
            return Ok(CommitOutcome {
                committed: committed_ids,
                rejected,
            });
        }
        let total_rows: usize = transformed.iter().map(|b| b.num_rows()).sum();

        let (batches, attest) = self.order_for_write(transformed, &target_schema);

        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        let write = if attest {
            write_sorted_parquet_partitioned(&self.table, stream, None).await
        } else {
            write_parquet_partitioned(&self.table, stream, None).await
        };
        let files = write.map_err(|e| {
            anyhow::anyhow!(
                "Failed to write Parquet files for Iceberg table {}: {e}",
                self.table.identifier()
            )
        })?;
        if files.is_empty() {
            return Err(anyhow::anyhow!(
                "write_parquet_partitioned produced no data files for {total_rows} rows"
            ));
        }

        let marker_key = wal_marker_key(wal_writer_id);
        let marker_value = encode_marker_ids(&committed_ids);
        let id_set: HashSet<uuid::Uuid> = committed_ids.iter().copied().collect();

        let mut attempt = 0;
        let mut delay = self.retry_config.initial_delay;
        loop {
            attempt += 1;
            let commit_result = self
                .table
                .new_transaction(None)
                .append_data(files.clone())
                .update_properties(vec![(marker_key.clone(), marker_value.clone())])
                .commit()
                .await;

            // The catalog is the source of truth for whether the commit
            // landed, regardless of what commit() returned.
            self.reload_table().await?;
            if self.read_marker(wal_writer_id) == id_set {
                if let Err(e) = commit_result {
                    tracing::warn!(
                        "Iceberg commit reported an error but the marker landed \
                         (treating as success): {e}"
                    );
                }
                tracing::info!(
                    rows = total_rows,
                    files = files.len(),
                    attempt,
                    table = %self.table.identifier(),
                    "Committed rows to Iceberg table"
                );
                return Ok(CommitOutcome {
                    committed: committed_ids,
                    rejected,
                });
            }

            let error = match commit_result {
                Ok(()) => anyhow::anyhow!(
                    "Iceberg commit reported success but the marker is absent \
                     (catalog CAS silently lost)"
                ),
                Err(e) => anyhow::anyhow!("Iceberg commit failed: {e}"),
            };
            if attempt >= self.retry_config.max_attempts {
                return Err(error.context(format!(
                    "Failed to commit {} entries to Iceberg table {} after {attempt} attempts",
                    id_set.len(),
                    self.table.identifier()
                )));
            }
            tracing::warn!(
                "Commit attempt {attempt} for Iceberg table {} did not land: {error}. \
                 Retrying in {delay:?}",
                self.table.identifier()
            );
            tokio::time::sleep(delay).await;
            delay = std::cmp::min(
                self.retry_config.max_delay,
                Duration::from_secs_f64(delay.as_secs_f64() * self.retry_config.backoff_multiplier),
            );
        }
    }
}

/// Prefix for the per-WAL idempotency marker stored in Iceberg table
/// properties. The full key is `signaldb.wal.committed.<wal-writer-id>`,
/// so concurrent writer nodes (distinct WAL directories) never clobber
/// each other's markers.
pub const WAL_MARKER_PREFIX: &str = "signaldb.wal.committed.";

fn wal_marker_key(wal_writer_id: &str) -> String {
    format!("{WAL_MARKER_PREFIX}{wal_writer_id}")
}

/// Prefix of the commit-time field a marker value leads with.
///
/// Without it nothing could date a marker, so nothing could retire one, and
/// the property set grew by one entry per writer id ever seen (#1307). Values
/// written before this existed have no such field and are handled explicitly
/// wherever age matters.
const MARKER_TIME_FIELD: &str = "t=";

fn encode_marker_ids(ids: &[uuid::Uuid]) -> String {
    let now = common::wal::unix_now_secs();
    let ids = ids
        .iter()
        .map(|id| id.simple().to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("{MARKER_TIME_FIELD}{now}:{ids}")
}

fn decode_marker_ids(value: &str) -> HashSet<uuid::Uuid> {
    // The id list is everything after the leading `t=<secs>:` field, if
    // present. Parsing is tolerant either way — a marker that decoded to
    // nothing would silently re-insert already-committed rows, so this must
    // never be stricter than it has to be.
    let (_committed_at, ids) = parse_marker(value);
    ids.split(',')
        .filter_map(|part| uuid::Uuid::parse_str(part.trim()).ok())
        .collect()
}

/// Split a marker value into its commit time (absent for values written
/// before markers were dated) and its id list.
///
/// One parser for both halves so their edge cases cannot drift: a value that
/// looks dated to one and undated to the other would either hide ids from
/// dedupe or make an undated marker look retirable.
fn parse_marker(value: &str) -> (Option<u64>, &str) {
    let Some(rest) = value.strip_prefix(MARKER_TIME_FIELD) else {
        return (None, value);
    };
    match rest.split_once(':') {
        Some((secs, ids)) => (secs.parse::<u64>().ok(), ids),
        // `t=` with no separator: not a shape we write. Treat the whole value
        // as ids and let uuid parsing discard what it cannot read, rather
        // than silently dropping evidence.
        None => (None, value),
    }
}

/// When this marker was committed, or `None` for a value written before
/// markers carried a commit time.
fn marker_committed_at(value: &str) -> Option<u64> {
    parse_marker(value).0
}

/// Which marker properties in `properties` are safe to delete.
///
/// A marker is live evidence that its writer committed rows it may not yet
/// have marked processed, so deleting one that is still needed re-inserts
/// those rows as duplicates on that writer's next replay. Three rules keep
/// that from happening:
///
/// - **Never our own.** It is the evidence for the commit being made now.
/// - **Only past `retention`.** A writer that committed within the window may
///   still be alive and mid-recovery.
/// - **Undated markers only once this process has itself been up longer than
///   the window** (`process_outlived_retention`). An undated marker predates
///   #1307 and could belong to a writer that is perfectly healthy but has not
///   committed since the deploy; waiting out the window proves otherwise,
///   because a live writer would have rewritten it with a dated one by then.
///
///   This uses the *sweeping* process's uptime as a proxy for "long enough
///   since dated markers shipped", which leaves one narrow window: a sweeper
///   up past the retention window, and a different writer that has just
///   returned from an outage longer than the window and has not made its
///   first post-restart commit yet. Its undated marker is retirable for those
///   few seconds. The precondition is the same one the dated path already
///   accepts — a writer holding undrained entries that has not committed in
///   `retention` — and it applies only to markers written before this shipped,
///   so it cannot recur once they are gone.
fn stale_marker_keys(
    properties: &HashMap<String, String>,
    own_writer_ids: &HashSet<String>,
    now_secs: u64,
    retention: Duration,
    process_outlived_retention: bool,
) -> Vec<String> {
    let own_keys: HashSet<String> = own_writer_ids.iter().map(|id| wal_marker_key(id)).collect();
    properties
        .iter()
        .filter(|(key, _)| key.starts_with(WAL_MARKER_PREFIX) && !own_keys.contains(*key))
        .filter(|(_, value)| match marker_committed_at(value) {
            Some(committed_at) => now_secs.saturating_sub(committed_at) >= retention.as_secs(),
            None => process_outlived_retention,
        })
        .map(|(key, _)| key.clone())
        .collect()
}

/// Project and cast a batch onto the table's Arrow schema (columns matched
/// by name). Extra batch columns are dropped; missing columns are an error,
/// as is a null in a column the table declares non-nullable.
fn coerce_batch_to_schema(batch: RecordBatch, target: &ArrowSchemaRef) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let Ok(index) = batch.schema().index_of(field.name()) else {
            // A nullable target column absent from the batch (e.g. a
            // materialized `label_<key>` on a table whose current config no
            // longer lists it) is filled with nulls; a required one is a
            // genuine schema mismatch.
            if field.is_nullable() {
                columns.push(new_null_array(field.data_type(), batch.num_rows()));
                continue;
            }
            return Err(anyhow::anyhow!(
                "Batch is missing column '{}' required by the table schema",
                field.name()
            ));
        };
        let column = batch.column(index);
        let column = if column.data_type() == field.data_type() {
            column.clone()
        } else if matches!(
            (column.data_type(), field.data_type()),
            (
                datafusion::arrow::datatypes::DataType::Utf8,
                datafusion::arrow::datatypes::DataType::Map(_, _)
            )
        ) {
            // Attribute maps: the transforms emit flat JSON objects as
            // strings; tables with a map-typed attribute column get the
            // parsed entries.
            json_strings_to_map_array(column, field)?
        } else {
            let options = datafusion::arrow::compute::CastOptions {
                safe: false,
                ..Default::default()
            };
            datafusion::arrow::compute::cast_with_options(column, field.data_type(), &options)
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to cast column '{}' from {:?} to {:?}: {e}",
                        field.name(),
                        column.data_type(),
                        field.data_type()
                    )
                })?
        };
        columns.push(column);
    }
    RecordBatch::try_new(target.clone(), columns)
        .map_err(|e| anyhow::anyhow!("Failed to build coerced batch: {e}"))
}

/// Parse a column of flat-JSON-object strings into a `MapArray` matching
/// `target_field`'s entry/key/value naming. Null or unparseable documents
/// become null map entries; non-string JSON values are rendered with
/// `to_string` (matching the substring-match era's serialized forms).
fn json_strings_to_map_array(
    column: &dyn datafusion::arrow::array::Array,
    target_field: &datafusion::arrow::datatypes::Field,
) -> Result<std::sync::Arc<dyn datafusion::arrow::array::Array>> {
    use datafusion::arrow::array::{Array, MapBuilder, MapFieldNames, StringArray, StringBuilder};
    use datafusion::arrow::datatypes::DataType;

    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("expected a Utf8 column for map coercion"))?;
    let DataType::Map(entry_field, _) = target_field.data_type() else {
        return Err(anyhow::anyhow!("target field is not a map"));
    };
    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        return Err(anyhow::anyhow!("map entries are not a struct"));
    };
    let names = MapFieldNames {
        entry: entry_field.name().clone(),
        key: kv_fields[0].name().clone(),
        value: kv_fields[1].name().clone(),
    };
    let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
    for i in 0..strings.len() {
        if strings.is_null(i) {
            builder.append(false)?;
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(strings.value(i)) {
            Ok(serde_json::Value::Object(map)) => {
                for (k, v) in map {
                    builder.keys().append_value(k);
                    match v {
                        serde_json::Value::String(s) => builder.values().append_value(s),
                        other => builder.values().append_value(other.to_string()),
                    }
                }
                builder.append(true)?;
            }
            _ => builder.append(false)?,
        }
    }
    let built = builder.finish();
    // Align entry-field nullability with the target (MapBuilder's inner
    // struct layout matches by construction; the cast is a no-op check).
    if built.data_type() == target_field.data_type() {
        Ok(std::sync::Arc::new(built))
    } else {
        datafusion::arrow::compute::cast(&built, target_field.data_type())
            .map_err(|e| anyhow::anyhow!("map coercion type mismatch: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::{Configuration, SchemaConfig, StorageConfig};
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[test]
    fn coerce_converts_json_strings_to_map_column() {
        use datafusion::arrow::array::MapArray;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "log_attributes",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![
                Some(r#"{"namespace":"prod","port":8080}"#),
                None,
                Some("not-json"),
            ]))],
        )
        .unwrap();

        let target = Arc::new(Schema::new(vec![Field::new_map(
            "log_attributes",
            "key_value",
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            false,
            true,
        )]));

        let out = coerce_batch_to_schema(batch, &target).unwrap();
        let map = out
            .column_by_name("log_attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        // Row 0: two entries, non-string value rendered as text.
        assert!(!map.is_null(0));
        let entries = map.value(0);
        assert_eq!(entries.len(), 2);
        // Rows 1 (null) and 2 (unparseable) become null maps.
        assert!(map.is_null(1));
        assert!(map.is_null(2));
    }

    #[test]
    fn coerce_fills_null_for_missing_nullable_column() {
        // Batch has only `body`; the target adds a nullable `label_ns`.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("hi"), Some("yo")]))],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("label_ns", DataType::Utf8, true),
        ]));

        let out = coerce_batch_to_schema(batch.clone(), &target).unwrap();
        assert_eq!(out.num_columns(), 2);
        let label = out.column_by_name("label_ns").unwrap();
        assert_eq!(label.null_count(), 2);

        // A missing *required* column is still a hard error.
        let required_target = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("must_have", DataType::Utf8, false),
        ]));
        assert!(coerce_batch_to_schema(batch, &required_target).is_err());
    }

    #[test]
    fn coerce_rejects_unrepresentable_cast_instead_of_nulling_it() {
        // W8: a type drift (batch column vs. table column) that `cast`
        // cannot represent must fail loudly, not silently null the row.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("abc")]))],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));

        let err = coerce_batch_to_schema(batch, &target)
            .expect_err("a non-numeric string cast to Int64 must error, not null the row");
        assert!(
            err.to_string().contains("Failed to cast column 'n'"),
            "unexpected error: {err}"
        );
    }

    async fn create_test_catalog_manager() -> CatalogManager {
        let config = Configuration {
            schema: SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: Default::default(),
                materialized_labels: Default::default(),
            },
            storage: StorageConfig {
                dsn: "memory://".to_string(),
            },
            ..Default::default()
        };

        CatalogManager::new(config).await.unwrap()
    }

    #[test]
    fn marker_ids_round_trip() {
        let ids = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
        let encoded = encode_marker_ids(&ids);
        let decoded = decode_marker_ids(&encoded);
        assert_eq!(decoded, ids.iter().copied().collect::<HashSet<_>>());
    }

    #[test]
    fn marker_decode_tolerates_garbage() {
        let id = uuid::Uuid::new_v4();
        let value = format!("not-a-uuid,{},", id.simple());
        let decoded = decode_marker_ids(&value);
        assert_eq!(decoded, HashSet::from([id]));
        assert!(decode_marker_ids("").is_empty());
    }

    #[test]
    fn marker_key_is_namespaced_per_writer() {
        assert_eq!(
            wal_marker_key("abc123"),
            "signaldb.wal.committed.abc123".to_string()
        );
        assert_ne!(wal_marker_key("writer-a"), wal_marker_key("writer-b"));
    }

    #[test]
    fn marker_carries_the_commit_time_and_still_decodes_legacy_values() {
        // Markers accumulate one property per writer id, forever (#1307), and
        // nothing could date them — so nothing could retire them. The value
        // now leads with the commit time. Values written before this change
        // have no timestamp and must still decode: they are live idempotency
        // evidence for whichever writer wrote them.
        let ids = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
        let encoded = encode_marker_ids(&ids);
        assert!(
            encoded.starts_with("t="),
            "a marker must record when it was written, got: {encoded}"
        );
        assert_eq!(
            decode_marker_ids(&encoded),
            ids.iter().copied().collect::<HashSet<_>>()
        );
        assert!(marker_committed_at(&encoded).is_some());

        let legacy = ids
            .iter()
            .map(|id| id.simple().to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            decode_marker_ids(&legacy),
            ids.iter().copied().collect::<HashSet<_>>(),
            "a pre-#1307 marker must still be readable"
        );
        assert!(
            marker_committed_at(&legacy).is_none(),
            "an undated marker must report no commit time rather than a fabricated one"
        );
    }

    #[test]
    fn only_other_writers_stale_markers_are_selected_for_removal() {
        let now = 1_800_000_000u64;
        let retention = Duration::from_secs(30 * 24 * 3600);
        let fresh = now - 60;
        let stale = now - 40 * 24 * 3600;

        let properties = HashMap::from([
            // Ours: never removed, however old — it is the evidence for the
            // commit we are about to make.
            (wal_marker_key("me"), format!("t={stale}:")),
            (wal_marker_key("other-fresh"), format!("t={fresh}:")),
            (wal_marker_key("other-stale"), format!("t={stale}:")),
            // Undated (pre-#1307): only retirable once this process has been
            // up longer than the window, so it can never be deleted out from
            // under a live writer that simply has not committed since deploy.
            (wal_marker_key("other-legacy"), "deadbeef".to_string()),
            // Not a marker at all.
            (
                "write.metadata.previous-versions-max".to_string(),
                "100".to_string(),
            ),
        ]);

        let own = HashSet::from(["me".to_string()]);
        let young_process = stale_marker_keys(&properties, &own, now, retention, false);
        assert_eq!(
            young_process,
            vec![wal_marker_key("other-stale")],
            "a young process must retire only markers it can date"
        );

        let old_process = stale_marker_keys(&properties, &own, now, retention, true);
        let mut old_process = old_process;
        old_process.sort();
        assert_eq!(
            old_process,
            vec![
                wal_marker_key("other-legacy"),
                wal_marker_key("other-stale")
            ],
            "once the process outlives the window, undated markers are retirable too"
        );
    }

    #[tokio::test]
    async fn test_iceberg_writer_with_memory_catalog() {
        let catalog_manager = create_test_catalog_manager().await;

        let object_store = Arc::new(InMemory::new());

        // With a real in-memory SQL catalog, creating the writer (and thus the
        // "traces" table) must deterministically succeed.
        let writer = IcebergTableWriter::new(
            &catalog_manager,
            object_store,
            "test-tenant".to_string(),
            "local".to_string(),
            "traces".to_string(),
        )
        .await
        .expect("IcebergTableWriter::new should succeed against an in-memory SQL catalog");

        assert_eq!(writer.table_identifier().name(), "traces");
    }

    #[tokio::test]
    async fn retiring_stale_markers_removes_them_from_the_table_and_keeps_the_live_one() {
        // Markers accumulated one property per writer id, forever, and every
        // property is paid for in metadata.json on every read and commit
        // (#1307). Retirement has to actually delete the property, not just
        // shrink its value — and must never touch a marker that is still
        // idempotency evidence.
        let catalog_manager = create_test_catalog_manager().await;
        let mut writer = IcebergTableWriter::new(
            &catalog_manager,
            Arc::new(InMemory::new()),
            "test-tenant".to_string(),
            "local".to_string(),
            "traces".to_string(),
        )
        .await
        .unwrap();

        // Seed three foreign markers: one committed long ago, one committed
        // just now, and one undated (as written before #1307).
        let now = common::wal::unix_now_secs();
        let ancient = now - 60 * 24 * 3600;
        let identifier = writer.table_identifier().clone();
        catalog_manager
            .catalog()
            .update_table(CommitTable {
                identifier: identifier.clone(),
                requirements: Vec::new(),
                updates: vec![TableUpdate::SetProperties {
                    updates: HashMap::from([
                        (wal_marker_key("gone"), format!("t={ancient}:")),
                        (wal_marker_key("live"), format!("t={now}:")),
                        (wal_marker_key("undated"), "deadbeef".to_string()),
                    ]),
                }],
            })
            .await
            .unwrap();
        writer.reload_table().await.unwrap();

        let retention = Duration::from_secs(30 * 24 * 3600);
        let retired = writer
            .retire_stale_markers(&HashSet::from(["me".to_string()]), retention, false)
            .await
            .unwrap();
        assert_eq!(retired, 1, "only the datably-stale marker is retirable");

        let properties = &writer.table.metadata().properties;
        assert!(
            !properties.contains_key(&wal_marker_key("gone")),
            "a stale marker must be removed from the table, not just emptied"
        );
        assert!(
            properties.contains_key(&wal_marker_key("live")),
            "a marker committed within the window is still idempotency evidence"
        );
        assert!(
            properties.contains_key(&wal_marker_key("undated")),
            "an undated marker must survive a process younger than the window"
        );

        // Once the process has outlived the window, the undated one goes too.
        let retired = writer
            .retire_stale_markers(&HashSet::from(["me".to_string()]), retention, true)
            .await
            .unwrap();
        assert_eq!(retired, 1);
        assert!(
            !writer
                .table
                .metadata()
                .properties
                .contains_key(&wal_marker_key("undated"))
        );
    }
}
