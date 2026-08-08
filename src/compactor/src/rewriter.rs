//! Parquet file reading, merging, and rewriting
//!
//! Handles the core compaction logic: reading a table's live data files,
//! merging and sorting the data, and writing optimized larger Parquet files
//! directly to the table's object store. The atomic snapshot commit that swaps
//! old files for new ones is performed by [`crate::commit::IcebergCommitter`].

use anyhow::{Context, Result};
use common::CatalogManager;
use common::schema::materialized_column_name;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::spec::manifest::DataFile;
use iceberg_rust::table::Table;
use std::collections::HashSet;
use std::sync::Arc;

/// What the promotion pass decided to do to this rewrite (epic #737,
/// #734). Empty/default when promotion is disabled or dry-run.
#[derive(Debug, Default)]
struct PromotionOutcome {
    /// `(attribute key, label column)` pairs whose columns should be
    /// recomputed from the attribute sources during this rewrite.
    backfill: Vec<(String, String)>,
    /// Whether the table's schema was evolved (label columns added or
    /// dropped) and must be reloaded before writing.
    evolved: bool,
    /// `label_<key>` columns dropped by the demotion half: the rewrite
    /// must project them out of the merged batches, since the read
    /// happened under the pre-demotion schema.
    dropped_columns: Vec<String>,
}

/// Result of rewriting a table's data files.
pub struct RewriteOutcome {
    /// Newly written data files (with real paths, sizes, and record counts)
    /// ready to be committed as a replacement snapshot.
    pub new_files: Vec<DataFile>,
    /// Total bytes written across the new files.
    pub output_size_bytes: u64,
    /// Total rows written (for integrity verification against the input).
    pub rows_written: u64,
}

/// Handles Parquet file merging and rewriting
pub struct ParquetRewriter {
    catalog_manager: Arc<CatalogManager>,
    /// Service catalog for persisting advisory attribute statistics
    /// (epic #737, #733). `None` (e.g. in tests) keeps the analyzer
    /// log-only.
    service_catalog: Option<Arc<common::catalog::Catalog>>,
}

impl ParquetRewriter {
    /// Create a new Parquet rewriter
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        Self {
            catalog_manager,
            service_catalog: None,
        }
    }

    /// Persist the advisory attribute statistics to this service catalog.
    pub fn set_service_catalog(&mut self, catalog: Arc<common::catalog::Catalog>) {
        self.service_catalog = Some(catalog);
    }

    /// Load a table with fresh metadata, without creating it if missing.
    ///
    /// Unlike `ensure_table`, this never creates the table: compaction must
    /// only operate on tables that already exist, and always on current
    /// metadata.
    pub async fn load_fresh_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Table> {
        let identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);
        let tabular = self
            .catalog_manager
            .catalog()
            .load_tabular(&identifier)
            .await
            .with_context(|| format!("Failed to load table {identifier} with fresh metadata"))?;
        match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(table) => Ok(table),
            _ => Err(anyhow::anyhow!("Expected table but got view: {identifier}")),
        }
    }

    /// Read, merge, sort, and rewrite ONE hour partition into new Parquet
    /// files.
    ///
    /// Reads only the rows belonging to `partition_hours` from `table` (which
    /// the caller loaded fresh and pinned to a snapshot), sorts them for query
    /// performance, and writes new Parquet files directly to the table's
    /// object store. No snapshot is committed here — the caller commits the
    /// returned files atomically as a delta.
    ///
    /// Scoping the read to a partition is what bounds compaction's cost
    /// (issue #933): the previous whole-table rewrite made write amplification
    /// and peak memory proportional to total table size, so compacting one new
    /// hour re-read and re-wrote months of history.
    ///
    /// Returns `None` when the partition has no data to compact.
    pub async fn rewrite_partition(
        &self,
        table: &Table,
        partition_hours: i64,
        target_file_size_bytes: u64,
    ) -> Result<Option<RewriteOutcome>> {
        let table_name = table.identifier().name().to_string();

        let merged_batches = self
            .read_and_merge(table, partition_hours)
            .await
            .context("Failed to read and merge partition data")?;

        if merged_batches.is_empty() || merged_batches.iter().all(|b| b.num_rows() == 0) {
            tracing::info!(
                table = %table_name,
                partition_hours,
                "No data found in partition, skipping compaction"
            );
            return Ok(None);
        }

        let rows_read: u64 = merged_batches.iter().map(|b| b.num_rows() as u64).sum();

        // Advisory attribute-stats pass (epic #737 L4a): the data is
        // already in memory for the rewrite, so per-key presence and
        // approximate cardinality come nearly free. Logs promotion
        // candidates; changes nothing.
        let (attr_stats, scanned) = crate::attr_stats::analyze_batches(&merged_batches);
        crate::attr_stats::log_promotion_candidates(&table_name, &attr_stats, scanned);
        let mut promotion = PromotionOutcome::default();
        if let Some(catalog) = &self.service_catalog {
            // The identifier namespace is [tenant_slug, dataset_slug].
            let ns = table.identifier().namespace();
            if let [tenant, dataset] = ns.as_ref() {
                crate::attr_stats::persist_stats(
                    catalog,
                    tenant,
                    dataset,
                    &table_name,
                    &attr_stats,
                    scanned,
                )
                .await;
                promotion = self
                    .run_promotion_pass(catalog, tenant, dataset, &table_name, table)
                    .await;
            }
        }

        // When the promotion pass evolved the schema (new label columns),
        // the write must happen under the reloaded table so the new files
        // are written with the evolved schema.
        let reloaded_table;
        let write_table: &Table = if promotion.evolved {
            reloaded_table = self
                .load_fresh_by_identifier(table.identifier())
                .await
                .context("Failed to reload table after schema evolution")?;
            &reloaded_table
        } else {
            table
        };

        // Drop demoted label columns from the merged batches: the read
        // happened under the pre-demotion schema, so the batches still
        // carry the columns the demotion just removed. The attribute
        // values themselves stay in the map attributes column, so nothing
        // is lost.
        let merged_batches = if promotion.dropped_columns.is_empty() {
            merged_batches
        } else {
            Self::drop_columns(merged_batches, &promotion.dropped_columns)
                .context("Failed to drop demoted label columns from rewrite batches")?
        };

        // Recompute the materialized label columns from the attribute
        // sources. Since every live row is rewritten, pre-existing rows
        // get their values backfilled by construction.
        let merged_batches = if promotion.backfill.is_empty() {
            merged_batches
        } else {
            let schema_columns: HashSet<String> = write_table
                .current_schema()
                .map(|schema| schema.fields().iter().map(|f| f.name.clone()).collect())
                .unwrap_or_default();
            let pairs: Vec<(String, String)> = promotion
                .backfill
                .into_iter()
                .filter(|(_, column)| schema_columns.contains(column))
                .collect();
            crate::attr_promotion::backfill_label_columns(merged_batches, &pairs)
                .context("Failed to backfill materialized label columns")?
        };

        // Chunk batches toward the target file size so the writer produces
        // reasonably sized files.
        let split_batches = self.split_batches_by_size(merged_batches, target_file_size_bytes)?;

        let batch_stream = futures::stream::iter(
            split_batches
                .into_iter()
                .map(Ok::<_, datafusion::arrow::error::ArrowError>),
        );

        let new_files =
            iceberg_rust::arrow::write::write_parquet_partitioned(write_table, batch_stream, None)
                .await
                .context("Failed to write compacted Parquet files")?;

        let output_size_bytes: u64 = new_files
            .iter()
            .map(|f| *f.file_size_in_bytes() as u64)
            .sum();
        let rows_written: u64 = new_files.iter().map(|f| *f.record_count() as u64).sum();

        // A rewrite must never lose rows. Fail loudly before the commit if
        // the written files do not account for every row that was read.
        anyhow::ensure!(
            rows_written == rows_read,
            "Compaction row count mismatch for {table_name}: read {rows_read} rows but wrote {rows_written}"
        );

        tracing::info!(
            table = %table_name,
            output_files = new_files.len(),
            output_bytes = output_size_bytes,
            rows = rows_written,
            "Rewrote table data into compacted files"
        );

        Ok(Some(RewriteOutcome {
            new_files,
            output_size_bytes,
            rows_written,
        }))
    }

    /// Auto-promotion pass (epic #737, #734): score the freshly persisted
    /// statistics against the configured guardrails, log the
    /// promotion/demotion decision, and persist the hysteresis streaks.
    ///
    /// With `dry_run = false` the pass also *acts* on the decision: it
    /// evolves the table schema (adds the promoted `label_<key>` columns
    /// and drops the demoted ones via the metadata-only
    /// AddSchema/SetCurrentSchema commits) and returns the backfill plan
    /// for this rewrite. The two commits per table — schema flip here,
    /// file replace after the rewrite — are safe in that order: writer
    /// and querier read the schema live, null-fill new columns until the
    /// rewrite lands, and fall back to map/JSON matching for dropped
    /// ones (label routing is derived from the table schema per query).
    /// Pinned `[schema.materialized_labels]` keys are never demoted —
    /// the decision engine excludes them.
    async fn run_promotion_pass(
        &self,
        catalog: &common::catalog::Catalog,
        tenant: &str,
        dataset: &str,
        table_name: &str,
        table: &Table,
    ) -> PromotionOutcome {
        let mut outcome = PromotionOutcome::default();
        let config = self.catalog_manager.config();
        let promotion = &config.compactor.attr_promotion;
        if !promotion.enabled {
            return outcome;
        }
        let signal = crate::attr_stats::signal_of_table(table_name);
        let stats = match catalog.get_attribute_stats(tenant, dataset, signal).await {
            Ok(stats) => stats,
            Err(e) => {
                tracing::warn!(error = %e, table = %table_name, "Failed to load attribute stats for promotion pass");
                return outcome;
            }
        };
        // The table's current label_<key> columns and the pinned allowlist.
        let label_columns: Vec<String> = table
            .current_schema()
            .map(|schema| {
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name.clone())
                    .filter(|n| n.starts_with("label_"))
                    .collect()
            })
            .unwrap_or_default();
        let materialized = crate::attr_promotion::materialized_keys_of(&label_columns, &stats);
        let tenant_schema = config.get_tenant_schema_config(tenant);
        let m = &tenant_schema.materialized_labels;
        let pinned: &[String] = match signal {
            "traces" => &m.traces,
            "logs" => &m.logs,
            "metrics" => &m.metrics,
            "profiles" => &m.profiles,
            _ => &[],
        };
        let (decision, new_streaks) =
            crate::attr_promotion::decide(&stats, &materialized, pinned, promotion);
        crate::attr_promotion::log_decision(table_name, &decision, promotion.dry_run);
        for (key, streak) in new_streaks {
            if let Err(e) = catalog
                .set_attribute_promote_streak(tenant, dataset, signal, &key, streak)
                .await
            {
                tracing::warn!(error = %e, attr_key = %key, "Failed to persist promotion streak");
            }
        }

        // Act on the decision when the pass is out of dry-run: evolve the
        // schema before the rewrite so the new files carry the promoted
        // columns. An evolution failure is logged and the compaction
        // continues under the old schema — promotion must never fail a
        // rewrite.
        if promotion.dry_run {
            return outcome;
        }
        if !decision.promote.is_empty() {
            match common::iceberg::evolution::add_label_columns(
                self.catalog_manager.catalog(),
                table.identifier(),
                &decision.promote,
            )
            .await
            {
                Ok(_) => {
                    outcome.evolved = true;
                    // TODO(#731): set bloom-filter table properties for the
                    // newly promoted label columns once the
                    // `bloom_filter_properties_for_labels` helper lands in
                    // common.
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        table = %table_name,
                        keys = ?decision.promote,
                        "Failed to evolve schema for attribute promotion; continuing compaction without it"
                    );
                }
            }
        }

        // Demotion (#734 P3): drop the long-unqueried auto-promoted
        // columns before the rewrite so the new files stop carrying
        // them. Like promotion, a failure is logged and the compaction
        // continues — at worst the column lives until the next cycle.
        let mut demoted: Vec<String> = Vec::new();
        if !decision.demote.is_empty() {
            match common::iceberg::evolution::remove_label_columns(
                self.catalog_manager.catalog(),
                table.identifier(),
                &decision.demote,
            )
            .await
            {
                Ok(_) => {
                    outcome.evolved = true;
                    demoted = decision.demote;
                    outcome.dropped_columns = demoted
                        .iter()
                        .map(|key| materialized_column_name(key))
                        .collect();
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        table = %table_name,
                        keys = ?decision.demote,
                        "Failed to evolve schema for attribute demotion; continuing compaction without it"
                    );
                }
            }
        }

        // Backfill plan: the freshly promoted keys plus every label column
        // whose source key is still known (already-materialized keys from
        // the stats, and the pinned allowlist), minus what was just
        // demoted. Recomputing existing columns heals rows the writer
        // left null during the transition window. Deduplicated by column
        // name — the key -> column encoding is lossy.
        let mut seen_columns = HashSet::new();
        let promoted: &[String] = if outcome.evolved {
            &decision.promote
        } else {
            &[]
        };
        for key in promoted
            .iter()
            .chain(materialized.iter())
            .chain(pinned)
            .filter(|key| !demoted.contains(key))
        {
            let column = materialized_column_name(key);
            if seen_columns.insert(column.clone()) {
                outcome.backfill.push((key.clone(), column));
            }
        }
        outcome
    }

    /// Project the named columns out of every batch (missing columns are
    /// ignored). Used to strip demoted label columns that were read under
    /// the pre-demotion schema.
    fn drop_columns(batches: Vec<RecordBatch>, columns: &[String]) -> Result<Vec<RecordBatch>> {
        batches
            .into_iter()
            .map(|batch| {
                let keep: Vec<usize> = batch
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| !columns.contains(field.name()))
                    .map(|(idx, _)| idx)
                    .collect();
                if keep.len() == batch.num_columns() {
                    return Ok(batch);
                }
                batch
                    .project(&keep)
                    .context("Failed to project batch without demoted columns")
            })
            .collect()
    }

    /// Load a table with fresh metadata by its Iceberg identifier.
    async fn load_fresh_by_identifier(&self, identifier: &Identifier) -> Result<Table> {
        let tabular = self
            .catalog_manager
            .catalog()
            .load_tabular(identifier)
            .await
            .with_context(|| format!("Failed to load table {identifier} with fresh metadata"))?;
        match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(table) => Ok(table),
            _ => Err(anyhow::anyhow!("Expected table but got view: {identifier}")),
        }
    }

    /// Get sort columns for a given table type
    ///
    /// Returns a list of (column_name, ascending, nulls_first) tuples
    /// for sorting compacted data. Returns empty vector for unknown tables.
    fn get_sort_columns(table_name: &str) -> Vec<(&str, bool, bool)> {
        match table_name {
            "traces" => vec![("timestamp", true, true), ("trace_id", true, true)],
            "logs" => vec![
                ("timestamp", true, true),
                ("service_name", true, true),
                ("severity_text", true, true),
            ],
            // All 5 metrics types use the same sort pattern
            "metrics_gauge"
            | "metrics_sum"
            | "metrics_histogram"
            | "metrics_exponential_histogram"
            | "metrics_summary" => vec![
                ("timestamp", true, true),
                ("metric_name", true, true),
                ("service_name", true, true),
            ],
            _ => {
                tracing::warn!(
                    "No sort configuration for table {table_name}, data will not be sorted"
                );
                vec![]
            }
        }
    }

    /// Build the compaction session context.
    ///
    /// The rewrite runs under a bounded memory pool, so DataFusion's own
    /// operators (the partition sort above all) spill to disk rather than
    /// growing the process heap without limit — the old unbounded
    /// `SessionContext::new()` is what let a big table OOM the compactor
    /// outright.
    ///
    /// The bound is **not** total: `read_and_merge` still calls `collect()`,
    /// and the resulting `Vec<RecordBatch>` lives outside the pool's
    /// accounting for the whole of attribute analysis, backfill, splitting
    /// and writing. A partition large enough can therefore still exhaust the
    /// heap despite this limit. Closing that gap means streaming the rewrite
    /// via `execute_stream` (openspec task 5.2), which is blocked on making
    /// the attribute-stats pass incremental (epic #737).
    ///
    /// The pool is a `FairSpillPool` (see
    /// [`common::datafusion_runtime`]), not DataFusion's greedy default:
    /// a greedy pool grants memory first-come, so concurrent spilling
    /// sorters exhaust it between them and one cannot obtain the
    /// reservation it needs to spill while its peers hold the memory.
    ///
    /// The fan-out is bounded too (`compactor.target_partitions`, default
    /// 1). Left at DataFusion's default the plan gets one `ExternalSorter`
    /// *per core*, each with its own unspillable merge reservation, and
    /// they divide the single pool between them — so the budget is
    /// exhausted by the sort's own concurrency rather than by any
    /// oversized partition (#1064). One sorter owning the whole budget
    /// spills within it instead, and makes the ceiling independent of the
    /// host's core count. Compaction is a background job, so the lost
    /// parallelism is a good trade.
    ///
    /// A runtime that fails to build falls back to an unbounded context
    /// rather than failing the cycle, which is logged.
    fn compaction_context(&self) -> SessionContext {
        let compactor = &self.catalog_manager.config().compactor;
        let memory_limit_mb = compactor.memory_limit_mb;
        let session_config = Self::compaction_session_config(compactor.target_partitions);
        let builder =
            datafusion::execution::runtime_env::RuntimeEnvBuilder::new().with_memory_pool(
                common::datafusion_runtime::bounded_memory_pool(memory_limit_mb * 1024 * 1024, 1.0),
            );
        match builder.build() {
            Ok(runtime_env) => {
                tracing::debug!(
                    memory_limit_mb,
                    target_partitions = session_config.target_partitions(),
                    "Compaction memory pool configured"
                );
                SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env))
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "Failed to build limited RuntimeEnv for compaction; falling back to unlimited memory"
                );
                SessionContext::new_with_config(session_config)
            }
        }
    }

    /// `SessionConfig` for the rewrite, with the partition fan-out pinned.
    ///
    /// `0` means "use DataFusion's default" — `with_target_partitions`
    /// rejects zero, so the knob is simply not applied.
    fn compaction_session_config(target_partitions: usize) -> datafusion::prelude::SessionConfig {
        let config = datafusion::prelude::SessionConfig::new();
        if target_partitions == 0 {
            config
        } else {
            config.with_target_partitions(target_partitions)
        }
    }

    /// Predicate selecting exactly the rows of one hour partition.
    ///
    /// The `timestamp_hour` partition transform is `Hour(timestamp)` and the
    /// Iceberg `Timestamp` type is microseconds since the epoch, so partition
    /// `h` is the half-open microsecond range `[h*3600e6, (h+1)*3600e6)`.
    ///
    /// `datafusion_iceberg` pushes this down twice — first pruning manifests
    /// by their partition summaries, then pruning data files by column
    /// statistics — so the scan reads only the target partition's files. The
    /// predicate is also applied as a row filter, so correctness does not
    /// depend on pruning being exact.
    fn partition_predicate(partition_hours: i64) -> Expr {
        const MICROS_PER_HOUR: i64 = 3_600 * 1_000_000;
        let start = partition_hours * MICROS_PER_HOUR;
        let end = start + MICROS_PER_HOUR;
        col("timestamp")
            .gt_eq(lit(ScalarValue::TimestampMicrosecond(Some(start), None)))
            .and(col("timestamp").lt(lit(ScalarValue::TimestampMicrosecond(Some(end), None))))
    }

    /// Read and merge one partition's live data, sorted for query performance.
    async fn read_and_merge(
        &self,
        table: &Table,
        partition_hours: i64,
    ) -> Result<Vec<RecordBatch>> {
        let ctx = self.compaction_context();

        let table_name = table.identifier().name().to_string();
        let datafusion_table = Arc::new(datafusion_iceberg::DataFusionTable::from(table.clone()));
        ctx.register_table(&table_name, datafusion_table)
            .context("Failed to register table with DataFusion")?;

        let df = ctx
            .table(&table_name)
            .await
            .context("Failed to read table")?
            .filter(Self::partition_predicate(partition_hours))
            .with_context(|| {
                format!("Failed to scope {table_name} read to partition {partition_hours}")
            })?;

        let sort_cols = Self::get_sort_columns(&table_name);
        let sorted_df = if !sort_cols.is_empty() {
            let sort_exprs: Vec<_> = sort_cols
                .into_iter()
                .map(|(col_name, asc, nulls_first)| col(col_name).sort(asc, nulls_first))
                .collect();

            df.sort(sort_exprs)
                .with_context(|| format!("Failed to sort {table_name} table"))?
        } else {
            df
        };

        let batches = sorted_df
            .collect()
            .await
            .context("Failed to collect query results")?;

        tracing::debug!(
            table = %table_name,
            partition_hours,
            batch_count = batches.len(),
            "Collected partition data for rewrite"
        );

        Ok(batches)
    }

    /// Split batches to target file size
    fn split_batches_by_size(
        &self,
        batches: Vec<RecordBatch>,
        target_size_bytes: u64,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = vec![];
        let mut current_batch_rows = vec![];
        let mut current_size = 0u64;

        for batch in batches {
            let batch_size = batch.get_array_memory_size() as u64;

            // If this batch alone exceeds target size, split it
            if batch_size > target_size_bytes && batch.num_rows() > 1 {
                // Flush current accumulated rows first
                if !current_batch_rows.is_empty() {
                    let merged = self.merge_batches(&current_batch_rows)?;
                    result.push(merged);
                    current_batch_rows.clear();
                    current_size = 0;
                }

                // Split large batch into smaller chunks
                // Use u128 to avoid overflow in multiplication
                let rows_per_chunk = ((batch.num_rows() as u128 * target_size_bytes as u128)
                    / batch_size as u128)
                    .min(usize::MAX as u128) as usize;
                let rows_per_chunk = rows_per_chunk.max(1);

                let mut offset = 0;
                while offset < batch.num_rows() {
                    let length = (batch.num_rows() - offset).min(rows_per_chunk);
                    let slice = batch.slice(offset, length);
                    result.push(slice);
                    offset += length;
                }
            } else if current_size + batch_size > target_size_bytes
                && !current_batch_rows.is_empty()
            {
                // Current accumulation would exceed target, flush it
                let merged = self.merge_batches(&current_batch_rows)?;
                result.push(merged);
                current_batch_rows.clear();
                current_batch_rows.push(batch);
                current_size = batch_size;
            } else {
                // Accumulate this batch
                current_batch_rows.push(batch);
                current_size += batch_size;
            }
        }

        // Flush remaining batches
        if !current_batch_rows.is_empty() {
            let merged = self.merge_batches(&current_batch_rows)?;
            result.push(merged);
        }

        Ok(result)
    }

    /// Merge multiple batches with the same schema into one
    fn merge_batches(&self, batches: &[RecordBatch]) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Err(anyhow::anyhow!("Cannot merge empty batch list"));
        }

        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        // Use DataFusion's concat_batches
        let schema = batches[0].schema();
        datafusion::arrow::compute::concat_batches(&schema, batches)
            .context("Failed to merge batches")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_merge_batches() {
        let catalog_manager = CatalogManager::new_in_memory().await.unwrap();
        let rewriter = ParquetRewriter::new(Arc::new(catalog_manager));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["Charlie", "Diana"])),
            ],
        )
        .unwrap();

        let merged = rewriter.merge_batches(&[batch1, batch2]).unwrap();

        assert_eq!(merged.num_rows(), 4);
        assert_eq!(merged.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_split_batches_by_size() {
        let catalog_manager = CatalogManager::new_in_memory().await.unwrap();
        let rewriter = ParquetRewriter::new(Arc::new(catalog_manager));

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Create a batch with 100 rows
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..100).collect::<Vec<i64>>()))],
        )
        .unwrap();

        let batch_size = batch.get_array_memory_size() as u64;

        // Split with target size smaller than batch
        let target_size = batch_size / 3;
        let split = rewriter
            .split_batches_by_size(vec![batch], target_size)
            .expect("Split should succeed");

        // Should be split into multiple batches
        assert!(split.len() > 1);

        // Total rows should be preserved
        let total_rows: usize = split.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    /// Build a rewriter whose config has been tweaked by `f`.
    async fn rewriter_with_config(
        f: impl FnOnce(&mut common::config::Configuration),
    ) -> ParquetRewriter {
        let mut config = common::config::Configuration::default();
        config.schema.catalog_uri = "sqlite::memory:".to_string();
        f(&mut config);
        let catalog_manager = CatalogManager::new(config).await.unwrap();
        ParquetRewriter::new(Arc::new(catalog_manager))
    }

    /// The rewrite must not fan out to the core count: each DataFusion
    /// partition gets its own `ExternalSorter` plus an unspillable merge
    /// reservation, and N of them divide the single `memory_limit_mb`
    /// pool — which is how a 512 MB budget is exhausted by concurrency
    /// alone rather than by any oversized partition (#1064).
    #[tokio::test]
    async fn compaction_context_does_not_fan_out_to_core_count() {
        let rewriter = rewriter_with_config(|_| {}).await;
        let ctx = rewriter.compaction_context();

        assert_eq!(
            ctx.state().config().target_partitions(),
            1,
            "compaction must default to a single sorter owning the whole memory budget"
        );
    }

    #[tokio::test]
    async fn compaction_context_honors_configured_target_partitions() {
        let rewriter = rewriter_with_config(|c| c.compactor.target_partitions = 3).await;
        let ctx = rewriter.compaction_context();

        assert_eq!(ctx.state().config().target_partitions(), 3);
    }

    /// The pool must be fair, not greedy: with two spilling consumers
    /// registered, neither may take the whole budget and starve the other
    /// into a failed allocation (#1064). Asserted behaviorally because
    /// `TrackConsumersPool` reports its own name, not the inner pool's.
    #[tokio::test]
    async fn compaction_context_uses_a_fair_spill_pool() {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let rewriter = rewriter_with_config(|c| c.compactor.memory_limit_mb = 100).await;
        let pool = rewriter
            .compaction_context()
            .runtime_env()
            .memory_pool
            .clone();

        let first = MemoryConsumer::new("ExternalSorter[0]")
            .with_can_spill(true)
            .register(&pool);
        let _second = MemoryConsumer::new("ExternalSorter[1]")
            .with_can_spill(true)
            .register(&pool);

        assert!(
            first.try_grow(90 * 1024 * 1024).is_err(),
            "one sorter must not be able to take 90% of the pool while a peer is registered — \
             that is the greedy behavior that fails compaction"
        );
    }

    /// `0` is the documented escape hatch back to DataFusion's own
    /// default. It must not reach `with_target_partitions`, which panics
    /// on zero.
    #[tokio::test]
    async fn compaction_context_treats_zero_target_partitions_as_auto() {
        let rewriter = rewriter_with_config(|c| c.compactor.target_partitions = 0).await;
        let ctx = rewriter.compaction_context();

        assert!(
            ctx.state().config().target_partitions() >= 1,
            "zero must fall back to DataFusion's available-parallelism default"
        );
    }
}
