//! Parquet file reading, merging, and rewriting
//!
//! Handles the core compaction logic: reading a table's live data files,
//! merging and sorting the data, and writing optimized larger Parquet files
//! directly to the table's object store. The atomic snapshot commit that swaps
//! old files for new ones is performed by [`crate::commit::IcebergCommitter`].

use anyhow::{Context, Result};
use common::CatalogManager;
use common::iceberg::sort::{DeclaredSortColumn, UndeclaredFallback, WriteSortKey, write_sort_key};
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

/// Floor on the per-sorter share of the memory pool.
///
/// Below roughly this much a spilling sort has no room for a batch plus
/// the reservation its spill merge needs, so it fails instead of
/// spilling — the #1064 failure in miniature.
const MIN_PER_SORTER_MB: u64 = 64;

/// Whether a partition read should be sorted for output, or is only being
/// scanned for statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SortRows {
    /// Sort by these columns, in this key order.
    By(Vec<DeclaredSortColumn>),
    /// Read in whatever order the scan produces.
    No,
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
    /// How many values per attribute key the analyzer keeps as a suggestion
    /// sketch for query discovery (`compactor.value_sketch_size`).
    value_sketch_size: usize,
}

impl ParquetRewriter {
    /// Create a new Parquet rewriter
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        Self {
            catalog_manager,
            service_catalog: None,
            value_sketch_size: crate::attr_stats::DEFAULT_VALUE_SKETCH_SIZE,
        }
    }

    /// Bound the per-key value sketch the analyzer keeps.
    pub fn set_value_sketch_size(&mut self, size: usize) {
        self.value_sketch_size = size;
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
        self.load_fresh_by_identifier(&identifier).await
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
    ///
    /// ## Streaming
    ///
    /// The partition is streamed, never collected. The old `collect()` put
    /// the whole sorted partition on the heap *outside* the DataFusion
    /// pool's accounting, which made `memory_limit_mb` a bound on the sort
    /// alone and left peak process memory proportional to partition size
    /// (#1064). Live memory is now a handful of batches plus one output
    /// file's worth of accumulation.
    ///
    /// That costs a second scan. The rewrite has a genuine ordering
    /// constraint: the promotion decision must be settled before the first
    /// output batch is written, but it is made from statistics gathered by
    /// scanning the very same data. Collecting satisfied both from one
    /// pass by keeping everything in memory. Streaming cannot, so the
    /// partition is read twice:
    ///
    /// 1. an **unsorted** scan that folds the attribute statistics — order
    ///    is irrelevant to presence and cardinality, so this pass plans no
    ///    sort at all and is comparatively cheap;
    /// 2. the **sorted** scan that feeds the transforms and the writer.
    ///
    /// Deciding promotion from the *previous* cycle's persisted statistics
    /// would avoid the second scan, but it would also mean the partition
    /// that first reveals a hot attribute key never materializes it — and
    /// since a compacted partition is not compacted again, those files
    /// would never gain the column. A background job can afford an extra
    /// unsorted scan more than the promotion pass can afford to skip a
    /// partition.
    ///
    /// Both passes read the table object the caller pinned — never a
    /// freshly loaded one, which would carry any snapshot committed since
    /// — so the row counts are independent observations of the *same*
    /// snapshot. That makes the read-vs-written parity check below
    /// stronger than it was when both numbers came from the same
    /// materialized `Vec`. Only the write uses the post-promotion schema.
    pub async fn rewrite_partition(
        &self,
        table: &Table,
        partition_hours: i64,
        target_file_size_bytes: u64,
    ) -> Result<Option<RewriteOutcome>> {
        let table_name = table.identifier().name().to_string();

        // Pass 1: fold the advisory attribute statistics over an unsorted
        // stream (epic #737 L4a).
        let mut stats_acc =
            crate::attr_stats::AttrStatsAccumulator::new().with_sketch_size(self.value_sketch_size);
        {
            let mut stream = self
                .partition_stream(table, partition_hours, SortRows::No)
                .await
                .context("Failed to read partition data for attribute analysis")?;
            use futures::StreamExt;
            while let Some(batch) = stream.next().await {
                let batch = batch.context("Failed to read partition data")?;
                stats_acc.push_batch(&batch);
            }
        }
        let (attr_stats, rows_read) = stats_acc.finish();

        if rows_read == 0 {
            tracing::info!(
                table = %table_name,
                partition_hours,
                "No data found in partition, skipping compaction"
            );
            return Ok(None);
        }

        crate::attr_stats::log_promotion_candidates(&table_name, &attr_stats, rows_read);
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
                    rows_read,
                )
                .await;
                promotion = self
                    .run_promotion_pass(catalog, tenant, dataset, &table_name, table)
                    .await;
            }
        }

        // A schema evolution is metadata-only (AddSchema/SetCurrentSchema),
        // so the reloaded table references the same data files — but the
        // second pass must both project and write under the new schema.
        let mut write_table: Table = if promotion.evolved {
            self.load_fresh_by_identifier(table.identifier())
                .await
                .context("Failed to reload table after schema evolution")?
        } else {
            table.clone()
        };

        // Roll output files on the writer's real bytes-written feedback
        // against *this* target (openspec task 5.4, design D4). The pinned
        // writer only rolls against the `write.target-file-size-bytes`
        // table property, and `ensure_table` never sets it, so without this
        // every table would roll at the writer's unset-property 512 MiB
        // fallback no matter what `target_file_size_bytes` says — one
        // output file per partition, in practice, since no partition here
        // gets near 512 MiB.
        common::iceberg::table_manager::ensure_target_file_size_property(
            &mut write_table,
            target_file_size_bytes,
        )
        .await;

        let backfill: Vec<(String, String)> = if promotion.backfill.is_empty() {
            vec![]
        } else {
            let schema_columns: HashSet<String> = write_table
                .current_schema()
                .map(|schema| schema.fields().iter().map(|f| f.name.clone()).collect())
                .unwrap_or_default();
            promotion
                .backfill
                .into_iter()
                .filter(|(_, column)| schema_columns.contains(column))
                .collect()
        };

        // Pass 2: the sorted stream that becomes the output files.
        //
        // Read from `table`, not `write_table`. `write_table` is a *fresh*
        // load, so it carries whatever snapshot is current now — which
        // after a late write into this partition is not the snapshot pass
        // 1 read, and not the input set the caller will commit against.
        // Reading it would rewrite rows the delta commit does not remove
        // (duplication, caught only by the row-parity check below, which
        // would then abort the job on every cycle that evolves the
        // schema). Both passes therefore read the caller's pinned table;
        // only the *write* uses the evolved schema. Batches read under the
        // old schema are reconciled to it by `dropped_columns` and
        // `backfill`, exactly as they were before the rewrite streamed.
        //
        // The sort key comes from `write_table`: it is the table the output
        // files will be attested against, so its declaration is the one they
        // must honor.
        let WriteSortKey {
            columns: sort_columns,
            attest,
        } = Self::rewrite_sort_key(&write_table);
        let stream = self
            .partition_stream(table, partition_hours, SortRows::By(sort_columns))
            .await
            .context("Failed to read and merge partition data")?;

        let output = Self::rewrite_stream(
            stream,
            promotion.dropped_columns,
            backfill,
            target_file_size_bytes,
        );

        // Attest the order only when the table declares one and the rows were
        // actually sorted by it. The rewrite's transforms and chunking
        // preserve row order, so what the sorted scan produced is what the
        // files contain.
        let new_files = if attest {
            iceberg_rust::arrow::write::write_sorted_parquet_partitioned(&write_table, output, None)
                .await
        } else {
            iceberg_rust::arrow::write::write_parquet_partitioned(&write_table, output, None).await
        }
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

    /// The rewrite pipeline: per-batch transforms, the statistics fold, and
    /// re-chunking toward the target file size — all lazily, so no more
    /// than the batches in flight are resident.
    ///
    /// This chunking is a *memory* bound, not the file-size control.
    /// `target_file_size_bytes` here measures `get_array_memory_size()` —
    /// decoded, in-memory bytes — so the accumulator caps how much unwritten
    /// data this pass can hold at once (proportional to one output file's
    /// worth, never to the partition); it says nothing by itself about how
    /// large the resulting Parquet file is, since decoded and encoded
    /// (compressed) bytes diverge by roughly 5–10x. The actual
    /// roll-to-a-new-file decision is made downstream by
    /// `write_parquet_partitioned`, which tracks its own real bytes-written
    /// and rolls against the table's `write.target-file-size-bytes`
    /// property — set to this same `target_file_size_bytes` by
    /// [`common::iceberg::table_manager::ensure_target_file_size_property`]
    /// before the write starts (openspec task 5.4, design D4).
    ///
    /// The writer only rolls *between* incoming batches — it never splits
    /// one — so this pass must never hand it a single unit larger than the
    /// target, or the writer gets no chance to roll at all. A whole-batch
    /// chunker would do exactly that for a partition small enough that
    /// DataFusion returns it as one `RecordBatch` (routine for a
    /// freshly-closed hour): that one batch already exceeds target on its
    /// own, so it would be the *only* batch the writer ever sees. Instead
    /// this pass slices every incoming batch into row ranges sized to
    /// `target_file_size_bytes` (via `RecordBatch::slice`, using the
    /// batch's own average row size), and accumulates slices toward the
    /// target exactly as it would whole small batches — so a single
    /// oversized source batch still becomes several target-sized units, and
    /// several small ones still coalesce into one. Encoded size is what the
    /// file boundary tracks; this pass only bounds memory and shapes the
    /// units the writer gets to check against.
    fn rewrite_stream(
        mut stream: datafusion::execution::SendableRecordBatchStream,
        dropped_columns: Vec<String>,
        backfill: Vec<(String, String)>,
        target_file_size_bytes: u64,
    ) -> impl futures::Stream<
        Item = std::result::Result<RecordBatch, datafusion::arrow::error::ArrowError>,
    > {
        use futures::StreamExt;

        async_stream::stream! {
            let mut pending: Vec<RecordBatch> = vec![];
            let mut pending_bytes: u64 = 0;

            while let Some(batch) = stream.next().await {
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        yield Err(datafusion::arrow::error::ArrowError::ExternalError(Box::new(e)));
                        return;
                    }
                };
                if batch.num_rows() == 0 {
                    continue;
                }

                // Defensive: the second pass reads under the post-demotion
                // schema, so demoted columns are already out of the
                // projection — but a batch that still carries one must not
                // reach the writer.
                let batch = match Self::drop_columns(vec![batch], &dropped_columns) {
                    Ok(mut batches) => batches.remove(0),
                    Err(e) => {
                        yield Err(datafusion::arrow::error::ArrowError::ExternalError(e.into()));
                        return;
                    }
                };

                let batch = match crate::attr_promotion::backfill_label_columns(vec![batch], &backfill) {
                    Ok(mut batches) => batches.remove(0),
                    Err(e) => {
                        yield Err(datafusion::arrow::error::ArrowError::ExternalError(e.into()));
                        return;
                    }
                };

                // Split an *oversized* batch into target-sized row windows;
                // a batch that already fits stays whole.
                //
                // Slicing at all is what makes the downstream roll work.
                // The writer rolls only *between* the batches it is handed,
                // never inside one, so a single batch bigger than the target
                // gives it no roll opportunity — and that is the common
                // case, since DataFusion returns a freshly-closed hour's
                // whole scan as one `RecordBatch`. A whole-batch-only
                // chunker therefore emits that one oversized unit and the
                // writer puts everything in one file, whatever
                // `write.target-file-size-bytes` says (openspec task 5.4).
                //
                // Slicing only the oversized ones is what keeps the
                // accounting honest. `bytes_per_row` is derived per batch
                // from `get_array_memory_size()`, which includes fixed
                // per-batch overhead — so the same data costs more per row
                // in a small batch than a large one. Slicing every batch to
                // top the accumulator up to exactly the target would cut
                // small batches in two over that inconsistency alone,
                // stranding tiny remainder chunks. Whole batches accumulate
                // as they always did; only a batch that cannot fit is cut.
                let batch_rows = batch.num_rows();
                let batch_bytes = batch.get_array_memory_size() as u64;
                let bytes_per_row = (batch_bytes / batch_rows as u64).max(1);

                let fits_whole = batch_bytes <= target_file_size_bytes;

                let mut offset = 0usize;
                while offset < batch_rows {
                    let take_rows = if fits_whole {
                        batch_rows
                    } else {
                        ((target_file_size_bytes / bytes_per_row).max(1) as usize)
                            .min(batch_rows - offset)
                    };

                    // An unsliced batch is charged its exact measured size:
                    // `bytes_per_row` is a truncating integer division, so
                    // re-deriving a whole batch's size from it would
                    // undercount by up to one row's worth per batch, and
                    // that error accumulates across a stream into flushes
                    // that come too late.
                    //
                    // For a slice, that per-row estimate is the best
                    // available — and deliberately not
                    // `slice.get_array_memory_size()`: a slice shares
                    // its parent's buffers, so Arrow reports the *parent's*
                    // full buffer capacity for any sub-range slice, not a
                    // prorated share (`arrow-data`'s `get_slice_memory_size`
                    // is the method that would prorate; `get_array_memory_size`
                    // does not). Using it here would overcount every partial
                    // slice by the whole batch's size, forcing a flush after
                    // the very first slice regardless of how small it is —
                    // silently defeating the coalescing this loop exists to
                    // do. `take_rows * bytes_per_row` is the slice's actual
                    // share, computed from the same per-row estimate that
                    // sized it.
                    pending_bytes += if fits_whole {
                        batch_bytes
                    } else {
                        take_rows as u64 * bytes_per_row
                    };
                    pending.push(batch.slice(offset, take_rows));
                    offset += take_rows;

                    if pending_bytes >= target_file_size_bytes {
                        match Self::concat(&pending) {
                            Ok(merged) => yield Ok(merged),
                            Err(e) => {
                                yield Err(datafusion::arrow::error::ArrowError::ExternalError(e.into()));
                                return;
                            }
                        }
                        pending.clear();
                        pending_bytes = 0;
                    }
                }
            }

            if !pending.is_empty() {
                match Self::concat(&pending) {
                    Ok(merged) => yield Ok(merged),
                    Err(e) => yield Err(datafusion::arrow::error::ArrowError::ExternalError(e.into())),
                }
            }
        }
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
        // The table's current schema -- the source of truth for which
        // keys are already materialized (via each label column's
        // origin-key `doc`, #814) -- and the pinned allowlist.
        let mut current_schema = match table.current_schema() {
            Ok(schema) => schema.clone(),
            Err(e) => {
                tracing::warn!(error = %e, table = %table_name, "Failed to resolve current schema for promotion pass");
                return outcome;
            }
        };
        let materialized = crate::attr_promotion::materialized_keys_of(&current_schema, &stats);
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
                Ok(evolved) => {
                    outcome.evolved = true;
                    current_schema = evolved;
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
            // Snapshot the pre-call schema so the actually-dropped columns
            // can be derived by diffing against whatever
            // `remove_label_columns` returns, rather than independently
            // re-resolving `decision.demote` against this local snapshot.
            // `remove_label_columns` reloads the table fresh from the
            // catalog and resolves columns against that live state; under
            // concurrent multi-instance compaction another instance may
            // have already demoted these keys and reused the freed column
            // name for an unrelated, colliding key (#814) before this call
            // lands. In that case `remove_label_columns` correctly no-ops
            // (the schema it returns is unchanged), and diffing yields an
            // empty `dropped_columns` instead of stale names that would
            // otherwise cause `rewrite_stream` to drop a live column
            // belonging to the colliding key.
            let schema_before_demote = current_schema.clone();
            match common::iceberg::evolution::remove_label_columns(
                self.catalog_manager.catalog(),
                table.identifier(),
                &decision.demote,
            )
            .await
            {
                Ok(pruned) => {
                    let dropped_columns = actually_dropped_columns(&schema_before_demote, &pruned);
                    outcome.evolved = outcome.evolved || !dropped_columns.is_empty();
                    demoted = decision.demote;
                    outcome.dropped_columns = dropped_columns;
                    current_schema = pruned;
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
        // left null during the transition window. Each key's column is
        // resolved from the freshly-evolved schema via its origin-key
        // `doc` (#814); a key with no column yet (e.g. pinned but never
        // promoted, or an evolution that failed) is skipped rather than
        // guessing a name. Deduplicated by column name.
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
            if let Some(column) = common::iceberg::evolution::column_for_key(&current_schema, key) {
                let column = column.to_string();
                if seen_columns.insert(column.clone()) {
                    outcome.backfill.push((key.clone(), column));
                }
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

    /// The key this rewrite sorts by, taken from the table's own declaration.
    ///
    /// The declared sort order is the single source of truth for the ordering
    /// contract (`common::iceberg::schemas::TableSchema::sort_key_columns`),
    /// so the compactor reads it rather than keeping a second copy that could
    /// drift from what the tables say and what the query engine is told.
    ///
    /// A table with no declaration — created by an older build and not yet
    /// reconciled by an `ensure_table` load — still gets sorted output, by the
    /// canonical key resolved by column name, but its files are written
    /// unattested: there is no declared order for them to attest. A table this
    /// crate does not recognize at all is compacted unsorted, and says so.
    fn rewrite_sort_key(table: &Table) -> WriteSortKey {
        let table_name = table.identifier().name();
        let key = write_sort_key(
            table.metadata(),
            table_name,
            UndeclaredFallback::CanonicalKey,
        );
        if key.columns.is_empty() {
            tracing::warn!("No sort configuration for table {table_name}, data will not be sorted");
        } else if !key.attest {
            tracing::debug!(
                table = %table_name,
                "Table declares no sort order; sorting by the canonical key and writing unattested"
            );
        }
        key
    }

    /// Warn about `[compactor]` memory settings that cannot work together.
    ///
    /// The knobs interact, and none of the bad combinations is obvious
    /// from any single value:
    ///
    /// * a job's peak memory is `memory_limit_mb` **plus** roughly one
    ///   `target_file_size_mb` — the chunker accumulates an output file
    ///   outside the pool, so a target at or above the pool means the
    ///   unaccounted half dominates the accounted one;
    /// * the pool is divided by `target_partitions`, so raising the
    ///   fan-out shrinks every sorter's share (#1064);
    /// * a share too small to hold a batch plus its spill-merge
    ///   reservation makes the sort fail rather than spill, which is the
    ///   failure this whole area exists to prevent;
    /// * `sort_spill_reservation_mb` comes *out of* that share rather
    ///   than adding to it, so a large one leaves the sort spilling from
    ///   the first batches on.
    ///
    /// These are warnings, not errors: an operator who has measured their
    /// workload may legitimately want an unusual ratio, and refusing to
    /// start a background service over a tuning choice is worse than
    /// saying so loudly.
    pub fn warn_on_incoherent_memory_config(config: &common::config::CompactorConfig) {
        let pool_mb = config.memory_limit_mb as u64;
        let per_sorter_mb = Self::per_sorter_mb(config);

        if config.target_file_size_mb >= pool_mb {
            tracing::warn!(
                memory_limit_mb = pool_mb,
                target_file_size_mb = config.target_file_size_mb,
                "[compactor] target_file_size_mb is at or above memory_limit_mb; the chunker \
                 accumulates an output file outside the memory pool, so peak job memory will be \
                 dominated by the part the pool does not account for"
            );
        }

        if config.sort_spill_reservation_mb * 2 >= per_sorter_mb {
            tracing::warn!(
                memory_limit_mb = pool_mb,
                target_partitions = config.target_partitions,
                per_sorter_mb,
                sort_spill_reservation_mb = config.sort_spill_reservation_mb,
                "[compactor] sort_spill_reservation_mb claims half or more of each sorter's \
                 share of memory_limit_mb; that headroom cannot hold data, so the sort will \
                 spill almost immediately"
            );
        }

        if per_sorter_mb < MIN_PER_SORTER_MB {
            tracing::warn!(
                memory_limit_mb = pool_mb,
                target_partitions = config.target_partitions,
                per_sorter_mb,
                minimum_mb = MIN_PER_SORTER_MB,
                "[compactor] memory_limit_mb divided by target_partitions leaves each sorter too \
                 little to spill within; lower target_partitions or raise memory_limit_mb, or the \
                 rewrite will fail instead of spilling"
            );
        }
    }

    /// Each sorter's slice of the pool: the budget is divided by the
    /// fan-out, and `0` means DataFusion picks the fan-out itself, which
    /// is at least one.
    fn per_sorter_mb(config: &common::config::CompactorConfig) -> u64 {
        config.memory_limit_mb as u64 / config.target_partitions.max(1) as u64
    }

    /// Build the compaction session context.
    ///
    /// The rewrite runs under a bounded memory pool, so DataFusion's own
    /// operators (the partition sort above all) spill to disk rather than
    /// growing the process heap without limit — the old unbounded
    /// `SessionContext::new()` is what let a big table OOM the compactor
    /// outright.
    ///
    /// The bound is now close to total. `rewrite_partition` streams both
    /// of its passes, so the only rewrite memory outside this pool is the
    /// chunker's accumulation — bounded by one output file — plus the
    /// per-key attribute statistics, bounded by the cardinality cap.
    /// Neither grows with the partition. Before that, the rewrite
    /// `collect()`ed the sorted partition into a `Vec<RecordBatch>` that
    /// lived outside the pool's accounting for the whole of attribute
    /// analysis, backfill, splitting and writing, which left peak process
    /// memory proportional to partition size no matter what this limit
    /// said (#1064).
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
        let session_config = Self::compaction_session_config(compactor);
        let builder =
            datafusion::execution::runtime_env::RuntimeEnvBuilder::new().with_memory_pool(
                common::datafusion_runtime::bounded_memory_pool(memory_limit_mb * 1024 * 1024, 1.0),
            );
        match builder.build() {
            Ok(runtime_env) => {
                tracing::debug!(
                    memory_limit_mb,
                    target_partitions = session_config.target_partitions(),
                    batch_size = session_config.batch_size(),
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

    /// `SessionConfig` for the rewrite: the partition fan-out, the scan's
    /// batch size, and the sort's spill headroom.
    ///
    /// The batch size is a memory bound rather than a throughput knob;
    /// [`common::config::CompactorConfig::scan_batch_size`] carries the
    /// reasoning.
    ///
    /// `0` means "use DataFusion's default" for both counts —
    /// `with_target_partitions` rejects zero, and a zero batch size would
    /// stall the scan.
    fn compaction_session_config(
        compactor: &common::config::CompactorConfig,
    ) -> datafusion::prelude::SessionConfig {
        let mut config = datafusion::prelude::SessionConfig::new()
            .with_sort_spill_reservation_bytes(
                compactor.sort_spill_reservation_mb as usize * 1024 * 1024,
            );
        if compactor.target_partitions > 0 {
            config = config.with_target_partitions(compactor.target_partitions);
        }
        if compactor.scan_batch_size > 0 {
            config = config.with_batch_size(compactor.scan_batch_size);
        }
        config
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

    /// Stream one partition's live data, sorted for query performance.
    ///
    /// Returns a lazy stream rather than a materialized `Vec<RecordBatch>`:
    /// the collected form put the whole sorted partition on the heap
    /// outside the memory pool's accounting, which is what made
    /// `memory_limit_mb` a bound on the sort alone (#1064).
    async fn partition_stream(
        &self,
        table: &Table,
        partition_hours: i64,
        sort: SortRows,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
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

        let sort_cols = match sort {
            SortRows::By(columns) => columns,
            // The statistics pass is order-independent, so it plans no
            // sort — which is what keeps the extra scan cheap.
            SortRows::No => vec![],
        };
        let sorted_df = if !sort_cols.is_empty() {
            let sort_exprs: Vec<_> = sort_cols
                .into_iter()
                .map(|column| col(column.name).sort(!column.descending, column.nulls_first))
                .collect();

            df.sort(sort_exprs)
                .with_context(|| format!("Failed to sort {table_name} table"))?
        } else {
            df
        };

        let stream = sorted_df
            .execute_stream()
            .await
            .context("Failed to execute partition read")?;

        tracing::debug!(
            table = %table_name,
            partition_hours,
            "Streaming partition data for rewrite"
        );

        Ok(stream)
    }

    /// Concatenate same-schema batches into one.
    ///
    /// The chunker calls this once per output file, over an accumulation
    /// already bounded by the target file size — so unlike the old
    /// collect-then-split path, the transient copy is proportional to one
    /// output file rather than to the partition.
    fn concat(batches: &[RecordBatch]) -> Result<RecordBatch> {
        match batches {
            [] => Err(anyhow::anyhow!("Cannot merge empty batch list")),
            [single] => Ok(single.clone()),
            _ => {
                let schema = batches[0].schema();
                datafusion::arrow::compute::concat_batches(&schema, batches)
                    .context("Failed to merge batches")
            }
        }
    }
}

/// The label columns actually removed by a `remove_label_columns` call,
/// derived by diffing `before` (the schema snapshot held prior to the
/// call) against `after` (the schema the call returned) — rather than
/// independently re-resolving the requested demotion keys against
/// `before` (see `ParquetRewriter::run_promotion_pass`'s demotion block).
///
/// `remove_label_columns` reloads the table fresh from the catalog and
/// resolves columns against that live state, not `before`. Under
/// concurrent multi-instance compaction of the same table, another
/// instance may already have demoted the same keys and reused the freed
/// column name for an unrelated, colliding key (#814) before this call
/// lands; `remove_label_columns` then correctly no-ops (`after` is
/// unchanged from the live schema, still carrying that column name — now
/// under the colliding key). Diffing by name yields an empty result in
/// that case, since the column name survives in both `before` and
/// `after`, instead of the stale pre-call resolution wrongly reporting it
/// dropped and causing `rewrite_stream` to project away the colliding
/// key's live, valid data.
fn actually_dropped_columns(
    before: &iceberg_rust::spec::schema::Schema,
    after: &iceberg_rust::spec::schema::Schema,
) -> Vec<String> {
    let after_names: HashSet<&str> = after.fields().iter().map(|f| f.name.as_str()).collect();
    before
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .filter(|name| !after_names.contains(name))
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn int_batch(rows: usize, offset: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(
                (0..rows as i64).map(|i| i + offset).collect::<Vec<i64>>(),
            ))],
        )
        .unwrap()
    }

    async fn drain(
        stream: impl futures::Stream<
            Item = std::result::Result<RecordBatch, datafusion::arrow::error::ArrowError>,
        >,
    ) -> Vec<RecordBatch> {
        use futures::StreamExt;
        let stream = std::pin::pin!(stream);
        stream
            .map(|b| b.expect("chunking must not fail"))
            .collect()
            .await
    }

    fn chunk(
        batches: Vec<RecordBatch>,
        target_size_bytes: u64,
    ) -> impl futures::Stream<
        Item = std::result::Result<RecordBatch, datafusion::arrow::error::ArrowError>,
    > {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        let schema = batches[0].schema();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        ParquetRewriter::rewrite_stream(inner, vec![], vec![], target_size_bytes)
    }

    #[tokio::test]
    async fn chunking_preserves_every_row_in_order() {
        let batches = vec![int_batch(10, 0), int_batch(10, 10), int_batch(10, 20)];
        let one_batch = batches[0].get_array_memory_size() as u64;

        // A target of ~2 batches forces more than one output chunk.
        let out = drain(chunk(batches, one_batch * 2)).await;

        assert!(out.len() > 1, "expected the chunker to emit several files");
        let ids: Vec<i64> = out
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, (0..30).collect::<Vec<i64>>());
    }

    /// The accumulator must not grow with the partition: it flushes as
    /// soon as it reaches the target, so its residency is bounded by one
    /// output file however long the stream runs.
    #[tokio::test]
    async fn chunking_flushes_at_the_target_size_rather_than_at_the_end() {
        let batches: Vec<_> = (0..20).map(|i| int_batch(10, i * 10)).collect();
        let one_batch = batches[0].get_array_memory_size() as u64;

        let out = drain(chunk(batches, one_batch * 2)).await;

        assert!(
            out.len() >= 10,
            "20 batches at a 2-batch target must produce ~10 files, got {}",
            out.len()
        );
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 200);
    }

    /// A trailing partial accumulation must still be emitted — otherwise
    /// the rewrite would silently drop the tail of the partition.
    #[tokio::test]
    async fn chunking_emits_the_trailing_partial_chunk() {
        let batches = vec![int_batch(10, 0)];
        let huge_target = 1024 * 1024 * 1024;

        let out = drain(chunk(batches, huge_target)).await;

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 10);
    }

    /// A single incoming batch already larger than the target must still be
    /// split into several chunks, not pushed and flushed whole.
    ///
    /// This is the case a whole-batch-only chunker gets wrong: DataFusion
    /// hands back a small partition's entire scan as one `RecordBatch`, so
    /// "accumulate whole batches up to target" degenerates to "one chunk,
    /// however large" — which then gives the downstream Parquet writer only
    /// one incoming unit to roll a new file between, defeating the real
    /// encoded-size roll regardless of what `write.target-file-size-bytes`
    /// says (openspec task 5.4; reproduced end-to-end in
    /// `tests-integration/tests/compactor/target_encoded_file_size.rs`
    /// before this test was added).
    #[tokio::test]
    async fn chunking_splits_a_single_oversized_incoming_batch() {
        let batch = int_batch(100, 0);
        let bytes_per_row = batch.get_array_memory_size() as u64 / 100;
        // A target well under the batch's own size forces multiple slices
        // out of this one incoming batch.
        let target = bytes_per_row * 10;

        let out = drain(chunk(vec![batch], target)).await;

        assert!(
            out.len() > 1,
            "a single 100-row batch at a ~10-row target must still split into several chunks, got {}",
            out.len()
        );
        let ids: Vec<i64> = out
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            ids,
            (0..100).collect::<Vec<i64>>(),
            "splitting must preserve every row, in order, exactly once"
        );
    }

    /// A trailing slice of an oversized batch must still coalesce with
    /// following small batches, exactly as whole small batches coalesce
    /// with each other.
    ///
    /// This pins the specific accounting bug the split fix above does not:
    /// `RecordBatch::slice` shares its parent's buffers, so
    /// `get_array_memory_size()` on a sub-range slice reports the *parent
    /// batch's* full size, not the slice's share. Using that to accumulate
    /// `pending_bytes` overcounts every partial slice, so the tail slice of
    /// an oversized batch always looks like it alone exceeds the target and
    /// gets flushed on its own -- even when its real share is small enough
    /// that it should keep accumulating with what follows, same as any
    /// other undersized chunk. The row-preservation assertions in
    /// `chunking_splits_a_single_oversized_incoming_batch` pass under that
    /// bug too (no row is lost or duplicated either way); only the chunk
    /// *count* and the tail chunk's size expose it.
    #[tokio::test]
    async fn chunking_coalesces_an_oversized_batchs_tail_with_following_small_batches() {
        // 105 rows at a ~50-row target: two full windows (rows 0-49,
        // 50-99) plus a 5-row remainder that must NOT become its own
        // chunk -- it has to wait for and merge with what follows.
        let huge = int_batch(105, 0);
        let bytes_per_row = huge.get_array_memory_size() as u64 / 105;
        let target = bytes_per_row * 50;

        // Two more small batches, together with the 5-row remainder still
        // comfortably under the target, so all three must land in one
        // final chunk when the stream ends.
        let small1 = int_batch(20, 105);
        let small2 = int_batch(20, 125);

        let out = drain(chunk(vec![huge, small1, small2], target)).await;

        assert_eq!(
            out.len(),
            3,
            "expected 2 full windows from the huge batch plus 1 merged tail, got {} chunks: {:?}",
            out.len(),
            out.iter().map(|b| b.num_rows()).collect::<Vec<_>>()
        );
        let last = out.last().expect("at least one chunk");
        assert_eq!(
            last.num_rows(),
            45,
            "the huge batch's 5-row tail must merge with both 20-row batches into one 45-row \
             chunk, not be flushed alone (got {} rows in the final chunk)",
            last.num_rows()
        );

        let ids: Vec<i64> = out
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, (0..145).collect::<Vec<i64>>());
    }

    /// The three memory knobs interact, and a bad combination is not
    /// visible from any one of them — so the service says so at startup
    /// rather than failing a job hours later (#1064).
    #[test]
    fn a_target_file_size_at_or_above_the_pool_is_flagged() {
        let config = common::config::CompactorConfig {
            memory_limit_mb: 128,
            target_file_size_mb: 128,
            target_partitions: 1,
            ..Default::default()
        };
        assert!(
            config.target_file_size_mb >= config.memory_limit_mb as u64,
            "fixture must actually be the incoherent case"
        );
        // Warnings are side effects; this asserts the predicate the
        // warning is built on, which is the part that can silently rot.
        ParquetRewriter::warn_on_incoherent_memory_config(&config);
    }

    #[test]
    fn per_sorter_share_is_the_pool_divided_by_the_fan_out() {
        // 512 MB across 16 partitions is 32 MB each — below the floor a
        // spilling sort needs, which is exactly how #1064 presented.
        let config = common::config::CompactorConfig {
            memory_limit_mb: 512,
            target_partitions: 16,
            ..Default::default()
        };
        assert!(ParquetRewriter::per_sorter_mb(&config) < MIN_PER_SORTER_MB);
        ParquetRewriter::warn_on_incoherent_memory_config(&config);
    }

    /// `target_partitions = 0` means "DataFusion's default", which must
    /// not divide by zero on the way to the warning.
    #[test]
    fn a_zero_fan_out_does_not_panic_the_coherence_check() {
        let config = common::config::CompactorConfig {
            memory_limit_mb: 512,
            target_partitions: 0,
            ..Default::default()
        };
        ParquetRewriter::warn_on_incoherent_memory_config(&config);
    }

    #[test]
    fn the_shipped_defaults_are_coherent() {
        let config = common::config::CompactorConfig::default();
        assert!(
            config.target_file_size_mb < config.memory_limit_mb as u64,
            "default target_file_size_mb ({}) must sit below memory_limit_mb ({})",
            config.target_file_size_mb,
            config.memory_limit_mb
        );
        let per_sorter_mb = ParquetRewriter::per_sorter_mb(&config);
        assert!(
            per_sorter_mb >= MIN_PER_SORTER_MB,
            "default per-sorter share must clear the spill floor"
        );
        assert!(
            config.sort_spill_reservation_mb * 2 < per_sorter_mb,
            "default spill headroom ({} MB) must leave a sorter most of its share",
            config.sort_spill_reservation_mb
        );
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

    /// Every signal table this crate can classify must get a non-empty sort
    /// key — a table silently falling through to the empty case compacts
    /// unsorted forever (issue #1014's failure mode). The key now comes from
    /// the shared declaration in `common`, so this also guards against the
    /// compactor and the table metadata drifting apart.
    #[test]
    fn the_canonical_sort_key_covers_every_known_signal_table() {
        for table in [
            "traces",
            "logs",
            "metrics_gauge",
            "metrics_sum",
            "metrics_histogram",
            "metrics_exponential_histogram",
            "metrics_summary",
            "profiles",
        ] {
            assert!(
                crate::retention::SignalType::from_table_name(table).is_ok(),
                "table '{table}' is not classified by this crate"
            );
            assert!(
                !common::iceberg::sort::canonical_sort_columns(table).is_empty(),
                "table '{table}' has no sort columns"
            );
        }
    }

    /// `0` is the documented escape hatch back to DataFusion's own
    /// default. It must not reach `with_target_partitions`, which panics
    /// on zero.
    #[tokio::test]
    async fn compaction_context_treats_zero_target_partitions_as_auto() {
        let rewriter = rewriter_with_config(|c| c.compactor.target_partitions = 0).await;
        let ctx = rewriter.compaction_context();

        assert_eq!(
            ctx.state().config().target_partitions(),
            datafusion::prelude::SessionConfig::new().target_partitions(),
            "zero must land on DataFusion's own default, not merely some positive count"
        );
    }

    /// The scan's batch size is a memory knob, not just a throughput one
    /// (see [`common::config::CompactorConfig::scan_batch_size`]), so the
    /// default has to sit below DataFusion's.
    #[tokio::test]
    async fn compaction_context_bounds_the_scan_batch_size() {
        let rewriter = rewriter_with_config(|_| {}).await;
        let ctx = rewriter.compaction_context();

        assert!(
            ctx.state().config().batch_size()
                < datafusion::prelude::SessionConfig::new().batch_size(),
            "compaction must read smaller batches than DataFusion's default, whose per-batch \
             reservation cannot spill and is unbounded in bytes"
        );
        assert_eq!(
            ctx.state().config().batch_size(),
            common::config::CompactorConfig::default().scan_batch_size
        );
    }

    #[tokio::test]
    async fn compaction_context_honors_configured_batch_size() {
        let rewriter = rewriter_with_config(|c| c.compactor.scan_batch_size = 256).await;
        let ctx = rewriter.compaction_context();

        assert_eq!(ctx.state().config().batch_size(), 256);
    }

    /// `0` is the escape hatch back to DataFusion's own default, matching
    /// `target_partitions`.
    #[tokio::test]
    async fn compaction_context_treats_zero_batch_size_as_auto() {
        let rewriter = rewriter_with_config(|c| c.compactor.scan_batch_size = 0).await;
        let ctx = rewriter.compaction_context();

        assert_eq!(
            ctx.state().config().batch_size(),
            datafusion::prelude::SessionConfig::new().batch_size()
        );
    }

    /// The headroom the sorter holds back so its spill merge can run is
    /// the knob DataFusion's own OOM message tells operators to tune, so
    /// the compactor has to expose it.
    #[tokio::test]
    async fn compaction_context_sets_the_sort_spill_reservation() {
        let rewriter = rewriter_with_config(|c| c.compactor.sort_spill_reservation_mb = 32).await;
        let ctx = rewriter.compaction_context();

        assert_eq!(
            ctx.state()
                .config()
                .options()
                .execution
                .sort_spill_reservation_bytes,
            32 * 1024 * 1024
        );
    }

    /// Headroom taken from the pool is memory the sort cannot fill with
    /// data, so a reservation that claims half a sorter's share leaves it
    /// spilling constantly — a combination invisible from either value.
    #[test]
    fn a_spill_reservation_that_eats_the_per_sorter_share_is_flagged() {
        let config = common::config::CompactorConfig {
            memory_limit_mb: 128,
            target_partitions: 1,
            sort_spill_reservation_mb: 64,
            ..Default::default()
        };
        assert!(
            config.sort_spill_reservation_mb * 2 >= ParquetRewriter::per_sorter_mb(&config),
            "fixture must actually be the incoherent case"
        );
        ParquetRewriter::warn_on_incoherent_memory_config(&config);
    }

    /// Unit-level proof of the diff itself: a "before" schema with
    /// `label_env` (doc `K1`) and an "after" schema that still carries
    /// `label_env`, unchanged, but now under a colliding key's doc `K2` --
    /// exactly what a no-op `remove_label_columns` returns when a
    /// concurrent instance already reused the freed column name (see the
    /// full end-to-end race reproduced below). The diff must report zero
    /// dropped columns, not the stale `label_env`.
    #[test]
    fn actually_dropped_columns_is_empty_when_the_column_survives_under_a_different_key() {
        use common::iceberg::evolution::label_doc;
        use iceberg_rust::spec::schema::Schema as IcebergSchema;
        use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};

        let label_field = |doc_key: &str| StructField {
            id: 1,
            name: "label_env".to_string(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: Some(label_doc(doc_key)),
            initial_default: None,
            write_default: None,
        };
        let before =
            IcebergSchema::from_struct_type(StructType::new(vec![label_field("K1")]), 0, None);
        // No-op removal: same column, same schema shape, but now doc'd to
        // K2 -- `remove_label_columns` returns the live schema untouched.
        let after =
            IcebergSchema::from_struct_type(StructType::new(vec![label_field("K2")]), 0, None);

        assert!(actually_dropped_columns(&before, &after).is_empty());
    }

    /// End-to-end reproduction of the race against a real in-memory
    /// catalog: two colliding keys (`http.method` / `http_method`, both
    /// sanitizing to `label_http_method`) interleaved exactly as described
    /// for the #814 follow-up. Instance A holds a stale schema snapshot
    /// from before instance B's demote-then-promote interleaving frees and
    /// reclaims the same column name for the other key. Diffing against
    /// what `remove_label_columns` actually returns must yield an empty
    /// `dropped_columns`, and the colliding key's column must survive.
    #[tokio::test]
    async fn demotion_diffs_against_the_post_removal_schema_not_a_stale_snapshot()
    -> anyhow::Result<()> {
        use common::iceberg::evolution::{add_label_columns, column_for_key, remove_label_columns};
        use iceberg_rust::catalog::create::CreateTableBuilder;
        use iceberg_rust::spec::partition::PartitionSpec;
        use iceberg_rust::spec::schema::Schema as IcebergSchema;
        use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};

        let manager = common::CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let namespace = common::iceberg::names::build_namespace("race", "test")?;
        catalog.clone().create_namespace(&namespace, None).await?;
        let identifier = common::iceberg::names::build_table_identifier("race", "test", "events");
        let base = IcebergSchema::from_struct_type(
            StructType::new(vec![StructField {
                id: 1,
                name: "timestamp".to_string(),
                required: true,
                field_type: Type::Primitive(PrimitiveType::Timestamp),
                doc: None,
                initial_default: None,
                write_default: None,
            }]),
            0,
            None,
        );
        let create = CreateTableBuilder::default()
            .with_name("events".to_string())
            .with_schema(base)
            .with_partition_spec(PartitionSpec::default())
            .with_location(common::iceberg::names::build_table_location(
                "race", "test", "events",
            ))
            .create()
            .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
        catalog
            .clone()
            .create_table(identifier.clone(), create)
            .await?;

        // Instance A promotes `http.method`, then holds this schema as its
        // stale local `current_schema` snapshot going into its demote call.
        let schema_before_demote =
            add_label_columns(catalog.clone(), &identifier, &["http.method".to_string()]).await?;
        assert_eq!(
            column_for_key(&schema_before_demote, "http.method"),
            Some("label_http_method")
        );

        // Meanwhile instance B: demotes `http.method` (freeing
        // `label_http_method`), then promotes the colliding `http_method`
        // onto the now-free column name.
        remove_label_columns(catalog.clone(), &identifier, &["http.method".to_string()]).await?;
        let live_after_b =
            add_label_columns(catalog.clone(), &identifier, &["http_method".to_string()]).await?;
        assert_eq!(
            column_for_key(&live_after_b, "http_method"),
            Some("label_http_method"),
            "the colliding key must reclaim the freed column name"
        );

        // Instance A now acts on its stale decision to demote
        // `http.method`. The real call resolves against live state, finds
        // no column for it anymore, and correctly no-ops.
        let pruned =
            remove_label_columns(catalog.clone(), &identifier, &["http.method".to_string()])
                .await?;
        assert_eq!(
            pruned, live_after_b,
            "a no-op removal must leave the live schema unchanged"
        );

        let dropped = actually_dropped_columns(&schema_before_demote, &pruned);
        assert!(
            dropped.is_empty(),
            "must not report label_http_method as dropped -- it now legitimately \
             belongs to http_method: {dropped:?}"
        );
        assert_eq!(
            column_for_key(&pruned, "http_method"),
            Some("label_http_method"),
            "the colliding key's column must survive"
        );
        Ok(())
    }
}
