//! Fixtures and measurements for the declared-ordering read path.
//!
//! The `declared-data-ordering` contract lets a scan over attested files
//! declare its order, so "order by time, take the first _n_" can stop after
//! the leading files instead of sorting the whole range. Whether that happens
//! is a question about *mechanism* — how many files the scan opened, how many
//! it pruned — not about wall-clock, which cannot tell an elided sort from a
//! quiet machine. This module gives benches and diagnostics the two things
//! they need to show the mechanism:
//!
//! - [`SequentialTraces`]: the population an ordered query is fast on — many
//!   files at successive instants, so their time ranges do not overlap — with
//!   every file attested, or the same rows with none attested;
//! - [`ScanReport`]: the scan-level metrics of one executed query.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use common::config::{QuerierConfig, QuerierDataFusionConfig};
use compactor::ManifestReader;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use futures::stream;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::table::Table;
use object_store::memory::InMemory;
use writer::IcebergTableWriter;

use crate::compaction_helpers::{aligned_hour_start, load_table};
use crate::fixtures::SequentialLayout;
use crate::generators;

const DATASET: &str = "ordering-dataset";
/// The name every [`SequentialTraces`] table is registered under.
pub const TABLE: &str = "traces";

impl SequentialLayout {
    /// Two hours of one-file-per-two-minutes ingest: 60 files of 1,000 spans,
    /// one row group per file, so "files" and "row groups" count the same
    /// thing and the newest rows sit in the last file.
    pub fn two_hours_of_ingest() -> Self {
        Self {
            files: 60,
            rows_per_file: 1_000,
            base_timestamp: aligned_hour_start(3),
            file_span_ms: 2 * 60_000,
        }
    }
}

/// A seeded traces table laid out by a [`SequentialLayout`].
pub struct SequentialTraces {
    catalog_manager: Arc<CatalogManager>,
    tenant: String,
    layout: SequentialLayout,
    writer: IcebergTableWriter,
}

impl SequentialTraces {
    /// Seed `layout` under `tenant` through ingest, so every file attests the
    /// table's declared order.
    pub async fn seed(tenant: &str, layout: &SequentialLayout) -> Result<Self> {
        let mut population = Self::empty(tenant, layout).await?;
        generators::generate_sequential_trace_files(&mut population.writer, layout).await?;
        Ok(population)
    }

    /// The same rows in the same files under `tenant`, written through the
    /// plain (non-attesting) path — the shape a build predating the ordering
    /// contract left behind. The two tables differ only in what the manifest
    /// says about their files.
    pub async fn unattested_copy(&self, tenant: &str) -> Result<Self> {
        let copy = Self::empty(tenant, &self.layout).await?;
        let mut table = copy.table().await?;
        for file in self.file_batches().await? {
            append_unattested(&mut table, file).await?;
        }
        Ok(copy)
    }

    async fn empty(tenant: &str, layout: &SequentialLayout) -> Result<Self> {
        let config = common::testing::TestConfigBuilder::new()
            .in_memory()
            .with_tenant(tenant, DATASET)
            .build();
        let catalog_manager = Arc::new(CatalogManager::new(config).await?);
        let writer = IcebergTableWriter::new(
            &catalog_manager,
            Arc::new(InMemory::new()),
            tenant.to_string(),
            DATASET.to_string(),
            TABLE.to_string(),
        )
        .await?;
        Ok(Self {
            catalog_manager,
            tenant: tenant.to_string(),
            layout: layout.clone(),
            writer,
        })
    }

    /// The table's rows in the stored schema, one batch per file, in file
    /// order. Files do not overlap in time, so a scan ordered by the sort key
    /// falls into file-sized runs.
    async fn file_batches(&self) -> Result<Vec<RecordBatch>> {
        let ctx = self.context(false).await?;
        let batches = ctx
            .sql(&format!(
                "SELECT * FROM {TABLE} ORDER BY timestamp ASC, trace_id ASC"
            ))
            .await?
            .collect()
            .await?;
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .context("the seeded table must have rows")?;
        let all = concat_batches(&schema, &batches)?;
        anyhow::ensure!(
            all.num_rows() == self.layout.total_rows(),
            "expected {} rows, read {}",
            self.layout.total_rows(),
            all.num_rows()
        );
        let rows = self.layout.rows_per_file;
        Ok((0..self.layout.files)
            .map(|file| all.slice(file * rows, rows))
            .collect())
    }

    pub async fn table(&self) -> Result<Table> {
        load_table(&self.catalog_manager, &self.tenant, DATASET, TABLE).await
    }

    /// What the table holds, so a [`ScanReport`] has a denominator.
    pub async fn footprint(&self) -> Result<TableFootprint> {
        let mut footprint = TableFootprint::default();
        for file in ManifestReader::new()
            .get_snapshot_files(&self.table().await?)
            .await?
        {
            footprint.files += 1;
            footprint.bytes += file.file_size_bytes as usize;
        }
        Ok(footprint)
    }

    /// The querier's real session (its options, optimizer rules, and footer
    /// cache) with this table registered as [`TABLE`].
    ///
    /// `split_file_groups_by_statistics` is the one option the ordering win
    /// depends on, so it is the one a caller can vary.
    pub async fn context(&self, split_file_groups_by_statistics: bool) -> Result<SessionContext> {
        let ctx = querier::flight::session_context_with_limits(&QuerierConfig {
            datafusion: QuerierDataFusionConfig {
                split_file_groups_by_statistics,
                ..QuerierDataFusionConfig::default()
            },
            ..QuerierConfig::default()
        });
        ctx.register_table(
            TABLE,
            Arc::new(datafusion_iceberg::DataFusionTable::from(
                self.table().await?,
            )),
        )?;
        Ok(ctx)
    }
}

/// Append `batch` to `table` through the plain write path, which neither
/// sorts nor attests — the shape a build predating the ordering contract
/// left behind.
pub async fn append_unattested(table: &mut Table, batch: RecordBatch) -> Result<()> {
    let files = write_parquet_partitioned(table, stream::iter(vec![Ok(batch)]), None).await?;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await?;
    Ok(())
}

/// The live data files of a table, as the scan sees them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TableFootprint {
    pub files: usize,
    pub bytes: usize,
}

impl std::fmt::Display for TableFootprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "files={} bytes={}", self.files, self.bytes)
    }
}

/// What one executed query cost at the scan, from DataFusion's own metrics.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ScanReport {
    /// Rows the query returned.
    pub rows: usize,
    /// Files the scan got as far as preparing to open. A file that early
    /// termination never reached is not counted.
    pub files_reached: usize,
    /// Reached files skipped whole on their statistics (a TopK's dynamic
    /// filter, mostly) before any of their bytes were read.
    pub files_pruned: usize,
    /// Reached files that were actually read.
    pub files_read: usize,
    /// Row groups read after statistics pruning.
    pub row_groups_read: usize,
    /// Row groups skipped by statistics or by the limit.
    pub row_groups_pruned: usize,
    /// Bytes read from Parquet files.
    pub bytes_scanned: usize,
    /// Whether the physical plan still sorts. `false` means the scan's
    /// declared ordering satisfied the query and the sort was elided.
    pub sorts: bool,
}

impl std::fmt::Display for ScanReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rows={} files reached={} pruned={} read={} row_groups read={} pruned={} bytes_scanned={} sort={}",
            self.rows,
            self.files_reached,
            self.files_pruned,
            self.files_read,
            self.row_groups_read,
            self.row_groups_pruned,
            self.bytes_scanned,
            if self.sorts { "kept" } else { "elided" }
        )
    }
}

/// Execute `sql` on `ctx`; report what its scans did, and hand back the rows
/// so a caller can check the answer without running the query again.
pub async fn scan_report(
    ctx: &SessionContext,
    sql: &str,
) -> Result<(ScanReport, Vec<RecordBatch>)> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;

    let mut report = ScanReport {
        rows: batches.iter().map(RecordBatch::num_rows).sum(),
        ..ScanReport::default()
    };
    let mut files = BTreeSet::new();
    gather_scan_metrics(&plan, &mut report, &mut files);
    report.files_reached = files.len();
    Ok((report, batches))
}

fn gather_scan_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    report: &mut ScanReport,
    files: &mut BTreeSet<String>,
) {
    report.sorts |= plan.downcast_ref::<SortExec>().is_some();
    if let Some(metrics) = plan.metrics() {
        // Per-file Parquet metrics are registered as a file is prepared for
        // opening, before its statistics are checked, so the `filename`
        // labels name every file the scan reached.
        files.extend(
            metrics
                .iter()
                .flat_map(|metric| metric.labels())
                .filter(|label| label.name() == "filename")
                .map(|label| label.value().to_string()),
        );
        let totals = metrics.aggregate_by_name();
        report.bytes_scanned += count(&totals, "bytes_scanned");
        let (pruned, read) = pruning(&totals, "files_ranges_pruned_statistics");
        report.files_pruned += pruned;
        report.files_read += read;
        let (pruned, read) = pruning(&totals, "row_groups_pruned_statistics");
        report.row_groups_pruned += pruned;
        report.row_groups_read += read;
        report.row_groups_pruned += pruning(&totals, "limit_pruned_row_groups").0;
    }
    for child in plan.children() {
        gather_scan_metrics(child, report, files);
    }
}

fn count(totals: &MetricsSet, name: &str) -> usize {
    match totals.sum_by_name(name) {
        Some(MetricValue::Count { count, .. }) => count.value(),
        _ => 0,
    }
}

/// `(pruned, matched)` of a pruning metric.
fn pruning(totals: &MetricsSet, name: &str) -> (usize, usize) {
    match totals.sum_by_name(name) {
        Some(MetricValue::PruningMetrics {
            pruning_metrics, ..
        }) => (pruning_metrics.pruned(), pruning_metrics.matched()),
        _ => (0, 0),
    }
}
