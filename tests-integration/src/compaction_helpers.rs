//! Helpers for tests that drive compaction directly.
//!
//! Compaction is partition-scoped (issue #933): a [`CompactionCandidate`]
//! names one `timestamp_hour` partition — hours since the Unix epoch — which
//! the executor rewrites and commits as a delta. Tests that build candidates
//! by hand therefore need the real partition value rather than a placeholder,
//! and it must come from the manifests, since the generator's timestamp range
//! determines which hours the writer actually produced files for.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use compactor::ManifestReader;
use datafusion::arrow::array::{Array, RecordBatch};
use iceberg_rust::table::Table;
use std::collections::HashMap;
use std::sync::Arc;

/// Milliseconds in one hour partition.
pub const MILLIS_PER_HOUR: i64 = 60 * 60 * 1000;

/// Start of the hour `hours_ago` hours before now, in epoch millis.
///
/// Aligning a generator's `base_timestamp` to an hour boundary keeps its rows
/// inside exactly one Iceberg `timestamp_hour` partition; an unaligned range
/// straddles two, which splits the files a test expects to compact together.
pub fn aligned_hour_start(hours_ago: i64) -> i64 {
    (chrono::Utc::now().timestamp_millis() / MILLIS_PER_HOUR - hours_ago) * MILLIS_PER_HOUR
}

/// Load a table with fresh metadata.
pub async fn load_table(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<Table> {
    let identifier = catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    match catalog_manager
        .catalog()
        .load_tabular(&identifier)
        .await
        .with_context(|| format!("Failed to load table {identifier}"))?
    {
        iceberg_rust::catalog::tabular::Tabular::Table(table) => Ok(table),
        _ => anyhow::bail!("Expected a table at {identifier}"),
    }
}

/// The `timestamp_hour` partition holding the most live data files.
///
/// Use this to build a compaction candidate for whichever partition the test's
/// generated data actually landed in.
pub async fn busiest_partition(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<i64> {
    let table = load_table(catalog_manager, tenant_id, dataset_id, table_name).await?;

    let mut counts: HashMap<i64, usize> = HashMap::new();
    for file in ManifestReader::new()
        .get_snapshot_files(&table)
        .await
        .context("Failed to read live files from manifests")?
    {
        if let Some(partition) = file.partition_hours {
            *counts.entry(partition).or_default() += 1;
        }
    }

    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(partition, _)| partition)
        .context("No partitioned data files found in table")
}

/// The sort order id each of the current snapshot's data files attests, keyed
/// by file path.
///
/// `None` means the file claims no ordering — either it was written before
/// the table declared one, or by a producer that could not guarantee the
/// order. The query engine reads such a file as unsorted, so this is the
/// observable form of the honesty invariant: a test can check exactly which
/// files claim to be ordered.
///
/// A `SessionContext` with `table` registered under `name`.
///
/// `split_file_groups_by_statistics` is the one execution option ordering
/// tests need to vary: it decides whether the scan regroups attested files by
/// their statistics before claiming an ordering.
pub fn context_for(
    name: &str,
    table: Table,
    split_file_groups_by_statistics: bool,
) -> Result<datafusion::prelude::SessionContext> {
    use datafusion::prelude::{SessionConfig, SessionContext};

    let mut config = SessionConfig::new().with_target_partitions(4);
    config
        .options_mut()
        .execution
        .split_file_groups_by_statistics = split_file_groups_by_statistics;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table(
        name,
        Arc::new(datafusion_iceberg::DataFusionTable::from(table)),
    )
    .context("Failed to register the table with DataFusion")?;
    Ok(ctx)
}

/// The `(timestamp, trace_id)` pairs of every row of `batches`, in batch
/// order — the traces table's declared sort key, as it is compared.
///
/// Scanning without an `ORDER BY` and reading the pairs out in scan order is
/// how ordering tests observe the physical layout: for a single data file it
/// is the order the rows are stored in.
pub fn trace_sort_keys(batches: &[RecordBatch]) -> Result<Vec<(i64, String)>> {
    use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray};

    let mut keys = Vec::new();
    for batch in batches {
        let timestamps = batch
            .column_by_name("timestamp")
            .context("no timestamp column")?
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .context("timestamp is not a microsecond timestamp column")?;
        let trace_ids = batch
            .column_by_name("trace_id")
            .context("no trace_id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("trace_id is not a string column")?;
        keys.extend(
            (0..batch.num_rows()).map(|i| (timestamps.value(i), trace_ids.value(i).to_string())),
        );
    }
    Ok(keys)
}

/// Scoped to the current snapshot, so files a compaction has already replaced
/// do not count against what the table holds now.
pub async fn attested_sort_order_ids(table: &Table) -> Result<HashMap<String, Option<i32>>> {
    Ok(ManifestReader::new()
        .get_snapshot_files(table)
        .await
        .context("Failed to read live files from manifests")?
        .into_iter()
        .map(|file| (file.file_path, file.sort_order_id))
        .collect())
}
