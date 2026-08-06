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
