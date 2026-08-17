//! Compaction converges a partition toward attested, sorted files.
//!
//! A table can hold files an older build wrote, before anything attested a
//! sort order. Compaction is the mechanism that converges them: the partition
//! it rewrites comes out fully attested and sorted by the declared key, and it
//! must never turn attested files back into unattested ones.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use compactor::executor::{CompactionExecutor, ExecutorConfig};
use compactor::planner::{CompactionPlanner, PlannerConfig};
use datafusion::arrow::array::RecordBatch;
use futures::stream;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use object_store::memory::InMemory;
use std::sync::Arc;
use tests_integration::compaction_helpers::{
    aligned_hour_start, attested_sort_order_ids, context_for, load_table, trace_sort_keys,
};
use tests_integration::generators;
use writer::IcebergTableWriter;

const TENANT: &str = "converge-tenant";
const DATASET: &str = "converge-dataset";
const TABLE: &str = "traces";

async fn scan_all(catalog_manager: &Arc<CatalogManager>) -> Result<Vec<RecordBatch>> {
    let table = load_table(catalog_manager, TENANT, DATASET, TABLE).await?;
    let ctx = context_for(TABLE, table, false)?;
    Ok(ctx
        .sql(&format!("SELECT * FROM {TABLE}"))
        .await?
        .collect()
        .await?)
}

/// Write `batch` into the table the way a build predating the ordering
/// contract did: straight through the unattested write path, so the resulting
/// file claims no sort order whatever its rows happen to look like.
async fn append_legacy_file(
    catalog_manager: &Arc<CatalogManager>,
    batch: RecordBatch,
) -> Result<()> {
    let mut table = load_table(catalog_manager, TENANT, DATASET, TABLE).await?;
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch)]), None).await?;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await?;
    Ok(())
}

#[tokio::test]
async fn compaction_makes_a_partition_of_legacy_files_fully_attested() -> Result<()> {
    tests_integration::init_test_logging();

    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);
    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        Arc::new(InMemory::new()),
        TENANT.to_string(),
        DATASET.to_string(),
        TABLE.to_string(),
    )
    .await?;

    // Two ingest files, attested by the writer's sorted persist path.
    let ingest_files: Vec<Vec<String>> = (0..2)
        .map(|file| {
            (0..10)
                .rev()
                .map(|row| format!("trace-{file}-{row:02}"))
                .collect()
        })
        .collect();
    generators::generate_trace_files_with_ids(&mut writer, &ingest_files, aligned_hour_start(2))
        .await?;

    // Two more files in the same partition, written the old way. Reusing the
    // rows already in the table keeps them in the table's physical schema
    // without hand-building it, and lands them in the same hour.
    let seed = scan_all(&catalog_manager)
        .await?
        .into_iter()
        .find(|batch| batch.num_rows() > 0)
        .context("the seeded table must have rows")?;
    for _ in 0..2 {
        append_legacy_file(&catalog_manager, seed.clone()).await?;
    }

    let table = load_table(&catalog_manager, TENANT, DATASET, TABLE).await?;
    let before = attested_sort_order_ids(&table).await?;
    assert_eq!(before.len(), 4, "two ingest files plus two legacy ones");
    assert_eq!(
        before.values().filter(|id| id.is_none()).count(),
        2,
        "the legacy files must start out unattested: {before:?}"
    );
    let rows_before = trace_sort_keys(&scan_all(&catalog_manager).await?)?.len();

    // Compact the partition holding all four files.
    let planner_config = PlannerConfig {
        file_count_threshold: 3,
        max_input_file_size_bytes: 64 * 1024 * 1024,
        target_file_size_bytes: 128 * 1024 * 1024,
        partition_lateness: std::time::Duration::ZERO,
        max_partition_input_bytes: 0,
    };
    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config.clone());
    let candidates = planner.plan().await?;
    assert_eq!(
        candidates.len(),
        1,
        "one partition to compact: {candidates:?}"
    );

    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::from(&planner_config),
        compactor::metrics::CompactionMetrics::new(),
    );
    let result = executor
        .execute_candidate(candidates.into_iter().next().unwrap())
        .await?;
    assert!(
        matches!(
            result.status,
            compactor::executor::CompactionStatus::Success
        ),
        "compaction must succeed: {:?}",
        result.error
    );

    // Convergence: every live file now attests the declared order.
    let table = load_table(&catalog_manager, TENANT, DATASET, TABLE).await?;
    let after = attested_sort_order_ids(&table).await?;
    assert!(
        after.values().all(|id| *id == Some(1)),
        "compaction must leave the partition fully attested: {after:?}"
    );
    assert!(
        after.len() < before.len(),
        "compaction must consolidate files ({} -> {})",
        before.len(),
        after.len()
    );

    // And the attestation is honest: the rows really are in key order, and
    // none were lost on the way.
    let rows = trace_sort_keys(&scan_all(&catalog_manager).await?)?;
    assert_eq!(rows.len(), rows_before, "compaction must not lose rows");
    assert!(
        rows.windows(2).all(|pair| pair[0] <= pair[1]),
        "compacted rows must be ordered by (timestamp, trace_id)"
    );

    Ok(())
}
