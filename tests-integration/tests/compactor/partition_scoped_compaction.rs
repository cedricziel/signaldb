//! Integration tests for partition-scoped compaction with delta commits
//! (issue #933).
//!
//! These pin the two properties that distinguish a scoped delta commit from
//! the whole-table `replace` it replaced:
//!
//! 1. **Isolation** — compacting one hour partition leaves every other
//!    partition's data files referenced exactly as they were. The old design
//!    rewrote and re-committed the entire table, so compacting one new hour
//!    rewrote months of history.
//! 2. **Coexistence with ingest** — a concurrent append does not invalidate
//!    the commit. The old design guarded on table-wide snapshot equality, so
//!    against a 5-second ingest commit cadence it lost the race essentially
//!    always, after writing a full duplicate copy of the table.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use compactor::ManifestReader;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use iceberg_rust::table::Table;
use object_store::memory::InMemory;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use writer::IcebergTableWriter;

const MILLIS_PER_HOUR: i64 = 3_600 * 1_000;

/// Load the table and group its live data file paths by `timestamp_hour`
/// partition, so a test can assert on exactly which files a commit touched.
async fn live_files_by_partition(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<HashMap<i64, HashSet<String>>> {
    let table = load_table(catalog_manager, tenant_id, dataset_id, table_name).await?;
    let files = ManifestReader::new()
        .get_snapshot_files(&table)
        .await
        .context("Failed to read live files from manifests")?;

    let mut by_partition: HashMap<i64, HashSet<String>> = HashMap::new();
    for file in files {
        let partition = file
            .partition_hours
            .context("Generated data file has no timestamp_hour partition value")?;
        by_partition
            .entry(partition)
            .or_default()
            .insert(file.file_path);
    }
    Ok(by_partition)
}

/// Total live row count across the whole table, from manifest metadata.
async fn live_row_count(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<u64> {
    let table = load_table(catalog_manager, tenant_id, dataset_id, table_name).await?;
    let files = ManifestReader::new().get_snapshot_files(&table).await?;
    Ok(files.iter().map(|f| f.record_count).sum())
}

async fn load_table(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<Table> {
    let identifier = catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    match catalog_manager.catalog().load_tabular(&identifier).await? {
        iceberg_rust::catalog::tabular::Tabular::Table(table) => Ok(table),
        _ => anyhow::bail!("Expected a table at {identifier}"),
    }
}

/// Start of the hour, `hours_ago` hours before now, in epoch millis. Aligning
/// to the hour boundary keeps each generated batch inside exactly one Iceberg
/// `timestamp_hour` partition.
fn aligned_hour_start(hours_ago: i64) -> i64 {
    let now_ms = chrono::Utc::now().timestamp_millis();
    (now_ms / MILLIS_PER_HOUR - hours_ago) * MILLIS_PER_HOUR
}

/// Compacting one partition must not touch any other partition's files.
///
/// This is the property the whole-table rewrite could not have: it replaced
/// the table's entire data file set on every cycle, so every partition's files
/// were rewritten whether or not they needed it.
#[tokio::test]
async fn compaction_rewrites_only_the_target_partition() -> Result<()> {
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create writer")?;

    // Three consecutive closed hour partitions, six small files each.
    let base_timestamp = aligned_hour_start(5);
    generators::generate_traces(
        &mut writer,
        &DataGeneratorConfig {
            partition_count: 3,
            files_per_partition: 6,
            rows_per_file: 50,
            base_timestamp,
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("Failed to generate partitioned trace data")?;

    let before =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let rows_before = live_row_count(&catalog_manager, tenant_id, dataset_id, table_name).await?;

    assert_eq!(
        before.len(),
        3,
        "expected three distinct hour partitions, got {:?}",
        before.keys().collect::<Vec<_>>()
    );

    // Compact the middle partition only.
    let mut partitions: Vec<i64> = before.keys().copied().collect();
    partitions.sort();
    let target = partitions[1];
    let untouched: Vec<i64> = vec![partitions[0], partitions[2]];

    let metrics = CompactionMetrics::new();
    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::default(),
        metrics.clone(),
    );

    let result = executor
        .execute_candidate(CompactionCandidate {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_name: table_name.to_string(),
            partition_id: target.to_string(),
            stats: PartitionStats {
                file_count: before[&target].len(),
                total_size_bytes: 6 * 1024,
                avg_file_size_bytes: 1024,
            },
        })
        .await
        .context("Compaction execution returned a hard error")?;

    assert_eq!(
        result.status,
        CompactionStatus::Success,
        "compacting a closed partition with no concurrent writer must succeed: {:?}",
        result.error
    );

    let after =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;

    // The untouched partitions must be byte-identical file sets. This is the
    // core delta-commit guarantee.
    for partition in untouched {
        assert_eq!(
            after.get(&partition),
            before.get(&partition),
            "partition {partition} was not the compaction target but its file set changed"
        );
    }

    // The target partition must have been genuinely rewritten: fewer files,
    // and none of the original paths still referenced.
    let target_before = &before[&target];
    let target_after = after
        .get(&target)
        .expect("target partition must still hold data after compaction");
    assert!(
        target_after.len() < target_before.len(),
        "compaction should reduce file count in partition {target}: {} -> {}",
        target_before.len(),
        target_after.len()
    );
    assert!(
        target_after.is_disjoint(target_before),
        "compacted partition must reference only newly written files"
    );

    // No rows may be lost or duplicated anywhere in the table.
    let rows_after = live_row_count(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    assert_eq!(
        rows_after, rows_before,
        "row count must be preserved across a delta commit"
    );

    Ok(())
}

/// An append landing while compaction runs must not invalidate the commit,
/// and the appended files must survive it.
///
/// Under the whole-table `replace` this was unwinnable: the commit guarded on
/// table-wide snapshot equality, so any concurrent append forced a retry (and
/// each retry had already written a full duplicate copy of the table).
#[tokio::test]
async fn concurrent_append_does_not_invalidate_the_delta_commit() -> Result<()> {
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create writer")?;

    // Seed a closed partition to compact.
    let target_hour_ms = aligned_hour_start(5);
    generators::generate_traces(
        &mut writer,
        &DataGeneratorConfig {
            partition_count: 1,
            files_per_partition: 8,
            rows_per_file: 50,
            base_timestamp: target_hour_ms,
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("Failed to generate seed data")?;

    let before =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let target = *before
        .keys()
        .next()
        .expect("seed data must produce one partition");
    let rows_before = live_row_count(&catalog_manager, tenant_id, dataset_id, table_name).await?;

    let metrics = CompactionMetrics::new();
    let executor = Arc::new(CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::default(),
        metrics.clone(),
    ));

    // Release compaction and the concurrent writer from the same starting
    // line, so the append genuinely overlaps the rewrite.
    let start_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let compaction_barrier = start_barrier.clone();
    let executor_clone = executor.clone();
    let candidate = CompactionCandidate {
        tenant_id: tenant_id.to_string(),
        dataset_id: dataset_id.to_string(),
        table_name: table_name.to_string(),
        partition_id: target.to_string(),
        stats: PartitionStats {
            file_count: before[&target].len(),
            total_size_bytes: 8 * 1024,
            avg_file_size_bytes: 1024,
        },
    };

    let compaction_handle = tokio::spawn(async move {
        compaction_barrier.wait().await;
        executor_clone.execute_candidate(candidate).await
    });

    start_barrier.wait().await;

    // Append into a *different*, still-open partition — the realistic shape of
    // live ingest running alongside compaction of an older hour.
    let mut concurrent_writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create concurrent writer")?;

    generators::generate_traces(
        &mut concurrent_writer,
        &DataGeneratorConfig {
            partition_count: 1,
            files_per_partition: 4,
            rows_per_file: 50,
            base_timestamp: aligned_hour_start(1),
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("Failed to write concurrent batches")?;

    let result = compaction_handle
        .await
        .context("Compaction task panicked")?
        .context("Compaction execution returned a hard error")?;

    assert_eq!(
        result.status,
        CompactionStatus::Success,
        "compaction must commit successfully alongside a concurrent append: {:?}",
        result.error
    );

    // Note what is and is not guaranteed here. Two commits to the same Iceberg
    // table still serialize on the catalog's compare-and-swap, so an append
    // landing inside the commit window can still force one retry. What the
    // scoped delta commit changes is that the retry is *winnable*: the
    // compaction's own input files are untouched by the append, so re-running
    // it converges. Under the whole-table `replace` the commit guarded on
    // table-wide snapshot equality, so the same race exhausted the retry
    // budget and the job ended in `Conflict` — after writing a full duplicate
    // copy of the table on every attempt.
    let summary = metrics.summary();
    assert_eq!(
        summary.jobs_succeeded, 1,
        "the compaction job must resolve as succeeded, not exhaust its retries"
    );
    // The executor makes at most `max_retries` attempts, so it can record at
    // most `max_retries - 1` retries — asserting `<= max_retries` could never
    // fail and would prove nothing.
    assert!(
        summary.retries_attempted < ExecutorConfig::default().max_retries as usize,
        "any conflict must resolve within the retry budget, got {} retries",
        summary.retries_attempted
    );

    // Both the compacted partition and the concurrently appended one must be
    // present, and every row accounted for.
    let after =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    assert!(
        after.len() >= 2,
        "the concurrently appended partition must survive the compaction commit"
    );

    // `Success` alone also covers the no-op path, so require that the target
    // partition was genuinely merged.
    let target_after = after
        .get(&target)
        .expect("target partition must still hold data after compaction");
    assert!(
        target_after.len() < before[&target].len(),
        "the target partition must be genuinely compacted: {} -> {}",
        before[&target].len(),
        target_after.len()
    );

    let rows_after = live_row_count(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    assert_eq!(
        rows_after,
        rows_before + 4 * 50,
        "the delta commit must preserve compacted rows and keep the concurrently appended ones"
    );

    Ok(())
}

/// A partition far larger than the configured compaction memory budget must
/// not take the process down (openspec task 5.1).
///
/// The spec's requirement is deliberately two-sided: the job either completes
/// by spilling within the budget, or fails with an *attributable* resource
/// error. What it must never do is exhaust host memory — which is exactly what
/// the previous unbounded `SessionContext::new()` did on a large table.
#[tokio::test]
async fn oversized_partition_stays_within_its_memory_budget() -> Result<()> {
    let mut config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant("test-tenant", "test-dataset")
        .build();
    // 1 MB is far below what sorting this partition needs, so the rewrite is
    // forced onto the bounded path rather than growing the heap freely.
    config.compactor.memory_limit_mb = 1;

    let catalog_manager = Arc::new(CatalogManager::new(config).await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create writer")?;

    generators::generate_traces(
        &mut writer,
        &DataGeneratorConfig {
            partition_count: 1,
            files_per_partition: 20,
            rows_per_file: 500,
            base_timestamp: aligned_hour_start(5),
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("Failed to generate trace data")?;

    let before =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let target = *before.keys().next().expect("one partition");
    let rows_before = live_row_count(&catalog_manager, tenant_id, dataset_id, table_name).await?;

    let metrics = CompactionMetrics::new();
    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::default(),
        metrics.clone(),
    );

    // Reaching this line at all is half the assertion: an OOM would abort the
    // test process rather than return.
    let result = executor
        .execute_candidate(CompactionCandidate {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_name: table_name.to_string(),
            partition_id: target.to_string(),
            stats: PartitionStats {
                file_count: before[&target].len(),
                total_size_bytes: 20 * 1024,
                avg_file_size_bytes: 1024,
            },
        })
        .await
        .context("Compaction execution returned a hard error")?;

    match result.status {
        CompactionStatus::Success => {
            // Spilled or fit; either way the data must be intact.
            let rows_after =
                live_row_count(&catalog_manager, tenant_id, dataset_id, table_name).await?;
            assert_eq!(
                rows_after, rows_before,
                "a budget-constrained rewrite must still preserve every row"
            );
        }
        CompactionStatus::Failed | CompactionStatus::Conflict => {
            let error = result
                .error
                .expect("a non-success outcome must carry an error to be attributable");
            assert!(
                !error.is_empty(),
                "the failure must name a cause, not fail silently"
            );
            // A failed compaction must leave the table exactly as it was.
            let after =
                live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name)
                    .await?;
            assert_eq!(
                after, before,
                "a failed rewrite must not mutate the table's live file set"
            );
        }
    }

    Ok(())
}

/// An unspillable reservation that exceeds the whole pool must fail the job
/// with an attributable resource error (openspec task 5.1, spec sentence
/// "Exceeding the budget SHALL fail the job with a resource error rather
/// than exhausting host memory").
///
/// `oversized_partition_stays_within_its_memory_budget` above pins the
/// success side of that guarantee — a spillable `ExternalSorter` degrades
/// to disk instead of failing — but never exercises the failure side,
/// because with the default `target_partitions = 1` a single sorter has
/// the whole pool to itself and this data volume always fits after
/// spilling.
///
/// `FairSpillPool` only fair-shares *spillable* consumers
/// (`common::datafusion_runtime`, "What it does not cover"). Raising
/// `target_partitions` gives the sort plan per-partition `ExternalSorter`s
/// feeding a `SortPreservingMergeExec`, plus one `ExternalSorterMerge` per
/// sorter to read its own spilled runs back — all unspillable and all
/// competing first-come-first-served for whatever the pool has left. With
/// an 8-way fan-out over a 1 MB pool, those unspillable reservations
/// exceed the pool before any single one of them can grow, and DataFusion
/// returns `ResourcesExhausted` naming the losing consumer rather than
/// growing the heap. This is a real operator configuration
/// (`compactor.target_partitions`), not a test-only code path.
#[tokio::test]
async fn unspillable_reservation_over_budget_fails_with_attributable_resource_error() -> Result<()>
{
    let mut config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant("test-tenant", "test-dataset")
        .build();
    // 1 MB total budget, fanned out 8 ways: the per-partition sorters' own
    // unspillable merge-back reservations already exceed the pool, so the
    // job must fail with a resource error rather than growing the heap.
    config.compactor.memory_limit_mb = 1;
    config.compactor.target_partitions = 8;

    let catalog_manager = Arc::new(CatalogManager::new(config).await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create writer")?;

    generators::generate_traces(
        &mut writer,
        &DataGeneratorConfig {
            partition_count: 1,
            files_per_partition: 20,
            rows_per_file: 500,
            base_timestamp: aligned_hour_start(5),
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("Failed to generate trace data")?;

    let before =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let target = *before.keys().next().expect("one partition");

    let metrics = CompactionMetrics::new();
    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::default(),
        metrics.clone(),
    );

    let result = executor
        .execute_candidate(CompactionCandidate {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_name: table_name.to_string(),
            partition_id: target.to_string(),
            stats: PartitionStats {
                file_count: before[&target].len(),
                total_size_bytes: 20 * 1024,
                avg_file_size_bytes: 1024,
            },
        })
        .await
        .context("Compaction execution returned a hard error")?;

    assert_eq!(
        result.status,
        CompactionStatus::Failed,
        "an unspillable reservation over budget must fail the job outright, \
         not succeed or hit the unrelated conflict path"
    );

    let error = result
        .error
        .expect("a Failed outcome must carry an error to be attributable");
    assert!(
        error.contains("Resources exhausted"),
        "the failure must be DataFusion's own resource-exhaustion error, not \
         some other failure mode, got: {error}"
    );
    assert!(
        error.contains("top memory consumers"),
        "the error must name the consumer via TrackConsumersPool, so an \
         operator can attribute the failure, got: {error}"
    );

    // A failed job must leave the table exactly as it was — no partial
    // commit, no corruption.
    let after =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    assert_eq!(
        after, before,
        "a failed rewrite must not mutate the table's live file set"
    );

    Ok(())
}

/// A delta commit whose inputs were mutated underneath it must abort as a
/// conflict, and must leave no snapshot behind (openspec task 6.1).
///
/// This is the "retention dropped the partition mid-rewrite" case, driven
/// directly through the committer: the job's input set names a file that is
/// not live, which is what a concurrent partition drop leaves behind.
#[tokio::test]
async fn delta_commit_aborts_when_its_inputs_are_no_longer_live() -> Result<()> {
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create writer")?;

    generators::generate_traces(
        &mut writer,
        &DataGeneratorConfig {
            partition_count: 1,
            files_per_partition: 4,
            rows_per_file: 50,
            base_timestamp: aligned_hour_start(5),
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("Failed to generate trace data")?;

    let before =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let target = *before.keys().next().expect("one partition");

    // The live file set alone cannot prove "no snapshot": a no-op snapshot
    // would leave it identical. Pin the snapshot id too.
    let snapshot_before = load_table(&catalog_manager, tenant_id, dataset_id, table_name)
        .await?
        .metadata()
        .current_snapshot_id;

    // The job's input set: the partition's real files, plus one that is not
    // live — standing in for a file retention removed while the rewrite ran.
    let mut inputs: HashSet<String> = before[&target].clone();
    inputs.insert(format!(
        "{tenant_id}/{dataset_id}/{table_name}/data/timestamp_hour={target}/dropped-by-retention.parquet"
    ));

    let committer = compactor::IcebergCommitter::new(catalog_manager.clone());
    let error = committer
        .commit_delta(
            tenant_id,
            dataset_id,
            table_name,
            target,
            &inputs,
            // Empty output: the liveness check must reject the commit before
            // any files are considered, so this never reaches the transaction.
            vec![],
        )
        .await
        .expect_err("a mutated input set must abort the commit");

    assert!(
        compactor::is_conflict_error(&error),
        "a mutated input set must classify as a conflict so the job retries, got: {error:#}"
    );

    // No snapshot may have been created: the table is exactly as it was, so
    // any files the rewrite had already written stay unreferenced and are
    // reclaimable by orphan cleanup.
    let after =
        live_files_by_partition(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    assert_eq!(
        after, before,
        "an aborted delta commit must leave the table's live file set untouched"
    );

    let snapshot_after = load_table(&catalog_manager, tenant_id, dataset_id, table_name)
        .await?
        .metadata()
        .current_snapshot_id;
    assert_eq!(
        snapshot_after, snapshot_before,
        "an aborted delta commit must not create a snapshot, not even a no-op one"
    );

    Ok(())
}
