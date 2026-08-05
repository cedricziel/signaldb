//! Integration test for compaction with concurrent writes
//!
//! This test verifies that the compactor correctly handles concurrent writes
//! from the Writer service using Iceberg's optimistic concurrency control.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use object_store::memory::InMemory;
use std::sync::Arc;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use writer::IcebergTableWriter;

/// Writes `num_files` single-file batches of `rows_per_file` rows each into a
/// single partition, via the shared [`generators::generate_traces`] helper.
///
/// This deliberately reuses the real v1 trace schema/generator (see
/// `tests-integration/src/generators/data_generator.rs`) rather than a
/// test-local hand-rolled `RecordBatch`: an earlier, narrower hand-rolled
/// schema here was missing columns the writer's schema requires (e.g.
/// `service_name`), and every write site was wrapped in a
/// `tracing::warn!`-and-`return Ok(())` guard that silently turned that write
/// failure into a passing test. Now that those guards are gone (writes are
/// hard errors), reusing the already-correct generator both fixes the
/// schema mismatch and avoids reintroducing a second, divergent copy of the
/// trace schema that could drift out of sync again.
async fn write_trace_files(
    writer: &mut IcebergTableWriter,
    num_files: usize,
    rows_per_file: usize,
) -> Result<()> {
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: num_files,
        rows_per_file,
        base_timestamp: chrono::Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    generators::generate_traces(writer, &config).await?;
    Ok(())
}

/// Test concurrent writes scenario:
/// 1. Create initial small files via Writer
/// 2. Start compaction job
/// 3. Concurrently write new data via Writer
/// 4. Verify both operations complete without corruption
#[tokio::test]
async fn test_compaction_with_concurrent_writes() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("=== Starting concurrent writes test ===");

    // Setup: Create in-memory catalog and object store
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    // Phase 1: Create initial small files via Writer
    tracing::info!("Phase 1: Creating initial small files via Writer");

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create writer for initial batches")?;

    // Write 10 small files (100 rows each = 1000 rows total)
    write_trace_files(&mut writer, 10, 100)
        .await
        .context("Failed to write initial batches")?;

    tracing::info!("Initial writes complete: 10 small batches created");

    // Phase 2: Setup compaction executor
    tracing::info!("Phase 2: Setting up compaction executor");

    let executor_config = ExecutorConfig::default();
    let max_retries = executor_config.max_retries;
    let metrics = CompactionMetrics::new();
    let executor = Arc::new(CompactionExecutor::new(
        catalog_manager.clone(),
        executor_config,
        metrics.clone(),
    ));

    // Create a compaction candidate manually
    // In real scenario, this would come from the planner
    let candidate = CompactionCandidate {
        tenant_id: tenant_id.to_string(),
        dataset_id: dataset_id.to_string(),
        table_name: table_name.to_string(),
        partition_id: "test-partition".to_string(),
        stats: PartitionStats {
            file_count: 10,
            total_size_bytes: 10 * 1024 * 1024, // ~10MB estimate
            avg_file_size_bytes: 1024 * 1024,   // ~1MB avg
        },
    };

    // Phase 3: Start compaction in background task
    tracing::info!("Phase 3: Starting compaction job in background");

    let executor_clone = executor.clone();
    let candidate_clone = candidate.clone();

    // Release the compaction task and the concurrent writer from the same
    // starting line via a Barrier instead of guessing a "head start" with a
    // fixed sleep: a constant-duration sleep either wastes wall-clock when
    // it's longer than needed, or, under CI load, fails to give compaction
    // any lead at all -- flaky in both directions. The Barrier removes the
    // arbitrary constant; it does not (and cannot, without adding test-only
    // hooks into the compactor's execution path) force *which* side wins the
    // resulting Iceberg OCC race, so every possible race outcome is asserted
    // explicitly in Phase 6 instead of only checking the branch that
    // happened to occur.
    let start_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let compaction_barrier = start_barrier.clone();

    let compaction_handle = tokio::spawn(async move {
        compaction_barrier.wait().await;
        tracing::info!("Compaction task started");
        let result = executor_clone.execute_candidate(candidate_clone).await;
        tracing::info!("Compaction task completed: {result:?}");
        result
    });

    start_barrier.wait().await;

    // Phase 4: Simulate concurrent write from Writer service
    tracing::info!("Phase 4: Writing concurrent data via Writer");

    let mut concurrent_writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create concurrent writer")?;

    // Write 5 new files concurrently. append_batches_with_marker already
    // retries Iceberg CAS conflicts internally (see
    // IcebergTableWriter::append_batches_with_marker), so an error surfacing
    // here past that retry loop is a genuine failure, not an expected
    // transient race with the concurrently running compaction -- it must
    // fail the test rather than being logged and skipped.
    write_trace_files(&mut concurrent_writer, 5, 100)
        .await
        .context("Failed to write concurrent batches")?;
    tracing::info!("Concurrent writes complete: 5 new batches created");

    // Phase 5: Wait for compaction to complete. A hard error here (task
    // panic, or a non-conflict failure surfacing out of execute_candidate)
    // must fail the test, not be logged and swallowed -- silently accepting
    // it would let the test "pass" having asserted nothing about the one
    // thing it exists to check.
    tracing::info!("Phase 5: Waiting for compaction to complete");
    let result = compaction_handle
        .await
        .context("Compaction task panicked")?
        .context("Compaction execution returned a hard error")?;

    // Phase 6: Verify results. Every branch of the race is asserted
    // unconditionally on the authoritative `result.status` -- no branch is
    // skipped, and no branch is "the incidental timing didn't hit it, so
    // nothing was checked".
    tracing::info!("Phase 6: Verifying results");
    tracing::info!("Compaction completed with status: {:?}", result.status);
    tracing::info!("Duration: {:?}", result.duration);
    tracing::info!(
        "Input files: {}, Output files: {}",
        result.input_files_count,
        result.output_files_count
    );

    let summary = metrics.summary();
    tracing::info!("=== Compaction Metrics ===");
    tracing::info!("Jobs started: {}", summary.jobs_started);
    tracing::info!("Jobs succeeded: {}", summary.jobs_succeeded);
    tracing::info!("Jobs failed: {}", summary.jobs_failed);
    tracing::info!("Conflicts detected: {}", summary.conflicts_detected);
    tracing::info!("Retries attempted: {}", summary.retries_attempted);
    tracing::info!("Total input files: {}", summary.total_input_files);
    tracing::info!("Total output files: {}", summary.total_output_files);

    assert_eq!(
        summary.jobs_started, 1,
        "Should have started exactly 1 compaction job"
    );
    assert_eq!(
        summary.jobs_succeeded + summary.jobs_failed,
        1,
        "The single compaction job should have resolved exactly once"
    );

    match result.status {
        CompactionStatus::Success => {
            assert_eq!(
                summary.jobs_succeeded, 1,
                "Success status must be reflected in the succeeded-jobs metric"
            );
            assert!(
                result.output_files_count <= result.input_files_count,
                "Successful compaction must not increase file count: {} -> {}",
                result.input_files_count,
                result.output_files_count
            );
        }
        CompactionStatus::Conflict => {
            assert_eq!(
                summary.jobs_failed, 1,
                "Conflict status must be reflected in the failed-jobs metric"
            );
            assert!(
                summary.conflicts_detected >= 1,
                "Conflict status must have recorded at least one conflict"
            );
            assert!(
                (1..=max_retries as usize).contains(&summary.retries_attempted),
                "Retries attempted ({}) should be between 1 and max_retries ({max_retries})",
                summary.retries_attempted,
            );
        }
        CompactionStatus::Failed => {
            // Self-authored OCC losses are now typed
            // (`compactor::commit::CommitError::SnapshotConflict`) and
            // `is_conflict_error` matches on the type through any
            // `.context(...)` wrapping (plus scanning the full cause chain
            // for stringly-typed iceberg-rust errors), so a lost race must
            // surface as `Conflict` (or `Success` after a retry). A `Failed`
            // status under concurrent OCC writes is a real bug again.
            panic!(
                "Compaction reported Failed under concurrent OCC writes; commit-time conflicts \
                 must be classified as Conflict and retried: {:?}",
                result.error
            );
        }
    }

    tracing::info!("=== Test completed successfully ===");
    Ok(())
}

/// Test that multiple concurrent compaction jobs on different partitions can run safely
#[tokio::test]
async fn test_concurrent_compactions_different_partitions() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("=== Starting concurrent compactions test ===");

    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    // Create initial data via Writer
    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create writer")?;

    // Write data (simulating two logical partitions)
    write_trace_files(&mut writer, 10, 100)
        .await
        .context("Failed to write test data")?;

    tracing::info!("Created test data");

    // Setup executor
    let executor_config = ExecutorConfig::default();
    let metrics = CompactionMetrics::new();
    let executor = Arc::new(CompactionExecutor::new(
        catalog_manager.clone(),
        executor_config,
        metrics.clone(),
    ));

    // Create candidates for two "partitions"
    let candidate1 = CompactionCandidate {
        tenant_id: tenant_id.to_string(),
        dataset_id: dataset_id.to_string(),
        table_name: table_name.to_string(),
        partition_id: "partition-1".to_string(),
        stats: PartitionStats {
            file_count: 5,
            total_size_bytes: 5 * 1024 * 1024,
            avg_file_size_bytes: 1024 * 1024,
        },
    };

    let candidate2 = CompactionCandidate {
        tenant_id: tenant_id.to_string(),
        dataset_id: dataset_id.to_string(),
        table_name: table_name.to_string(),
        partition_id: "partition-2".to_string(),
        stats: PartitionStats {
            file_count: 5,
            total_size_bytes: 5 * 1024 * 1024,
            avg_file_size_bytes: 1024 * 1024,
        },
    };

    // Run both compactions concurrently
    let executor1 = executor.clone();
    let executor2 = executor.clone();

    let handle1 = tokio::spawn(async move {
        tracing::info!("Starting compaction for partition 1");
        executor1.execute_candidate(candidate1).await
    });

    let handle2 = tokio::spawn(async move {
        tracing::info!("Starting compaction for partition 2");
        executor2.execute_candidate(candidate2).await
    });

    // Wait for both to complete
    let (result1, result2) = tokio::try_join!(handle1, handle2)?;

    tracing::info!("Partition 1 result: {result1:?}");
    tracing::info!("Partition 2 result: {result2:?}");

    // Check metrics
    let summary = metrics.summary();
    tracing::info!("Final metrics:");
    tracing::info!("  Jobs started: {}", summary.jobs_started);
    tracing::info!("  Jobs succeeded: {}", summary.jobs_succeeded);
    tracing::info!("  Jobs failed: {}", summary.jobs_failed);
    tracing::info!("  Conflicts: {}", summary.conflicts_detected);

    // Should have started 2 jobs (one per partition)
    assert_eq!(summary.jobs_started, 2, "Should have started 2 jobs");

    tracing::info!("=== Test completed successfully ===");
    Ok(())
}
