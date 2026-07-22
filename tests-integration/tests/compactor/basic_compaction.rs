//! Basic compaction integration test
//!
//! Tests the end-to-end compaction flow:
//! 1. Create a table with small files
//! 2. Run compaction
//! 3. Verify data integrity and file consolidation

use anyhow::Result;
use common::catalog_manager::CatalogManager;
use compactor::executor::{CompactionExecutor, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, CompactionPlanner, PartitionStats, PlannerConfig};
use object_store::memory::InMemory;
use std::sync::Arc;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use writer::IcebergTableWriter;

/// Test basic compaction with a simple scenario
#[tokio::test]
async fn test_basic_compaction() -> Result<()> {
    // Initialize logging for the test
    let _ = env_logger::builder().is_test(true).try_init();

    // Setup: the planner discovers work by iterating configured tenants,
    // so the test tenant must exist in auth config.
    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant("test-tenant", "test-dataset")
        .build();
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
    .expect("Failed to create Iceberg writer");

    // 5 separate writes -> 5 small live files: the table genuinely needs
    // compaction.
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 100,
        base_timestamp: chrono::Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    for _ in 0..5 {
        generators::generate_traces(&mut writer, &config).await?;
    }

    // Count the REAL live files before compaction.
    let manifest_reader = compactor::iceberg::ManifestReader::new();
    let table_identifier =
        catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    let load_table = || async {
        match catalog_manager
            .catalog()
            .load_tabular(&table_identifier)
            .await
            .expect("Failed to load table")
        {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => panic!("Expected table"),
        }
    };
    let files_before = manifest_reader
        .get_snapshot_files(&load_table().await)
        .await?;
    let rows_before: u64 = files_before.iter().map(|f| f.record_count).sum();
    assert!(
        files_before.len() >= 2,
        "Test setup must produce multiple small files, got {}",
        files_before.len()
    );
    assert_eq!(rows_before, 500, "5 writes x 100 rows");

    // Create compaction planner
    let planner_config = PlannerConfig {
        file_count_threshold: 3, // Low threshold for testing (above post-compaction steady state)
        min_input_file_size_bytes: 100,
        max_files_per_job: 50,
        target_file_size_bytes: 128 * 1024 * 1024,
    };

    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config.clone());

    // Planning must be based on the REAL file set (issue #559): the table
    // has multiple small files, so it is a candidate with real stats.
    let candidates = planner.plan().await?;
    assert_eq!(
        candidates.len(),
        1,
        "expected exactly one whole-table candidate, got {candidates:?}"
    );
    let candidate = &candidates[0];
    assert_eq!(candidate.partition_id, "all");
    assert_eq!(
        candidate.stats.file_count,
        files_before.len(),
        "candidate must report the real live file count"
    );

    // Execute the compaction and verify it actually reduced the file set.
    let executor_config = ExecutorConfig::from(&planner_config);
    let metrics = CompactionMetrics::new();
    let executor =
        CompactionExecutor::new(catalog_manager.clone(), executor_config, metrics.clone());
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

    let files_after = manifest_reader
        .get_snapshot_files(&load_table().await)
        .await?;
    let rows_after: u64 = files_after.iter().map(|f| f.record_count).sum();
    assert!(
        files_after.len() < files_before.len(),
        "compaction must reduce the live file count ({} -> {})",
        files_before.len(),
        files_after.len()
    );
    assert_eq!(rows_after, rows_before, "compaction must not lose rows");

    // With the table compacted below the threshold, re-planning finds
    // nothing — the planner no longer flags every table forever.
    let candidates = planner.plan().await?;
    assert!(
        candidates.is_empty(),
        "compacted table must not be re-flagged: {candidates:?}"
    );

    log::info!("Basic compaction test completed successfully");

    Ok(())
}

/// Test that compaction handles empty tables gracefully
#[tokio::test]
async fn test_compaction_empty_table() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);

    let planner_config = PlannerConfig {
        file_count_threshold: 10,
        min_input_file_size_bytes: 1024 * 1024,
        max_files_per_job: 50,
        target_file_size_bytes: 128 * 1024 * 1024,
    };

    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config);

    // Planning should succeed even with no tables
    let candidates = planner.plan().await?;

    // Should find no candidates (no tables exist)
    assert_eq!(candidates.len(), 0);

    log::info!("Empty table compaction test passed");

    Ok(())
}

/// Test compaction candidate creation
#[test]
fn test_compaction_candidate_logging() {
    let candidate = CompactionCandidate {
        tenant_id: "test".to_string(),
        dataset_id: "dataset".to_string(),
        table_name: "traces".to_string(),
        partition_id: "2026-02-08-14".to_string(),
        stats: PartitionStats {
            file_count: 15,
            total_size_bytes: 30 * 1024 * 1024,
            avg_file_size_bytes: 2 * 1024 * 1024,
        },
    };

    // Just verify logging doesn't panic
    candidate.log();
}
