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
use compactor::planner::{CompactionPlanner, PlannerConfig};
use object_store::memory::InMemory;
use std::sync::Arc;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use writer::IcebergTableWriter;

/// Test basic compaction with a simple scenario
#[tokio::test]
async fn test_basic_compaction() -> Result<()> {
    // Initialize logging for the test
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

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
    // compaction. The base timestamp is aligned to the start of a past hour
    // so every row lands in ONE closed `timestamp_hour` partition: compaction
    // is partition-scoped (issue #933), so an unaligned range would split the
    // writes across two partitions and leave the still-open current hour out
    // of the candidate set.
    const MILLIS_PER_HOUR: i64 = 60 * 60 * 1000;
    let base_timestamp =
        (chrono::Utc::now().timestamp_millis() / MILLIS_PER_HOUR - 2) * MILLIS_PER_HOUR;
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 100,
        base_timestamp,
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
        max_input_file_size_bytes: 64 * 1024 * 1024,
        target_file_size_bytes: 128 * 1024 * 1024,
        // Tests seed data into recent hours and compact it immediately;
        // a production lateness allowance would defer every such partition.
        partition_lateness: std::time::Duration::ZERO,
        max_partition_input_bytes: 0,
    };

    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config.clone());

    // Planning must be based on the REAL file set (issue #559): the partition
    // has multiple small files, so it is a candidate with real stats.
    let candidates = planner.plan().await?;
    assert_eq!(
        candidates.len(),
        1,
        "expected exactly one partition candidate, got {candidates:?}"
    );
    let candidate = &candidates[0];
    // The candidate names the hour partition the data was written into
    // (hours since the Unix epoch), not a whole-table placeholder.
    let expected_partition = base_timestamp / MILLIS_PER_HOUR;
    assert_eq!(
        candidate.partition_id,
        expected_partition.to_string(),
        "candidate must name the real timestamp_hour partition"
    );
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

    tracing::info!("Basic compaction test completed successfully");

    Ok(())
}

/// Test that compaction handles empty tables gracefully
#[tokio::test]
async fn test_compaction_empty_table() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);

    let planner_config = PlannerConfig {
        file_count_threshold: 10,
        max_input_file_size_bytes: 64 * 1024 * 1024,
        target_file_size_bytes: 128 * 1024 * 1024,
        // Tests seed data into recent hours and compact it immediately;
        // a production lateness allowance would defer every such partition.
        partition_lateness: std::time::Duration::ZERO,
        max_partition_input_bytes: 0,
    };

    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config);

    // Planning should succeed even with no tables
    let candidates = planner.plan().await?;

    // Should find no candidates (no tables exist)
    assert_eq!(candidates.len(), 0);

    tracing::info!("Empty table compaction test passed");

    Ok(())
}
