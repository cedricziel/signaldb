//! Orphan file cleanup integration tests
//!
//! Tests the orphan detection and cleanup system that identifies and removes
//! data files no longer referenced by any live snapshot.

use anyhow::{Context, Result};
use chrono::Utc;
use compactor::orphan::{
    cleaner::OrphanCleaner, config::OrphanCleanupConfig, detector::OrphanDetector,
};
use futures::TryStreamExt;
use iceberg_rust::catalog::tabular::Tabular;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use std::sync::Arc;
use tests_integration::fixtures::{
    DataGeneratorConfig, PartitionGranularity, RetentionTestContext,
};
use tests_integration::generators;

/// Test: Orphan detection finds unreferenced files
///
/// Creates a table with data files, manually adds orphan files to storage,
/// and verifies that orphan detection correctly identifies orphans while
/// not flagging live files.
#[tokio::test]
async fn test_orphan_detection_finds_unreferenced_files() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Create table with 10 data files (1 partition, 10 files)
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 10,
        rows_per_file: 50,
        base_timestamp: Utc::now().timestamp_millis() - (48 * 60 * 60 * 1000), // 2 days ago
        partition_granularity: PartitionGranularity::Day,
    };

    tracing::info!("Creating table with 10 legitimate data files");
    generators::generate_traces(&mut writer, &config).await?;

    // Manually add 3 orphan files to storage (simulate deleted snapshots)
    let table_path = format!("{}/{}/{}", tenant_id, dataset_id, table_name);
    let orphan_paths = vec![
        format!("{}/data/orphan-1.parquet", table_path),
        format!("{}/data/orphan-2.parquet", table_path),
        format!("{}/data/orphan-3.parquet", table_path),
    ];

    tracing::info!("Adding 3 orphan files to storage");
    let orphan_data = bytes::Bytes::from(vec![0u8; 1024]); // 1KB dummy data
    for path in &orphan_paths {
        ctx.object_store()
            .put(&ObjectPath::from(path.as_str()), orphan_data.clone().into())
            .await
            .context("Failed to put orphan file")?;
    }

    // Give orphan files time to be old enough (grace period check)
    // Note: In-memory store doesn't have real timestamps, so we configure detector
    // with 0 grace period for testing
    let detector_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0, // No grace period for test
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };

    let detector = OrphanDetector::new(
        detector_config,
        ctx.catalog_manager().clone(),
        ctx.object_store().clone(),
    );

    // Run orphan detection
    tracing::info!("Running orphan detection");
    let orphans = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await
        .context("Orphan detection failed")?;

    tracing::info!("Found {} orphan candidates", orphans.len());

    // Verify we found the 3 orphans we added
    assert_eq!(orphans.len(), 3, "Expected to find exactly 3 orphan files");

    // Verify the orphan paths match what we added
    let found_paths: std::collections::HashSet<_> =
        orphans.iter().map(|o| o.path.as_str()).collect();

    for expected_path in &orphan_paths {
        assert!(
            found_paths.contains(expected_path.as_str()),
            "Expected orphan path {} not found in results",
            expected_path
        );
    }

    Ok(())
}

/// Test: Orphan cleanup grace period configuration
///
/// Note: This test cannot fully validate time-based grace period filtering because
/// the in-memory object store doesn't maintain real file timestamps. It verifies that
/// the grace period configuration is respected and the detection logic completes successfully.
/// For time-based validation, see unit tests in src/compactor/src/orphan/detector.rs
#[tokio::test]
async fn test_orphan_cleanup_grace_period_configuration() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "logs";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Create a base table
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 2,
        rows_per_file: 10,
        base_timestamp: Utc::now().timestamp_millis() - (72 * 60 * 60 * 1000), // 3 days ago
        partition_granularity: PartitionGranularity::Day,
    };

    tracing::info!("Creating base table");
    generators::generate_logs(&mut writer, &config).await?;

    // Add orphan files to storage
    // Note: In-memory object store doesn't support real timestamps,
    // so this test validates the grace period logic conceptually
    let table_path = format!("{}/{}/{}", tenant_id, dataset_id, table_name);

    let old_orphan_path = format!("{}/data/old-orphan.parquet", table_path);
    let recent_orphan_path = format!("{}/data/recent-orphan.parquet", table_path);

    let orphan_data = bytes::Bytes::from(vec![0u8; 512]);

    // In a real scenario with actual object store timestamps:
    // - old_orphan would be > 24h old
    // - recent_orphan would be < 24h old

    ctx.object_store()
        .put(
            &ObjectPath::from(old_orphan_path.as_str()),
            orphan_data.clone().into(),
        )
        .await?;

    ctx.object_store()
        .put(
            &ObjectPath::from(recent_orphan_path.as_str()),
            orphan_data.clone().into(),
        )
        .await?;

    // Configure detector with 24h grace period
    let detector_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 24,
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };

    let detector = OrphanDetector::new(
        detector_config.clone(),
        ctx.catalog_manager().clone(),
        ctx.object_store().clone(),
    );

    tracing::info!("Running orphan detection with 24h grace period");
    let orphans = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;

    tracing::info!(
        "Found {} orphan candidates with 24h grace period",
        orphans.len()
    );

    // With in-memory store, all files appear old enough
    // In production with real timestamps, only old_orphan would be found
    // Just verify detection completes successfully (any count is valid)

    // Now test with 0 grace period - should find all orphans
    let no_grace_config = OrphanCleanupConfig {
        grace_period_hours: 0,
        ..detector_config
    };

    let detector_no_grace = OrphanDetector::new(
        no_grace_config,
        ctx.catalog_manager().clone(),
        ctx.object_store().clone(),
    );

    let orphans_no_grace = detector_no_grace
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;

    tracing::info!(
        "Found {} orphan candidates with 0h grace period",
        orphans_no_grace.len()
    );

    assert_eq!(
        orphans_no_grace.len(),
        2,
        "With 0 grace period, should find both orphan files"
    );

    Ok(())
}

/// Test: Orphan cleanup batch deletion
///
/// Creates 100 orphan files and verifies they are deleted in batches
/// with correct metrics tracking.
#[tokio::test]
async fn test_orphan_cleanup_batch_deletion() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "metrics_gauge";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Create base table
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 10,
        base_timestamp: Utc::now().timestamp_millis() - (48 * 60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Day,
    };

    generators::generate_metrics(&mut writer, &config).await?;

    // Add 100 orphan files
    let table_path = format!("{}/{}/{}", tenant_id, dataset_id, table_name);
    let orphan_data = bytes::Bytes::from(vec![0u8; 2048]); // 2KB per file

    tracing::info!("Creating 100 orphan files");
    for i in 0..100 {
        let path = format!("{}/data/orphan-{}.parquet", table_path, i);
        ctx.object_store()
            .put(&ObjectPath::from(path.as_str()), orphan_data.clone().into())
            .await?;
    }

    // Configure cleaner with batch_size = 10
    let cleanup_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 10,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };

    let detector = Arc::new(OrphanDetector::new(
        cleanup_config.clone(),
        ctx.catalog_manager().clone(),
        ctx.object_store().clone(),
    ));

    // Detect orphans
    let orphans = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;

    tracing::info!("Detected {} orphan files", orphans.len());
    assert_eq!(orphans.len(), 100, "Should detect all 100 orphan files");

    // Create cleaner and delete in batches
    let cleaner =
        OrphanCleaner::with_detector(cleanup_config.clone(), ctx.object_store().clone(), detector);

    tracing::info!("Deleting orphans in batches of 10");
    let result = cleaner.delete_orphans_batch(orphans).await?;

    // Verify results
    tracing::info!(
        "Deletion result: deleted={}, failed={}, bytes_freed={}",
        result.deleted_count,
        result.failed_count,
        result.total_bytes_freed
    );

    assert_eq!(
        result.deleted_count, 100,
        "Should delete all 100 orphan files"
    );
    assert_eq!(result.failed_count, 0, "Should have no failures");

    // Each file is 2KB = 2048 bytes
    let expected_bytes = 100 * 2048;
    assert_eq!(
        result.total_bytes_freed, expected_bytes,
        "Should free correct amount of bytes"
    );

    assert!(
        result.failed_deletions.is_empty(),
        "Should have no failed deletions"
    );

    Ok(())
}

/// Test: Orphan cleanup dry-run mode
///
/// Verifies that dry-run mode identifies orphans but does not actually
/// delete them, while still reporting metrics.
#[tokio::test]
async fn test_orphan_cleanup_dry_run_mode() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Create base table
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 2,
        rows_per_file: 10,
        base_timestamp: Utc::now().timestamp_millis() - (48 * 60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Day,
    };

    generators::generate_traces(&mut writer, &config).await?;

    // Add orphan files
    let table_path = format!("{}/{}/{}", tenant_id, dataset_id, table_name);
    let orphan_paths = vec![
        format!("{}/data/orphan-1.parquet", table_path),
        format!("{}/data/orphan-2.parquet", table_path),
        format!("{}/data/orphan-3.parquet", table_path),
    ];

    let orphan_data = bytes::Bytes::from(vec![0u8; 1024]);
    for path in &orphan_paths {
        ctx.object_store()
            .put(&ObjectPath::from(path.as_str()), orphan_data.clone().into())
            .await?;
    }

    // Configure with dry_run = true
    let dry_run_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 100,
        dry_run: true, // Dry-run mode
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };

    let detector = Arc::new(OrphanDetector::new(
        dry_run_config.clone(),
        ctx.catalog_manager().clone(),
        ctx.object_store().clone(),
    ));

    // Detect orphans
    let orphans = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;

    tracing::info!("[DRY-RUN] Found {} orphan files", orphans.len());
    assert_eq!(orphans.len(), 3, "Should detect 3 orphan files");

    // Run cleanup in dry-run mode
    let cleaner =
        OrphanCleaner::with_detector(dry_run_config, ctx.object_store().clone(), detector);

    let result = cleaner.delete_orphans_batch(orphans).await?;

    tracing::info!(
        "[DRY-RUN] Result: deleted={}, bytes_freed={}",
        result.deleted_count,
        result.total_bytes_freed
    );

    // Verify dry-run metrics are tracked (dry_run mode uses would_delete_count, not deleted_count)
    assert_eq!(
        result.would_delete_count, 3,
        "Dry-run should report 3 files as 'would delete'"
    );
    assert_eq!(
        result.would_free_bytes,
        3 * 1024,
        "Dry-run should report bytes that would be freed"
    );
    // Actual deletion counters must be zero in dry-run
    assert_eq!(
        result.deleted_count, 0,
        "Dry-run must not increment deleted_count"
    );
    assert_eq!(
        result.total_bytes_freed, 0,
        "Dry-run must not increment total_bytes_freed"
    );

    // Verify files still exist (not actually deleted)
    for path in &orphan_paths {
        let exists = ctx
            .object_store()
            .get(&ObjectPath::from(path.as_str()))
            .await
            .is_ok();

        assert!(
            exists,
            "Dry-run should NOT delete files, but {} was deleted",
            path
        );
    }

    Ok(())
}

/// Test: Orphan cleanup preserves live files
///
/// Creates a table with active data files and verifies that orphan cleanup
/// does not flag or delete any live files, and the table remains queryable.
#[tokio::test]
async fn test_orphan_cleanup_preserves_live_files() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "logs";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Create table with live data
    let config = DataGeneratorConfig {
        partition_count: 2,
        files_per_partition: 5,
        rows_per_file: 100,
        base_timestamp: Utc::now().timestamp_millis() - (24 * 60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Day,
    };

    tracing::info!("Creating table with live data (2 partitions, 5 files each)");
    let partitions = generators::generate_logs(&mut writer, &config).await?;

    tracing::info!(
        "Created {} partitions with total {} files",
        partitions.len(),
        partitions.iter().map(|p| p.file_count).sum::<usize>()
    );

    // Run orphan detection (should find nothing)
    let detector_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };

    let detector = OrphanDetector::new(
        detector_config.clone(),
        ctx.catalog_manager().clone(),
        ctx.object_store().clone(),
    );

    tracing::info!("Running orphan detection on table with only live files");
    let orphans = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;

    tracing::info!("Orphan detection found {} candidates", orphans.len());

    // Should find NO orphans (all files are live)
    assert_eq!(
        orphans.len(),
        0,
        "Should find no orphans in table with only live data"
    );

    // Verify table is still queryable by loading it
    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);

    let tabular = ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await
        .context("Failed to load table after orphan detection")?;

    let table = match tabular {
        Tabular::Table(t) => t,
        _ => anyhow::bail!("Expected table"),
    };

    // Verify table metadata is intact
    let metadata = table.metadata();
    assert!(
        metadata.current_snapshot_id.is_some(),
        "Table should still have a current snapshot"
    );

    tracing::info!(
        "Table verified: current_snapshot_id={:?}",
        metadata.current_snapshot_id
    );

    // Run cleanup (with no orphans, should be a no-op)
    let cleaner = OrphanCleaner::new(detector_config, ctx.object_store().clone());
    let result = cleaner.delete_orphans_batch(vec![]).await?;

    assert_eq!(
        result.deleted_count, 0,
        "Should delete nothing when no orphans exist"
    );
    assert_eq!(
        result.total_bytes_freed, 0,
        "Should free no bytes when no orphans exist"
    );

    Ok(())
}

/// Test: `max_live_files_threshold` causes early-return with empty candidate list.
///
/// When the estimated live file count from manifest metadata exceeds
/// `max_live_files_threshold`, orphan cleanup is skipped entirely and
/// `identify_orphan_candidates` returns an empty list rather than
/// (incorrectly) treating all files as orphans.
#[tokio::test]
async fn test_orphan_cleanup_threshold_skips_cleanup() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;
    let tenant_id = "threshold-tenant";
    let dataset_id = "threshold-dataset";
    let table_name = "traces";

    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Write 5 data files so manifest metadata reports >= 5 live files.
    let gen_config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 5,
        rows_per_file: 10,
        base_timestamp: chrono::Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Day,
    };
    generators::generate_traces(&mut writer, &gen_config).await?;

    // Set threshold to 1 — will be exceeded by the 5 files written above.
    let threshold_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 1, // force threshold exceeded
    };

    let detector = OrphanDetector::new(
        threshold_config,
        ctx.catalog_manager().clone(),
        ctx.object_store().clone(),
    );

    let orphans = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;

    // Threshold exceeded → cleanup skipped → no candidates returned.
    assert!(
        orphans.is_empty(),
        "Expected no candidates when threshold exceeded, got {}",
        orphans.len()
    );
    Ok(())
}

/// Regression test for #925: an idle table — one that has received no
/// commits for an arbitrarily long time — must never have its live files
/// flagged as orphans. Liveness derives from the retained snapshots'
/// manifests, never from snapshot or manifest age.
#[tokio::test]
async fn idle_table_live_files_are_never_orphan_candidates() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // 10 live data files in one partition.
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 10,
        rows_per_file: 50,
        base_timestamp: Utc::now().timestamp_millis() - (48 * 60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Day,
    };
    generators::generate_traces(&mut writer, &config).await?;

    // The catalog writes table data through its own object store — inject
    // the orphans into and scan *that* store, so the detector sees the real
    // data files alongside the orphans.
    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);
    let table_store = match ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await?
    {
        Tabular::Table(t) => t.object_store(),
        _ => anyhow::bail!("expected a table"),
    };

    // 3 genuine orphans next to them.
    let table_path = format!("{}/{}/{}", tenant_id, dataset_id, table_name);
    let orphan_data = bytes::Bytes::from(vec![0u8; 1024]);
    for i in 0..3 {
        let path = format!("{}/data/orphan-{}.parquet", table_path, i);
        table_store
            .put(&ObjectPath::from(path.as_str()), orphan_data.clone().into())
            .await?;
    }

    // The table receives no further commits past this point (idle).
    let detector_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };
    let detector = OrphanDetector::new(detector_config, ctx.catalog_manager().clone(), table_store);

    let orphans = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;

    // Only the injected orphans may be flagged — never the live files.
    assert_eq!(
        orphans.len(),
        3,
        "live files of an idle table were flagged as orphans: {:?}",
        orphans.iter().map(|o| &o.path).collect::<Vec<_>>()
    );
    Ok(())
}

/// Regression test for #925 (time-travel safety): data files replaced by
/// compaction are still referenced by earlier snapshots. Until those
/// snapshots are expired, the files must not be orphan candidates —
/// deleting them would break time travel and queries pinned to a
/// retained snapshot.
#[tokio::test]
async fn files_replaced_by_compaction_stay_protected_while_snapshots_retained() -> Result<()> {
    use compactor::executor::{CompactionExecutor, ExecutorConfig};
    use compactor::planner::{CompactionPlanner, PlannerConfig};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    // The planner iterates configured tenants, so the tenant must exist.
    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant("test-tenant", "test-dataset")
        .build();
    let catalog_manager = Arc::new(common::catalog_manager::CatalogManager::new(config).await?);
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = writer::IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await?;

    // 5 separate appends -> 5 small files, 5 snapshots.
    let data_config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 100,
        base_timestamp: Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    for _ in 0..5 {
        generators::generate_traces(&mut writer, &data_config).await?;
    }

    // Compact: the current snapshot now references one rewritten file,
    // while the 5 input files remain referenced by the earlier snapshots.
    let planner_config = PlannerConfig {
        file_count_threshold: 3,
        max_input_file_size_bytes: 64 * 1024 * 1024,
        target_file_size_bytes: 128 * 1024 * 1024,
        // Tests seed data into recent hours and compact it immediately;
        // a production lateness allowance would defer every such partition.
        partition_lateness: std::time::Duration::ZERO,
        max_partition_input_bytes: 0,
    };
    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config.clone());
    let candidates = planner.plan().await?;
    assert!(
        !candidates.is_empty(),
        "table must be a compaction candidate"
    );
    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::from(&planner_config),
        compactor::metrics::CompactionMetrics::new(),
    );
    executor
        .execute_candidate(candidates.into_iter().next().unwrap())
        .await?;

    // Scan the store the catalog actually wrote through, so the old and
    // new data files are both visible to the detector.
    let table_identifier =
        catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    let (table_store, pre_compaction_files, mut table) = match catalog_manager
        .catalog()
        .load_tabular(&table_identifier)
        .await?
    {
        Tabular::Table(t) => {
            // The manifests of the retained pre-compaction snapshots still
            // reference the 5 replaced input files.
            let files: std::collections::HashSet<String> = {
                let manifests = compactor::iceberg::ManifestReader::new()
                    .collect_retained_manifests(&t)
                    .await?;
                compactor::iceberg::ManifestReader::new()
                    .read_live_files(&t, &manifests)
                    .await?
            };
            (t.object_store(), files, t)
        }
        _ => anyhow::bail!("expected a table"),
    };

    // Guard against a vacuous scan: the detector lists
    // {tenant}/{dataset}/{table}/data/ on this store — the 5 replaced input
    // files plus the compacted output must actually be there.
    let data_prefix = ObjectPath::from(format!("{tenant_id}/{dataset_id}/{table_name}/data/"));
    let scanned: Vec<_> = table_store
        .list(Some(&data_prefix))
        .try_collect::<Vec<_>>()
        .await?;
    assert!(
        scanned.len() > 1,
        "expected the replaced input files and the compacted output under the data prefix, found {}",
        scanned.len()
    );

    let detector_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };
    let detector = OrphanDetector::new(detector_config, catalog_manager, table_store);

    let orphans = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;

    // No snapshot has been expired, so every file on disk is still
    // referenced by some retained snapshot: nothing may be flagged.
    assert!(
        orphans.is_empty(),
        "files referenced by retained snapshots were flagged as orphans: {:?}",
        orphans.iter().map(|o| &o.path).collect::<Vec<_>>()
    );

    // Expire everything but the current snapshot: the replaced input files
    // lose their last references and MUST become orphan candidates — this
    // is the reclamation half of the contract, without which cleanup would
    // silently reclaim nothing forever.
    table
        .new_transaction(None)
        .expire_snapshots(None, Some(1), false, true, false)
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("snapshot expiration failed: {e}"))?;

    let orphans_after_expiry = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await?;
    let flagged: std::collections::HashSet<String> = orphans_after_expiry
        .iter()
        .map(|o| o.path.clone())
        .collect();

    let current_live = detector
        .live_file_set_for_table(tenant_id, dataset_id, table_name)
        .await?;
    let expected: std::collections::HashSet<String> = pre_compaction_files
        .difference(&current_live)
        .cloned()
        .collect();
    assert!(
        !expected.is_empty(),
        "compaction must have replaced at least one input file"
    );
    assert_eq!(
        flagged, expected,
        "after expiration, exactly the replaced input files must be flagged"
    );
    Ok(())
}

/// Pre-deletion revalidation must skip candidates that are (still or again)
/// referenced by a retained snapshot and delete genuine orphans — fed a
/// mixed candidate list directly, so the check is exercised independently of
/// detection.
#[tokio::test]
async fn revalidation_skips_live_files_and_deletes_orphans() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 2,
        rows_per_file: 20,
        base_timestamp: Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    generators::generate_traces(&mut writer, &config).await?;

    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);
    let (table_store, live_file) = match ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await?
    {
        Tabular::Table(t) => {
            let live = compactor::iceberg::ManifestReader::new()
                .get_snapshot_files(&t)
                .await?
                .first()
                .map(|f| f.file_path.clone())
                .expect("table has live files");
            (t.object_store(), live)
        }
        _ => anyhow::bail!("expected a table"),
    };

    let orphan_path = format!("{tenant_id}/{dataset_id}/{table_name}/data/genuine-orphan.parquet");
    table_store
        .put(
            &ObjectPath::from(orphan_path.as_str()),
            bytes::Bytes::from(vec![0u8; 256]).into(),
        )
        .await?;

    let cleanup_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };
    let detector = Arc::new(OrphanDetector::new(
        cleanup_config.clone(),
        ctx.catalog_manager().clone(),
        table_store.clone(),
    ));
    let cleaner =
        OrphanCleaner::with_detector(cleanup_config, table_store.clone(), detector.clone());

    // Feed a candidate list containing a file that IS referenced by the
    // current snapshot alongside a genuine orphan: revalidation must sort
    // them apart.
    let make_candidate = |path: &str| compactor::orphan::OrphanCandidate {
        path: path.to_string(),
        size_bytes: 256,
        last_modified: Utc::now() - chrono::Duration::hours(48),
        table_identifier: format!("{tenant_id}/{dataset_id}/{table_name}"),
    };
    let result = cleaner
        .delete_orphans_batch(vec![
            make_candidate(&live_file),
            make_candidate(&orphan_path),
        ])
        .await?;

    assert_eq!(
        result.deleted_count, 1,
        "only the genuine orphan is deleted"
    );
    assert!(
        table_store
            .get(&ObjectPath::from(live_file.as_str()))
            .await
            .is_ok(),
        "live file must survive revalidation"
    );
    assert!(
        table_store
            .get(&ObjectPath::from(orphan_path.as_str()))
            .await
            .is_err(),
        "orphan must be deleted"
    );
    Ok(())
}

/// Metadata reclamation (#935/#959): unreferenced metadata files — old
/// metadata.json versions that fell out of the metadata-log and
/// manifest/manifest-list avro of expired snapshots — must be flagged,
/// while everything the table still references (current metadata.json,
/// metadata-log entries, retained snapshots' manifest lists, manifests,
/// version-hint.text) must never be.
#[tokio::test]
async fn metadata_orphans_are_reclaimed_but_live_metadata_is_protected() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // A few commits so the metadata directory has real, referenced files.
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 20,
        base_timestamp: Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    for _ in 0..3 {
        generators::generate_traces(&mut writer, &config).await?;
    }

    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);
    let table_store = match ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await?
    {
        Tabular::Table(t) => t.object_store(),
        _ => anyhow::bail!("expected a table"),
    };

    // Inject unreferenced metadata files: a metadata.json version that fell
    // out of the metadata-log and an expired snapshot's manifest list.
    let metadata_dir = format!("{tenant_id}/{dataset_id}/{table_name}/metadata");
    let orphan_metadata =
        format!("{metadata_dir}/00000-dead0000-dead-dead-dead-deaddead0000.metadata.json");
    let orphan_avro = format!("{metadata_dir}/snap-4242424242424242424-0-dead0000.avro");
    let junk = bytes::Bytes::from(vec![0u8; 256]);
    for path in [&orphan_metadata, &orphan_avro] {
        table_store
            .put(&ObjectPath::from(path.as_str()), junk.clone().into())
            .await?;
    }

    let detector_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };
    let detector = OrphanDetector::new(detector_config, ctx.catalog_manager().clone(), table_store);

    let orphans = detector
        .identify_orphan_metadata_candidates(tenant_id, dataset_id, table_name)
        .await?;

    let found: std::collections::HashSet<_> = orphans.iter().map(|o| o.path.as_str()).collect();
    assert_eq!(
        found,
        [orphan_metadata.as_str(), orphan_avro.as_str()]
            .into_iter()
            .collect(),
        "exactly the injected unreferenced metadata files must be flagged"
    );
    Ok(())
}

/// The version hint names a *version*, not a file: the same version is
/// `<hint>.metadata.json` uncompressed and `<hint>.gz.metadata.json` when the
/// table sets `write.metadata.compression-codec = gzip`. Reconstructing only
/// the uncompressed name would leave a gzipped current metadata.json outside
/// the live set — and orphan cleanup deletes for real by default, so that is
/// the live metadata pointer gone.
#[tokio::test]
async fn a_gzipped_current_metadata_file_is_never_flagged() -> Result<()> {
    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 20,
        base_timestamp: Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    generators::generate_traces(&mut writer, &config).await?;

    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);
    let table_store = match ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await?
    {
        Tabular::Table(t) => t.object_store(),
        _ => anyhow::bail!("expected a table"),
    };

    let metadata_dir = format!("{tenant_id}/{dataset_id}/{table_name}/metadata");

    // Stand in for a gzip-configured table: point the version hint at a
    // version that exists only as a `.gz.metadata.json`.
    let version = "00007-abcd0000-abcd-abcd-abcd-abcdabcd0000";
    let gzipped_current = format!("{metadata_dir}/{version}.gz.metadata.json");
    // A higher-numbered unreferenced file, so the detector's "never flag the
    // newest version seen" fallback protects that one instead. Without it the
    // fallback would shield the gzipped file regardless of the hint, and this
    // test would pass whether or not the hint is handled correctly.
    let newer_orphan =
        format!("{metadata_dir}/00009-dead0000-dead-dead-dead-deaddead0000.metadata.json");
    for path in [&gzipped_current, &newer_orphan] {
        table_store
            .put(
                &ObjectPath::from(path.as_str()),
                bytes::Bytes::from(vec![0u8; 256]).into(),
            )
            .await?;
    }
    table_store
        .put(
            &ObjectPath::from(format!("{metadata_dir}/version-hint.text").as_str()),
            bytes::Bytes::from(version.as_bytes().to_vec()).into(),
        )
        .await?;

    let detector_config = OrphanCleanupConfig {
        enabled: true,
        grace_period_hours: 0,
        batch_size: 100,
        dry_run: false,
        cleanup_interval_hours: 24,
        max_live_files_threshold: 500_000,
    };
    let detector = OrphanDetector::new(detector_config, ctx.catalog_manager().clone(), table_store);

    let orphans = detector
        .identify_orphan_metadata_candidates(tenant_id, dataset_id, table_name)
        .await?;

    assert!(
        !orphans.iter().any(|o| o.path == gzipped_current),
        "the gzipped current metadata.json must not be flagged, got {:?}",
        orphans.iter().map(|o| &o.path).collect::<Vec<_>>()
    );
    Ok(())
}
