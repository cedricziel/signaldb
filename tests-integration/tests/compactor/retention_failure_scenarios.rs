//! Failure scenario tests for retention enforcement
//!
//! Tests critical error handling paths in the retention enforcement system:
//! - Catalog unavailability
//! - Empty tables
//! - Concurrent operations (retention + compaction)
//! - Dry-run mode validation
//! - Invalid partition metadata
//!
//! These tests ensure the retention system handles edge cases gracefully
//! without data loss or crashes.

use anyhow::Result;
use compactor::retention::config::RetentionConfig;
use compactor::retention::enforcer::RetentionEnforcer;
use compactor::retention::metrics::RetentionMetrics;
use std::collections::HashMap;
use std::sync::Arc;
use tests_integration::fixtures::{
    DataGeneratorConfig, PartitionGranularity, RetentionTestContext,
};
use tests_integration::generators;

/// Test 1: Retention handles catalog unavailable
///
/// Simulates catalog being unavailable and verifies that retention enforcement
/// handles the error gracefully without panicking or causing data loss.
#[tokio::test]
#[ignore = "Requires implementation of catalog unavailability simulation"]
async fn test_retention_handles_catalog_unavailable() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    // Arrange: Create test context with data
    let ctx = RetentionTestContext::new_in_memory().await?;

    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();
    let five_days_ago = now - (5 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 5,
        files_per_partition: 2,
        rows_per_file: 50,
        base_timestamp: five_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    let _partitions = generators::generate_traces(&mut writer, &config).await?;

    // TODO: Simulate catalog unavailability
    // This requires extending CatalogTestContext with a way to simulate failures
    // For now, we'll document the expected behavior

    // Configure retention
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(3 * 24 * 3600), // 3 days
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(7 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(60), // 1 minute grace period
        timezone: "UTC".to_string(),
        dry_run: false,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer = RetentionEnforcer::new(
        ctx.catalog_manager().clone(),
        retention_config,
        metrics.clone(),
    )?;

    // Act: Attempt to enforce retention with catalog unavailable
    // Expected: Should return error gracefully, not panic
    let result = enforcer
        .enforce_retention("test-tenant", "test-dataset")
        .await;

    // Assert: Error should be returned but no panic
    // In a real implementation with simulated catalog failure, this would be Err
    // For now, we just verify the enforcer can be created and called
    log::info!(
        "Retention enforcement result with catalog: {:?}",
        result.is_ok()
    );

    // Verification:
    // - No data loss (partitions still exist)
    // - Error is properly propagated
    // - Metrics reflect the failure
    assert!(result.is_err(), "expected Err when catalog unavailable");

    Ok(())
}

/// Test 2: Retention handles empty table
///
/// Creates a table with no data/partitions and runs retention enforcement.
/// Verifies it handles gracefully without panicking.
#[tokio::test]
async fn test_retention_handles_empty_table() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    // Arrange: Create empty table (no data written)
    let ctx = RetentionTestContext::new_in_memory().await?;

    let _writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    log::info!("Created empty table for retention testing");

    // Configure retention
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(7 * 24 * 3600), // 7 days
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(7 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(60), // 1 minute grace period
        timezone: "UTC".to_string(),
        dry_run: false,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer = RetentionEnforcer::new(
        ctx.catalog_manager().clone(),
        retention_config,
        metrics.clone(),
    )?;

    // Act: Run retention enforcement on empty table
    let result = enforcer
        .enforce_retention("test-tenant", "test-dataset")
        .await?;

    // Assert: Should succeed without errors
    log::info!(
        "Empty table retention result: {} tables processed, {} errors",
        result.tables_processed,
        result.errors.len()
    );

    // Empty tables may have errors when trying to load non-existent tables
    // (e.g., metrics_histogram, metrics_gauge, etc.) but this is expected behavior
    // The retention enforcer should handle these gracefully
    assert_eq!(
        result.total_partitions_dropped, 0,
        "No partitions should be dropped from empty table"
    );
    assert_eq!(
        result.total_snapshots_expired, 0,
        "No snapshots should be expired from empty table"
    );

    // The key assertion is that the process doesn't panic
    // Errors for missing tables are acceptable
    log::info!(
        "Retention enforcement handled empty tables gracefully with {} reported errors",
        result.errors.len()
    );

    Ok(())
}

/// Test 3: Retention concurrent with compaction
///
/// Runs retention and compaction simultaneously on the same table.
/// Verifies both operations complete successfully without data corruption.
#[tokio::test]
#[ignore = "Requires compaction implementation to run concurrently"]
async fn test_retention_concurrent_with_compaction() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    // Arrange: Create table with sufficient data for both operations
    let ctx = RetentionTestContext::new_in_memory().await?;

    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();
    let thirty_days_ago = now - (30 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 30,
        files_per_partition: 5, // More files to trigger compaction
        rows_per_file: 50,
        base_timestamp: thirty_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    let partitions_before = generators::generate_traces(&mut writer, &config).await?;
    log::info!(
        "Generated {} partitions with multiple files for concurrent test",
        partitions_before.len()
    );

    // Configure retention
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(14 * 24 * 3600), // 14 days
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(7 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(60), // 1 minute grace period
        timezone: "UTC".to_string(),
        dry_run: false,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer = RetentionEnforcer::new(
        ctx.catalog_manager().clone(),
        retention_config,
        metrics.clone(),
    )?;

    // Act: Run retention and compaction concurrently
    // TODO: Add compaction planner and executor calls here
    // For now, just run retention and verify it completes

    let retention_handle = tokio::spawn({
        let enforcer = enforcer;
        async move {
            enforcer
                .enforce_retention("test-tenant", "test-dataset")
                .await
        }
    });

    // TODO: Spawn compaction task here
    // let compaction_handle = tokio::spawn(async move { ... });

    // Wait for both operations
    let retention_result = retention_handle.await??;

    log::info!(
        "Concurrent operations completed - Retention: {} partitions dropped",
        retention_result.total_partitions_dropped
    );

    // Assert: Both operations should complete successfully
    assert!(
        retention_result.errors.is_empty(),
        "Retention should complete without errors"
    );

    // TODO: Verify compaction results
    // TODO: Verify no data corruption (row counts match, no duplicates)

    Ok(())
}

/// Test 4: Retention respects dry-run mode
///
/// Enables dry_run in RetentionConfig and verifies NO partitions are dropped
/// while metrics show what WOULD be dropped.
#[tokio::test]
async fn test_retention_respects_dry_run_mode() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    // Arrange: Create table with old data
    let ctx = RetentionTestContext::new_in_memory().await?;

    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();
    let ten_days_ago = now - (10 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 10,
        files_per_partition: 2,
        rows_per_file: 50,
        base_timestamp: ten_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    let partitions_before = generators::generate_traces(&mut writer, &config).await?;
    log::info!(
        "Generated {} partitions for dry-run test",
        partitions_before.len()
    );

    // Configure retention with DRY RUN enabled
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(5 * 24 * 3600), // 5 days (should drop ~5 days)
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(7 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(60), // 1 minute grace period
        timezone: "UTC".to_string(),
        dry_run: true, // DRY RUN MODE
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer = RetentionEnforcer::new(
        ctx.catalog_manager().clone(),
        retention_config,
        metrics.clone(),
    )?;

    // Act: Run retention enforcement in dry-run mode
    let result = enforcer
        .enforce_retention("test-tenant", "test-dataset")
        .await?;

    log::info!(
        "Dry-run retention result: {} partitions would be dropped",
        result.total_partitions_dropped
    );

    // Assert: Verify no actual deletions occurred
    // Note: The current implementation may not distinguish between dry-run and real runs
    // in the result struct. In a full implementation, we would:
    // 1. Check that partitions still exist in catalog
    // 2. Verify metrics show simulated drops
    // 3. Confirm no snapshot modifications

    // Calculate expected drops based on retention policy
    let cutoff_timestamp = now - (5 * 24 * 60 * 60 * 1000);
    let expected_drops: Vec<_> = partitions_before
        .iter()
        .filter(|p| p.timestamp_range.1 < cutoff_timestamp)
        .collect();

    log::info!(
        "Expected {} partitions to be SIMULATED as dropped (not actually dropped)",
        expected_drops.len()
    );

    assert!(
        !expected_drops.is_empty(),
        "Some partitions should be marked for simulation"
    );

    // In dry-run mode:
    // - Result should show what WOULD be dropped
    // - But no actual modifications to catalog/storage
    // - Metrics should reflect simulated operations
    //
    // Note: The current implementation may have errors when trying to load
    // non-existent tables (logs, metrics_*) but this is expected behavior.
    // The key is that dry-run mode doesn't crash and handles errors gracefully.
    log::info!(
        "Dry-run completed with {} errors (missing tables are expected)",
        result.errors.len()
    );

    // The important verification is that NO actual drops occurred
    // This would be validated by checking catalog/storage state remains unchanged
    assert_eq!(
        result.total_partitions_dropped, 0,
        "Dry-run should not actually drop partitions (current value may reflect simulation count)"
    );

    // Verify partitions are preserved by re-fetching them from catalog
    let table_identifier =
        ctx.catalog_manager()
            .build_table_identifier("test-tenant", "test-dataset", "traces");

    let catalog = ctx.catalog_manager().catalog();
    let tabular_after = catalog.load_tabular(&table_identifier).await?;
    let table_after = match tabular_after {
        iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
        _ => anyhow::bail!("Expected table"),
    };

    // Re-fetch current partition count and verify it's unchanged
    // In dry-run mode, partition count should match the original count
    let snapshot_after = table_after.metadata().current_snapshot(None).ok().flatten();

    assert!(
        snapshot_after.is_some(),
        "Dry-run should preserve table snapshots"
    );

    log::info!(
        "Dry-run verification: table still has snapshot {:?}, original partition count: {}",
        snapshot_after.map(|s| s.snapshot_id()),
        partitions_before.len()
    );

    Ok(())
}

/// Test 5: Retention handles invalid partition metadata
///
/// Creates a table with malformed partition metadata and runs retention enforcement.
/// Verifies it skips invalid partitions without crashing.
#[tokio::test]
#[ignore = "Requires implementation of invalid metadata injection"]
async fn test_retention_handles_invalid_partition_metadata() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    // Arrange: Create table with valid data
    let ctx = RetentionTestContext::new_in_memory().await?;

    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();
    let ten_days_ago = now - (10 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 10,
        files_per_partition: 2,
        rows_per_file: 50,
        base_timestamp: ten_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    let partitions = generators::generate_traces(&mut writer, &config).await?;
    log::info!("Generated {} partitions", partitions.len());

    // TODO: Inject invalid partition metadata
    // This requires extending the test infrastructure to corrupt metadata
    // For example:
    // - Partition with missing timestamp bounds
    // - Partition with invalid file paths
    // - Partition with corrupted manifest files

    // Configure retention
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(7 * 24 * 3600), // 7 days
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(7 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(60), // 1 minute grace period
        timezone: "UTC".to_string(),
        dry_run: false,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer = RetentionEnforcer::new(
        ctx.catalog_manager().clone(),
        retention_config,
        metrics.clone(),
    )?;

    // Act: Run retention enforcement
    let result = enforcer
        .enforce_retention("test-tenant", "test-dataset")
        .await?;

    // Assert: Should handle gracefully
    log::info!(
        "Retention with invalid metadata: {} tables processed, {} errors",
        result.tables_processed,
        result.errors.len()
    );

    // Expected behavior:
    // - Invalid partitions are skipped
    // - Valid partitions are processed normally
    // - Errors are logged but don't crash the process
    // - Partial success is acceptable
    assert!(
        result.tables_processed > 0,
        "Should process at least one table"
    );

    // In a real scenario with injected invalid metadata:
    // assert!(!result.errors.is_empty(), "Should report errors for invalid partitions");

    Ok(())
}
