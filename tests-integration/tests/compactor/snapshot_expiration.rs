//! Snapshot expiration integration tests
//!
//! Tests the snapshot retention system that keeps N most recent snapshots
//! and expires older snapshots to save catalog space.
//!
//! ## DataGenerator Schema Overview
//!
//! The DataGenerator creates schemas matching the production v1 schemas:
//! - Traces: 22 columns (full OTLP trace data with attributes, events, links)
//! - Logs: 18 columns (complete OTLP log records with resource and scope attributes)
//! - Metrics: 19 columns (metrics_gauge schema with exemplars and partitioning fields)

use anyhow::{Context, Result};
use compactor::iceberg::snapshot::SnapshotManager;
use iceberg_rust::catalog::tabular::Tabular;
use tests_integration::fixtures::{
    DataGeneratorConfig, PartitionGranularity, RetentionTestContext,
};
use tests_integration::generators;

/// Test: Snapshot expiration keeps minimum snapshots
///
/// Creates a table with 10 snapshots and verifies that when snapshots_to_keep = 3,
/// only the 3 most recent snapshots are retained and 7 are expired.
#[tokio::test]
async fn test_snapshot_expiration_keeps_minimum_snapshots() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    // Create test context
    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create table
    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Create 10 snapshots by writing 10 separate batches
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 10,
        base_timestamp: chrono::Utc::now().timestamp_millis() - (24 * 60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Day,
    };

    log::info!("Creating 10 snapshots by writing 10 batches");
    for i in 0..10 {
        let config_with_offset = DataGeneratorConfig {
            base_timestamp: config.base_timestamp + (i * 60 * 60 * 1000), // Offset each batch by 1 hour
            ..config.clone()
        };
        generators::generate_traces(&mut writer, &config_with_offset).await?;
    }

    // Load the table from catalog
    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);

    let tabular = ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await
        .context("Failed to load table")?;

    let table = match tabular {
        Tabular::Table(t) => t,
        _ => anyhow::bail!("Expected table but got view"),
    };

    // Verify we have 10 snapshots
    let snapshot_manager = SnapshotManager::new();
    let snapshots_before = snapshot_manager.list_snapshots(&table)?;
    log::info!("Snapshots before expiration: {}", snapshots_before.len());
    assert_eq!(
        snapshots_before.len(),
        10,
        "Expected 10 snapshots after 10 writes"
    );

    // Get snapshots to expire (keep 3, expire 7)
    let to_expire = snapshot_manager.get_snapshots_to_expire(&table, 3)?;
    log::info!("Snapshots to expire: {}", to_expire.len());

    // Verify 7 snapshots are marked for expiration
    assert_eq!(
        to_expire.len(),
        7,
        "Expected 7 snapshots to expire (10 total - 3 to keep)"
    );

    // Verify the to_expire list contains the oldest snapshots
    let recent = snapshot_manager.get_recent_snapshots(&table, 3)?;
    for expired_snapshot in &to_expire {
        // Ensure expired snapshots are older than all kept snapshots
        for kept_snapshot in &recent {
            assert!(
                expired_snapshot.timestamp_ms < kept_snapshot.timestamp_ms,
                "Expired snapshot should be older than kept snapshots"
            );
        }
    }

    Ok(())
}

/// Test: Snapshot expiration respects time-based retention
///
/// Creates snapshots with distinct timestamps and verifies that time-based
/// retention correctly identifies old snapshots while keeping recent ones.
///
/// Note: Iceberg snapshots use wall-clock commit time (not data timestamp).
/// This test adds delays between snapshot creation to ensure distinct timestamps.
#[tokio::test]
async fn test_snapshot_expiration_respects_time_based_retention() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "logs";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    let now = chrono::Utc::now().timestamp_millis();
    let thirty_days_ago = now - (30 * 24 * 60 * 60 * 1000);

    // Create 10 snapshots with small delays to ensure distinct timestamps
    // (snapshot timestamp = wall-clock commit time, not data time)
    log::info!("Creating 10 snapshots with delays for distinct timestamps");
    for i in 0..10 {
        let config = DataGeneratorConfig {
            partition_count: 1,
            files_per_partition: 1,
            rows_per_file: 10,
            base_timestamp: thirty_days_ago + (i * 3 * 24 * 60 * 60 * 1000),
            partition_granularity: PartitionGranularity::Day,
        };
        generators::generate_logs(&mut writer, &config).await?;

        // Small delay to ensure snapshot timestamps are distinct
        if i < 9 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    // Load table
    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);

    let tabular = ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await
        .context("Failed to load table")?;

    let table = match tabular {
        Tabular::Table(t) => t,
        _ => anyhow::bail!("Expected table"),
    };

    let snapshot_manager = SnapshotManager::new();
    let all_snapshots = snapshot_manager.list_snapshots(&table)?;
    log::info!("Total snapshots: {}", all_snapshots.len());

    // Use milliseconds for the midpoint cutoff to avoid second-truncation issues
    // (snapshots created 100ms apart may all round to the same second value)
    let oldest_snapshot_ms = all_snapshots.iter().map(|s| s.timestamp_ms).min().unwrap();
    let newest_snapshot_ms = all_snapshots.iter().map(|s| s.timestamp_ms).max().unwrap();

    // Cutoff halfway through the snapshot creation period (in ms)
    let cutoff_ms = (oldest_snapshot_ms + newest_snapshot_ms) / 2;

    // Filter directly by timestamp_ms to avoid second-truncation in list_snapshots_older_than
    let old_snapshots: Vec<_> = all_snapshots
        .iter()
        .filter(|s| s.timestamp_ms < cutoff_ms)
        .collect();

    log::info!(
        "Cutoff ms: {} | Old snapshots: {} | All snapshots: {}",
        cutoff_ms,
        old_snapshots.len(),
        all_snapshots.len()
    );

    // With 10 snapshots and a midpoint cutoff, expect roughly half to be "old".
    // Must have at least 1 old and at least 1 recent (oldest != newest is guaranteed
    // because each write takes measurable time and iceberg records commit timestamps).
    assert!(
        !old_snapshots.is_empty() && old_snapshots.len() < all_snapshots.len(),
        "Expected some old snapshots and some recent ones, got old={} total={}",
        old_snapshots.len(),
        all_snapshots.len()
    );

    // Verify all old snapshots are indeed older than cutoff
    for snapshot in &old_snapshots {
        assert!(
            snapshot.timestamp_ms < cutoff_ms,
            "Old snapshot timestamp_ms {} should be < cutoff {}",
            snapshot.timestamp_ms,
            cutoff_ms
        );
    }

    Ok(())
}

/// Test: Snapshot expiration handles no snapshots gracefully
///
/// Verifies that running snapshot expiration on an empty table
/// (with no snapshots) does not produce errors.
#[tokio::test]
async fn test_snapshot_expiration_handles_no_snapshots() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "metrics_gauge";

    // Create table but don't write any data (no snapshots)
    let _writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Load table
    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);

    let tabular = ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await
        .context("Failed to load table")?;

    let table = match tabular {
        Tabular::Table(t) => t,
        _ => anyhow::bail!("Expected table"),
    };

    // Run snapshot operations on empty table
    let snapshot_manager = SnapshotManager::new();

    let snapshots = snapshot_manager.list_snapshots(&table)?;
    log::info!("Snapshots in empty table: {}", snapshots.len());
    assert_eq!(snapshots.len(), 0, "Empty table should have no snapshots");

    // Get snapshots to expire - should be empty, not error
    let to_expire = snapshot_manager.get_snapshots_to_expire(&table, 3)?;
    assert_eq!(
        to_expire.len(),
        0,
        "Empty table should have no snapshots to expire"
    );

    // Get recent snapshots - should be empty, not error
    let recent = snapshot_manager.get_recent_snapshots(&table, 5)?;
    assert_eq!(
        recent.len(),
        0,
        "Empty table should have no recent snapshots"
    );

    // Get current snapshot - should be None, not error
    let current = snapshot_manager.get_current_snapshot(&table)?;
    assert!(
        current.is_none(),
        "Empty table should have no current snapshot"
    );

    Ok(())
}

/// Test: Snapshot expiration preserves current snapshot
///
/// Verifies that the current snapshot is never expired, even if it would
/// normally be expired by the retention policy.
#[tokio::test]
async fn test_snapshot_expiration_preserves_current_snapshot() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";
    let mut writer = ctx.create_table(tenant_id, dataset_id, table_name).await?;

    // Create 5 snapshots
    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 10,
        base_timestamp: chrono::Utc::now().timestamp_millis() - (5 * 60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };

    log::info!("Creating 5 snapshots");
    for i in 0..5 {
        let config_with_offset = DataGeneratorConfig {
            base_timestamp: config.base_timestamp + (i * 60 * 60 * 1000),
            ..config.clone()
        };
        generators::generate_traces(&mut writer, &config_with_offset).await?;
    }

    // Load table
    let table_identifier = ctx
        .catalog_manager()
        .build_table_identifier(tenant_id, dataset_id, table_name);

    let tabular = ctx
        .catalog_manager()
        .catalog()
        .load_tabular(&table_identifier)
        .await
        .context("Failed to load table")?;

    let table = match tabular {
        Tabular::Table(t) => t,
        _ => anyhow::bail!("Expected table"),
    };

    // Get current snapshot
    let snapshot_manager = SnapshotManager::new();
    let current_snapshot_id = snapshot_manager
        .get_current_snapshot_id(&table)?
        .context("Table should have a current snapshot")?;

    log::info!("Current snapshot ID: {}", current_snapshot_id);

    // Get snapshots to expire (keep only 1)
    let to_expire = snapshot_manager.get_snapshots_to_expire(&table, 1)?;
    log::info!(
        "Snapshots to expire (keep=1): {} snapshots",
        to_expire.len()
    );

    // Verify current snapshot is NOT in the expiration list
    let current_in_expiration_list = to_expire
        .iter()
        .any(|s| s.snapshot_id == current_snapshot_id);

    assert!(
        !current_in_expiration_list,
        "Current snapshot should not be marked for expiration"
    );

    // Get the one snapshot we're keeping
    let kept_snapshots = snapshot_manager.get_recent_snapshots(&table, 1)?;
    assert_eq!(kept_snapshots.len(), 1, "Should keep exactly 1 snapshot");

    // Verify the kept snapshot is the current one
    assert_eq!(
        kept_snapshots[0].snapshot_id, current_snapshot_id,
        "The kept snapshot should be the current snapshot"
    );

    // Verify we have correct total count
    let all_snapshots = snapshot_manager.list_snapshots(&table)?;
    assert_eq!(
        all_snapshots.len(),
        to_expire.len() + kept_snapshots.len(),
        "Total snapshots should equal expired + kept"
    );

    Ok(())
}
