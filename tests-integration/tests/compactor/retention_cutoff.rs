//! Integration tests for retention cutoff computation
//!
//! Tests the calculation of retention cutoff timestamps based on various
//! retention policies including global, per-tenant, and per-dataset configurations.
//!
//! Every test drives `RetentionEnforcer::enforce_retention` (the real,
//! production entry point) and asserts on its reported result. None of these
//! tests recompute the cutoff or re-filter the generated partition list
//! themselves — that would let a real bug in cutoff computation or partition
//! filtering slip through undetected.

use anyhow::Result;
use common::catalog::Catalog;
use compactor::retention::config::{
    DatasetRetentionConfig, RetentionConfig, TenantRetentionConfig,
};
use compactor::retention::enforcer::RetentionEnforcer;
use compactor::retention::metrics::RetentionMetrics;
use std::collections::HashMap;
use std::sync::Arc;
use tests_integration::fixtures::{
    DataGeneratorConfig, PartitionGranularity, RetentionTestContext,
};
use tests_integration::generators;

/// Test 1.1: Basic retention cutoff calculation
///
/// Verifies that retention cutoff is computed correctly with a simple policy:
/// with 30 days of data and a 14-day retention window, some data is old
/// enough to be dropped and some is retained.
#[tokio::test]
async fn test_retention_cutoff_basic() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    // Create test context
    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create table with 30 days of data
    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();
    let thirty_days_ago = now - (30 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 30,
        files_per_partition: 2,
        rows_per_file: 50,
        base_timestamp: thirty_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    // Generate data
    let partitions = generators::generate_traces(&mut writer, &config).await?;
    tracing::info!("Generated {} logical day partitions", partitions.len());

    // Set retention policy to 14 days
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(14 * 24 * 3600), // 14 days
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(30 * 24 * 3600),
        profiles: std::time::Duration::from_secs(30 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(1), // Minimum 1 second for validation
        timezone: "UTC".to_string(),
        dry_run: true,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer =
        RetentionEnforcer::new(ctx.catalog_manager().clone(), retention_config, metrics)?;

    // Act: run retention enforcement through the real public API.
    let result = enforcer
        .enforce_retention("test-tenant", "test-dataset")
        .await?;

    tracing::info!(
        "Retention result: {} dropped, table_results={:?}",
        result.total_partitions_dropped,
        result.table_results
    );

    // Assert: exactly the "traces" table was processed.
    assert_eq!(
        result.table_results.len(),
        1,
        "Expected exactly one table (traces) to be processed"
    );
    let table_result = &result.table_results[0];
    assert_eq!(table_result.table_name, "traces");

    // With 30 days of data and 14-day retention, some (physical, hourly)
    // partitions must be old enough to be dropped...
    assert!(
        table_result.partitions_dropped > 0,
        "Expected at least one partition older than the 14-day cutoff to be dropped, got 0 of {}",
        table_result.partitions_evaluated
    );
    // ...and some (the most recent ~14 days) must be retained.
    assert!(
        table_result.partitions_dropped < table_result.partitions_evaluated,
        "Expected some partitions to be retained, but all {} were dropped",
        table_result.partitions_evaluated
    );

    // Top-level aggregate must match the sole table's contribution.
    assert_eq!(
        result.total_partitions_dropped, table_result.partitions_dropped,
        "Run-level total must equal the single table's dropped count"
    );

    Ok(())
}

/// Test 1.2: Per-tenant retention overrides
///
/// Verifies that tenant-specific retention policies override global defaults:
/// tenant-a gets a 30-day override, tenant-b uses the 7-day global default,
/// so with identical 40-day-old data tenant-b (shorter retention) must drop
/// strictly more partitions than tenant-a.
#[tokio::test]
async fn test_retention_per_tenant_override() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create tables for two tenants
    let mut writer_a = ctx.create_table("tenant-a", "production", "traces").await?;
    let mut writer_b = ctx.create_table("tenant-b", "production", "traces").await?;

    let now = chrono::Utc::now().timestamp_millis();
    let forty_days_ago = now - (40 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 40,
        files_per_partition: 1,
        rows_per_file: 50,
        base_timestamp: forty_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    // Generate identical data for both tenants
    generators::generate_traces(&mut writer_a, &config).await?;
    generators::generate_traces(&mut writer_b, &config).await?;

    tracing::info!("Generated 40 days of data for both tenants");

    // Create tenant override for tenant-a with 30-day retention
    let mut tenant_overrides = HashMap::new();
    tenant_overrides.insert(
        "tenant-a".to_string(),
        TenantRetentionConfig {
            traces: Some(std::time::Duration::from_secs(30 * 24 * 3600)), // 30 days
            logs: None,
            metrics: None,
            profiles: None,
            dataset_overrides: HashMap::new(),
        },
    );

    // Tenant B uses global defaults (7 days)
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(7 * 24 * 3600), // 7 days (global default)
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(30 * 24 * 3600),
        profiles: std::time::Duration::from_secs(30 * 24 * 3600),
        tenant_overrides,
        grace_period: std::time::Duration::from_secs(1), // Minimum 1 second for validation
        timezone: "UTC".to_string(),
        dry_run: true,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer = RetentionEnforcer::new(
        ctx.catalog_manager().clone(),
        retention_config,
        metrics.clone(),
    )?;

    // Act: enforce retention for both tenants through the real public API.
    let result_a = enforcer.enforce_retention("tenant-a", "production").await?;
    let result_b = enforcer.enforce_retention("tenant-b", "production").await?;

    tracing::info!(
        "Tenant A (30d override) dropped {}; Tenant B (7d default) dropped {}",
        result_a.total_partitions_dropped,
        result_b.total_partitions_dropped
    );

    assert_eq!(result_a.table_results.len(), 1);
    assert_eq!(result_b.table_results.len(), 1);
    let evaluated_a = result_a.table_results[0].partitions_evaluated;
    let evaluated_b = result_b.table_results[0].partitions_evaluated;

    // Tenant A (30-day override): some old data dropped, some retained.
    assert!(
        result_a.total_partitions_dropped > 0,
        "Expected tenant A to drop at least one partition older than 30 days"
    );
    assert!(
        result_a.total_partitions_dropped < evaluated_a,
        "Expected tenant A to retain at least one partition within 30 days"
    );

    // Tenant B (7-day global default): some old data dropped, some retained.
    assert!(
        result_b.total_partitions_dropped > 0,
        "Expected tenant B to drop at least one partition older than 7 days"
    );
    assert!(
        result_b.total_partitions_dropped < evaluated_b,
        "Expected tenant B to retain at least one partition within 7 days"
    );

    // Tenant B's shorter retention must drop strictly more partitions than
    // tenant A's override, given identical input data.
    assert!(
        result_b.total_partitions_dropped > result_a.total_partitions_dropped,
        "Tenant B (7-day retention) should drop more partitions than tenant A (30-day override): a={}, b={}",
        result_a.total_partitions_dropped,
        result_b.total_partitions_dropped
    );

    Ok(())
}

/// Test 1.3: Per-dataset retention overrides
///
/// Verifies that dataset-specific retention policies override tenant
/// defaults: production gets a 90-day override, staging gets a 3-day
/// override, so with identical 100-day-old data staging must drop strictly
/// more partitions than production.
#[tokio::test]
async fn test_retention_per_dataset_override() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create tables for different datasets
    let mut writer_prod = ctx.create_table("tenant-a", "production", "traces").await?;
    let mut writer_staging = ctx.create_table("tenant-a", "staging", "traces").await?;

    let now = chrono::Utc::now().timestamp_millis();
    let hundred_days_ago = now - (100 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 100,
        files_per_partition: 1,
        rows_per_file: 20,
        base_timestamp: hundred_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    // Generate identical data for both datasets
    generators::generate_traces(&mut writer_prod, &config).await?;
    generators::generate_traces(&mut writer_staging, &config).await?;

    tracing::info!("Generated 100 days of data for production and staging");

    // Create dataset-specific overrides
    let mut dataset_overrides = HashMap::new();
    dataset_overrides.insert(
        "production".to_string(),
        DatasetRetentionConfig {
            traces: Some(std::time::Duration::from_secs(90 * 24 * 3600)), // 90 days
            logs: None,
            metrics: None,
            profiles: None,
        },
    );
    dataset_overrides.insert(
        "staging".to_string(),
        DatasetRetentionConfig {
            traces: Some(std::time::Duration::from_secs(3 * 24 * 3600)), // 3 days
            logs: None,
            metrics: None,
            profiles: None,
        },
    );

    let mut tenant_overrides = HashMap::new();
    tenant_overrides.insert(
        "tenant-a".to_string(),
        TenantRetentionConfig {
            traces: None, // Use global default
            logs: None,
            metrics: None,
            profiles: None,
            dataset_overrides,
        },
    );

    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(30 * 24 * 3600), // 30 days (global default)
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(30 * 24 * 3600),
        profiles: std::time::Duration::from_secs(30 * 24 * 3600),
        tenant_overrides,
        grace_period: std::time::Duration::from_secs(1), // Minimum 1 second for validation
        timezone: "UTC".to_string(),
        dry_run: true,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer = RetentionEnforcer::new(
        ctx.catalog_manager().clone(),
        retention_config,
        metrics.clone(),
    )?;

    // Act: enforce retention for both datasets through the real public API.
    let result_prod = enforcer.enforce_retention("tenant-a", "production").await?;
    let result_staging = enforcer.enforce_retention("tenant-a", "staging").await?;

    tracing::info!(
        "Production (90d override) dropped {}; Staging (3d override) dropped {}",
        result_prod.total_partitions_dropped,
        result_staging.total_partitions_dropped
    );

    assert_eq!(result_prod.table_results.len(), 1);
    assert_eq!(result_staging.table_results.len(), 1);
    let evaluated_prod = result_prod.table_results[0].partitions_evaluated;
    let evaluated_staging = result_staging.table_results[0].partitions_evaluated;

    // Production (90-day override): some old data dropped, some retained.
    assert!(
        result_prod.total_partitions_dropped > 0,
        "Expected production to drop at least one partition older than 90 days"
    );
    assert!(
        result_prod.total_partitions_dropped < evaluated_prod,
        "Expected production to retain at least one partition within 90 days"
    );

    // Staging (3-day override): some old data dropped, some retained.
    assert!(
        result_staging.total_partitions_dropped > 0,
        "Expected staging to drop at least one partition older than 3 days"
    );
    assert!(
        result_staging.total_partitions_dropped < evaluated_staging,
        "Expected staging to retain at least one partition within 3 days"
    );

    // Staging's shorter retention must drop strictly more partitions than
    // production's, given identical input data.
    assert!(
        result_staging.total_partitions_dropped > result_prod.total_partitions_dropped,
        "Staging (3-day retention) should drop more partitions than production (90-day override): prod={}, staging={}",
        result_prod.total_partitions_dropped,
        result_staging.total_partitions_dropped
    );

    Ok(())
}

/// Test 1.4: Zero retention policy (validation test)
///
/// Verifies that zero-day retention is correctly rejected by validation.
/// This is a safety test ensuring invalid configurations cannot be created.
#[tokio::test]
async fn test_retention_zero_days() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create retention config with zero retention (invalid)
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(0), // Zero retention - INVALID
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(30 * 24 * 3600),
        profiles: std::time::Duration::from_secs(30 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(1),
        timezone: "UTC".to_string(),
        dry_run: true,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();

    // Attempt to create enforcer with invalid config
    let result = RetentionEnforcer::new(ctx.catalog_manager().clone(), retention_config, metrics);

    // Verify that creation fails with validation error
    match result {
        Err(err) => {
            let err_msg = format!("{:?}", err); // Use Debug format to see full error chain
            tracing::info!("Validation correctly rejected zero retention: {}", err_msg);

            // Check that error chain contains validation message
            assert!(
                err_msg.contains("Invalid retention period")
                    || err_msg.contains("must be positive")
                    || err_msg.contains("Configuration validation failed"),
                "Error message should indicate invalid retention period: {}",
                err_msg
            );
        }
        Ok(_) => {
            panic!("RetentionEnforcer should reject zero retention period");
        }
    }

    Ok(())
}

/// Test 1.5: Retention with clock skew
///
/// Verifies that future-dated partitions are not incorrectly dropped: with
/// data spanning from 5 days ago to 5 days in the future and a 3-day
/// retention window, only a minority of the timeline (roughly the oldest 2
/// of 10 days) is old enough to expire, so the real enforcer must drop
/// strictly fewer than half of the evaluated partitions.
#[tokio::test]
async fn test_retention_with_clock_skew() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();

    // Create data with some partitions in the future (clock skew scenario):
    // 10 day-wide logical partitions starting 5 days ago cover
    // [now - 5d, now + 5d).
    let five_days_ago = now - (5 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 10,
        files_per_partition: 1,
        rows_per_file: 50,
        base_timestamp: five_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    generators::generate_traces(&mut writer, &config).await?;
    tracing::info!("Generated 10 days of data (5 in the past, 5 in the future)");

    // Create retention config with 3-day retention
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(3 * 24 * 3600), // 3 days
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(30 * 24 * 3600),
        profiles: std::time::Duration::from_secs(30 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(1), // Minimum 1 second for validation
        timezone: "UTC".to_string(),
        dry_run: true,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer = RetentionEnforcer::new(
        ctx.catalog_manager().clone(),
        retention_config,
        metrics.clone(),
    )?;

    // Act: run retention enforcement through the real public API.
    let result = enforcer
        .enforce_retention("test-tenant", "test-dataset")
        .await?;

    tracing::info!(
        "Clock skew test: {} of {} evaluated partitions dropped",
        result.total_partitions_dropped,
        result
            .table_results
            .first()
            .map(|r| r.partitions_evaluated)
            .unwrap_or(0)
    );

    assert_eq!(result.table_results.len(), 1);
    let evaluated = result.table_results[0].partitions_evaluated;

    // Some data (the oldest slice, older than 3 days) should be expired.
    assert!(
        result.total_partitions_dropped > 0,
        "Expected at least one partition older than the 3-day cutoff to be dropped"
    );

    // The data timeline spans 10 days total; only the oldest ~2 days
    // (older than the 3-day cutoff) can be expired, so dropped partitions
    // must be a small minority. If future-dated partitions were being
    // dropped too, this fraction would blow well past half.
    assert!(
        result.total_partitions_dropped * 2 < evaluated,
        "Expected dropped partitions ({}) to be a minority of evaluated partitions ({}); \
         a higher fraction suggests future-dated partitions were incorrectly dropped",
        result.total_partitions_dropped,
        evaluated
    );

    Ok(())
}

/// Test 1.6: A database-sourced tenant's over-age data is selected under the
/// resolved retention policy (`unified-tenant-catalog-registry` tasks.md 5.3).
///
/// The compactor's retention cycle (`CompactorService::run_retention_cycle`)
/// enumerates tenants through `CatalogManager::list_active_tenants` — the
/// same source-agnostic registry the planner already proved reaches database
/// tenants (`plan_enumerates_database_tenants` in planner.rs). This test
/// closes the equivalent gap for retention: a tenant that exists ONLY in the
/// database (admin-API created, `source = "database"`, no `[[auth.tenants]]`
/// config entry) must be both *discoverable* through that same registry call
/// and have its over-age data *actually selected* when
/// `RetentionEnforcer::enforce_retention` runs for the tenant/dataset pair
/// the registry returned — exactly how the retention loop drives it.
#[tokio::test]
async fn test_retention_for_database_sourced_tenant() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    const DB_TENANT: &str = "gamma-tenant";
    const DB_DATASET: &str = "production";

    // A tenant that exists only in the database, with no config block.
    let source = Arc::new(Catalog::new_in_memory().await?);
    source
        .upsert_tenant(DB_TENANT, "Gamma Tenant", Some(DB_DATASET), "database")
        .await?;
    source.create_dataset(DB_TENANT, DB_DATASET).await?;

    let ctx = RetentionTestContext::new_in_memory_with_tenant_source(source).await?;

    let mut writer = ctx.create_table(DB_TENANT, DB_DATASET, "traces").await?;

    let now = chrono::Utc::now().timestamp_millis();
    let thirty_days_ago = now - (30 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 30,
        files_per_partition: 2,
        rows_per_file: 50,
        base_timestamp: thirty_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };
    generators::generate_traces(&mut writer, &config).await?;

    // The database tenant must be discoverable through the same registry
    // enumeration the retention loop drives off — not just resolvable when
    // addressed directly by ID.
    let active_tenants = ctx.catalog_manager().list_active_tenants().await?;
    let resolved = active_tenants
        .iter()
        .find(|t| t.id == DB_TENANT)
        .expect("database-sourced tenant must be enumerated by the registry");
    assert!(
        resolved.datasets.iter().any(|d| d.id == DB_DATASET),
        "expected the database tenant's dataset to be resolved: {:?}",
        resolved.datasets
    );

    // 14-day retention over 30 days of data: some must be dropped, some retained.
    let retention_config = RetentionConfig {
        enabled: true,
        retention_check_interval: std::time::Duration::from_secs(3600),
        traces: std::time::Duration::from_secs(14 * 24 * 3600),
        logs: std::time::Duration::from_secs(7 * 24 * 3600),
        metrics: std::time::Duration::from_secs(30 * 24 * 3600),
        profiles: std::time::Duration::from_secs(30 * 24 * 3600),
        tenant_overrides: HashMap::new(),
        grace_period: std::time::Duration::from_secs(1),
        timezone: "UTC".to_string(),
        dry_run: true,
        snapshots_to_keep: Some(10),
    };

    let metrics = RetentionMetrics::new();
    let enforcer =
        RetentionEnforcer::new(ctx.catalog_manager().clone(), retention_config, metrics)?;

    // Act: enforce retention exactly as the loop does — using the
    // tenant/dataset IDs the registry resolved, not hardcoded strings.
    let result = enforcer.enforce_retention(&resolved.id, DB_DATASET).await?;

    tracing::info!(
        "Database tenant retention: {} dropped of {} evaluated",
        result.total_partitions_dropped,
        result
            .table_results
            .first()
            .map(|r| r.partitions_evaluated)
            .unwrap_or(0)
    );

    assert_eq!(result.table_results.len(), 1);
    let table_result = &result.table_results[0];

    assert!(
        table_result.partitions_dropped > 0,
        "expected at least one partition older than the 14-day cutoff to be dropped for the \
         database tenant, got 0 of {}",
        table_result.partitions_evaluated
    );
    assert!(
        table_result.partitions_dropped < table_result.partitions_evaluated,
        "expected some partitions to be retained, but all {} were dropped",
        table_result.partitions_evaluated
    );

    Ok(())
}
