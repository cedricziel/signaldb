//! Retention Enforcement Engine
//!
//! This module implements the core retention enforcement logic that identifies
//! and drops expired partitions, expires old snapshots, and coordinates with
//! the orphan cleanup system.
//!
//! ## Safety Guarantees
//!
//! - Grace period prevents premature deletion
//! - Dry-run mode for testing without actual deletion
//! - Comprehensive logging and metrics for auditing
//! - Transactional partition drops via Iceberg

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tracing::{info, warn};

use common::CatalogManager;

use crate::iceberg::{ManifestReader, PartitionInfo, PartitionManager, SnapshotManager};
use crate::retention::config::RetentionConfig;
use crate::retention::policy::RetentionPolicyResolver;

use super::config::SignalType;
use super::metrics::RetentionMetrics;

/// Result of a retention enforcement run for a single table
#[derive(Debug, Clone)]
pub struct TableRetentionResult {
    pub tenant_id: String,
    pub dataset_id: String,
    pub table_name: String,
    pub signal_type: SignalType,
    pub partitions_evaluated: usize,
    pub partitions_dropped: usize,
    pub snapshots_expired: usize,
    pub bytes_reclaimed: u64,
    pub duration_ms: u64,
    pub errors: Vec<String>,
}

/// Result of a complete retention enforcement run across all tables
#[derive(Debug, Clone)]
pub struct RetentionRunResult {
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub tables_processed: usize,
    pub total_partitions_dropped: usize,
    pub total_snapshots_expired: usize,
    pub total_bytes_reclaimed: u64,
    pub table_results: Vec<TableRetentionResult>,
    pub errors: Vec<String>,
}

/// Retention Enforcement Engine
///
/// Coordinates partition dropping and snapshot expiration according to
/// configured retention policies.
pub struct RetentionEnforcer {
    catalog_manager: Arc<CatalogManager>,
    policy_resolver: RetentionPolicyResolver,
    partition_manager: PartitionManager,
    snapshot_manager: SnapshotManager,
    #[allow(dead_code)] // Will be used in orphan cleanup phase
    manifest_reader: ManifestReader,
    metrics: RetentionMetrics,
    config: RetentionConfig,
}

impl RetentionEnforcer {
    /// Create a new retention enforcer
    pub fn new(
        catalog_manager: Arc<CatalogManager>,
        config: RetentionConfig,
        metrics: RetentionMetrics,
    ) -> Result<Self> {
        let policy_resolver = RetentionPolicyResolver::new(config.clone())
            .context("Failed to create retention policy resolver")?;

        Ok(Self {
            catalog_manager,
            policy_resolver,
            partition_manager: PartitionManager::new(),
            snapshot_manager: SnapshotManager::new(),
            manifest_reader: ManifestReader::new(),
            metrics,
            config,
        })
    }

    /// Run retention enforcement for all tables in a tenant/dataset
    pub async fn enforce_retention(
        &self,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<RetentionRunResult> {
        let run_id = format!("retention_{}", Utc::now().timestamp_millis());
        let started_at = Utc::now();

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            run_id = %run_id,
            dry_run = self.config.dry_run,
            "Starting retention enforcement run"
        );

        let mut table_results = vec![];
        let mut errors = vec![];

        // Get all tables for this tenant/dataset
        let tables = self
            .get_tables(tenant_id, dataset_id)
            .await
            .context("Failed to list tables")?;

        for (table_name, signal_type) in tables {
            match self
                .enforce_table_retention(tenant_id, dataset_id, &table_name, signal_type)
                .await
            {
                Ok(result) => {
                    info!(
                        tenant_id = %tenant_id,
                        dataset_id = %dataset_id,
                        table_name = %table_name,
                        partitions_dropped = result.partitions_dropped,
                        snapshots_expired = result.snapshots_expired,
                        "Table retention enforcement completed"
                    );
                    table_results.push(result);
                }
                Err(e) => {
                    let error_msg =
                        format!("Failed to enforce retention on table {}: {}", table_name, e);
                    warn!(
                        tenant_id = %tenant_id,
                        dataset_id = %dataset_id,
                        table_name = %table_name,
                        error = %e,
                        "Table retention enforcement failed"
                    );
                    errors.push(error_msg);
                }
            }
        }

        let completed_at = Utc::now();
        let total_partitions_dropped: usize =
            table_results.iter().map(|r| r.partitions_dropped).sum();
        let total_snapshots_expired: usize =
            table_results.iter().map(|r| r.snapshots_expired).sum();
        let total_bytes_reclaimed: u64 = table_results.iter().map(|r| r.bytes_reclaimed).sum();

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            run_id = %run_id,
            tables_processed = table_results.len(),
            total_partitions_dropped,
            total_snapshots_expired,
            total_bytes_reclaimed,
            duration_ms = (completed_at - started_at).num_milliseconds(),
            "Retention enforcement run completed"
        );

        Ok(RetentionRunResult {
            run_id,
            started_at,
            completed_at,
            tables_processed: table_results.len(),
            total_partitions_dropped,
            total_snapshots_expired,
            total_bytes_reclaimed,
            table_results,
            errors,
        })
    }

    /// Enforce retention for a single table
    async fn enforce_table_retention(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        signal_type: SignalType,
    ) -> Result<TableRetentionResult> {
        let started_at = Utc::now();

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            signal_type = %signal_type,
            "Starting table retention enforcement"
        );

        // Compute retention cutoff for this table
        let cutoff = self
            .policy_resolver
            .compute_cutoff(tenant_id, dataset_id, signal_type)
            .context("Failed to compute retention cutoff")?;

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            signal_type = %signal_type,
            cutoff_timestamp = %cutoff.cutoff_timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
            retention_period = ?cutoff.retention_period,
            source = ?cutoff.source,
            "Retention cutoff computed"
        );

        self.metrics.record_cutoff_computed();

        // Get table from catalog
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);

        let catalog = self.catalog_manager.catalog();
        let tabular = catalog
            .load_tabular(&table_identifier)
            .await
            .with_context(|| format!("Failed to load table {}", table_name))?;

        let table = match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => {
                anyhow::bail!("Expected table but got view for {table_name}");
            }
        };

        // Step 1: Drop expired partitions
        let (partitions_evaluated, partitions_dropped, bytes_reclaimed) = self
            .drop_expired_partitions(tenant_id, dataset_id, table_name, &table, &cutoff)
            .await
            .context("Failed to drop expired partitions")?;

        // Step 2: Expire old snapshots (keep N most recent)
        let snapshots_expired = self
            .expire_old_snapshots(tenant_id, dataset_id, table_name, &table)
            .await
            .context("Failed to expire old snapshots")?;

        let completed_at = Utc::now();
        let duration_ms = (completed_at - started_at).num_milliseconds() as u64;

        // Update metrics
        self.metrics.record_duration_ms(duration_ms);
        if bytes_reclaimed > 0 {
            self.metrics.record_bytes_reclaimed(bytes_reclaimed);
        }

        Ok(TableRetentionResult {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_name: table_name.to_string(),
            signal_type,
            partitions_evaluated,
            partitions_dropped,
            snapshots_expired,
            bytes_reclaimed,
            duration_ms,
            errors: vec![],
        })
    }

    /// Drop expired partitions based on retention cutoff
    ///
    /// Returns (partitions_evaluated, partitions_dropped, bytes_reclaimed)
    async fn drop_expired_partitions(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        table: &iceberg_rust::table::Table,
        cutoff: &super::policy::RetentionCutoff,
    ) -> Result<(usize, usize, u64)> {
        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            cutoff_timestamp = %cutoff.cutoff_timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
            "Identifying expired partitions"
        );

        // List all partitions in the table
        let all_partitions = self
            .partition_manager
            .list_partitions(table)
            .await
            .context("Failed to list partitions")?;

        let total_partitions = all_partitions.len();

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            total_partitions,
            "Listed all partitions"
        );

        self.metrics.record_partitions_evaluated(total_partitions);

        // Filter partitions older than cutoff
        let expired_partitions = self
            .partition_manager
            .filter_partitions_older_than(all_partitions.clone(), &cutoff.cutoff_timestamp);

        if expired_partitions.is_empty() {
            info!(
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                table_name = %table_name,
                "No expired partitions found"
            );
            return Ok((total_partitions, 0, 0));
        }

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            expired_count = expired_partitions.len(),
            "Found expired partitions"
        );

        if self.config.dry_run {
            // Calculate bytes that would be reclaimed in dry-run
            // Compute this for all expired partitions since none will fail in dry-run
            let bytes_to_reclaim: u64 = expired_partitions
                .iter()
                .filter_map(|p| p.total_size_bytes)
                .sum();

            info!(
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                table_name = %table_name,
                partitions_to_drop = expired_partitions.len(),
                bytes_to_reclaim,
                "[DRY RUN] Would drop expired partitions"
            );

            for partition in &expired_partitions {
                info!(
                    tenant_id = %tenant_id,
                    dataset_id = %dataset_id,
                    table_name = %table_name,
                    partition_hour = ?partition.get_hour_value(),
                    file_count = partition.file_count,
                    size_bytes = partition.total_size_bytes,
                    "[DRY RUN] Would drop partition"
                );
            }

            return Ok((
                all_partitions.len(),
                expired_partitions.len(),
                bytes_to_reclaim,
            ));
        }

        // Actually drop partitions
        let ctx = self
            .create_datafusion_context(tenant_id, dataset_id)
            .await?;

        let mut dropped_count = 0;
        let mut bytes_reclaimed = 0u64;

        for partition in &expired_partitions {
            match self
                .drop_partition(&ctx, table_name, partition)
                .await
                .with_context(|| {
                    format!("Failed to drop partition {:?}", partition.get_hour_value())
                }) {
                Ok(_) => {
                    info!(
                        tenant_id = %tenant_id,
                        dataset_id = %dataset_id,
                        table_name = %table_name,
                        partition_hour = ?partition.get_hour_value(),
                        file_count = partition.file_count,
                        size_bytes = partition.total_size_bytes,
                        "Partition dropped successfully"
                    );

                    dropped_count += 1;

                    // Only accumulate bytes after successful drop
                    if let Some(size) = partition.total_size_bytes {
                        bytes_reclaimed += size;
                    }

                    self.metrics.record_partitions_dropped(1);
                }
                Err(e) => {
                    warn!(
                        tenant_id = %tenant_id,
                        dataset_id = %dataset_id,
                        table_name = %table_name,
                        partition_hour = ?partition.get_hour_value(),
                        error = %e,
                        "Failed to drop partition"
                    );
                }
            }
        }

        Ok((all_partitions.len(), dropped_count, bytes_reclaimed))
    }

    /// Drop a single partition using DataFusion
    async fn drop_partition(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        partition: &PartitionInfo,
    ) -> Result<()> {
        let hour_value = partition
            .get_hour_value()
            .ok_or_else(|| anyhow::anyhow!("Partition has no hour value"))?;

        let sql = self
            .partition_manager
            .generate_partition_drop_sql(table_name, hour_value)?;

        info!(
            table_name = %table_name,
            partition_hour = ?partition.get_hour_value(),
            sql = %sql,
            "Executing partition drop"
        );

        ctx.sql(&sql)
            .await
            .context("Failed to execute DROP PARTITION SQL")?
            .collect()
            .await
            .context("Failed to collect DROP PARTITION results")?;

        Ok(())
    }

    /// Expire old snapshots, keeping N most recent
    async fn expire_old_snapshots(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        table: &iceberg_rust::table::Table,
    ) -> Result<usize> {
        let snapshots_to_keep = self.config.snapshots_to_keep.unwrap_or(10);

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            snapshots_to_keep,
            "Checking snapshots for expiration"
        );

        let snapshots_to_expire = self
            .snapshot_manager
            .get_snapshots_to_expire(table, snapshots_to_keep)
            .context("Failed to get snapshots to expire")?;

        if snapshots_to_expire.is_empty() {
            info!(
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                table_name = %table_name,
                "No snapshots to expire"
            );
            return Ok(0);
        }

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            snapshots_to_expire_count = snapshots_to_expire.len(),
            "Found snapshots to expire"
        );

        if self.config.dry_run {
            info!(
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                table_name = %table_name,
                snapshots_to_expire_count = snapshots_to_expire.len(),
                "[DRY RUN] Would expire old snapshots"
            );

            for snapshot in &snapshots_to_expire {
                info!(
                    tenant_id = %tenant_id,
                    dataset_id = %dataset_id,
                    table_name = %table_name,
                    snapshot_id = snapshot.snapshot_id,
                    timestamp = %DateTime::<Utc>::from_timestamp(snapshot.timestamp_secs(), 0)
                        .unwrap_or_default()
                        .format("%Y-%m-%d %H:%M:%S UTC"),
                    "[DRY RUN] Would expire snapshot"
                );
            }

            return Ok(snapshots_to_expire.len());
        }

        // In a real implementation, we would call Iceberg's expire_snapshots API here
        // For now, we log a warning that this is not yet implemented
        warn!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            "Snapshot expiration not yet implemented in iceberg-rust"
        );

        // Do not record metrics for unimplemented functionality
        // self.metrics.record_snapshots_expired(...) will be called once implemented

        Ok(0) // Return 0 until actually implemented
    }

    /// Get all tables for a tenant/dataset with their signal types
    async fn get_tables(
        &self,
        _tenant_id: &str,
        _dataset_id: &str,
    ) -> Result<Vec<(String, SignalType)>> {
        // In a real implementation, we would list tables from the catalog
        // For now, we return the standard signal tables
        let tables = vec![
            ("traces".to_string(), SignalType::Traces),
            ("logs".to_string(), SignalType::Logs),
            ("metrics_gauge".to_string(), SignalType::Metrics),
            ("metrics_counter".to_string(), SignalType::Metrics),
            ("metrics_histogram".to_string(), SignalType::Metrics),
        ];

        Ok(tables)
    }

    /// Create a DataFusion context for executing SQL
    async fn create_datafusion_context(
        &self,
        _tenant_id: &str,
        _dataset_id: &str,
    ) -> Result<SessionContext> {
        // Create a new DataFusion session context
        // In a real implementation, we would configure it with the catalog
        let _ctx = SessionContext::new();

        // TODO: Register tables with the catalog
        anyhow::bail!(
            "SQL execution not available until tables are registered in DataFusion context"
        )
    }

    /// Get the policy resolver for testing
    #[cfg(test)]
    pub fn policy_resolver(&self) -> &RetentionPolicyResolver {
        &self.policy_resolver
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retention::config::RetentionConfig;
    use std::collections::HashMap;

    fn create_test_config() -> RetentionConfig {
        RetentionConfig {
            enabled: true,
            retention_check_interval: std::time::Duration::from_secs(3600),
            traces: std::time::Duration::from_secs(7 * 86400), // 7 days
            logs: std::time::Duration::from_secs(30 * 86400),  // 30 days
            metrics: std::time::Duration::from_secs(90 * 86400), // 90 days
            tenant_overrides: HashMap::new(),
            grace_period: std::time::Duration::from_secs(3600), // 1 hour
            timezone: "UTC".to_string(),
            dry_run: true,
            snapshots_to_keep: Some(10),
        }
    }

    #[tokio::test]
    async fn test_enforcer_creation() {
        let config = create_test_config();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let metrics = RetentionMetrics::new_mock();

        let enforcer = RetentionEnforcer::new(catalog_manager, config, metrics);
        assert!(enforcer.is_ok());
    }

    #[tokio::test]
    async fn test_dry_run_mode() {
        let mut config = create_test_config();
        config.dry_run = true;

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let metrics = RetentionMetrics::new_mock();

        let enforcer = RetentionEnforcer::new(catalog_manager, config, metrics).unwrap();
        assert!(enforcer.config.dry_run);
    }

    #[tokio::test]
    async fn test_get_tables() {
        let config = create_test_config();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let metrics = RetentionMetrics::new_mock();

        let enforcer = RetentionEnforcer::new(catalog_manager, config, metrics).unwrap();

        let tables = enforcer
            .get_tables("test_tenant", "test_dataset")
            .await
            .unwrap();
        assert_eq!(tables.len(), 5);
        assert!(tables.iter().any(|(name, _)| name == "traces"));
        assert!(tables.iter().any(|(name, _)| name == "logs"));
    }
}
