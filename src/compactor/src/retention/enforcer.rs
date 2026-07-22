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
//! - Transactional partition drops via Iceberg: one CAS-guarded and
//!   post-verified `replace` commit removes every data file in the
//!   expired partitions (the same model the compaction executor uses);
//!   physical file deletion stays with the orphan cleaner
//! - Snapshot expiration is metadata-only (`RemoveSnapshots`); the
//!   orphan cleaner remains the sole deletion authority

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use iceberg_rust::spec::manifest::Status;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

use common::CatalogManager;

use crate::commit::{IcebergCommitter, is_conflict_error};
use crate::iceberg::{ManifestReader, PartitionManager, SnapshotManager};
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

        // Step 2: Expire old snapshots (keep N most recent). Loads the
        // table fresh internally — step 1 may have advanced the snapshot.
        let snapshots_expired = self
            .expire_old_snapshots(tenant_id, dataset_id, table_name)
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

        // Actually drop partitions: one replace commit removes every data
        // file in the expired partitions. Retried on CAS conflicts with
        // concurrent compaction/ingest commits.
        let expired_hours: HashSet<String> = expired_partitions
            .iter()
            .filter_map(|p| p.partition_values.get("timestamp_hour").cloned())
            .collect();

        const MAX_ATTEMPTS: usize = 3;
        let mut attempt = 0;
        let (dropped_partitions, dropped_files, bytes_reclaimed) = loop {
            attempt += 1;
            match self
                .try_drop_partitions_once(tenant_id, dataset_id, table_name, &expired_hours)
                .await
            {
                Ok(result) => break result,
                Err(e) if is_conflict_error(&e) && attempt < MAX_ATTEMPTS => {
                    warn!(
                        tenant_id = %tenant_id,
                        dataset_id = %dataset_id,
                        table_name = %table_name,
                        attempt,
                        error = %e,
                        "Partition drop hit a snapshot conflict; retrying against fresh metadata"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(200 * attempt as u64))
                        .await;
                }
                Err(e) => return Err(e).context("Failed to commit partition drop"),
            }
        };

        if dropped_partitions > 0 {
            info!(
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                table_name = %table_name,
                dropped_partitions,
                dropped_files,
                bytes_reclaimed,
                "Dropped expired partitions"
            );
            self.metrics.record_partitions_dropped(dropped_partitions);
        }

        Ok((all_partitions.len(), dropped_partitions, bytes_reclaimed))
    }

    /// One attempt at dropping the expired partitions: load the table
    /// fresh, split the live data files into kept vs expired by their
    /// `timestamp_hour=<N>` partition value, and commit a CAS-guarded,
    /// post-verified `replace` with only the kept files. Physical file
    /// deletion is left to the orphan cleaner.
    ///
    /// Returns (partitions_dropped, files_dropped, bytes_reclaimed).
    async fn try_drop_partitions_once(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        expired_hours: &HashSet<String>,
    ) -> Result<(usize, usize, u64)> {
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);
        let tabular = self
            .catalog_manager
            .catalog()
            .load_tabular(&table_identifier)
            .await
            .with_context(|| format!("Failed to load table {table_name} for partition drop"))?;
        let table = match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => anyhow::bail!("Expected table but got view for {table_name}"),
        };
        let original_snapshot_id = table.metadata().current_snapshot_id;

        let manifests = table
            .manifests(None, None)
            .await
            .context("Failed to read manifest list for partition drop")?;
        if manifests.is_empty() {
            return Ok((0, 0, 0));
        }
        let file_iter = table
            .datafiles(&manifests, None, (None, None))
            .await
            .context("Failed to read data files for partition drop")?;

        let mut kept_files = Vec::new();
        let mut dropped_hours: HashSet<String> = HashSet::new();
        let mut dropped_files = 0usize;
        let mut dropped_bytes = 0u64;

        let mut file_iter = std::pin::pin!(file_iter);
        while let Some(result) = file_iter.next().await {
            let (_, entry) = result.context("Failed to read manifest entry")?;
            if *entry.status() == Status::Deleted {
                continue;
            }
            let data_file = entry.data_file();
            let partition_hour = data_file
                .file_path()
                .split('/')
                .find(|component| component.starts_with("timestamp_hour="))
                .and_then(|component| component.strip_prefix("timestamp_hour="));

            match partition_hour {
                Some(hour) if expired_hours.contains(hour) => {
                    dropped_hours.insert(hour.to_string());
                    dropped_files += 1;
                    dropped_bytes += *data_file.file_size_in_bytes() as u64;
                    debug!(
                        file_path = %data_file.file_path(),
                        partition_hour = %hour,
                        "Dropping expired data file"
                    );
                }
                _ => kept_files.push(data_file.clone()),
            }
        }

        if dropped_files == 0 {
            // Nothing left to drop (e.g. a concurrent compaction already
            // rewrote the expired partitions away).
            return Ok((0, 0, 0));
        }

        let committer = IcebergCommitter::new(self.catalog_manager.clone());
        committer
            .commit_compaction(
                tenant_id,
                dataset_id,
                table_name,
                original_snapshot_id,
                kept_files,
            )
            .await
            .context("Failed to commit partition-drop replace snapshot")?;

        Ok((dropped_hours.len(), dropped_files, dropped_bytes))
    }

    /// Expire old snapshots, keeping N most recent.
    ///
    /// Loads the table fresh (the partition-drop step may have advanced
    /// the snapshot) and commits a metadata-only `RemoveSnapshots` update.
    /// Data files referenced only by expired snapshots become orphans and
    /// are reclaimed by the orphan cleaner after its grace period — that
    /// grace window is also what protects in-flight queries, since
    /// queriers do not pin snapshots.
    async fn expire_old_snapshots(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<usize> {
        let snapshots_to_keep = self.config.snapshots_to_keep.unwrap_or(10);

        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);
        let tabular = self
            .catalog_manager
            .catalog()
            .load_tabular(&table_identifier)
            .await
            .with_context(|| format!("Failed to load table {table_name} for snapshot expiry"))?;
        let mut table = match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => anyhow::bail!("Expected table but got view for {table_name}"),
        };

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            snapshots_to_keep,
            "Checking snapshots for expiration"
        );

        let snapshots_to_expire = self
            .snapshot_manager
            .get_snapshots_to_expire(&table, snapshots_to_keep)
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

        // Metadata-only expiration: a RemoveSnapshots update through the
        // catalog CAS. retain_ref_snapshots keeps branch/tag-referenced
        // snapshots; the current snapshot is never expired by iceberg-rust.
        // clean_orphan_files is deliberately false — physical reclamation
        // is the orphan cleaner's job (and the flag is a no-op in this
        // iceberg-rust revision anyway).
        let expired_count = snapshots_to_expire.len();
        table
            .new_transaction(None)
            .expire_snapshots(None, Some(snapshots_to_keep), false, true, false)
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit snapshot expiration: {e}"))?;

        info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            snapshots_expired = expired_count,
            snapshots_to_keep,
            "Expired old snapshots"
        );
        self.metrics.record_snapshots_expired(expired_count);

        Ok(expired_count)
    }

    /// Get all signal tables for a tenant/dataset by listing the catalog
    /// namespace, so retention only touches tables that actually exist.
    async fn get_tables(
        &self,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<Vec<(String, SignalType)>> {
        let namespace = self
            .catalog_manager
            .build_namespace(tenant_id, dataset_id)
            .context("Failed to build namespace for table listing")?;
        let identifiers = self
            .catalog_manager
            .catalog()
            .list_tabulars(&namespace)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list tables in {namespace:?}: {e}"))?;

        let mut tables: Vec<(String, SignalType)> = identifiers
            .iter()
            .filter_map(|identifier| {
                let name = identifier.name();
                let signal_type = match name {
                    "traces" => SignalType::Traces,
                    "logs" => SignalType::Logs,
                    n if n.starts_with("metrics") => SignalType::Metrics,
                    _ => return None,
                };
                Some((name.to_string(), signal_type))
            })
            .collect();
        tables.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(tables)
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
    async fn get_tables_lists_only_existing_tables() {
        let config = create_test_config();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let metrics = RetentionMetrics::new_mock();

        let enforcer = RetentionEnforcer::new(catalog_manager.clone(), config, metrics).unwrap();

        // Nothing in the catalog: no phantom tables to enforce on.
        let tables = enforcer
            .get_tables("test_tenant", "test_dataset")
            .await
            .unwrap();
        assert!(
            tables.is_empty(),
            "Empty catalog must yield no tables, got {tables:?}"
        );

        // A created table shows up with its signal type.
        catalog_manager
            .ensure_table("test_tenant", "test_dataset", "traces")
            .await
            .unwrap();
        let tables = enforcer
            .get_tables("test_tenant", "test_dataset")
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].0, "traces");
        assert_eq!(tables[0].1, SignalType::Traces);
    }
}
