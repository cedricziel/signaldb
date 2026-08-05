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
use iceberg_rust::spec::manifest::{DataFile, Status};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

use common::CatalogManager;

use crate::commit::{IcebergCommitter, is_conflict_error};
use crate::iceberg::partition::{TIMESTAMP_HOUR_FIELD, data_file_partition_hours};
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
        use tracing::Instrument;

        let span = common::self_monitoring::spans::job_span(
            "retention_enforcement",
            tenant_id,
            dataset_id,
            None,
        );
        let record_span = span.clone();
        let result = self
            .enforce_retention_inner(tenant_id, dataset_id)
            .instrument(span)
            .await;
        if let Ok(run) = &result {
            record_span.record(
                "signaldb.job.partitions_dropped",
                run.total_partitions_dropped as i64,
            );
            record_span.record(
                "signaldb.job.snapshots_expired",
                run.total_snapshots_expired as i64,
            );
        }
        result
    }

    async fn enforce_retention_inner(
        &self,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<RetentionRunResult> {
        let run_id = format!("retention_{}", Utc::now().timestamp_millis());
        let started_at = Utc::now();

        info!(
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.job.run_id = %run_id,
            signaldb.job.dry_run = self.config.dry_run,
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
                        signaldb.tenant.id = %tenant_id,
                        signaldb.dataset.id = %dataset_id,
                        signaldb.table = %table_name,
                        signaldb.job.partitions_dropped = result.partitions_dropped,
                        signaldb.job.snapshots_expired = result.snapshots_expired,
                        "Table retention enforcement completed"
                    );
                    table_results.push(result);
                }
                Err(e) => {
                    let error_msg =
                        format!("Failed to enforce retention on table {}: {}", table_name, e);
                    warn!(
                        signaldb.tenant.id = %tenant_id,
                        signaldb.dataset.id = %dataset_id,
                        signaldb.table = %table_name,
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
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.job.run_id = %run_id,
            signaldb.job.tables_processed = table_results.len(),
            signaldb.job.partitions_dropped = total_partitions_dropped,
            signaldb.job.snapshots_expired = total_snapshots_expired,
            signaldb.job.bytes_reclaimed = total_bytes_reclaimed,
            signaldb.job.duration_ms = (completed_at - started_at).num_milliseconds(),
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

        debug!(
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

        debug!(
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
        debug!(
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

        debug!(
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
            debug!(
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                table_name = %table_name,
                "No expired partitions found"
            );
            return Ok((total_partitions, 0, 0));
        }

        debug!(
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
                signaldb.tenant.id = %tenant_id,
                signaldb.dataset.id = %dataset_id,
                signaldb.table = %table_name,
                signaldb.job.dry_run = true,
                signaldb.job.partitions_dropped = expired_partitions.len(),
                signaldb.job.bytes_reclaimed = bytes_to_reclaim,
                "[DRY RUN] Would drop expired partitions"
            );

            for partition in &expired_partitions {
                debug!(
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
        let expired_hours: HashSet<i64> = expired_partitions
            .iter()
            .filter_map(|p| p.partition_values.get(TIMESTAMP_HOUR_FIELD))
            .filter_map(|hours| hours.parse::<i64>().ok())
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
                        signaldb.tenant.id = %tenant_id,
                        signaldb.dataset.id = %dataset_id,
                        signaldb.table = %table_name,
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
                signaldb.tenant.id = %tenant_id,
                signaldb.dataset.id = %dataset_id,
                signaldb.table = %table_name,
                signaldb.job.partitions_dropped = dropped_partitions,
                signaldb.job.files_deleted = dropped_files,
                signaldb.job.bytes_reclaimed = bytes_reclaimed,
                "Dropped expired partitions"
            );
            self.metrics.record_partitions_dropped(dropped_partitions);
        }

        Ok((all_partitions.len(), dropped_partitions, bytes_reclaimed))
    }

    /// One attempt at dropping the expired partitions: load the table
    /// fresh, split the live data files into kept vs expired by the
    /// `timestamp_hour` partition value recorded in each manifest entry,
    /// and commit a CAS-guarded, post-verified `replace` with only the
    /// kept files. Files whose partition value cannot be determined are
    /// kept (safe default), logged, and counted in
    /// `compactor_unclassifiable_files_total`. Physical file deletion is
    /// left to the orphan cleaner.
    ///
    /// Returns (partitions_dropped, files_dropped, bytes_reclaimed).
    async fn try_drop_partitions_once(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        expired_hours: &HashSet<i64>,
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
        let mut dropped_hours: HashSet<i64> = HashSet::new();
        let mut dropped_files = 0usize;
        let mut dropped_bytes = 0u64;
        let mut unclassifiable_files = 0usize;

        let mut file_iter = std::pin::pin!(file_iter);
        while let Some(result) = file_iter.next().await {
            let (_, entry) = result.context("Failed to read manifest entry")?;
            if *entry.status() == Status::Deleted {
                continue;
            }
            let data_file = entry.data_file();

            match classify_data_file(data_file, expired_hours) {
                FileDisposition::Drop(hours) => {
                    dropped_hours.insert(hours);
                    dropped_files += 1;
                    dropped_bytes += *data_file.file_size_in_bytes() as u64;
                    debug!(
                        file_path = %data_file.file_path(),
                        partition_hour = hours,
                        "Dropping expired data file"
                    );
                }
                FileDisposition::Keep => kept_files.push(data_file.clone()),
                FileDisposition::KeepUnclassifiable => {
                    unclassifiable_files += 1;
                    warn!(
                        signaldb.tenant.id = %tenant_id,
                        signaldb.dataset.id = %dataset_id,
                        signaldb.table = %table_name,
                        file.path = %data_file.file_path(),
                        "Data file has no recoverable timestamp_hour partition value; \
                         keeping it and excluding it from retention"
                    );
                    kept_files.push(data_file.clone());
                }
            }
        }

        if unclassifiable_files > 0 {
            self.metrics
                .record_unclassifiable_files(unclassifiable_files);
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

        debug!(
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
            debug!(
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                table_name = %table_name,
                "No snapshots to expire"
            );
            return Ok(0);
        }

        debug!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            snapshots_to_expire_count = snapshots_to_expire.len(),
            "Found snapshots to expire"
        );

        if self.config.dry_run {
            info!(
                signaldb.tenant.id = %tenant_id,
                signaldb.dataset.id = %dataset_id,
                signaldb.table = %table_name,
                signaldb.job.dry_run = true,
                signaldb.job.snapshots_expired = snapshots_to_expire.len(),
                "[DRY RUN] Would expire old snapshots"
            );

            for snapshot in &snapshots_to_expire {
                debug!(
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
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.table = %table_name,
            signaldb.job.snapshots_expired = expired_count,
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

/// How a partition-drop pass treats a single data file.
#[derive(Debug, PartialEq, Eq)]
enum FileDisposition {
    /// The file belongs to an expired partition (hours since epoch) and is
    /// removed from the replace commit.
    Drop(i64),
    /// The file belongs to a live partition and is kept.
    Keep,
    /// The file's partition value could not be determined from the manifest
    /// entry or the file path. It is kept as the safe default and surfaced
    /// via a warning and `compactor_unclassifiable_files_total`.
    KeepUnclassifiable,
}

/// Classify a data file against the set of expired partition hours using
/// the manifest entry's partition value (see [`data_file_partition_hours`]).
fn classify_data_file(data_file: &DataFile, expired_hours: &HashSet<i64>) -> FileDisposition {
    match data_file_partition_hours(data_file) {
        Some(hours) if expired_hours.contains(&hours) => FileDisposition::Drop(hours),
        Some(_) => FileDisposition::Keep,
        None => FileDisposition::KeepUnclassifiable,
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

    /// Full drop-vs-dry-run coverage against real *expired* partitions lives
    /// in `tests-integration/compactor/retention_cutoff.rs`, which seeds
    /// tables with real partitioned data via the data generators. This test
    /// drives the same public `enforce_retention` entry point with
    /// `dry_run = true` against a real (if empty) catalog table, guarding
    /// that a dry run completes cleanly end to end and reports zero drops
    /// rather than reaching into the enforcer's private config field.
    #[tokio::test]
    async fn dry_run_enforcement_reports_no_drops_on_a_real_empty_table() {
        let mut config = create_test_config();
        config.dry_run = true;

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        catalog_manager
            .ensure_table("test_tenant", "test_dataset", "traces")
            .await
            .unwrap();
        let metrics = RetentionMetrics::new_mock();

        let enforcer = RetentionEnforcer::new(catalog_manager, config, metrics).unwrap();

        let result = enforcer
            .enforce_retention("test_tenant", "test_dataset")
            .await
            .unwrap();

        assert_eq!(result.table_results.len(), 1);
        let table_result = &result.table_results[0];
        assert_eq!(table_result.table_name, "traces");
        assert_eq!(table_result.partitions_dropped, 0);
        assert_eq!(table_result.snapshots_expired, 0);
        assert!(
            table_result.errors.is_empty(),
            "expected no errors, got {:?}",
            table_result.errors
        );
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

    #[test]
    fn classify_data_file_drops_expired_partitions_from_manifest_values() {
        use crate::iceberg::partition::test_support::{hour_partition, test_data_file};

        let expired: HashSet<i64> = [473_364].into_iter().collect();

        // Paths deliberately carry NO timestamp_hour= component: the manifest
        // entry's partition struct alone must classify the file.
        let expired_file = test_data_file(
            hour_partition(473_364),
            "s3://bucket/3af9c2/data/00000-0-expired.parquet",
        );
        assert_eq!(
            classify_data_file(&expired_file, &expired),
            FileDisposition::Drop(473_364)
        );

        let live_file = test_data_file(
            hour_partition(473_400),
            "s3://bucket/9c2e1b/data/00000-0-live.parquet",
        );
        assert_eq!(
            classify_data_file(&live_file, &expired),
            FileDisposition::Keep
        );
    }

    #[test]
    fn classify_data_file_keeps_unclassifiable_files() {
        use crate::iceberg::partition::test_support::test_data_file;
        use iceberg_rust::spec::values::{Struct, Value};

        // No partition value in the manifest AND no timestamp_hour= path
        // component: the file must be kept (safe default) and flagged.
        let empty_partition = Struct::from_iter(std::iter::empty::<(String, Option<Value>)>());
        let file = test_data_file(empty_partition, "s3://bucket/data/00000-0-mystery.parquet");
        let expired: HashSet<i64> = [473_364].into_iter().collect();

        assert_eq!(
            classify_data_file(&file, &expired),
            FileDisposition::KeepUnclassifiable
        );
    }

    /// Group 8 (`otel-compliant-self-tracing`): a retention run exports a
    /// root job span identifying tenant/dataset with affected-object counts.
    #[tokio::test]
    async fn retention_run_emits_job_span_with_counts() {
        use opentelemetry::trace::{SpanKind, TracerProvider as _};
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        async {
            let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
            let metrics = RetentionMetrics::new_mock();
            let enforcer =
                RetentionEnforcer::new(catalog_manager, create_test_config(), metrics).unwrap();
            let _ = enforcer.enforce_retention("acme", "prod").await.unwrap();
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();
        let span = spans
            .iter()
            .find(|s| s.name == "retention_enforcement")
            .unwrap_or_else(|| panic!("no retention job span; exported = {names:?}"));

        assert_eq!(span.span_kind, SpanKind::Internal);
        let attr = |key: &str| {
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().to_string())
        };
        assert_eq!(attr("signaldb.tenant.id").as_deref(), Some("acme"));
        assert_eq!(attr("signaldb.dataset.id").as_deref(), Some("prod"));
        assert_eq!(
            attr("signaldb.job.partitions_dropped").as_deref(),
            Some("0")
        );
        assert_eq!(attr("signaldb.job.snapshots_expired").as_deref(), Some("0"));
    }

    /// Log events inside the retention job span become OTel span events, and
    /// `weaver registry live-check` validates their attribute keys against
    /// otel/registry/. At the default `info` level every event field must be
    /// a registered `signaldb.*` attribute; `level`/`target` are stamped
    /// unconditionally by tracing-opentelemetry's event bridge and are
    /// whitelisted in .weaver.toml instead.
    #[tokio::test]
    async fn retention_run_span_events_use_registry_attribute_keys() {
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        use opentelemetry::trace::TracerProvider as _;
        let tracer = provider.tracer("test");
        // The production bridge layer (disables the code.* location
        // attributes), filtered to the production default level
        // (RUST_LOG=info): debug-level detail events are filtered out and
        // never become span events.
        let subscriber = tracing_subscriber::registry().with(
            common::self_monitoring::otel_span_layer(tracer)
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        );

        async {
            let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
            let metrics = RetentionMetrics::new_mock();
            let enforcer =
                RetentionEnforcer::new(catalog_manager, create_test_config(), metrics).unwrap();
            let _ = enforcer.enforce_retention("acme", "prod").await.unwrap();
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|s| s.name == "retention_enforcement")
            .expect("no retention job span");

        assert!(!span.events.is_empty(), "expected in-span log events");
        let offending: Vec<String> = span
            .events
            .iter()
            .flat_map(|e| e.attributes.iter())
            .map(|kv| kv.key.as_str().to_string())
            .filter(|k| !k.starts_with("signaldb.") && k != "level" && k != "target")
            .collect();
        assert!(
            offending.is_empty(),
            "span-event attribute keys missing from the signaldb registry namespace: {offending:?}"
        );
    }
}
