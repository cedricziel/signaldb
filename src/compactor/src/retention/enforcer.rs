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
use crate::iceberg::{PartitionManager, SnapshotManager};
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
    metrics: RetentionMetrics,
    config: RetentionConfig,
    /// Serializes this table's partition drops and snapshot expiration
    /// against compaction commits on the same table (D6). Defaults to a
    /// private registry via [`Self::new`]; [`Self::with_table_locks`] shares
    /// one registry with the compaction executor so the two actors actually
    /// gate each other — `CompactorService::new` does this for the
    /// long-running compactor process.
    table_locks: crate::table_lock::TableLockRegistry,
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
            metrics,
            config,
            table_locks: crate::table_lock::TableLockRegistry::new(),
        })
    }

    /// Share a [`crate::table_lock::TableLockRegistry`] with other lifecycle
    /// actors (compaction) so partition drops and snapshot expiration on a
    /// table serialize against them (D6). Without this call the enforcer
    /// gates only against itself.
    pub fn with_table_locks(mut self, table_locks: crate::table_lock::TableLockRegistry) -> Self {
        self.table_locks = table_locks;
        self
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
        let run_id = format!("retention_{}", uuid::Uuid::new_v4());
        let started_at = Utc::now();
        let run_clock = std::time::Instant::now();

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
                    if result.errors.is_empty() {
                        info!(
                            signaldb.tenant.id = %tenant_id,
                            signaldb.dataset.id = %dataset_id,
                            signaldb.table = %table_name,
                            signaldb.job.partitions_dropped = result.partitions_dropped as i64,
                            signaldb.job.snapshots_expired = result.snapshots_expired as i64,
                            "Table retention enforcement completed"
                        );
                    } else {
                        // Step 1 (partition drop) committed real work, but
                        // step 2 (snapshot expiry) failed — surface the
                        // failure without discarding step 1's counts (#1010).
                        for table_error in &result.errors {
                            warn!(
                                signaldb.tenant.id = %tenant_id,
                                signaldb.dataset.id = %dataset_id,
                                signaldb.table = %table_name,
                                signaldb.job.partitions_dropped = result.partitions_dropped as i64,
                                error = %table_error,
                                "Table retention enforcement completed with errors"
                            );
                            errors.push(format!("Table {table_name}: {table_error}"));
                        }
                    }
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
            signaldb.job.tables_processed = table_results.len() as i64,
            signaldb.job.partitions_dropped = total_partitions_dropped as i64,
            signaldb.job.snapshots_expired = total_snapshots_expired as i64,
            signaldb.job.bytes_reclaimed = total_bytes_reclaimed as i64,
            signaldb.job.duration_ms = run_clock.elapsed().as_millis() as i64,
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
        let table_clock = std::time::Instant::now();

        debug!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            signal_type = %signal_type,
            "Starting table retention enforcement"
        );

        // Serialize against compaction commits on this same table (D6): a
        // compaction job holds this same per-table lock across its retries
        // (see `CompactionExecutor::execute_candidate`), so a commit in
        // flight there and this drop+expire pass cannot interleave. Held
        // across both steps below, not just the commit calls, so the two
        // steps stay atomic relative to compaction as a whole.
        let _table_guard = self
            .table_locks
            .lock(tenant_id, dataset_id, table_name)
            .await;

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

        // Step 1: Drop expired partitions. A failure here has not mutated
        // anything worth preserving (either it never committed, or
        // `try_drop_partitions_once` already retried past transient
        // conflicts), so it still aborts the whole table via `?`.
        let (partitions_evaluated, partitions_dropped, bytes_reclaimed) = self
            .drop_expired_partitions(tenant_id, dataset_id, table_name, &table, &cutoff)
            .await
            .context("Failed to drop expired partitions")?;

        // Step 2: Expire old snapshots (keep N most recent). Loads the
        // table fresh internally — step 1 may have advanced the snapshot.
        //
        // Deliberately NOT `?`: step 1 may already have committed a real
        // partition-drop replace snapshot by this point, so a step-2 failure
        // must not discard those counts (#1010) — captured into
        // `build_table_retention_result` instead of propagated.
        let snapshot_expiry = self
            .expire_old_snapshots(tenant_id, dataset_id, table_name)
            .await;

        let duration_ms = table_clock.elapsed().as_millis() as u64;

        // Update metrics
        self.metrics.record_duration_ms(duration_ms);
        if bytes_reclaimed > 0 {
            self.metrics.record_bytes_reclaimed(bytes_reclaimed);
        }

        Ok(build_table_retention_result(
            tenant_id,
            dataset_id,
            table_name,
            signal_type,
            partitions_evaluated,
            partitions_dropped,
            bytes_reclaimed,
            duration_ms,
            snapshot_expiry,
        ))
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
                signaldb.job.partitions_dropped = expired_partitions.len() as i64,
                signaldb.job.bytes_reclaimed = bytes_to_reclaim as i64,
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
                        signaldb.job.attempt = attempt as i64,
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
                signaldb.job.partitions_dropped = dropped_partitions as i64,
                signaldb.job.files_deleted = dropped_files as i64,
                signaldb.job.bytes_reclaimed = bytes_reclaimed as i64,
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
                signaldb.job.snapshots_expired = snapshots_to_expire.len() as i64,
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
            signaldb.job.snapshots_expired = expired_count as i64,
            "Expired old snapshots"
        );
        self.metrics.record_snapshots_expired(expired_count);

        Ok(expired_count)
    }

    /// Get all signal tables for a tenant/dataset by listing the catalog
    /// namespace, so retention only touches tables that actually exist.
    ///
    /// Classification goes through [`SignalType::from_table_name`] — the one
    /// predicate every lifecycle job shares — rather than a local name
    /// allowlist that silently skipped whole signals (#1014).
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
                let signal_type = SignalType::from_table_name(name).ok()?;
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

/// Assemble a table's retention result from step 1's (partition-drop) counts
/// and step 2's (snapshot-expiration) outcome.
///
/// A step-2 failure does not discard step-1's already-committed work: the
/// counts it accumulated stay on the result, with the failure surfaced via
/// `errors` instead of propagated. Before this existed, `enforce_table_retention`
/// used `?` on step 2 directly, so a partition-drop commit that landed
/// followed by a snapshot-expiry failure zeroed out the run's reported counts
/// for real, already-mutated work (#1010).
#[allow(clippy::too_many_arguments)]
fn build_table_retention_result(
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
    signal_type: SignalType,
    partitions_evaluated: usize,
    partitions_dropped: usize,
    bytes_reclaimed: u64,
    duration_ms: u64,
    snapshot_expiry: Result<usize>,
) -> TableRetentionResult {
    let (snapshots_expired, errors) = match snapshot_expiry {
        Ok(count) => (count, Vec::new()),
        Err(e) => (0, vec![format!("Failed to expire old snapshots: {e}")]),
    };

    TableRetentionResult {
        tenant_id: tenant_id.to_string(),
        dataset_id: dataset_id.to_string(),
        table_name: table_name.to_string(),
        signal_type,
        partitions_evaluated,
        partitions_dropped,
        snapshots_expired,
        bytes_reclaimed,
        duration_ms,
        errors,
    }
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
    use crate::retention::config::{RetentionConfig, TenantRetentionConfig};
    use std::collections::HashMap;

    /// Load a table fresh from the catalog, panicking if it doesn't exist or
    /// resolves to a view. Shared by tests that seed data via a direct
    /// `Table::new_transaction` (rather than `enforce_retention`) and so need
    /// the loaded `Table` handle themselves.
    async fn load_table(
        catalog_manager: &CatalogManager,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> iceberg_rust::table::Table {
        let identifier = catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
        match catalog_manager.catalog().load_tabular(&identifier).await {
            Ok(iceberg_rust::catalog::tabular::Tabular::Table(t)) => t,
            Ok(_) => panic!("expected a table, got a view for {table_name}"),
            Err(e) => panic!("failed to load table {table_name}: {e}"),
        }
    }

    fn create_test_config() -> RetentionConfig {
        RetentionConfig {
            enabled: true,
            retention_check_interval: std::time::Duration::from_secs(3600),
            traces: std::time::Duration::from_secs(7 * 86400), // 7 days
            logs: std::time::Duration::from_secs(30 * 86400),  // 30 days
            metrics: std::time::Duration::from_secs(90 * 86400), // 90 days
            profiles: std::time::Duration::from_secs(14 * 86400), // 14 days
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

    /// D6: compaction commits and retention drops/snapshot expiration must
    /// not interleave on the same table. This drives the two real
    /// production entry points — `CompactionExecutor::execute_candidate`
    /// (the single method both the background compaction loop and the
    /// Flight `compact_now` action call, since they share one
    /// `Arc<CompactionExecutor>` — see `CompactorService`) and
    /// `RetentionEnforcer::enforce_retention` — against a `TableLockRegistry`
    /// shared exactly the way `CompactorService::new` wires it. Without that
    /// wiring (e.g. each actor keeping its own private default registry) the
    /// `!handle.is_finished()` assertions below would fail, because grabbing
    /// the lock from the externally-held `table_locks` handle would not
    /// block either real call at all.
    ///
    /// This is deliberately *not* wall-clock based (an earlier version used
    /// `timeout(200ms, ...)`/`timeout(5s, ...)` and was flaky under CI load
    /// — a busy machine can blow either bound without the guard being
    /// broken). Instead:
    ///
    /// - "did not proceed while the lock is held" is asserted via
    ///   `JoinHandle::is_finished()`, which needs no timing guess at all:
    ///   holding the guard makes it *impossible* for the other task to have
    ///   completed, so the assertion is deterministically true regardless of
    ///   machine speed. The `started` rendezvous plus a bounded
    ///   `yield_now()` loop only give the spawned task real scheduling turns
    ///   to run its non-contended work up to the point where it blocks on
    ///   the lock, so the assertion exercises the guard instead of an
    ///   unscheduled task — it does not gate correctness.
    /// - "proceeds once released" waits on the task's own join handle rather
    ///   than a guessed deadline; the `timeout` around it is a generous
    ///   deadlock backstop (fires only on a genuine hang), not a timing
    ///   assertion.
    #[tokio::test(flavor = "multi_thread")]
    async fn compaction_and_retention_do_not_interleave_on_the_same_table() {
        use crate::executor::{CompactionExecutor, ExecutorConfig};
        use crate::planner::{CompactionCandidate, PartitionStats};
        use crate::table_lock::TableLockRegistry;
        use tokio::sync::Notify;
        use tokio::time::{Duration as TokioDuration, timeout};

        // Deadlock backstop only — if this ever fires, the guard is
        // genuinely stuck, not merely slow, so a generous bound is fine.
        const DEADLOCK_BACKSTOP: TokioDuration = TokioDuration::from_secs(30);

        // How long a blocked task is given to prove it is blocked.
        //
        // This is deliberately a real wait, and it is safe in the direction
        // that matters: a task blocked on a held mutex can *never* complete,
        // so this assertion cannot fail spuriously no matter how loaded the
        // machine is. The bound only has to be long enough that an
        // *unguarded* run would finish inside it — both entry points here
        // work against an in-memory catalog and complete in milliseconds, so
        // two seconds is orders of magnitude of headroom.
        //
        // An earlier version yielded on the *test's* task and then checked
        // `JoinHandle::is_finished()`. That was vacuous: the spawned task
        // runs on another worker doing real catalog I/O, so it had not
        // finished yet either way, and the assertion held whether or not the
        // guard existed — it failed to catch the lock being commented out
        // entirely.
        const BLOCKED_PROOF: TokioDuration = TokioDuration::from_secs(2);

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        catalog_manager
            .ensure_table("acme", "prod", "traces")
            .await
            .unwrap();

        let table_locks = TableLockRegistry::new();

        let enforcer = RetentionEnforcer::new(
            catalog_manager.clone(),
            create_test_config(),
            RetentionMetrics::new_mock(),
        )
        .unwrap()
        .with_table_locks(table_locks.clone());

        let executor = CompactionExecutor::new(
            catalog_manager.clone(),
            ExecutorConfig::default(),
            crate::metrics::CompactionMetrics::new(),
        )
        .with_table_locks(table_locks.clone());

        // Direction 1: compaction is "mid-commit" on the table (standing in
        // for `execute_candidate` holding its guard — see
        // `CompactionExecutor::execute_candidate`). A real retention pass on
        // the same table must wait, then proceed once the guard is dropped.
        let held = table_locks.lock("acme", "prod", "traces").await;

        let started = Arc::new(Notify::new());
        let mut retention_pass = tokio::spawn({
            let enforcer = enforcer;
            let started = started.clone();
            async move {
                started.notify_one();
                enforcer.enforce_retention("acme", "prod").await
            }
        });

        timeout(DEADLOCK_BACKSTOP, started.notified())
            .await
            .expect("spawned retention task never started");

        assert!(
            timeout(BLOCKED_PROOF, &mut retention_pass).await.is_err(),
            "retention ran to completion while compaction held the table lock"
        );

        drop(held);

        timeout(DEADLOCK_BACKSTOP, &mut retention_pass)
            .await
            .expect("retention never proceeded after the table lock was released")
            .expect("task does not panic")
            .expect("retention enforcement succeeds");

        // Direction 2: retention is mid-drop/expire on the table. A real
        // `execute_candidate` call — the same method both compaction entry
        // points use — must wait, then proceed once released. The table has
        // no data files, so once unblocked the job completes immediately
        // with nothing to compact; only the ordering is under test here.
        let held = table_locks.lock("acme", "prod", "traces").await;

        let candidate = CompactionCandidate {
            tenant_id: "acme".to_string(),
            dataset_id: "prod".to_string(),
            table_name: "traces".to_string(),
            partition_id: "0".to_string(),
            stats: PartitionStats {
                file_count: 0,
                total_size_bytes: 0,
                avg_file_size_bytes: 0,
            },
        };

        let started = Arc::new(Notify::new());
        let mut compaction_pass = tokio::spawn({
            let started = started.clone();
            async move {
                started.notify_one();
                executor.execute_candidate(candidate).await
            }
        });

        timeout(DEADLOCK_BACKSTOP, started.notified())
            .await
            .expect("spawned compaction task never started");

        assert!(
            timeout(BLOCKED_PROOF, &mut compaction_pass).await.is_err(),
            "compaction ran to completion while retention held the table lock"
        );

        drop(held);

        timeout(DEADLOCK_BACKSTOP, &mut compaction_pass)
            .await
            .expect("compaction never proceeded after the table lock was released")
            .expect("task does not panic")
            .expect("compaction completes (no live files to compact)");
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

    /// #1014: `profiles` matched no arm of the old hardcoded filter, so it
    /// received neither retention nor snapshot expiration and grew an
    /// unbounded metadata backlog.
    #[tokio::test]
    async fn get_tables_covers_every_signal_table_including_profiles() {
        let config = create_test_config();
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let metrics = RetentionMetrics::new_mock();
        let enforcer = RetentionEnforcer::new(catalog_manager.clone(), config, metrics).unwrap();

        for table in ["traces", "logs", "metrics_gauge", "profiles"] {
            catalog_manager
                .ensure_table("test_tenant", "test_dataset", table)
                .await
                .unwrap();
        }

        let tables = enforcer
            .get_tables("test_tenant", "test_dataset")
            .await
            .unwrap();

        assert_eq!(
            tables,
            vec![
                ("logs".to_string(), SignalType::Logs),
                ("metrics_gauge".to_string(), SignalType::Metrics),
                ("profiles".to_string(), SignalType::Profiles),
                ("traces".to_string(), SignalType::Traces),
            ]
        );
    }

    /// #1010: a step-2 (snapshot expiry) failure must not zero out step-1's
    /// (partition drop) already-committed counts. Pure/deterministic —
    /// exercises the exact combining logic `enforce_table_retention` uses,
    /// without touching a catalog.
    #[test]
    fn build_table_retention_result_preserves_partition_counts_when_snapshot_expiry_fails() {
        let result = build_table_retention_result(
            "acme",
            "prod",
            "traces",
            SignalType::Traces,
            10,
            3,
            2048,
            42,
            Err(anyhow::anyhow!("simulated snapshot expiry failure")),
        );

        assert_eq!(result.partitions_evaluated, 10);
        assert_eq!(result.partitions_dropped, 3, "step 1's drop count is lost");
        assert_eq!(result.bytes_reclaimed, 2048, "step 1's bytes are lost");
        assert_eq!(result.snapshots_expired, 0);
        assert_eq!(result.errors.len(), 1);
        assert!(
            result.errors[0].contains("simulated snapshot expiry failure"),
            "errors = {:?}",
            result.errors
        );
    }

    #[test]
    fn build_table_retention_result_reports_no_errors_on_full_success() {
        let result = build_table_retention_result(
            "acme",
            "prod",
            "traces",
            SignalType::Traces,
            5,
            2,
            1024,
            7,
            Ok(4),
        );

        assert_eq!(result.partitions_dropped, 2);
        assert_eq!(result.snapshots_expired, 4);
        assert!(result.errors.is_empty());
    }

    /// #1010: proves the scenario `build_table_retention_result`'s test above
    /// assumes is actually reachable — step 1 (partition drop) can commit
    /// real work against a real catalog, and step 2 (snapshot expiry) can
    /// independently fail against that same real catalog. Driven as two
    /// direct calls to the production step methods (rather than one call to
    /// `enforce_table_retention`) so the step-2 failure is deterministic: an
    /// in-process commit race that fails only step 2 needs genuine
    /// concurrency, which is exactly the kind of wall-clock-dependent
    /// flakiness `compaction_and_retention_do_not_interleave_on_the_same_table`'s
    /// own comment above rejects elsewhere in this file.
    #[tokio::test]
    async fn table_retention_steps_are_independently_realizable_success_then_failure() {
        use crate::iceberg::partition::test_support::{hour_partition, test_data_file};

        let mut config = create_test_config();
        config.dry_run = false;

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let mut table = catalog_manager
            .ensure_table("acme", "prod", "traces")
            .await
            .unwrap();

        // Seed a data file in a partition well past the 7-day trace cutoff.
        // `IcebergCommitter::commit_compaction` is a *replace* and requires an
        // existing snapshot, so a virgin table's first snapshot is seeded via
        // a plain `append_data` transaction instead.
        let old_hours = Utc::now().timestamp() / 3600 - 24 * 30;
        table
            .new_transaction(None)
            .append_data(vec![test_data_file(
                hour_partition(old_hours as i32),
                "s3://bucket/x/data/00000-expired.parquet",
            )])
            .commit()
            .await
            .unwrap();

        let enforcer = RetentionEnforcer::new(
            catalog_manager.clone(),
            config,
            RetentionMetrics::new_mock(),
        )
        .unwrap();
        let cutoff = enforcer
            .policy_resolver()
            .compute_cutoff("acme", "prod", SignalType::Traces)
            .unwrap();

        let table_identifier = catalog_manager.build_table_identifier("acme", "prod", "traces");
        let table = load_table(&catalog_manager, "acme", "prod", "traces").await;

        // Step 1: a real, committed partition-drop replace snapshot.
        let (_, partitions_dropped, bytes_reclaimed) = enforcer
            .drop_expired_partitions("acme", "prod", "traces", &table, &cutoff)
            .await
            .unwrap();
        assert!(
            partitions_dropped > 0,
            "expected the seeded expired partition to be dropped"
        );
        assert!(bytes_reclaimed > 0);

        // Remove the table so step 2 cannot load it — a deterministic stand-in
        // for "step 2 fails after step 1 already mutated the table".
        catalog_manager
            .catalog()
            .drop_table(&table_identifier)
            .await
            .unwrap();

        let step2_result = enforcer
            .expire_old_snapshots("acme", "prod", "traces")
            .await;
        assert!(
            step2_result.is_err(),
            "expected snapshot expiry to fail against a dropped table"
        );
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
    /// the resolved registry (otel/registry/ + upstream semconv). At the
    /// default `info` level every event field must be a registered
    /// `signaldb.*` attribute, an upstream semconv attribute (`file.path`),
    /// or one of the keys whitelisted in .weaver.toml: `level`/`target`
    /// (stamped unconditionally by tracing-opentelemetry's event bridge) and
    /// `error` (the workspace-wide failure-event idiom).
    ///
    /// Runs a real (non-dry-run) enforcement pass against non-empty catalog
    /// state so every real-path INFO+ event site actually fires, rather than
    /// asserting only against the two run-level start/completion events an
    /// empty catalog produces (#1010). `dry_run = true` branches are covered
    /// by the sibling [`retention_run_span_events_cover_dry_run_branches`],
    /// with its own catalog so the two enforcement passes don't share a
    /// SQLite connection pool.
    ///
    /// `acme`/`prod`, `snapshots_to_keep = 1`: the `traces` table has a real
    /// expired partition (dropped) plus a live partition (kept), giving the
    /// post-drop snapshot count enough history to exceed `snapshots_to_keep`
    /// so a real `RemoveSnapshots` commit happens. The `logs` table has a
    /// tenant override (`Duration::MAX`) that overflows `compute_cutoff`,
    /// driving the table-failure branch.
    ///
    /// Not exercised:
    /// - The snapshot-conflict retry warn (`signaldb.job.attempt`) needs a
    ///   genuine concurrent commit racing the drop, which would make this
    ///   test flaky under CI load for the same reason
    ///   `compaction_and_retention_do_not_interleave_on_the_same_table`'s
    ///   comment gives for avoiding wall-clock-dependent synchronization.
    /// - The unclassifiable-file warns (`PartitionManager::list_partitions`'s
    ///   and `try_drop_partitions_once`'s "no recoverable timestamp_hour",
    ///   plus `data_file_partition_hours`' "unexpected type") need a manifest
    ///   entry with a null or wrong-typed `timestamp_hour` partition value.
    ///   iceberg-rust rejects both at commit time — a null value fails its
    ///   Rust-level bounding-box check (`partition_struct_to_vec`) and a
    ///   wrong-typed one fails Avro serialization against the field's `Long`
    ///   schema — so real production data can only reach this branch via a
    ///   manifest predating a schema change, not a fresh commit. Their keys
    ///   (`file.path`, `signaldb.job.partition_value`) are fixed and covered
    ///   by the pure `classify_data_file_keeps_unclassifiable_files` unit
    ///   test above, but left unexercised here.
    ///
    /// Both keys' fixes are verified by code inspection rather than a fixture
    /// in this test.
    #[tokio::test]
    async fn retention_run_span_events_use_registry_attribute_keys() {
        use crate::iceberg::partition::test_support::{hour_partition, test_data_file};
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
            let old_hours = Utc::now().timestamp() / 3600 - 24 * 30;
            let recent_hours = Utc::now().timestamp() / 3600;

            // `acme`/`prod`/`logs`: a tenant override that overflows
            // `compute_cutoff`, so this table's retention pass fails outright.
            let mut tenant_overrides = HashMap::new();
            tenant_overrides.insert(
                "acme".to_string(),
                TenantRetentionConfig {
                    traces: None,
                    logs: Some(std::time::Duration::MAX),
                    metrics: None,
                    profiles: None,
                    dataset_overrides: HashMap::new(),
                },
            );

            // `acme`/`prod`/`traces`: real drop + real snapshot expiry.
            catalog_manager
                .ensure_table("acme", "prod", "traces")
                .await
                .unwrap();
            catalog_manager
                .ensure_table("acme", "prod", "logs")
                .await
                .unwrap();
            // One commit for both files: the resulting single pre-existing
            // snapshot is already enough for `snapshots_to_keep = 1` to have
            // a real snapshot to expire once step 1 adds its own (fewer
            // separate catalog round-trips is also kinder to the in-memory
            // SQLite pool under parallel test load).
            let mut traces_table = load_table(&catalog_manager, "acme", "prod", "traces").await;
            traces_table
                .new_transaction(None)
                .append_data(vec![
                    test_data_file(
                        hour_partition(old_hours as i32),
                        "s3://bucket/x/data/00000-expired.parquet",
                    ),
                    test_data_file(
                        hour_partition(recent_hours as i32),
                        "s3://bucket/x/data/00000-live.parquet",
                    ),
                ])
                .commit()
                .await
                .unwrap();

            let real_config = RetentionConfig {
                dry_run: false,
                snapshots_to_keep: Some(1),
                tenant_overrides,
                ..create_test_config()
            };
            let real_enforcer =
                RetentionEnforcer::new(catalog_manager, real_config, RetentionMetrics::new_mock())
                    .unwrap();
            let _ = real_enforcer.enforce_retention("acme", "prod").await;
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
        let observed_event_names: std::collections::BTreeSet<String> =
            span.events.iter().map(|e| e.name.to_string()).collect();
        let expected_event_names: std::collections::BTreeSet<String> = [
            "Starting retention enforcement run",
            "Retention enforcement run completed",
            "Table retention enforcement completed",
            "Table retention enforcement failed",
            "Dropped expired partitions",
            "Expired old snapshots",
            "Committing compaction (replace data files)",
            "Compaction commit verified",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(
            observed_event_names, expected_event_names,
            "retention span events drifted from what these fixtures are meant to exercise \
             (missing = branch stopped firing, extra = a new event needs a fixture)"
        );

        let observed_keys: std::collections::BTreeSet<String> = span
            .events
            .iter()
            .flat_map(|e| e.attributes.iter())
            .map(|kv| kv.key.as_str().to_string())
            .collect();
        let expected_keys: std::collections::BTreeSet<String> = [
            "level",
            "target",
            "error",
            "signaldb.tenant.id",
            "signaldb.dataset.id",
            "signaldb.table",
            "signaldb.job.run_id",
            "signaldb.job.dry_run",
            "signaldb.job.tables_processed",
            "signaldb.job.partitions_dropped",
            "signaldb.job.snapshots_expired",
            "signaldb.job.bytes_reclaimed",
            "signaldb.job.duration_ms",
            "signaldb.job.files_deleted",
            "signaldb.job.files_written",
            "signaldb.job.snapshot_id",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(
            observed_keys, expected_keys,
            "span-event attribute keys drifted from the signaldb registry namespace \
             (see otel/registry/signaldb.yaml)"
        );
    }

    /// Sibling to [`retention_run_span_events_use_registry_attribute_keys`]:
    /// covers the two `dry_run = true` event sites, which that test's real
    /// (non-dry-run) pass never reaches. A separate catalog rather than a
    /// second dataset in the same one, so the two enforcement passes never
    /// share a SQLite connection pool.
    ///
    /// `acme`/`staging`/`traces`, `snapshots_to_keep = 1`: an expired
    /// partition plus a second pre-existing snapshot (two real commits, since
    /// a dry run never commits its own) drive both dry-run branches without
    /// anything actually committing.
    #[tokio::test]
    async fn retention_run_span_events_cover_dry_run_branches() {
        use crate::iceberg::partition::test_support::{hour_partition, test_data_file};
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        use opentelemetry::trace::TracerProvider as _;
        let tracer = provider.tracer("test");
        let subscriber = tracing_subscriber::registry().with(
            common::self_monitoring::otel_span_layer(tracer)
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        );

        async {
            let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
            let old_hours = Utc::now().timestamp() / 3600 - 24 * 30;
            let recent_hours = Utc::now().timestamp() / 3600;

            catalog_manager
                .ensure_table("acme", "staging", "traces")
                .await
                .unwrap();
            let mut staging_table = load_table(&catalog_manager, "acme", "staging", "traces").await;
            staging_table
                .new_transaction(None)
                .append_data(vec![test_data_file(
                    hour_partition(old_hours as i32),
                    "s3://bucket/y/data/00000-expired.parquet",
                )])
                .commit()
                .await
                .unwrap();
            staging_table
                .new_transaction(None)
                .append_data(vec![test_data_file(
                    hour_partition(recent_hours as i32),
                    "s3://bucket/y/data/00000-live.parquet",
                )])
                .commit()
                .await
                .unwrap();

            let dry_run_config = RetentionConfig {
                dry_run: true,
                snapshots_to_keep: Some(1),
                ..create_test_config()
            };
            let dry_run_enforcer = RetentionEnforcer::new(
                catalog_manager,
                dry_run_config,
                RetentionMetrics::new_mock(),
            )
            .unwrap();
            let _ = dry_run_enforcer.enforce_retention("acme", "staging").await;
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|s| s.name == "retention_enforcement")
            .expect("no retention job span");

        let observed_event_names: std::collections::BTreeSet<String> =
            span.events.iter().map(|e| e.name.to_string()).collect();
        let expected_event_names: std::collections::BTreeSet<String> = [
            "Starting retention enforcement run",
            "Retention enforcement run completed",
            "Table retention enforcement completed",
            "[DRY RUN] Would drop expired partitions",
            "[DRY RUN] Would expire old snapshots",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(
            observed_event_names, expected_event_names,
            "retention span events drifted from what these fixtures are meant to exercise \
             (missing = branch stopped firing, extra = a new event needs a fixture)"
        );

        let observed_keys: std::collections::BTreeSet<String> = span
            .events
            .iter()
            .flat_map(|e| e.attributes.iter())
            .map(|kv| kv.key.as_str().to_string())
            .collect();
        let expected_keys: std::collections::BTreeSet<String> = [
            "level",
            "target",
            "signaldb.tenant.id",
            "signaldb.dataset.id",
            "signaldb.table",
            "signaldb.job.run_id",
            "signaldb.job.dry_run",
            "signaldb.job.tables_processed",
            "signaldb.job.partitions_dropped",
            "signaldb.job.snapshots_expired",
            "signaldb.job.bytes_reclaimed",
            "signaldb.job.duration_ms",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(
            observed_keys, expected_keys,
            "span-event attribute keys drifted from the signaldb registry namespace \
             (see otel/registry/signaldb.yaml)"
        );
    }
}
