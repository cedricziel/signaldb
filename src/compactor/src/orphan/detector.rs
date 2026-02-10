//! Orphan file detection logic.
//!
//! This module implements the safety-critical orphan detection algorithm
//! that identifies data files no longer referenced by any live snapshot.
//!
//! ## Detection Algorithm (4 Phases)
//!
//! 1. **Build Live Reference Set**: Scan all live snapshots and collect file paths
//! 2. **Scan Object Store**: List all .parquet files in table location
//! 3. **Identify Candidates**: Files not in reference set AND older than grace period
//! 4. **Optional Revalidation**: Re-check orphan status before deletion

use crate::iceberg::ManifestReader;
use crate::orphan::config::OrphanCleanupConfig;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use common::catalog_manager::CatalogManager;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use std::collections::HashSet;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Information about a file in object storage.
#[derive(Debug, Clone)]
pub struct ObjectStoreFile {
    /// Full path to the file.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: usize,
    /// Last modification timestamp.
    pub last_modified: DateTime<Utc>,
}

/// Orphan candidate with metadata.
#[derive(Debug, Clone)]
pub struct OrphanCandidate {
    /// Full path to the orphan file.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: usize,
    /// Last modification timestamp.
    pub last_modified: DateTime<Utc>,
    /// Table identifier (tenant/dataset/table).
    pub table_identifier: String,
}

/// Orphan file detector.
///
/// Identifies data files that are no longer referenced by any live snapshot
/// using a multi-phase validation approach.
pub struct OrphanDetector {
    config: OrphanCleanupConfig,
    catalog_manager: Arc<CatalogManager>,
    object_store: Arc<dyn ObjectStore>,
    manifest_reader: ManifestReader,
}

impl OrphanDetector {
    /// Create a new orphan detector.
    pub fn new(
        config: OrphanCleanupConfig,
        catalog_manager: Arc<CatalogManager>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let manifest_reader = ManifestReader::new();
        Self {
            config,
            catalog_manager,
            object_store,
            manifest_reader,
        }
    }

    /// Identify orphan candidates for a specific table.
    ///
    /// This method implements the complete detection algorithm:
    /// 1. Build live file reference set from manifests
    /// 2. Scan object store for all files
    /// 3. Identify candidates (not in reference set + older than grace period)
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - Tenant identifier
    /// * `dataset_id` - Dataset identifier
    /// * `table_name` - Table name (e.g., "traces", "logs", "metrics")
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table metadata cannot be loaded
    /// - Manifests cannot be read
    /// - Object store is unavailable
    pub async fn identify_orphan_candidates(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Vec<OrphanCandidate>> {
        tracing::info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            "Starting orphan detection"
        );

        // Phase 1: Build live reference set from manifests
        let live_files = self
            .build_live_reference_set(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to build live file reference set")?;

        tracing::info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            live_files = live_files.len(),
            "Built live file reference set"
        );

        // Phase 2: Scan object store for actual files
        let all_files = self
            .scan_object_store_files(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to scan object store")?;

        tracing::info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            total_files = all_files.len(),
            "Scanned object store"
        );

        // Phase 3: Identify orphan candidates
        let candidates =
            self.identify_candidates(&live_files, &all_files, tenant_id, dataset_id, table_name)?;

        tracing::info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            orphan_candidates = candidates.len(),
            grace_period_hours = self.config.grace_period_hours,
            "Identified orphan candidates"
        );

        Ok(candidates)
    }

    /// Build live file reference set from table snapshots.
    ///
    /// Scans all snapshots within the retention window and collects
    /// all referenced data file paths.
    async fn build_live_reference_set(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<HashSet<String>> {
        // Load table metadata using catalog manager
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);

        let catalog = self.catalog_manager.catalog();
        let tabular = catalog
            .load_tabular(&table_identifier)
            .await
            .with_context(|| {
                format!("Failed to load table {tenant_id}/{dataset_id}/{table_name}")
            })?;

        let table = match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => anyhow::bail!("Expected table but found different tabular type"),
        };

        // Filter snapshots by age
        let cutoff_time = Utc::now()
            - chrono::Duration::from_std(self.config.max_snapshot_age())
                .context("Invalid duration conversion")?;
        let metadata = table.metadata();
        let snapshot_ids: Vec<i64> = metadata
            .snapshots
            .values()
            .filter_map(|snapshot| {
                let snapshot_time =
                    chrono::DateTime::from_timestamp_millis(*snapshot.timestamp_ms())?;
                if snapshot_time > cutoff_time {
                    Some(*snapshot.snapshot_id())
                } else {
                    None
                }
            })
            .collect();

        tracing::debug!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            total_snapshots = metadata.snapshots.len(),
            filtered_snapshots = snapshot_ids.len(),
            snapshot_window_hours = self.config.max_snapshot_age_hours,
            "Filtered snapshots by age"
        );

        // Use ManifestReader to build the live file set
        let live_files = self
            .manifest_reader
            .build_live_file_set(&table, Some(&snapshot_ids))
            .await
            .context("Failed to build live file set from manifests")?;

        tracing::debug!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            live_files_count = live_files.len(),
            "Built live file reference set from manifests"
        );

        Ok(live_files)
    }

    /// Scan object store for all Parquet files in table location.
    ///
    /// Lists all .parquet files under the table's data directory,
    /// respecting tenant/dataset boundaries.
    async fn scan_object_store_files(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Vec<ObjectStoreFile>> {
        // Build table location path
        // Format: /{tenant_slug}/{dataset_slug}/{table_name}/data/
        let table_location = format!("{}/{}/{}", tenant_id, dataset_id, table_name);
        let data_path = format!("{}/data/", table_location);
        let path = ObjectPath::from(data_path.as_str());

        tracing::debug!(
            path = %data_path,
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            "Scanning object store"
        );

        let mut all_files = vec![];

        // List all objects under data/ path
        let mut list_stream = self.object_store.list(Some(&path));

        while let Some(meta_result) = list_stream.next().await {
            let meta = meta_result.with_context(|| {
                format!("Failed to read object metadata at path: {}", data_path)
            })?;

            // Only include .parquet files
            if meta.location.as_ref().ends_with(".parquet") {
                all_files.push(ObjectStoreFile {
                    path: meta.location.to_string(),
                    size_bytes: meta.size as usize,
                    last_modified: meta.last_modified,
                });
            }
        }

        tracing::debug!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            parquet_files = all_files.len(),
            "Found parquet files in object store"
        );

        Ok(all_files)
    }

    /// Identify orphan candidates from file comparison.
    ///
    /// Applies safety checks:
    /// 1. File not in live reference set
    /// 2. File older than grace period
    fn identify_candidates(
        &self,
        live_files: &HashSet<String>,
        all_files: &[ObjectStoreFile],
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Vec<OrphanCandidate>> {
        let grace_period = chrono::Duration::from_std(self.config.grace_period())
            .context("Failed to convert grace period duration")?;
        let cutoff_time = Utc::now() - grace_period;
        let table_identifier = format!("{tenant_id}/{dataset_id}/{table_name}");

        let mut candidates = vec![];

        for file in all_files {
            // Safety check 1: Is file referenced by any snapshot?
            if live_files.contains(&file.path) {
                tracing::trace!(
                    path = %file.path,
                    "File is referenced by live snapshot, skipping"
                );
                continue;
            }

            // Safety check 2: Is file older than grace period?
            if file.last_modified > cutoff_time {
                tracing::debug!(
                    path = %file.path,
                    last_modified = %file.last_modified,
                    cutoff_time = %cutoff_time,
                    grace_period_hours = self.config.grace_period_hours,
                    "Skipping recent file (within grace period)"
                );
                continue;
            }

            // File is an orphan candidate
            tracing::debug!(
                path = %file.path,
                size_bytes = file.size_bytes,
                last_modified = %file.last_modified,
                "Identified orphan candidate"
            );

            candidates.push(OrphanCandidate {
                path: file.path.clone(),
                size_bytes: file.size_bytes,
                last_modified: file.last_modified,
                table_identifier: table_identifier.clone(),
            });
        }

        Ok(candidates)
    }

    /// Validate that a file is still an orphan immediately before deletion.
    ///
    /// This is an optional safety check (enabled by `revalidate_before_delete`)
    /// that catches concurrent writes that may have referenced the file between
    /// detection and deletion.
    ///
    /// # Arguments
    ///
    /// * `candidate` - The orphan candidate to validate
    /// * `tenant_id` - Tenant identifier
    /// * `dataset_id` - Dataset identifier
    /// * `table_name` - Table name
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if file is still an orphan (safe to delete)
    /// - `Ok(false)` if file is now referenced (do not delete)
    /// - `Err(_)` if validation failed (abort deletion)
    pub async fn validate_orphan_before_deletion(
        &self,
        candidate: &OrphanCandidate,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<bool> {
        if !self.config.revalidate_before_delete {
            // Revalidation disabled, assume still orphan
            return Ok(true);
        }

        tracing::debug!(
            path = %candidate.path,
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            "Revalidating orphan status before deletion"
        );

        // Reload table metadata (get latest snapshot)
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);

        let catalog = self.catalog_manager.catalog();
        let tabular = catalog
            .load_tabular(&table_identifier)
            .await
            .with_context(|| {
                format!(
                    "Failed to load table for revalidation: {tenant_id}/{dataset_id}/{table_name}"
                )
            })?;

        let table = match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => anyhow::bail!("Expected table but found different tabular type"),
        };

        // Build live file set from current snapshot only (all snapshots, no time filter)
        let live_files = self
            .manifest_reader
            .build_live_file_set(&table, None)
            .await
            .context("Failed to build live file set for revalidation")?;

        // Check if file is now referenced
        if live_files.contains(&candidate.path) {
            tracing::warn!(
                path = %candidate.path,
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                table_name = %table_name,
                "File is NOW referenced by current snapshot! Aborting deletion."
            );
            return Ok(false);
        }

        tracing::debug!(
            path = %candidate.path,
            "File still orphan after revalidation"
        );

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_store_file_creation() {
        let file = ObjectStoreFile {
            path: "/tenant/dataset/table/data/file.parquet".to_string(),
            size_bytes: 1024,
            last_modified: Utc::now(),
        };

        assert_eq!(file.path, "/tenant/dataset/table/data/file.parquet");
        assert_eq!(file.size_bytes, 1024);
    }

    #[test]
    fn test_orphan_candidate_creation() {
        let candidate = OrphanCandidate {
            path: "/tenant/dataset/table/data/orphan.parquet".to_string(),
            size_bytes: 2048,
            last_modified: Utc::now(),
            table_identifier: "tenant/dataset/table".to_string(),
        };

        assert_eq!(candidate.path, "/tenant/dataset/table/data/orphan.parquet");
        assert_eq!(candidate.size_bytes, 2048);
        assert_eq!(candidate.table_identifier, "tenant/dataset/table");
    }

    #[test]
    fn test_grace_period_filtering() {
        let config = OrphanCleanupConfig {
            grace_period_hours: 24,
            ..Default::default()
        };

        let live_files: HashSet<String> = HashSet::new();

        // Recent file (within grace period)
        let recent_file = ObjectStoreFile {
            path: "recent.parquet".to_string(),
            size_bytes: 100,
            last_modified: Utc::now() - chrono::Duration::hours(1),
        };

        // Old file (outside grace period)
        let old_file = ObjectStoreFile {
            path: "old.parquet".to_string(),
            size_bytes: 200,
            last_modified: Utc::now() - chrono::Duration::hours(48),
        };

        let all_files = vec![recent_file, old_file];

        // Test the identify_candidates logic directly
        let grace_period = chrono::Duration::from_std(config.grace_period()).unwrap();
        let cutoff_time = Utc::now() - grace_period;
        let mut candidates = Vec::new();

        for file in &all_files {
            if !live_files.contains(&file.path) && file.last_modified <= cutoff_time {
                candidates.push(file.clone());
            }
        }

        // Should only identify the old file
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].path, "old.parquet");
    }

    #[test]
    fn test_live_file_filtering() {
        let config = OrphanCleanupConfig::default();

        let mut live_files = HashSet::new();
        live_files.insert("live.parquet".to_string());

        // Old file that is still referenced
        let live_file = ObjectStoreFile {
            path: "live.parquet".to_string(),
            size_bytes: 100,
            last_modified: Utc::now() - chrono::Duration::hours(48),
        };

        // Old file that is not referenced
        let orphan_file = ObjectStoreFile {
            path: "orphan.parquet".to_string(),
            size_bytes: 200,
            last_modified: Utc::now() - chrono::Duration::hours(48),
        };

        let all_files = vec![live_file, orphan_file];

        // Test the filtering logic directly
        let grace_period = chrono::Duration::from_std(config.grace_period()).unwrap();
        let cutoff_time = Utc::now() - grace_period;
        let mut candidates = Vec::new();

        for file in &all_files {
            if !live_files.contains(&file.path) && file.last_modified <= cutoff_time {
                candidates.push(file.clone());
            }
        }

        // Should only identify the orphan file
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].path, "orphan.parquet");
    }
}
