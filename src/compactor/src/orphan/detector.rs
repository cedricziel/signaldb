//! Orphan file detection logic.
//!
//! This module implements the safety-critical orphan detection algorithm
//! that identifies data files no longer referenced by any retained snapshot.
//!
//! ## Detection Algorithm (4 Phases)
//!
//! 1. **Build Live Reference Set**: Union the manifests of every retained
//!    snapshot and collect their non-deleted file paths. Liveness is never
//!    derived from snapshot or manifest age (issue #925) — snapshot
//!    expiration is what shrinks the retained set.
//! 2. **Scan Object Store**: List all .parquet files in table location
//! 3. **Identify Candidates**: Files not in reference set AND older than grace period
//! 4. **Optional Revalidation**: Re-check orphan status before deletion

use crate::iceberg::ManifestReader;
use crate::orphan::config::OrphanCleanupConfig;
use crate::orphan::metrics::{OrphanMetrics, SkipReason};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use common::catalog_manager::CatalogManager;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
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
    metrics: OrphanMetrics,
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
            metrics: OrphanMetrics::new(),
        }
    }

    /// Return a reference to the accumulated metrics for this detector.
    pub fn metrics(&self) -> &OrphanMetrics {
        &self.metrics
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

        // Phase 1: Build live reference set from manifests.
        // Returns None when the threshold guard trips → skip cleanup for this table.
        let live_files = match self
            .build_live_reference_set(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to build live file reference set")?
        {
            Some(set) => set,
            None => {
                // Threshold exceeded; return empty candidate list (cleanup skipped).
                return Ok(vec![]);
            }
        };

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
    /// Unions the manifests of every retained snapshot and collects all
    /// referenced data file paths.
    ///
    /// Returns `Ok(None)` when the `max_live_files_threshold` guard trips,
    /// signalling that the caller should skip orphan cleanup for this table.
    async fn build_live_reference_set(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Option<HashSet<String>>> {
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

        // The live set is the union of every retained snapshot's manifests:
        // Iceberg reuses manifests across snapshots, so a manifest's age says
        // nothing about whether its files are still referenced (issue #925).
        // Snapshot expiration — not an age window here — is what makes files
        // eligible for cleanup.
        let manifests = self
            .manifest_reader
            .collect_retained_manifests(&table)
            .await
            .context("Failed to collect retained snapshot manifests")?;

        // Cheap threshold check using manifest list metadata before reading
        // the manifest files themselves. Sums the file count estimates from
        // ManifestListEntry metadata (no manifest reads needed). If the
        // estimate exceeds the configured cap, skip cleanup with a warning
        // rather than risking excessive memory use.
        if self.config.max_live_files_threshold > 0 {
            let estimated_live_files: usize = manifests
                .iter()
                .map(|m| {
                    m.added_files_count.unwrap_or(0).max(0) as usize
                        + m.existing_files_count.unwrap_or(0).max(0) as usize
                })
                .sum();
            if estimated_live_files > self.config.max_live_files_threshold {
                tracing::warn!(
                    tenant_id = %tenant_id,
                    dataset_id = %dataset_id,
                    table_name = %table_name,
                    estimated_live_files,
                    threshold = self.config.max_live_files_threshold,
                    skip_reason = SkipReason::LiveFilesThresholdExceeded.as_str(),
                    "Skipping orphan cleanup: estimated live file count exceeds threshold. \
                     Run snapshot expiration first to reduce file counts, or raise \
                     max_live_files_threshold if memory allows."
                );
                self.metrics
                    .record_cleanup_skipped(SkipReason::LiveFilesThresholdExceeded);
                return Ok(None);
            }
        }

        let live_files = self
            .manifest_reader
            .read_live_files(&table, &manifests)
            .await
            .context("Failed to build live file set from manifests")?;

        tracing::debug!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            live_files_count = live_files.len(),
            "Built live file reference set from manifests"
        );

        Ok(Some(live_files))
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

    /// Identify unreferenced metadata files for a specific table.
    ///
    /// Metadata orphans are files in the table's `metadata/` directory that
    /// nothing references anymore: metadata.json versions that fell out of
    /// the metadata-log, and manifest-list/manifest avro files whose
    /// snapshots have been expired. Iceberg's delete-after-commit pruning
    /// only removes files it still tracks, so a backlog predating pruning
    /// (or left by expired snapshots) is reclaimable only here (#935, #959).
    ///
    /// Protected regardless of age: every metadata-log entry, the current
    /// metadata.json (via version-hint plus the newest scanned version as a
    /// fallback), every retained snapshot's manifest list, every retained
    /// manifest, and `version-hint.text` itself (never scanned).
    pub async fn identify_orphan_metadata_candidates(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Vec<OrphanCandidate>> {
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);
        let tabular = self
            .catalog_manager
            .catalog()
            .load_tabular(&table_identifier)
            .await
            .with_context(|| {
                format!("Failed to load table {tenant_id}/{dataset_id}/{table_name}")
            })?;
        let table = match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => anyhow::bail!("Expected table but found different tabular type"),
        };

        let mut live = self
            .build_live_metadata_set(&table)
            .await
            .context("Failed to build live metadata set")?;

        let metadata_dir = format!("{tenant_id}/{dataset_id}/{table_name}/metadata");

        // The current metadata.json is not part of the metadata-log; protect
        // the file the version hint points at.
        let hint_path = ObjectPath::from(format!("{metadata_dir}/version-hint.text").as_str());
        if let Ok(hint) = self.object_store.get(&hint_path).await
            && let Ok(bytes) = hint.bytes().await
        {
            let hint = String::from_utf8_lossy(&bytes).trim().to_string();
            if !hint.is_empty() {
                live.insert(format!("{metadata_dir}/{hint}.metadata.json"));
            }
        }

        // Scan the metadata directory for reclaimable file types.
        let mut all_files = vec![];
        let path = ObjectPath::from(format!("{metadata_dir}/").as_str());
        let mut list_stream = self.object_store.list(Some(&path));
        while let Some(meta_result) = list_stream.next().await {
            let meta = meta_result.with_context(|| {
                format!("Failed to read object metadata at path: {metadata_dir}/")
            })?;
            let location = meta.location.as_ref();
            if location.ends_with(".metadata.json") || location.ends_with(".avro") {
                all_files.push(ObjectStoreFile {
                    path: meta.location.to_string(),
                    size_bytes: meta.size as usize,
                    last_modified: meta.last_modified,
                });
            }
        }

        // Belt and braces against a stale or missing version hint: never
        // flag the newest metadata.json version we can see. Version stems
        // are zero-padded, so lexicographic max is the newest.
        if let Some(newest) = all_files
            .iter()
            .filter(|f| f.path.ends_with(".metadata.json"))
            .map(|f| f.path.clone())
            .max()
        {
            live.insert(newest);
        }

        let candidates =
            self.identify_candidates(&live, &all_files, tenant_id, dataset_id, table_name)?;

        tracing::info!(
            tenant_id = %tenant_id,
            dataset_id = %dataset_id,
            table_name = %table_name,
            scanned_metadata_files = all_files.len(),
            orphan_candidates = candidates.len(),
            grace_period_hours = self.config.grace_period_hours,
            "Identified orphan metadata candidates"
        );

        Ok(candidates)
    }

    /// Build the set of metadata files still referenced by the table:
    /// metadata-log entries, retained snapshots' manifest lists, and every
    /// retained manifest.
    async fn build_live_metadata_set(
        &self,
        table: &iceberg_rust::table::Table,
    ) -> Result<HashSet<String>> {
        let metadata = table.metadata();
        let mut live = HashSet::new();
        for entry in &metadata.metadata_log {
            live.insert(entry.metadata_file.clone());
        }
        for snapshot in metadata.snapshots.values() {
            live.insert(snapshot.manifest_list().clone());
        }
        for manifest in self
            .manifest_reader
            .collect_retained_manifests(table)
            .await?
        {
            live.insert(manifest.manifest_path);
        }
        Ok(live)
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

    /// Build the current live data-file set for a table from a fresh
    /// metadata load.
    ///
    /// Used by pre-deletion revalidation to catch concurrent writes that
    /// referenced a detected candidate between detection and deletion. The
    /// caller builds this once per table per batch — rebuilding it per
    /// candidate would issue one manifest-list read per retained snapshot
    /// for every file.
    pub async fn live_file_set_for_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<HashSet<String>> {
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);
        let tabular = self
            .catalog_manager
            .catalog()
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
        self.manifest_reader
            .build_live_file_set(&table)
            .await
            .context("Failed to build live file set for revalidation")
    }

    /// Build the current live metadata-file set for a table from a fresh
    /// metadata load: metadata-log entries, retained snapshots' manifest
    /// lists, and every retained manifest.
    ///
    /// Used by pre-deletion revalidation of metadata orphan candidates;
    /// like [`Self::live_file_set_for_table`], the caller builds this once
    /// per table per batch.
    pub async fn live_metadata_set_for_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<HashSet<String>> {
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);
        let tabular = self
            .catalog_manager
            .catalog()
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
        self.build_live_metadata_set(&table)
            .await
            .context("Failed to build live metadata set for revalidation")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a real `OrphanDetector` backed by an in-memory catalog and
    /// object store, so tests drive `identify_candidates` itself rather than
    /// a duplicated copy of its filtering logic.
    async fn make_detector(config: OrphanCleanupConfig) -> OrphanDetector {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        OrphanDetector::new(config, catalog_manager, object_store)
    }

    #[tokio::test]
    async fn identify_candidates_excludes_files_within_grace_period() {
        // Arrange
        let config = OrphanCleanupConfig {
            grace_period_hours: 24,
            ..Default::default()
        };
        let detector = make_detector(config).await;

        let live_files: HashSet<String> = HashSet::new();
        let recent_file = ObjectStoreFile {
            path: "recent.parquet".to_string(),
            size_bytes: 100,
            last_modified: Utc::now() - chrono::Duration::hours(1),
        };
        let old_file = ObjectStoreFile {
            path: "old.parquet".to_string(),
            size_bytes: 200,
            last_modified: Utc::now() - chrono::Duration::hours(48),
        };
        let all_files = vec![recent_file, old_file];

        // Act
        let candidates = detector
            .identify_candidates(&live_files, &all_files, "tenant", "dataset", "traces")
            .unwrap();

        // Assert: only the file older than the 24h grace period is a candidate.
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].path, "old.parquet");
    }

    #[tokio::test]
    async fn identify_candidates_excludes_files_still_referenced_by_a_live_snapshot() {
        // Arrange
        let config = OrphanCleanupConfig::default();
        let detector = make_detector(config).await;

        let mut live_files = HashSet::new();
        live_files.insert("live.parquet".to_string());

        // Both files are old enough to clear the grace period; only
        // liveness should distinguish them.
        let live_file = ObjectStoreFile {
            path: "live.parquet".to_string(),
            size_bytes: 100,
            last_modified: Utc::now() - chrono::Duration::hours(48),
        };
        let orphan_file = ObjectStoreFile {
            path: "orphan.parquet".to_string(),
            size_bytes: 200,
            last_modified: Utc::now() - chrono::Duration::hours(48),
        };
        let all_files = vec![live_file, orphan_file];

        // Act
        let candidates = detector
            .identify_candidates(&live_files, &all_files, "tenant", "dataset", "traces")
            .unwrap();

        // Assert: the file still referenced by a live snapshot is excluded.
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].path, "orphan.parquet");
    }
}
