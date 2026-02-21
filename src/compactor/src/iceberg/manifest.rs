//! Manifest reading operations for Iceberg tables
//!
//! Provides functionality to read manifest lists and manifest files
//! to extract data file paths for orphan detection.

use anyhow::{Context, Result};
use iceberg_rust::spec::manifest::Status;
use iceberg_rust::table::Table;
use std::collections::HashSet;

/// Information about a data file from a manifest
#[derive(Debug, Clone)]
pub struct ManifestFileInfo {
    /// Full path to the data file
    pub file_path: String,
    /// File size in bytes
    pub file_size_bytes: u64,
    /// Record count in the file
    pub record_count: u64,
}

/// Reads manifest files to extract data file information
pub struct ManifestReader;

impl ManifestReader {
    /// Create a new manifest reader
    pub fn new() -> Self {
        Self
    }

    /// Build a set of live data file paths from the current table snapshot.
    ///
    /// Reads the manifest list for the current snapshot and collects all
    /// data file paths with ADDED or EXISTING status. Files with DELETED
    /// status are excluded as they are no longer live.
    ///
    /// # Arguments
    /// * `table` - The Iceberg table to scan
    /// * `snapshot_ids` - Optional list of snapshot IDs; when provided, only
    ///   manifests added by those snapshots are included. When None, all
    ///   manifests in the current snapshot are used.
    ///
    /// # Returns
    /// A HashSet of absolute file paths that are currently referenced.
    ///
    /// # Safety
    /// Run snapshot expiration before calling this to ensure that files
    /// referenced only by expired snapshots are not protected from cleanup.
    pub async fn build_live_file_set(
        &self,
        table: &Table,
        snapshot_ids: Option<&[i64]>,
    ) -> Result<HashSet<String>> {
        // Read all manifests from the current snapshot's manifest list.
        // table.manifests(None, None) reads the current snapshot and returns
        // all ManifestListEntry records (one per manifest file).
        let all_manifests = table
            .manifests(None, None)
            .await
            .context("Failed to read manifest list from current snapshot")?;

        // Optionally filter to only manifests added by specific snapshots.
        let manifests = if let Some(ids) = snapshot_ids {
            let ids_set: HashSet<i64> = ids.iter().copied().collect();
            all_manifests
                .into_iter()
                .filter(|m| ids_set.contains(&m.added_snapshot_id))
                .collect::<Vec<_>>()
        } else {
            all_manifests
        };

        tracing::debug!(
            manifest_count = manifests.len(),
            "Reading manifests to build live file set"
        );

        if manifests.is_empty() {
            tracing::debug!("No manifests found, live file set is empty");
            return Ok(HashSet::new());
        }

        // Read all data file entries from the manifests.
        let file_iter = table
            .datafiles(&manifests, None, (None, None))
            .await
            .context("Failed to read data files from manifests")?;

        let mut live_files = HashSet::new();
        for result in file_iter {
            let (_, entry) = result.context("Failed to read manifest entry")?;
            // Only include ADDED and EXISTING files; DELETED entries are no longer live.
            if *entry.status() != Status::Deleted {
                live_files.insert(entry.data_file().file_path().to_string());
            }
        }

        tracing::debug!(
            live_files_count = live_files.len(),
            "Built live file set from manifests"
        );

        Ok(live_files)
    }

    /// Extract data file paths from a snapshot
    ///
    /// This is a simplified version that gets file information from
    /// the table's metadata without reading individual manifests.
    ///
    /// # Note
    /// This is a placeholder for Phase 3. Full manifest reading would
    /// require iceberg-rust APIs that may not be stable yet.
    pub async fn get_snapshot_files(
        &self,
        _table: &Table,
        _snapshot_id: i64,
    ) -> Result<Vec<ManifestFileInfo>> {
        // Placeholder: Real implementation would read manifest list,
        // then read each manifest file to extract data file entries.

        tracing::warn!(
            "Manifest reading not fully implemented yet - requires iceberg-rust manifest APIs"
        );

        Ok(vec![])
    }

    /// Build live file set using DataFusion table scan (alternative approach)
    ///
    /// This uses DataFusion's Iceberg integration to enumerate files
    /// instead of reading manifests directly. This is more reliable
    /// given the current state of iceberg-rust APIs.
    ///
    /// # Arguments
    /// * `table_location` - Base location of the table (e.g., "s3://bucket/path")
    ///
    /// # Returns
    /// A HashSet of file paths found in the table
    pub async fn build_live_file_set_via_datafusion(
        &self,
        _table_location: &str,
    ) -> Result<HashSet<String>> {
        // This will be implemented when we integrate with DataFusion
        // context in the orphan cleanup module.

        tracing::debug!("Building live file set via DataFusion table scan");

        Ok(HashSet::new())
    }

    /// Read manifest list file to get manifest file paths
    ///
    /// This is a low-level operation that parses the manifest list Avro file.
    /// The manifest list contains references to individual manifest files.
    #[allow(dead_code)] // Reserved for future implementation
    async fn read_manifest_list(&self, _manifest_list_path: &str) -> Result<Vec<String>> {
        // Placeholder for actual manifest list reading
        // Would use Avro reader to parse the manifest list file

        tracing::debug!("Would read manifest list from path");
        Ok(vec![])
    }

    /// Read a manifest file to extract data file entries
    ///
    /// This parses an individual manifest Avro file to get the list of
    /// data files (Parquet files) it references.
    #[allow(dead_code)] // Reserved for future implementation
    async fn read_manifest_file(&self, _manifest_path: &str) -> Result<Vec<ManifestFileInfo>> {
        // Placeholder for actual manifest reading
        // Would use Avro reader to parse the manifest file

        tracing::debug!("Would read manifest file");
        Ok(vec![])
    }
}

impl Default for ManifestReader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_reader_creation() {
        let _reader = ManifestReader::new();
        // Basic smoke test - reader is constructible
    }

    #[test]
    fn test_manifest_file_info() {
        let info = ManifestFileInfo {
            file_path: "s3://bucket/data/file.parquet".to_string(),
            file_size_bytes: 1024,
            record_count: 100,
        };

        assert_eq!(info.file_path, "s3://bucket/data/file.parquet");
        assert_eq!(info.file_size_bytes, 1024);
        assert_eq!(info.record_count, 100);
    }
}
