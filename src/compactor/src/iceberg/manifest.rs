//! Manifest reading operations for Iceberg tables
//!
//! Provides functionality to read manifest lists and manifest files
//! to extract data file paths for orphan detection.

use anyhow::{Context, Result};
use futures::StreamExt;
use iceberg_rust::spec::manifest::Status;
use iceberg_rust::spec::manifest_list::ManifestListEntry;
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
    /// Path of the manifest that carries this file's live entry.
    ///
    /// Required to build the `files_to_overwrite` map of a scoped delta
    /// commit: Iceberg's overwrite operation rewrites manifests, so it is
    /// keyed by manifest path rather than by data file alone.
    pub manifest_path: String,
    /// Hours since the Unix epoch of this file's `timestamp_hour` partition,
    /// read from the manifest entry's typed partition struct.
    ///
    /// `None` marks an unclassifiable file (no usable partition value and no
    /// recoverable path component). Such files are never compacted — they are
    /// left untouched rather than silently swept into another partition's
    /// rewrite.
    pub partition_hours: Option<i64>,
}

/// Reads manifest files to extract data file information
pub struct ManifestReader;

impl ManifestReader {
    /// Create a new manifest reader
    pub fn new() -> Self {
        Self
    }

    /// Collect the deduplicated manifest list entries of every retained
    /// snapshot.
    ///
    /// Iceberg reuses manifest files across snapshots and a manifest added
    /// long ago can still carry live EXISTING entries, so liveness must never
    /// be derived from snapshot or manifest age (issue #925). Any file
    /// referenced by any retained snapshot is reachable — through the current
    /// snapshot, time travel, or a query pinned to an older snapshot.
    ///
    /// Expired snapshots are removed from table metadata, so the retained
    /// set shrinks as snapshot expiration runs; run expiration first for
    /// cleanup to reclaim anything.
    pub async fn collect_retained_manifests(
        &self,
        table: &Table,
    ) -> Result<Vec<ManifestListEntry>> {
        let metadata = table.metadata();
        let mut seen_manifest_paths = HashSet::new();
        let mut manifests = Vec::new();
        for snapshot_id in metadata.snapshots.keys() {
            let snapshot_manifests = table
                .manifests(None, Some(*snapshot_id))
                .await
                .with_context(|| {
                    format!("Failed to read manifest list of snapshot {snapshot_id}")
                })?;
            for manifest in snapshot_manifests {
                if seen_manifest_paths.insert(manifest.manifest_path.clone()) {
                    manifests.push(manifest);
                }
            }
        }

        tracing::debug!(
            retained_snapshots = metadata.snapshots.len(),
            manifest_count = manifests.len(),
            "Collected manifests across retained snapshots"
        );

        Ok(manifests)
    }

    /// Read the live data file paths referenced by the given manifests.
    ///
    /// A file with a DELETED entry in one manifest may still be live through
    /// an ADDED or EXISTING entry in another retained manifest, so DELETED
    /// entries are simply skipped: the union of non-deleted entries across
    /// all retained manifests is the live set.
    pub async fn read_live_files(
        &self,
        table: &Table,
        manifests: &[ManifestListEntry],
    ) -> Result<HashSet<String>> {
        if manifests.is_empty() {
            tracing::debug!("No manifests found, live file set is empty");
            return Ok(HashSet::new());
        }

        let file_iter = table
            .datafiles(manifests, None, (None, None))
            .await
            .context("Failed to read data files from manifests")?;

        let mut live_files = HashSet::new();
        let mut file_iter = std::pin::pin!(file_iter);
        while let Some(result) = file_iter.next().await {
            let (_, entry) = result.context("Failed to read manifest entry")?;
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

    /// Build the set of data file paths referenced by any retained snapshot.
    ///
    /// # Safety
    /// Run snapshot expiration before calling this to ensure that files
    /// referenced only by expired snapshots are not protected from cleanup.
    pub async fn build_live_file_set(&self, table: &Table) -> Result<HashSet<String>> {
        let manifests = self.collect_retained_manifests(table).await?;
        self.read_live_files(table, &manifests).await
    }

    /// Extract live data file information from the table's current snapshot.
    ///
    /// Reads the manifest list and every manifest to collect path, size, and
    /// record count for each live (non-deleted) data file. Used by the
    /// compaction executor to determine the real input file set for a rewrite.
    pub async fn get_snapshot_files(&self, table: &Table) -> Result<Vec<ManifestFileInfo>> {
        let manifests = table
            .manifests(None, None)
            .await
            .context("Failed to read manifest list from current snapshot")?;

        if manifests.is_empty() {
            return Ok(vec![]);
        }

        let file_iter = table
            .datafiles(&manifests, None, (None, None))
            .await
            .context("Failed to read data files from manifests")?;

        let mut files = Vec::new();
        let mut file_iter = std::pin::pin!(file_iter);
        while let Some(result) = file_iter.next().await {
            let (manifest_path, entry) = result.context("Failed to read manifest entry")?;
            if *entry.status() != Status::Deleted {
                let data_file = entry.data_file();
                files.push(ManifestFileInfo {
                    file_path: data_file.file_path().to_string(),
                    file_size_bytes: *data_file.file_size_in_bytes() as u64,
                    record_count: *data_file.record_count() as u64,
                    manifest_path,
                    partition_hours: crate::iceberg::partition::data_file_partition_hours(
                        data_file,
                    ),
                });
            }
        }

        Ok(files)
    }
}

impl Default for ManifestReader {
    fn default() -> Self {
        Self::new()
    }
}
