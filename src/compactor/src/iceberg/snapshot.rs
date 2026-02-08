//! Snapshot operations for Iceberg tables
//!
//! Provides functionality to list, filter, and manage table snapshots
//! for retention enforcement.

use anyhow::Result;
use iceberg_rust::table::Table;
use std::collections::HashMap;

/// Information about an Iceberg table snapshot
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Snapshot ID
    pub snapshot_id: i64,
    /// Timestamp in milliseconds since epoch
    pub timestamp_ms: i64,
    /// Parent snapshot ID (if any)
    pub parent_snapshot_id: Option<i64>,
    /// Path to manifest list
    pub manifest_list: String,
    /// Summary statistics
    pub summary: HashMap<String, String>,
}

impl SnapshotInfo {
    /// Get snapshot timestamp as seconds since epoch
    pub fn timestamp_secs(&self) -> i64 {
        self.timestamp_ms / 1000
    }

    /// Check if snapshot is older than the given timestamp (in seconds)
    pub fn is_older_than_secs(&self, cutoff_secs: i64) -> bool {
        self.timestamp_secs() < cutoff_secs
    }
}

/// Manages Iceberg table snapshot operations
pub struct SnapshotManager;

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new() -> Self {
        Self
    }

    /// Get current snapshot ID from a table
    pub fn get_current_snapshot_id(&self, table: &Table) -> Result<Option<i64>> {
        let metadata = table.metadata();
        Ok(metadata.current_snapshot_id)
    }

    /// Get current snapshot information
    pub fn get_current_snapshot(&self, table: &Table) -> Result<Option<SnapshotInfo>> {
        let metadata = table.metadata();

        if let Some(snapshot_id) = metadata.current_snapshot_id
            && let Some((_id, snapshot)) = metadata
                .snapshots
                .iter()
                .find(|(id, _s)| **id == snapshot_id)
        {
            return Ok(Some(Self::snapshot_to_info(snapshot)));
        }

        Ok(None)
    }

    /// List all snapshots for a table
    pub fn list_snapshots(&self, table: &Table) -> Result<Vec<SnapshotInfo>> {
        let metadata = table.metadata();
        let snapshots = &metadata.snapshots;

        let snapshot_infos = snapshots.values().map(Self::snapshot_to_info).collect();

        Ok(snapshot_infos)
    }

    /// List snapshots older than the given timestamp (in seconds since epoch)
    pub fn list_snapshots_older_than(
        &self,
        table: &Table,
        cutoff_secs: i64,
    ) -> Result<Vec<SnapshotInfo>> {
        let all_snapshots = self.list_snapshots(table)?;

        let old_snapshots = all_snapshots
            .into_iter()
            .filter(|s| s.is_older_than_secs(cutoff_secs))
            .collect();

        Ok(old_snapshots)
    }

    /// Get the N most recent snapshots
    pub fn get_recent_snapshots(&self, table: &Table, count: usize) -> Result<Vec<SnapshotInfo>> {
        let mut snapshots = self.list_snapshots(table)?;

        // Sort by timestamp descending (most recent first)
        snapshots.sort_by(|a, b| b.timestamp_ms.cmp(&a.timestamp_ms));

        // Take the first N
        snapshots.truncate(count);

        Ok(snapshots)
    }

    /// Get snapshots to expire (keep N most recent, return rest for expiration)
    ///
    /// Returns snapshots that should be expired, sorted oldest first.
    /// Ensures at least one snapshot is always kept.
    pub fn get_snapshots_to_expire(
        &self,
        table: &Table,
        keep_count: usize,
    ) -> Result<Vec<SnapshotInfo>> {
        let mut all_snapshots = self.list_snapshots(table)?;

        // Always keep at least one snapshot
        let keep_count = keep_count.max(1);

        if all_snapshots.len() <= keep_count {
            // Nothing to expire
            return Ok(vec![]);
        }

        // Sort by timestamp descending (most recent first)
        all_snapshots.sort_by(|a, b| b.timestamp_ms.cmp(&a.timestamp_ms));

        // Keep the first N, expire the rest
        let to_expire: Vec<SnapshotInfo> = all_snapshots.into_iter().skip(keep_count).collect();

        // Return sorted oldest first for consistent processing
        let mut to_expire = to_expire;
        to_expire.sort_by(|a, b| a.timestamp_ms.cmp(&b.timestamp_ms));

        Ok(to_expire)
    }

    /// Convert iceberg-rust Snapshot to our SnapshotInfo
    fn snapshot_to_info(snapshot: &iceberg_rust::spec::snapshot::Snapshot) -> SnapshotInfo {
        SnapshotInfo {
            snapshot_id: *snapshot.snapshot_id(),
            timestamp_ms: *snapshot.timestamp_ms(),
            parent_snapshot_id: *snapshot.parent_snapshot_id(),
            manifest_list: snapshot.manifest_list().to_string(),
            summary: snapshot
                .summary()
                .other
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_info_timestamp_conversion() {
        let info = SnapshotInfo {
            snapshot_id: 1,
            timestamp_ms: 1609459200000, // 2021-01-01 00:00:00 UTC
            parent_snapshot_id: None,
            manifest_list: "s3://bucket/manifest.avro".to_string(),
            summary: HashMap::new(),
        };

        assert_eq!(info.timestamp_secs(), 1609459200);
    }

    #[test]
    fn test_snapshot_info_is_older_than() {
        let info = SnapshotInfo {
            snapshot_id: 1,
            timestamp_ms: 1609459200000, // 2021-01-01 00:00:00 UTC
            parent_snapshot_id: None,
            manifest_list: "s3://bucket/manifest.avro".to_string(),
            summary: HashMap::new(),
        };

        // Cutoff: 2021-01-02 00:00:00 UTC
        let cutoff = 1609545600;
        assert!(info.is_older_than_secs(cutoff));

        // Cutoff: 2020-12-31 00:00:00 UTC
        let cutoff = 1609372800;
        assert!(!info.is_older_than_secs(cutoff));
    }
}
