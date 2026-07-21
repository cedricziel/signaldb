//! Iceberg atomic commit operations for compaction
//!
//! Provides optimistic concurrency control for safely committing compaction results
//! while handling concurrent writes from other services.

use anyhow::{Context, Result};
use common::CatalogManager;
use iceberg_rust::table::Table;
use std::sync::Arc;

/// Helper to detect if an error is a conflict/concurrency error
pub fn is_conflict_error(error: &anyhow::Error) -> bool {
    let msg = error.to_string().to_lowercase();
    msg.contains("snapshot")
        && (msg.contains("changed")
            || msg.contains("conflict")
            || msg.contains("concurrent")
            || msg.contains("version")
            || msg.contains("mismatch"))
}

/// Information about a data file to add or remove (reporting/metrics only)
#[derive(Debug, Clone)]
pub struct DataFileChange {
    pub file_path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

/// Handles atomic commits to Iceberg tables with optimistic concurrency control
pub struct IcebergCommitter {
    catalog_manager: Arc<CatalogManager>,
}

impl IcebergCommitter {
    /// Create a new committer
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        Self { catalog_manager }
    }

    /// Commit a compaction atomically: replace all of the table's data files
    /// with the newly written compacted files.
    ///
    /// Uses Iceberg's snapshot-based optimistic concurrency:
    /// 1. Load fresh table metadata and verify the snapshot has not moved
    ///    since compaction started (`original_snapshot_id`)
    /// 2. Commit a `replace` transaction: the new files become the table's
    ///    complete data file set; all previously live files are removed from
    ///    the new snapshot (physical deletion is left to orphan cleanup)
    /// 3. Reload the table and verify the commit took effect — the SQL
    ///    catalog's compare-and-swap does not surface lost races as errors,
    ///    so a post-commit verification guards against silently dropped
    ///    commits
    ///
    /// Returns the snapshot ID created by the commit.
    pub async fn commit_compaction(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        original_snapshot_id: Option<i64>,
        new_files: Vec<iceberg_rust::spec::manifest::DataFile>,
    ) -> Result<i64> {
        tracing::info!(
            tenant_id,
            dataset_id,
            table_name,
            new_file_count = new_files.len(),
            "Committing compaction (replace data files)"
        );

        let table = self
            .load_fresh(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to load table for commit")?;

        // Check if snapshot has changed since we started compaction
        let current_snapshot_id = Self::get_current_snapshot_id(&table)?;
        if let Some(original_id) = original_snapshot_id
            && current_snapshot_id != original_id
        {
            return Err(anyhow::anyhow!(
                "Snapshot conflict: table snapshot changed from {} to {} during compaction",
                original_id,
                current_snapshot_id
            ));
        }

        let mut table = table;
        table
            .new_transaction(None)
            .replace(new_files)
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit compaction snapshot: {e}"))?;

        // The commit mutated our local handle to the snapshot it created.
        let committed_snapshot_id = Self::get_current_snapshot_id(&table)?;

        // Post-commit verification: reload from the catalog and confirm OUR
        // snapshot is the current one. The SQL catalog's UPDATE ... WHERE
        // metadata_location = <previous> does not report a failed CAS, so a
        // concurrent commit racing ours could silently win; treat that as a
        // conflict so the caller retries against fresh metadata.
        let verified = self
            .load_fresh(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to reload table for post-commit verification")?;
        let verified_snapshot_id = Self::get_current_snapshot_id(&verified)?;
        if verified_snapshot_id != committed_snapshot_id {
            return Err(anyhow::anyhow!(
                "Snapshot conflict: compaction commit did not take effect (expected snapshot {committed_snapshot_id}, catalog has {verified_snapshot_id}); a concurrent commit likely won the race"
            ));
        }

        tracing::info!(
            tenant_id,
            dataset_id,
            table_name,
            snapshot_id = verified_snapshot_id,
            "Compaction commit verified"
        );

        Ok(verified_snapshot_id)
    }

    /// Load a table with fresh metadata directly from the catalog.
    async fn load_fresh(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Table> {
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);

        let catalog = self.catalog_manager.catalog();
        let table = catalog
            .load_tabular(&table_identifier)
            .await
            .with_context(|| {
                format!("Failed to load table {tenant_id}/{dataset_id}/{table_name}")
            })?;

        match table {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => Ok(t),
            _ => Err(anyhow::anyhow!(
                "Expected table but got view for {tenant_id}/{dataset_id}/{table_name}"
            )),
        }
    }

    /// Get the current snapshot ID from a table
    fn get_current_snapshot_id(table: &Table) -> Result<i64> {
        let metadata = table.metadata();

        let snapshot_id = metadata
            .current_snapshot_id
            .ok_or_else(|| anyhow::anyhow!("Table has no current snapshot"))?;

        Ok(snapshot_id)
    }

    /// Reload table metadata to get fresh snapshot information
    ///
    /// This is used after detecting a conflict to get the latest state
    pub async fn reload_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Table> {
        tracing::debug!(
            "Reloading table metadata for {}/{}/{}",
            tenant_id,
            dataset_id,
            table_name
        );

        // Note: For Phase 2, we simply reload the table.
        // A full implementation would invalidate caching if the catalog supports it.

        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);

        let catalog = self.catalog_manager.catalog();
        let table = catalog
            .load_tabular(&table_identifier)
            .await
            .with_context(|| {
                format!("Failed to reload table {tenant_id}/{dataset_id}/{table_name}")
            })?;

        match table {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => Ok(t),
            _ => Err(anyhow::anyhow!(
                "Expected table but got view for {tenant_id}/{dataset_id}/{table_name}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_conflict_error() {
        // Test various conflict error messages
        let conflict_errors = vec![
            "snapshot changed from 123 to 456",
            "Snapshot conflict detected",
            "concurrent modification on snapshot",
            "version mismatch in snapshot",
        ];

        for msg in conflict_errors {
            let error = anyhow::anyhow!("{}", msg);
            assert!(
                is_conflict_error(&error),
                "Should detect conflict in: {}",
                msg
            );
        }

        // Test non-conflict errors
        let non_conflict_errors = vec!["file not found", "network timeout", "invalid schema"];

        for msg in non_conflict_errors {
            let error = anyhow::anyhow!("{}", msg);
            assert!(
                !is_conflict_error(&error),
                "Should not detect conflict in: {}",
                msg
            );
        }
    }

    #[test]
    fn test_data_file_change() {
        let change = DataFileChange {
            file_path: "data/file1.parquet".to_string(),
            size_bytes: 1024 * 1024,
            record_count: 10000,
        };

        assert_eq!(change.file_path, "data/file1.parquet");
        assert_eq!(change.size_bytes, 1024 * 1024);
        assert_eq!(change.record_count, 10000);
    }

    #[tokio::test]
    async fn test_committer_creation() {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let committer = IcebergCommitter::new(catalog_manager);

        // Just verify it constructs properly
        assert!(std::mem::size_of_val(&committer) > 0);
    }
}
