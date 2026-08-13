//! Iceberg atomic commit operations for compaction
//!
//! Provides optimistic concurrency control for safely committing compaction results
//! while handling concurrent writes from other services.

use anyhow::{Context, Result};
use common::CatalogManager;
use iceberg_rust::error::Error as IcebergError;
use iceberg_rust::table::Table;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Typed error for commit failures that SignalDB itself detects.
///
/// Conflict classification (`is_conflict_error`) matches on this type, so
/// self-authored conflicts are recognized regardless of message text and
/// regardless of how many `.context(...)` layers callers wrap around them.
#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    /// A concurrent commit won the optimistic-concurrency race: either the
    /// table snapshot moved while compaction was running, or the post-commit
    /// verification found that our snapshot never became current.
    ///
    /// The payload is human-readable detail only; classification must never
    /// depend on it.
    #[error("{0}")]
    SnapshotConflict(String),
}

/// Helper to detect if an error is a conflict/concurrency error
pub fn is_conflict_error(error: &anyhow::Error) -> bool {
    // Self-authored conflicts carry a typed marker. `downcast_ref` searches
    // the whole error chain, so `.context(...)` wrapping at call sites
    // cannot mask it.
    if matches!(
        error.downcast_ref::<CommitError>(),
        Some(CommitError::SnapshotConflict(_))
    ) {
        return true;
    }

    // Substring fallback, kept ONLY for errors originating in the
    // iceberg-rust fork: it surfaces commit races as stringly-typed errors
    // whose shape we don't control, so there is no type to match on. Scan
    // every cause in the chain because anyhow's Display renders only the
    // outermost context message.
    error.chain().any(|cause| {
        let msg = cause.to_string().to_lowercase();
        msg.contains("snapshot")
            && (msg.contains("changed")
                || msg.contains("conflict")
                || msg.contains("concurrent")
                || msg.contains("version")
                || msg.contains("mismatch"))
    })
}

/// Message the pinned iceberg-rust fork attaches to a failed
/// `check_table_requirements` call, across every catalog backend.
const TABLE_REQUIREMENTS_NOT_VALID: &str = "Table requirements not valid";

/// Classify a commit failure raised by the iceberg-rust fork.
///
/// A catalog compare-and-swap failure means a concurrent writer advanced the
/// table between our metadata read and the commit. That is routine rather than
/// exceptional — ingest commits every few seconds per table while a partition
/// rewrite takes seconds — so it must reach the executor's retry path, which
/// re-runs the job against freshly loaded metadata and a re-derived input set.
///
/// The fork exposes two CAS shapes and only one of them is typed:
/// `CommitConflict` for the guarded SQL UPDATE matching no rows, and a generic
/// `InvalidFormat` for the Iceberg table-requirements assertion. The latter is
/// indistinguishable by type from genuinely malformed metadata, so its message
/// is the only signal available until a distinct variant lands upstream in
/// JanKaul/iceberg-rust.
pub fn classify_commit_error(error: IcebergError) -> anyhow::Error {
    let conflict_detail = match &error {
        IcebergError::CommitConflict(_) => Some(error.to_string()),
        IcebergError::InvalidFormat(detail) if detail == TABLE_REQUIREMENTS_NOT_VALID => Some(
            "Iceberg table requirements check failed: a concurrent commit advanced the table \
             between the metadata read and this commit"
                .to_string(),
        ),
        _ => None,
    };

    match conflict_detail {
        // Losing this race is expected operation, so it is logged here at
        // DEBUG; ERROR is reserved for the executor exhausting its retries.
        Some(detail) => {
            tracing::debug!(
                iceberg_error = %error,
                "Catalog compare-and-swap failed; classifying as retryable snapshot conflict"
            );
            CommitError::SnapshotConflict(detail).into()
        }
        None => anyhow::Error::new(error),
    }
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
            return Err(CommitError::SnapshotConflict(format!(
                "Snapshot conflict: table snapshot changed from {original_id} to \
                 {current_snapshot_id} during compaction"
            ))
            .into());
        }

        let mut table = table;
        table
            .new_transaction(None)
            .replace(new_files)
            .commit()
            .await
            .map_err(classify_commit_error)
            .context("Failed to commit compaction snapshot")?;

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
            return Err(CommitError::SnapshotConflict(format!(
                "Snapshot conflict: compaction commit did not take effect (expected snapshot \
                 {committed_snapshot_id}, catalog has {verified_snapshot_id}); a concurrent \
                 commit likely won the race"
            ))
            .into());
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

    /// Commit a partition-scoped compaction as a *delta*: remove exactly the
    /// input files, add the newly written ones, leave the rest of the table
    /// untouched.
    ///
    /// This is what makes compaction survive live ingest (issue #933). The
    /// whole-table `replace` path guards on table-wide snapshot equality, so
    /// any concurrent append — to any partition — invalidates it; against a
    /// 5-second ingest commit cadence that race is unwinnable, and every lost
    /// attempt has already written a full duplicate copy of the table.
    ///
    /// A delta commit narrows the conflict domain to the files it actually
    /// touches:
    /// 1. Reload fresh metadata and re-derive each input file's *current*
    ///    manifest. An input file that is no longer live means another actor
    ///    (retention, a second compactor) mutated our inputs — a real
    ///    conflict, so abort before writing anything.
    /// 2. Commit an `overwrite` naming only our input files. Files that
    ///    arrived in the same partition meanwhile are not in the map and stay
    ///    live; appends to other partitions are irrelevant by construction.
    /// 3. Verify post-commit, as the `replace` path does — the SQL catalog's
    ///    compare-and-swap does not surface a lost race as an error.
    ///
    /// Returns the snapshot ID created by the commit.
    pub async fn commit_delta(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        partition_hours: i64,
        input_file_paths: &HashSet<String>,
        new_files: Vec<iceberg_rust::spec::manifest::DataFile>,
    ) -> Result<i64> {
        tracing::info!(
            tenant_id,
            dataset_id,
            table_name,
            partition_hours,
            input_file_count = input_file_paths.len(),
            new_file_count = new_files.len(),
            "Committing compaction (partition-scoped delta)"
        );

        let table = self
            .load_fresh(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to load table for commit")?;

        // Re-derive the input files' current manifests. Iceberg's overwrite is
        // keyed by manifest path because it rewrites manifests, and a manifest
        // path read from the pinned snapshot may already be stale.
        let live = crate::iceberg::ManifestReader::new()
            .get_snapshot_files(&table)
            .await
            .context("Failed to re-read manifests for delta commit")?;

        let mut files_to_overwrite: HashMap<String, Vec<String>> = HashMap::new();
        let mut still_live = HashSet::new();
        for file in live {
            if input_file_paths.contains(&file.file_path) {
                still_live.insert(file.file_path.clone());
                files_to_overwrite
                    .entry(file.manifest_path)
                    .or_default()
                    .push(file.file_path);
            }
        }

        // Any input file that vanished means our input set was mutated under
        // us. Committing now would remove files whose rows we did read while
        // leaving rows we did not — abort and let the caller retry against
        // fresh metadata.
        if still_live.len() != input_file_paths.len() {
            let missing = input_file_paths.len() - still_live.len();
            return Err(CommitError::SnapshotConflict(format!(
                "Delta commit conflict for {tenant_id}/{dataset_id}/{table_name} partition \
                 {partition_hours}: {missing} of {} input files are no longer live; another \
                 compaction or retention pass mutated them during the rewrite",
                input_file_paths.len()
            ))
            .into());
        }

        let mut table = table;
        table
            .new_transaction(None)
            .overwrite(new_files, files_to_overwrite)
            .commit()
            .await
            .map_err(classify_commit_error)
            .context("Failed to commit compaction delta snapshot")?;

        let committed_snapshot_id = Self::get_current_snapshot_id(&table)?;

        let verified = self
            .load_fresh(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to reload table for post-commit verification")?;
        let verified_snapshot_id = Self::get_current_snapshot_id(&verified)?;
        if verified_snapshot_id != committed_snapshot_id {
            return Err(CommitError::SnapshotConflict(format!(
                "Snapshot conflict: compaction delta commit did not take effect (expected \
                 snapshot {committed_snapshot_id}, catalog has {verified_snapshot_id}); a \
                 concurrent commit likely won the race"
            ))
            .into());
        }

        tracing::info!(
            tenant_id,
            dataset_id,
            table_name,
            partition_hours,
            snapshot_id = verified_snapshot_id,
            "Compaction delta commit verified"
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
    fn is_conflict_error_detects_snapshot_changed_message() {
        let error = anyhow::anyhow!("snapshot changed from 123 to 456");
        assert!(is_conflict_error(&error));
    }

    #[test]
    fn is_conflict_error_detects_snapshot_conflict_message() {
        let error = anyhow::anyhow!("Snapshot conflict detected");
        assert!(is_conflict_error(&error));
    }

    #[test]
    fn is_conflict_error_detects_concurrent_modification_message() {
        let error = anyhow::anyhow!("concurrent modification on snapshot");
        assert!(is_conflict_error(&error));
    }

    #[test]
    fn is_conflict_error_detects_version_mismatch_message() {
        let error = anyhow::anyhow!("version mismatch in snapshot");
        assert!(is_conflict_error(&error));
    }

    #[test]
    fn is_conflict_error_ignores_file_not_found() {
        let error = anyhow::anyhow!("file not found");
        assert!(!is_conflict_error(&error));
    }

    #[test]
    fn is_conflict_error_ignores_network_timeout() {
        let error = anyhow::anyhow!("network timeout");
        assert!(!is_conflict_error(&error));
    }

    #[test]
    fn is_conflict_error_ignores_invalid_schema() {
        let error = anyhow::anyhow!("invalid schema");
        assert!(!is_conflict_error(&error));
    }

    #[test]
    fn typed_snapshot_conflict_is_classified_without_message_keywords() {
        // The payload deliberately contains none of the substrings the
        // legacy text matcher looks for: classification must come from the
        // error type alone.
        let error = anyhow::Error::new(CommitError::SnapshotConflict("xyzzy".to_string()));
        assert!(
            is_conflict_error(&error),
            "typed SnapshotConflict must be classified as conflict regardless of message text"
        );
    }

    #[test]
    fn typed_snapshot_conflict_survives_context_wrapping() {
        // Callers (executor.rs) wrap commit errors in `.context(...)`;
        // anyhow's Display then renders only the outermost message, which
        // used to mask the conflict from the text matcher entirely.
        let error = anyhow::Error::new(CommitError::SnapshotConflict("xyzzy".to_string()))
            .context("Failed to commit compaction");
        assert!(
            is_conflict_error(&error),
            "context wrapping must not mask a typed SnapshotConflict"
        );
    }

    #[test]
    fn iceberg_conflict_message_is_detected_through_context_chain() {
        // Errors from the iceberg-rust fork are stringly-typed; when a
        // caller adds context, the conflict text is only visible in the
        // cause chain, not in the outermost Display.
        let error = anyhow::anyhow!("snapshot changed from 123 to 456")
            .context("Failed to commit compaction");
        assert!(
            is_conflict_error(&error),
            "stringly-typed conflict in the cause chain must be detected"
        );
    }

    #[test]
    fn unrelated_error_with_context_is_not_classified_as_conflict() {
        let error = anyhow::anyhow!("network timeout").context("Failed to commit compaction");
        assert!(
            !is_conflict_error(&error),
            "unrelated errors must not be classified as conflicts"
        );
    }

    #[test]
    fn table_requirements_failure_is_classified_as_conflict() {
        // The catalog's compare-and-swap assertion. Wrapped generically this
        // reads "Table requirements not valid doesn't have the right format",
        // which matches none of the text heuristics and used to be counted as
        // a permanent, non-retryable failure (#1065).
        let error = classify_commit_error(IcebergError::InvalidFormat(
            "Table requirements not valid".to_owned(),
        ));
        assert!(
            matches!(
                error.downcast_ref::<CommitError>(),
                Some(CommitError::SnapshotConflict(_))
            ),
            "catalog CAS failure must carry the typed SnapshotConflict marker"
        );
        assert!(is_conflict_error(&error));
    }

    #[test]
    fn table_requirements_failure_stays_a_conflict_under_context_wrapping() {
        let error = classify_commit_error(IcebergError::InvalidFormat(
            "Table requirements not valid".to_owned(),
        ))
        .context("Failed to commit compaction delta snapshot");
        assert!(is_conflict_error(&error));
    }

    #[test]
    fn catalog_commit_conflict_is_classified_as_conflict() {
        // The fork's other CAS shape: the SQL UPDATE guarded on the previous
        // metadata location matched no rows.
        let error =
            classify_commit_error(IcebergError::CommitConflict("acme.prod.traces".to_owned()));
        assert!(is_conflict_error(&error));
    }

    #[test]
    fn genuine_invalid_format_is_not_classified_as_conflict() {
        // Same variant, different cause — real malformed metadata must stay a
        // permanent failure.
        let error = classify_commit_error(IcebergError::InvalidFormat(
            "Table update on entity that is not a table".to_owned(),
        ));
        assert!(
            error.downcast_ref::<CommitError>().is_none(),
            "a genuine format error must not be marked as a conflict"
        );
        assert!(!is_conflict_error(&error));
    }

    #[test]
    fn unrelated_iceberg_error_is_preserved_verbatim() {
        let error = classify_commit_error(IcebergError::NotFound("manifest".to_owned()));
        assert!(!is_conflict_error(&error));
        assert!(error.downcast_ref::<IcebergError>().is_some());
    }
}
