//! Orphan file deletion with batch processing.
//!
//! This module implements safe batch deletion of orphan files with:
//! - Configurable batch sizes
//! - Progress tracking for resumability
//! - Dry-run mode for testing
//! - Rate limiting between batches

use crate::orphan::config::OrphanCleanupConfig;
use crate::orphan::detector::{OrphanCandidate, OrphanDetector};
use anyhow::{Context, Result};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Result of a deletion operation.
#[derive(Debug, Clone)]
pub struct DeletionResult {
    /// Number of files successfully deleted (0 in dry-run mode).
    pub deleted_count: usize,
    /// Number of files that failed to delete.
    pub failed_count: usize,
    /// Total bytes freed by deletion (0 in dry-run mode).
    pub total_bytes_freed: u64,
    /// Number of files that would be deleted in dry-run mode (0 in actual deletion).
    pub would_delete_count: usize,
    /// Total bytes that would be freed in dry-run mode (0 in actual deletion).
    pub would_free_bytes: u64,
    /// List of files that failed to delete with error messages.
    pub failed_deletions: Vec<(String, String)>,
}

/// Orphan file cleaner with batch processing support.
pub struct OrphanCleaner {
    config: OrphanCleanupConfig,
    object_store: Arc<dyn ObjectStore>,
    detector: Option<Arc<OrphanDetector>>,
}

impl OrphanCleaner {
    /// Create a new orphan cleaner **without** a detector.
    ///
    /// Only usable for dry runs. Re-validation is mandatory before every real
    /// deletion batch, so a cleaner built here with `dry_run = false` — which
    /// is [`OrphanCleanupConfig`]'s default — fails at the first batch. Use
    /// [`OrphanCleaner::with_detector`] for anything that actually deletes.
    pub fn new(config: OrphanCleanupConfig, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config,
            object_store,
            detector: None,
        }
    }

    /// Create a new orphan cleaner with detector for revalidation.
    pub fn with_detector(
        config: OrphanCleanupConfig,
        object_store: Arc<dyn ObjectStore>,
        detector: Arc<OrphanDetector>,
    ) -> Self {
        Self {
            config,
            object_store,
            detector: Some(detector),
        }
    }

    /// Delete orphan files in batches.
    ///
    /// Processes files in configurable batches with rate limiting between
    /// them. Every real deletion batch is re-validated first; a dry run skips
    /// that pass, since it deletes nothing.
    ///
    /// # Arguments
    ///
    /// * `candidates` - List of orphan candidates to delete
    ///
    /// # Returns
    ///
    /// Summary of deletion operation including counts and failures.
    ///
    /// # Errors
    ///
    /// Returns an error if the deletion operation cannot proceed (e.g.,
    /// object store unavailable). Individual file deletion failures are
    /// tracked in the result but do not fail the entire operation.
    pub async fn delete_orphans_batch(
        &self,
        candidates: Vec<OrphanCandidate>,
    ) -> Result<DeletionResult> {
        use tracing::Instrument;

        let span = tracing::info_span!(
            "orphan_delete_batch",
            signaldb.job.candidates = candidates.len() as i64,
            signaldb.job.files_deleted = tracing::field::Empty,
        );
        let record_span = span.clone();
        let result = self
            .delete_orphans_batch_inner(candidates)
            .instrument(span)
            .await;
        if let Ok(r) = &result {
            record_span.record("signaldb.job.files_deleted", r.deleted_count as i64);
        }
        result
    }

    async fn delete_orphans_batch_inner(
        &self,
        candidates: Vec<OrphanCandidate>,
    ) -> Result<DeletionResult> {
        if candidates.is_empty() {
            tracing::info!("No orphan candidates to delete");
            return Ok(DeletionResult {
                deleted_count: 0,
                failed_count: 0,
                total_bytes_freed: 0,
                would_delete_count: 0,
                would_free_bytes: 0,
                failed_deletions: vec![],
            });
        }

        tracing::info!(
            signaldb.job.candidates = candidates.len() as i64,
            signaldb.job.dry_run = self.config.dry_run,
            signaldb.job.batch_size = self.config.batch_size as i64,
            "Starting batch deletion of orphan files"
        );

        // Deletion runs strictly sequentially (batch-by-batch, rate-limited
        // in between), so these are plain counters, not atomics.
        let mut deleted_count: usize = 0;
        let mut total_bytes_freed: u64 = 0;
        let mut would_delete_count: usize = 0;
        let mut would_free_bytes: u64 = 0;
        let mut failed_deletions = vec![];

        // Process in batches
        let total_batches = candidates.len().div_ceil(self.config.batch_size);
        for (batch_idx, batch) in candidates.chunks(self.config.batch_size).enumerate() {
            tracing::info!(
                batch = batch_idx + 1,
                total_batches = total_batches,
                batch_size = batch.len(),
                dry_run = self.config.dry_run,
                "Processing deletion batch"
            );

            // Re-validation is unconditional before a real deletion:
            // detection is correct on its own (#925), and this is the
            // defense-in-depth pass on top of it. A dry run deletes nothing,
            // so there is nothing to guard — and it is the one mode that may
            // legitimately run without a detector attached.
            let validated_batch = if self.config.dry_run {
                batch.to_vec()
            } else {
                self.revalidate_batch(batch).await?
            };

            tracing::debug!(
                original_count = batch.len(),
                validated_count = validated_batch.len(),
                "Batch validation complete"
            );

            // Delete validated files
            for candidate in &validated_batch {
                if self.config.dry_run {
                    // Dry-run mode: log without deleting
                    tracing::info!(
                        path = %candidate.path,
                        size_bytes = candidate.size_bytes,
                        last_modified = %candidate.last_modified,
                        table = %candidate.table_identifier,
                        "[DRY-RUN] Would delete orphan file"
                    );

                    // Track dry-run metrics separately
                    would_delete_count += 1;
                    would_free_bytes += candidate.size_bytes as u64;
                } else {
                    // Actually delete the file
                    match self.delete_file(&candidate.path).await {
                        Ok(_) => {
                            tracing::info!(
                                path = %candidate.path,
                                size_bytes = candidate.size_bytes,
                                table = %candidate.table_identifier,
                                "Deleted orphan file"
                            );

                            deleted_count += 1;
                            total_bytes_freed += candidate.size_bytes as u64;
                        }
                        Err(e) => {
                            tracing::error!(
                                path = %candidate.path,
                                error = %e,
                                table = %candidate.table_identifier,
                                "Failed to delete orphan file"
                            );

                            failed_deletions.push((candidate.path.clone(), e.to_string()));
                        }
                    }
                }
            }

            // Rate limiting between batches
            if batch_idx + 1 < total_batches {
                tracing::debug!("Rate limiting between batches");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        let result = DeletionResult {
            deleted_count,
            failed_count: failed_deletions.len(),
            total_bytes_freed,
            would_delete_count,
            would_free_bytes,
            failed_deletions,
        };

        if self.config.dry_run {
            tracing::info!(
                signaldb.job.files_deleted = result.would_delete_count as i64,
                signaldb.job.bytes_reclaimed = result.would_free_bytes as i64,
                signaldb.job.deletion_failures = result.failed_count as i64,
                signaldb.job.dry_run = true,
                "Batch deletion complete"
            );
        } else {
            tracing::info!(
                signaldb.job.files_deleted = result.deleted_count as i64,
                signaldb.job.bytes_reclaimed = result.total_bytes_freed as i64,
                signaldb.job.deletion_failures = result.failed_count as i64,
                dry_run = false,
                "Batch deletion complete"
            );
        }

        Ok(result)
    }

    /// Revalidate a batch of orphan candidates before deletion.
    ///
    /// Checks each candidate against current table metadata to ensure
    /// it is still an orphan. This catches concurrent writes that may
    /// have referenced the file between detection and deletion.
    ///
    /// The live set is built **once per table per batch** — rebuilding it
    /// per candidate would read one manifest list per retained snapshot for
    /// every file, scaling object-store reads with candidates × snapshots.
    /// A table whose live set cannot be built has all of its candidates
    /// skipped for safety.
    async fn revalidate_batch(&self, batch: &[OrphanCandidate]) -> Result<Vec<OrphanCandidate>> {
        let detector = self.detector.as_ref().context(
            "Detector required for revalidation but not provided: \
                 build the cleaner with OrphanCleaner::with_detector, or set dry_run",
        )?;

        // One freshly-loaded live set per (table, candidate kind) in this
        // batch: metadata candidates check the metadata reference set, data
        // candidates the data-file set across all retained snapshots. `None`
        // marks a set that could not be built (bad identifier or load
        // failure): its candidates are skipped, never deleted blind.
        let mut live_sets: HashMap<(&str, bool), Option<HashSet<String>>> = HashMap::new();
        for candidate in batch {
            let identifier = candidate.table_identifier.as_str();
            let is_metadata = candidate.path.contains("/metadata/");
            if live_sets.contains_key(&(identifier, is_metadata)) {
                continue;
            }
            let parts: Vec<&str> = identifier.split('/').collect();
            let set = if let [tenant_id, dataset_id, table_name] = parts[..] {
                let built = if is_metadata {
                    detector
                        .live_metadata_set_for_table(tenant_id, dataset_id, table_name)
                        .await
                } else {
                    detector
                        .live_file_set_for_table(tenant_id, dataset_id, table_name)
                        .await
                };
                match built {
                    Ok(set) => Some(set),
                    Err(e) => {
                        tracing::error!(
                            table = %identifier,
                            error = %e,
                            "Revalidation failed, skipping table's candidates for safety"
                        );
                        None
                    }
                }
            } else {
                tracing::error!(
                    table_identifier = %identifier,
                    "Invalid table identifier format, skipping file"
                );
                None
            };
            live_sets.insert((identifier, is_metadata), set);
        }

        let mut validated = vec![];
        for candidate in batch {
            let key = (
                candidate.table_identifier.as_str(),
                candidate.path.contains("/metadata/"),
            );
            match live_sets.get(&key).and_then(|set| set.as_ref()) {
                None => {} // set unavailable: skipped for safety (logged above)
                Some(live) if live.contains(&candidate.path) => {
                    // File is now referenced, skip deletion
                    tracing::warn!(
                        path = %candidate.path,
                        table = %candidate.table_identifier,
                        "File no longer orphan after revalidation, skipping deletion"
                    );
                }
                Some(_) => validated.push(candidate.clone()),
            }
        }

        tracing::debug!(
            original = batch.len(),
            validated = validated.len(),
            skipped = batch.len() - validated.len(),
            "Revalidation complete"
        );

        Ok(validated)
    }

    /// Delete a single file from object storage.
    async fn delete_file(&self, path: &str) -> Result<()> {
        let object_path = ObjectPath::from(path);
        self.object_store
            .delete(&object_path)
            .await
            .with_context(|| format!("Failed to delete file: {}", path))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn test_empty_candidates() {
        let config = OrphanCleanupConfig::default();
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cleaner = OrphanCleaner::new(config, object_store);

        let result = cleaner.delete_orphans_batch(vec![]).await.unwrap();

        assert_eq!(result.deleted_count, 0);
        assert_eq!(result.failed_count, 0);
        assert_eq!(result.total_bytes_freed, 0);
        assert_eq!(result.would_delete_count, 0);
        assert_eq!(result.would_free_bytes, 0);
    }

    #[tokio::test]
    async fn test_dry_run_mode() {
        let config = OrphanCleanupConfig {
            dry_run: true,
            batch_size: 10,
            ..Default::default()
        };
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cleaner = OrphanCleaner::new(config, object_store);

        let candidates = vec![
            OrphanCandidate {
                path: "file1.parquet".to_string(),
                size_bytes: 1024,
                last_modified: Utc::now(),
                table_identifier: "tenant/dataset/table".to_string(),
            },
            OrphanCandidate {
                path: "file2.parquet".to_string(),
                size_bytes: 2048,
                last_modified: Utc::now(),
                table_identifier: "tenant/dataset/table".to_string(),
            },
        ];

        let result = cleaner.delete_orphans_batch(candidates).await.unwrap();

        // In dry-run mode, files should be tracked in would_delete fields, not deleted fields
        assert_eq!(result.deleted_count, 0);
        assert_eq!(result.total_bytes_freed, 0);
        assert_eq!(result.would_delete_count, 2);
        assert_eq!(result.would_free_bytes, 3072);
        assert_eq!(result.failed_count, 0);
    }

    #[tokio::test]
    async fn dry_run_mode_accounts_for_every_candidate_across_multiple_batches() {
        // Arrange: batch_size smaller than the candidate count forces
        // delete_orphans_batch to split the work across several batches.
        let config = OrphanCleanupConfig {
            dry_run: true,
            batch_size: 2,
            ..Default::default()
        };
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cleaner = OrphanCleaner::new(config, object_store);

        let candidates: Vec<OrphanCandidate> = (0..5)
            .map(|i| OrphanCandidate {
                path: format!("file{i}.parquet"),
                size_bytes: 1024,
                last_modified: Utc::now(),
                table_identifier: "tenant/dataset/table".to_string(),
            })
            .collect();

        // Act
        let result = cleaner.delete_orphans_batch(candidates).await.unwrap();

        // Assert: every candidate is accounted for exactly once, even though
        // it took three separate batches (2 + 2 + 1) to process them all.
        assert_eq!(result.would_delete_count, 5);
        assert_eq!(result.would_free_bytes, 5 * 1024);
    }
}
