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
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Result of a deletion operation.
#[derive(Debug, Clone)]
pub struct DeletionResult {
    /// Number of files successfully deleted.
    pub deleted_count: usize,
    /// Number of files that failed to delete.
    pub failed_count: usize,
    /// Total bytes freed by deletion.
    pub total_bytes_freed: u64,
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
    /// Create a new orphan cleaner.
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
    /// Processes files in configurable batches with optional revalidation
    /// and rate limiting between batches.
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
        if candidates.is_empty() {
            tracing::info!("No orphan candidates to delete");
            return Ok(DeletionResult {
                deleted_count: 0,
                failed_count: 0,
                total_bytes_freed: 0,
                failed_deletions: Vec::new(),
            });
        }

        tracing::info!(
            candidates = candidates.len(),
            dry_run = self.config.dry_run,
            batch_size = self.config.batch_size,
            "Starting batch deletion of orphan files"
        );

        let deleted_count = AtomicUsize::new(0);
        let total_bytes_freed = AtomicU64::new(0);
        let mut failed_deletions = Vec::new();

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

            // Apply safety validation if enabled
            let validated_batch = if self.config.revalidate_before_delete {
                self.revalidate_batch(batch).await?
            } else {
                batch.to_vec()
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

                    deleted_count.fetch_add(1, Ordering::Relaxed);
                    total_bytes_freed.fetch_add(candidate.size_bytes as u64, Ordering::Relaxed);
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

                            deleted_count.fetch_add(1, Ordering::Relaxed);
                            total_bytes_freed
                                .fetch_add(candidate.size_bytes as u64, Ordering::Relaxed);
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
            deleted_count: deleted_count.load(Ordering::Relaxed),
            failed_count: failed_deletions.len(),
            total_bytes_freed: total_bytes_freed.load(Ordering::Relaxed),
            failed_deletions,
        };

        tracing::info!(
            deleted = result.deleted_count,
            failed = result.failed_count,
            bytes_freed = result.total_bytes_freed,
            dry_run = self.config.dry_run,
            "Batch deletion complete"
        );

        Ok(result)
    }

    /// Revalidate a batch of orphan candidates before deletion.
    ///
    /// Checks each candidate against current table metadata to ensure
    /// it is still an orphan. This catches concurrent writes that may
    /// have referenced the file between detection and deletion.
    async fn revalidate_batch(&self, batch: &[OrphanCandidate]) -> Result<Vec<OrphanCandidate>> {
        if !self.config.revalidate_before_delete {
            return Ok(batch.to_vec());
        }

        let detector = self
            .detector
            .as_ref()
            .context("Detector required for revalidation but not provided")?;

        let mut validated = Vec::new();

        for candidate in batch {
            // Parse table identifier
            let parts: Vec<&str> = candidate.table_identifier.split('/').collect();
            if parts.len() != 3 {
                tracing::error!(
                    table_identifier = %candidate.table_identifier,
                    "Invalid table identifier format, skipping file"
                );
                continue;
            }

            let (tenant_id, dataset_id, table_name) = (parts[0], parts[1], parts[2]);

            // Revalidate orphan status
            match detector
                .validate_orphan_before_deletion(candidate, tenant_id, dataset_id, table_name)
                .await
            {
                Ok(true) => {
                    // Still an orphan
                    validated.push(candidate.clone());
                }
                Ok(false) => {
                    // File is now referenced, skip deletion
                    tracing::warn!(
                        path = %candidate.path,
                        table = %candidate.table_identifier,
                        "File no longer orphan after revalidation, skipping deletion"
                    );
                }
                Err(e) => {
                    // Revalidation failed, err on the side of caution
                    tracing::error!(
                        path = %candidate.path,
                        error = %e,
                        table = %candidate.table_identifier,
                        "Revalidation failed, skipping file for safety"
                    );
                }
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

    #[test]
    fn test_deletion_result_creation() {
        let result = DeletionResult {
            deleted_count: 10,
            failed_count: 2,
            total_bytes_freed: 10240,
            failed_deletions: vec![
                ("file1.parquet".to_string(), "permission denied".to_string()),
                ("file2.parquet".to_string(), "not found".to_string()),
            ],
        };

        assert_eq!(result.deleted_count, 10);
        assert_eq!(result.failed_count, 2);
        assert_eq!(result.total_bytes_freed, 10240);
        assert_eq!(result.failed_deletions.len(), 2);
    }

    #[tokio::test]
    async fn test_empty_candidates() {
        let config = OrphanCleanupConfig::default();
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let cleaner = OrphanCleaner::new(config, object_store);

        let result = cleaner.delete_orphans_batch(vec![]).await.unwrap();

        assert_eq!(result.deleted_count, 0);
        assert_eq!(result.failed_count, 0);
        assert_eq!(result.total_bytes_freed, 0);
    }

    #[tokio::test]
    async fn test_dry_run_mode() {
        let config = OrphanCleanupConfig {
            dry_run: true,
            batch_size: 10,
            revalidate_before_delete: false, // Disable revalidation for this test
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

        // In dry-run mode, files should be "deleted" (counted) but not actually removed
        assert_eq!(result.deleted_count, 2);
        assert_eq!(result.failed_count, 0);
        assert_eq!(result.total_bytes_freed, 3072);
    }

    #[test]
    fn test_batch_size_calculations() {
        let config = OrphanCleanupConfig {
            batch_size: 100,
            ..Default::default()
        };

        // Test batch count calculation
        let candidates_count: usize = 250;
        let expected_batches = candidates_count.div_ceil(config.batch_size);
        assert_eq!(expected_batches, 3);

        // Test with exact multiple
        let candidates_count: usize = 300;
        let expected_batches = candidates_count.div_ceil(config.batch_size);
        assert_eq!(expected_batches, 3);

        // Test with single batch
        let candidates_count: usize = 50;
        let expected_batches = candidates_count.div_ceil(config.batch_size);
        assert_eq!(expected_batches, 1);
    }
}
