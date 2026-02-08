//! Compaction execution orchestration
//!
//! Coordinates the compaction process: reading manifests, merging files,
//! writing output, and committing changes atomically.

use crate::commit::{DataFileChange, IcebergCommitter, is_conflict_error};
use crate::metrics::CompactionMetrics;
use crate::planner::{CompactionCandidate, PlannerConfig};
use crate::rewriter::ParquetRewriter;
use anyhow::{Context, Result};
use common::CatalogManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Information about a data file for compaction
#[derive(Debug, Clone)]
pub struct DataFileInfo {
    pub file_path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    pub partition_values: HashMap<String, String>,
}

/// A compaction job ready for execution
#[derive(Debug)]
pub struct CompactionJob {
    pub job_id: String,
    pub tenant_id: String,
    pub dataset_id: String,
    pub table_name: String,
    pub partition_id: String,
    pub input_files: Vec<DataFileInfo>,
    pub input_files_count: usize, // Expected count from candidate stats
    pub target_file_size_bytes: u64,
    pub created_at: Instant,
}

/// Result of a compaction job execution
#[derive(Debug)]
pub struct CompactionResult {
    pub job_id: String,
    pub status: CompactionStatus,
    pub input_files_count: usize,
    pub output_files_count: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub duration: Duration,
    pub error: Option<String>,
}

/// Status of a compaction job
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStatus {
    Success,
    Conflict,
    Failed,
}

/// Configuration for compaction execution
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub target_file_size_bytes: u64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB default
        }
    }
}

impl From<&PlannerConfig> for ExecutorConfig {
    fn from(config: &PlannerConfig) -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            target_file_size_bytes: config.target_file_size_bytes,
        }
    }
}

/// Orchestrates compaction job execution
pub struct CompactionExecutor {
    catalog_manager: Arc<CatalogManager>,
    committer: IcebergCommitter,
    rewriter: ParquetRewriter,
    metrics: CompactionMetrics,
    config: ExecutorConfig,
}

impl CompactionExecutor {
    /// Create a new compaction executor
    pub fn new(
        catalog_manager: Arc<CatalogManager>,
        config: ExecutorConfig,
        metrics: CompactionMetrics,
    ) -> Self {
        let committer = IcebergCommitter::new(catalog_manager.clone());
        let rewriter = ParquetRewriter::new(catalog_manager.clone());

        Self {
            catalog_manager,
            committer,
            rewriter,
            metrics,
            config,
        }
    }

    /// Execute compaction for a candidate
    pub async fn execute_candidate(
        &self,
        candidate: CompactionCandidate,
    ) -> Result<CompactionResult> {
        // Create a compaction job from the candidate
        let job = self.create_job(candidate).await?;

        // Execute the job with retry logic
        self.execute_job_with_retry(job).await
    }

    /// Create a compaction job from a candidate
    async fn create_job(&self, candidate: CompactionCandidate) -> Result<CompactionJob> {
        log::debug!(
            "Creating compaction job for {}/{}/{} partition {}",
            candidate.tenant_id,
            candidate.dataset_id,
            candidate.table_name,
            candidate.partition_id
        );

        // Generate a unique job ID
        let job_id = uuid::Uuid::new_v4().to_string();

        // For Phase 2, we'll use a simplified approach where input_files
        // are derived from the candidate's partition stats.
        // The actual file list will be read from manifests during execution.
        let input_files = vec![]; // Will be populated during execution

        let job = CompactionJob {
            job_id,
            tenant_id: candidate.tenant_id.clone(),
            dataset_id: candidate.dataset_id.clone(),
            table_name: candidate.table_name.clone(),
            partition_id: candidate.partition_id.clone(),
            input_files,
            input_files_count: candidate.stats.file_count,
            target_file_size_bytes: self.config.target_file_size_bytes,
            created_at: Instant::now(),
        };

        log::info!(
            "Created compaction job {} for table {}/{}/{}",
            job.job_id,
            job.tenant_id,
            job.dataset_id,
            job.table_name
        );

        Ok(job)
    }

    /// Execute a compaction job with retry logic for conflicts
    async fn execute_job_with_retry(&self, job: CompactionJob) -> Result<CompactionResult> {
        let job_id = job.job_id.clone();
        self.metrics.record_job_start();

        for attempt in 1..=self.config.max_retries {
            log::debug!(
                "Executing compaction job {} (attempt {}/{})",
                job_id,
                attempt,
                self.config.max_retries
            );

            match self.execute_job(&job).await {
                Ok(result) => {
                    if attempt > 1 {
                        log::info!(
                            "Compaction job {} succeeded after {} attempts",
                            job_id,
                            attempt
                        );
                    }

                    // Record success metrics
                    self.metrics.record_job_success(
                        result.input_files_count,
                        result.output_files_count,
                        result.bytes_before,
                        result.bytes_after,
                        result.duration,
                    );

                    return Ok(result);
                }
                Err(e) => {
                    // Check if this is a conflict error
                    if is_conflict_error(&e) {
                        self.metrics.record_conflict();

                        if attempt < self.config.max_retries {
                            // Calculate exponential backoff delay
                            let delay_ms = self.config.base_delay_ms * 2_u64.pow(attempt - 1);
                            let delay = Duration::from_millis(delay_ms);

                            log::warn!(
                                "Conflict detected for job {} (attempt {}/{}), retrying after {:?}",
                                job_id,
                                attempt,
                                self.config.max_retries,
                                delay
                            );

                            self.metrics.record_retry();
                            tokio::time::sleep(delay).await;
                            continue;
                        } else {
                            log::error!(
                                "Job {} failed after {} conflict retry attempts",
                                job_id,
                                self.config.max_retries
                            );

                            self.metrics.record_job_failure();

                            return Ok(CompactionResult {
                                job_id,
                                status: CompactionStatus::Conflict,
                                input_files_count: job.input_files_count,
                                output_files_count: 0,
                                bytes_before: 0,
                                bytes_after: 0,
                                duration: job.created_at.elapsed(),
                                error: Some(e.to_string()),
                            });
                        }
                    } else {
                        // Non-conflict error, fail immediately
                        log::error!("Job {} failed with non-conflict error: {}", job_id, e);

                        self.metrics.record_job_failure();

                        return Ok(CompactionResult {
                            job_id,
                            status: CompactionStatus::Failed,
                            input_files_count: job.input_files_count,
                            output_files_count: 0,
                            bytes_before: 0,
                            bytes_after: 0,
                            duration: job.created_at.elapsed(),
                            error: Some(e.to_string()),
                        });
                    }
                }
            }
        }

        // Should never reach here due to loop logic, but handle it
        self.metrics.record_job_failure();
        Ok(CompactionResult {
            job_id,
            status: CompactionStatus::Failed,
            input_files_count: job.input_files_count,
            output_files_count: 0,
            bytes_before: 0,
            bytes_after: 0,
            duration: job.created_at.elapsed(),
            error: Some("Max retries exceeded".to_string()),
        })
    }

    /// Execute a single compaction job (no retry logic)
    async fn execute_job(&self, job: &CompactionJob) -> Result<CompactionResult> {
        let start_time = Instant::now();

        log::info!(
            "Starting compaction job {}: table={}/{}/{}, partition={}",
            job.job_id,
            job.tenant_id,
            job.dataset_id,
            job.table_name,
            job.partition_id
        );

        // Step 1: Get the current snapshot ID before we start
        let table_identifier = self.catalog_manager.build_table_identifier(
            &job.tenant_id,
            &job.dataset_id,
            &job.table_name,
        );

        let catalog = self.catalog_manager.catalog();
        let table = catalog
            .load_tabular(&table_identifier)
            .await
            .with_context(|| {
                format!(
                    "Failed to load table {}/{}/{}",
                    job.tenant_id, job.dataset_id, job.table_name
                )
            })?;

        let table = match table {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected table but got view for {}/{}/{}",
                    job.tenant_id,
                    job.dataset_id,
                    job.table_name
                ));
            }
        };

        let original_snapshot_id = table.metadata().current_snapshot_id;

        log::debug!(
            "Job {}: Original snapshot ID: {:?}",
            job.job_id,
            original_snapshot_id
        );

        // Step 2: Read and merge files using the rewriter
        // For Phase 2, we'll use a simplified approach
        // where we assume the input files are already known
        let (output_files, input_size, output_size) = self
            .rewriter
            .compact_partition(
                &job.tenant_id,
                &job.dataset_id,
                &job.table_name,
                &job.partition_id,
                job.target_file_size_bytes,
            )
            .await
            .context("Failed to compact partition")?;

        log::debug!(
            "Job {}: Compacted {} input bytes into {} output bytes ({} files)",
            job.job_id,
            input_size,
            output_size,
            output_files.len()
        );

        // Step 3: Commit the changes atomically
        let new_files: Vec<DataFileChange> = output_files
            .iter()
            .map(|f| DataFileChange {
                file_path: f.clone(),
                size_bytes: 0, // Actual size will be tracked by Iceberg
                record_count: 0,
            })
            .collect();

        // Phase 2 limitation: We don't populate old_files because we lack manifest reading
        //
        // WARNING: This means old files are NOT removed from the Iceberg snapshot,
        // which has data correctness implications:
        // - Query engines will continue to read the old (uncompacted) files
        // - Storage is not reclaimed
        // - Compaction provides no benefit
        //
        // TODO(Phase 3): Implement manifest reading in planner to:
        // 1. Get actual list of files in the partition being compacted
        // 2. Track which files were read during compaction
        // 3. Pass those file paths here as old_files to be removed
        // 4. This enables proper file replacement: add new, remove old
        //
        // For Phase 2 testing, this limitation is acceptable to validate the
        // overall execution flow, but Phase 3 must address this for production use.
        let old_files = vec![];

        log::warn!(
            "Phase 2 limitation: old_files not populated - compacted files will not be removed from snapshot. \
             This is expected for Phase 2 testing but must be fixed in Phase 3."
        );

        self.committer
            .commit_compaction(
                &job.tenant_id,
                &job.dataset_id,
                &job.table_name,
                original_snapshot_id,
                new_files,
                old_files,
            )
            .await
            .context("Failed to commit compaction")?;

        let duration = start_time.elapsed();

        log::info!(
            "Compaction job {} completed: {} input bytes → {} output bytes ({} files), duration={:?}",
            job.job_id,
            input_size,
            output_size,
            output_files.len(),
            duration
        );

        Ok(CompactionResult {
            job_id: job.job_id.clone(),
            status: CompactionStatus::Success,
            input_files_count: job.input_files_count,
            output_files_count: output_files.len(),
            bytes_before: input_size,
            bytes_after: output_size,
            duration,
            error: None,
        })
    }

    /// Get the metrics tracker
    pub fn metrics(&self) -> &CompactionMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_config_default() {
        let config = ExecutorConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay_ms, 100);
    }

    #[test]
    fn test_data_file_info() {
        let mut partition_values = HashMap::new();
        partition_values.insert("year".to_string(), "2026".to_string());
        partition_values.insert("month".to_string(), "02".to_string());

        let file_info = DataFileInfo {
            file_path: "data/file1.parquet".to_string(),
            size_bytes: 1024 * 1024,
            record_count: 10000,
            partition_values,
        };

        assert_eq!(file_info.file_path, "data/file1.parquet");
        assert_eq!(file_info.size_bytes, 1024 * 1024);
        assert_eq!(file_info.record_count, 10000);
        assert_eq!(file_info.partition_values.len(), 2);
    }

    #[tokio::test]
    async fn test_executor_creation() {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let config = ExecutorConfig::default();
        let metrics = CompactionMetrics::new();

        let executor = CompactionExecutor::new(catalog_manager, config, metrics);

        assert_eq!(executor.config.max_retries, 3);
        assert_eq!(executor.metrics().jobs_started(), 0);
    }
}
