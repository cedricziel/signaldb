//! Compaction execution orchestration
//!
//! Coordinates the compaction process: reading manifests, merging files,
//! writing output, and committing changes atomically.

use crate::commit::{IcebergCommitter, is_conflict_error};
use crate::iceberg::ManifestReader;
use crate::metrics::CompactionMetrics;
use crate::planner::{CompactionCandidate, PlannerConfig};
use crate::rewriter::ParquetRewriter;
use anyhow::{Context, Result};
use common::CatalogManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::Instrument;

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
        let rewriter = ParquetRewriter::new(catalog_manager);

        Self {
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
        let span = tracing::info_span!(
            "compaction_job",
            tenant_id = %candidate.tenant_id,
            dataset_id = %candidate.dataset_id,
            table = %candidate.table_name,
            partition = %candidate.partition_id,
        );

        async {
            // Create a compaction job from the candidate
            let job = self.create_job(candidate).await?;

            // Execute the job with retry logic
            self.execute_job_with_retry(job).await
        }
        .instrument(span)
        .await
    }

    /// Create a compaction job from a candidate
    async fn create_job(&self, candidate: CompactionCandidate) -> Result<CompactionJob> {
        tracing::debug!(
            "Creating compaction job for {}/{}/{} partition {}",
            candidate.tenant_id,
            candidate.dataset_id,
            candidate.table_name,
            candidate.partition_id
        );

        // Generate a unique job ID
        let job_id = uuid::Uuid::new_v4().to_string();

        // The real input file list is read from the snapshot's manifests
        // during execution; the candidate only carries planner estimates.
        let input_files = vec![];

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

        tracing::info!(
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
            tracing::debug!(
                "Executing compaction job {} (attempt {}/{})",
                job_id,
                attempt,
                self.config.max_retries
            );

            match self.execute_job(&job).await {
                Ok(result) => {
                    if attempt > 1 {
                        tracing::info!(
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

                            tracing::warn!(
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
                            tracing::error!(
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
                        tracing::error!("Job {} failed with non-conflict error: {}", job_id, e);

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

        tracing::info!(
            "Starting compaction job {}: table={}/{}/{}, partition={}",
            job.job_id,
            job.tenant_id,
            job.dataset_id,
            job.table_name,
            job.partition_id
        );

        // Step 1: Load the table with fresh metadata and pin the snapshot.
        // All reads below use this pinned table handle, so the rewrite sees a
        // consistent snapshot even if concurrent writes land meanwhile; the
        // commit detects such writes as conflicts and the job retries.
        let table = self
            .rewriter
            .load_fresh_table(&job.tenant_id, &job.dataset_id, &job.table_name)
            .await
            .context("Failed to load table for compaction")?;

        let original_snapshot_id = table.metadata().current_snapshot_id;

        tracing::debug!(
            "Job {}: Original snapshot ID: {:?}",
            job.job_id,
            original_snapshot_id
        );

        // Step 2: Read the real input file set from the snapshot's manifests.
        let manifest_reader = ManifestReader::new();
        let input_files = manifest_reader
            .get_snapshot_files(&table)
            .await
            .context("Failed to read input file list from manifests")?;

        if input_files.len() <= 1 {
            tracing::info!(
                "Job {}: table {}/{}/{} has {} live data file(s), nothing to compact",
                job.job_id,
                job.tenant_id,
                job.dataset_id,
                job.table_name,
                input_files.len()
            );
            return Ok(CompactionResult {
                job_id: job.job_id.clone(),
                status: CompactionStatus::Success,
                input_files_count: input_files.len(),
                output_files_count: input_files.len(),
                bytes_before: 0,
                bytes_after: 0,
                duration: start_time.elapsed(),
                error: None,
            });
        }

        let input_size: u64 = input_files.iter().map(|f| f.file_size_bytes).sum();
        let input_rows: u64 = input_files.iter().map(|f| f.record_count).sum();

        // Step 3: Read, merge, sort, and write new compacted files.
        let outcome = match self
            .rewriter
            .rewrite_table(&table, job.target_file_size_bytes)
            .await
            .context("Failed to rewrite table data")?
        {
            Some(outcome) => outcome,
            None => {
                return Ok(CompactionResult {
                    job_id: job.job_id.clone(),
                    status: CompactionStatus::Success,
                    input_files_count: input_files.len(),
                    output_files_count: 0,
                    bytes_before: 0,
                    bytes_after: 0,
                    duration: start_time.elapsed(),
                    error: None,
                });
            }
        };

        // Defense in depth: the rewritten files must account for every row
        // the manifests said was live. Abort before committing otherwise.
        anyhow::ensure!(
            outcome.rows_written == input_rows,
            "Compaction row count mismatch for {}/{}/{}: manifests report {} live rows but rewrite produced {}",
            job.tenant_id,
            job.dataset_id,
            job.table_name,
            input_rows,
            outcome.rows_written
        );

        tracing::debug!(
            "Job {}: Rewrote {} input files ({} bytes) into {} output files ({} bytes)",
            job.job_id,
            input_files.len(),
            input_size,
            outcome.new_files.len(),
            outcome.output_size_bytes
        );

        // Step 4: Commit atomically — the new files replace all previous data
        // files in a single snapshot; old files become orphans handled by the
        // orphan cleanup cycle after snapshot expiration.
        let output_files_count = outcome.new_files.len();
        let output_size = outcome.output_size_bytes;
        self.committer
            .commit_compaction(
                &job.tenant_id,
                &job.dataset_id,
                &job.table_name,
                original_snapshot_id,
                outcome.new_files,
            )
            .await
            .context("Failed to commit compaction")?;

        let duration = start_time.elapsed();

        tracing::info!(
            "Compaction job {} completed: {} files ({} bytes) → {} files ({} bytes), duration={:?}",
            job.job_id,
            input_files.len(),
            input_size,
            output_files_count,
            output_size,
            duration
        );

        Ok(CompactionResult {
            job_id: job.job_id.clone(),
            status: CompactionStatus::Success,
            input_files_count: input_files.len(),
            output_files_count,
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
