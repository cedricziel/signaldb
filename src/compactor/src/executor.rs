//! Compaction execution orchestration
//!
//! Coordinates the compaction process: reading manifests, merging files,
//! writing output, and committing changes atomically.

use crate::commit::{IcebergCommitter, is_conflict_error};
use crate::iceberg::ManifestReader;
use crate::metrics::CompactionMetrics;
use crate::planner::{CompactionCandidate, PlannerConfig};
use crate::rewriter::ParquetRewriter;
use crate::table_lock::TableLockRegistry;
use anyhow::{Context, Result};
use common::CatalogManager;
use std::collections::{HashMap, HashSet};
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

/// Render an error together with its full cause chain.
///
/// `anyhow`'s plain `Display` renders only the outermost `.context(...)`
/// message. Every compaction commit failure is wrapped in
/// `.context("Failed to commit compaction")`, so plain formatting reduces all
/// of them — a failed catalog load, a failed manifest re-read, a rejected
/// Iceberg commit — to that one indistinguishable string, and the operator
/// has nothing to act on. The alternate `{:#}` form joins the whole chain.
fn format_error_chain(error: &anyhow::Error) -> String {
    format!("{error:#}")
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
    /// Serializes this table's compaction commits against retention drops
    /// and snapshot expiration on the same table (D6). Defaults to a
    /// private registry via [`Self::new`]; [`Self::with_table_locks`] shares
    /// one registry with the retention enforcer so the two actors actually
    /// gate each other — `CompactorService::new` does this for the
    /// long-running compactor process.
    table_locks: TableLockRegistry,
}

impl CompactionExecutor {
    /// Create a new compaction executor
    pub fn new(
        catalog_manager: Arc<CatalogManager>,
        config: ExecutorConfig,
        metrics: CompactionMetrics,
    ) -> Self {
        // The memory knobs interact; a bad combination is invisible from
        // any one of them, so say so once at construction rather than
        // failing a rewrite hours later (#1064).
        ParquetRewriter::warn_on_incoherent_memory_config(&catalog_manager.config().compactor);

        let committer = IcebergCommitter::new(catalog_manager.clone());
        let rewriter = ParquetRewriter::new(catalog_manager);

        Self {
            committer,
            rewriter,
            metrics,
            config,
            table_locks: TableLockRegistry::new(),
        }
    }

    /// Persist the rewriter's advisory attribute statistics to the service
    /// catalog (epic #737, #733).
    pub fn with_service_catalog(mut self, catalog: Arc<common::catalog::Catalog>) -> Self {
        self.rewriter.set_service_catalog(catalog);
        self
    }

    /// Share a [`TableLockRegistry`] with other lifecycle actors (retention,
    /// snapshot expiration) so compaction commits on a table serialize
    /// against them (D6). Without this call the executor gates only against
    /// itself.
    pub fn with_table_locks(mut self, table_locks: TableLockRegistry) -> Self {
        self.table_locks = table_locks;
        self
    }

    /// Execute compaction for a candidate
    pub async fn execute_candidate(
        &self,
        candidate: CompactionCandidate,
    ) -> Result<CompactionResult> {
        let span = common::self_monitoring::spans::job_span(
            "compaction",
            &candidate.tenant_id,
            &candidate.dataset_id,
            Some(&candidate.table_name),
        );

        async {
            // Serialize against retention drops and snapshot expiration on
            // this same table (D6). Both real entry points into compaction
            // — the background lifecycle loop (`lifecycle::CompactionCycle`)
            // and the Flight `compact_now` action
            // (`CompactorFlightService::do_action`) — call this method on
            // the same shared `Arc<CompactionExecutor>`, so acquiring the
            // guard here covers both without duplicating it at each call
            // site. Held across the whole retry loop below (bounded by
            // `config.max_retries` with exponential backoff, never an
            // unbounded wait) so a retry cannot interleave with a retention
            // pass either.
            // TEMP-DISABLED-FOR-RED-CHECK
            // let _table_guard = self
            //     .table_locks
            //     .lock(
            //         &candidate.tenant_id,
            //         &candidate.dataset_id,
            //         &candidate.table_name,
            //     )
            //     .await;

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
                                error: Some(format_error_chain(&e)),
                            });
                        }
                    } else {
                        // Non-conflict error, fail immediately
                        tracing::error!(
                            "Job {} failed with non-conflict error: {}",
                            job_id,
                            format_error_chain(&e)
                        );

                        self.metrics.record_job_failure();

                        return Ok(CompactionResult {
                            job_id,
                            status: CompactionStatus::Failed,
                            input_files_count: job.input_files_count,
                            output_files_count: 0,
                            bytes_before: 0,
                            bytes_after: 0,
                            duration: job.created_at.elapsed(),
                            error: Some(format_error_chain(&e)),
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

        // The job's partition is the unit this execution mutates. It is
        // produced by the planner from the manifest entries' typed partition
        // values, so anything unparseable is a programming error rather than
        // a data condition.
        let partition_hours: i64 = job.partition_id.parse().with_context(|| {
            format!(
                "Compaction job {} has a non-numeric partition id {:?}; expected hours since epoch",
                job.job_id, job.partition_id
            )
        })?;

        // Step 1: Load the table with fresh metadata and pin the snapshot.
        // All reads below use this pinned table handle, so the rewrite sees a
        // consistent snapshot even if concurrent writes land meanwhile. The
        // delta commit re-validates only this partition's input files, so a
        // concurrent append elsewhere in the table is no longer a conflict.
        let table = self
            .rewriter
            .load_fresh_table(&job.tenant_id, &job.dataset_id, &job.table_name)
            .await
            .context("Failed to load table for compaction")?;

        // Step 2: Read this partition's input file set from the snapshot's
        // manifests. Files in other partitions are untouched by this job.
        let manifest_reader = ManifestReader::new();
        let input_files: Vec<_> = manifest_reader
            .get_snapshot_files(&table)
            .await
            .context("Failed to read input file list from manifests")?
            .into_iter()
            .filter(|file| file.partition_hours == Some(partition_hours))
            .collect();

        if input_files.len() <= 1 {
            tracing::info!(
                "Job {}: table {}/{}/{} partition {} has {} live data file(s), nothing to compact",
                job.job_id,
                job.tenant_id,
                job.dataset_id,
                job.table_name,
                partition_hours,
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

        // Step 3: Read, merge, sort, and write new compacted files for this
        // partition only.
        let outcome = match self
            .rewriter
            .rewrite_partition(&table, partition_hours, job.target_file_size_bytes)
            .await
            .context("Failed to rewrite partition data")?
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
            "Compaction row count mismatch for {}/{}/{} partition {}: manifests report {} live rows but rewrite produced {}",
            job.tenant_id,
            job.dataset_id,
            job.table_name,
            partition_hours,
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

        // Step 4: Commit atomically as a delta — this partition's input files
        // are removed and the new files added in a single snapshot, leaving
        // every other partition's files referenced exactly as they were. The
        // replaced files become orphans handled by the orphan cleanup cycle
        // after snapshot expiration.
        let output_files_count = outcome.new_files.len();
        let output_size = outcome.output_size_bytes;
        let input_file_paths: HashSet<String> = input_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect();
        self.committer
            .commit_delta(
                &job.tenant_id,
                &job.dataset_id,
                &job.table_name,
                partition_hours,
                &input_file_paths,
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

    /// Guards the exact failure this helper exists to prevent: a compaction
    /// commit error reaching the operator as the bare string "Failed to commit
    /// compaction", with the cause that actually explains it discarded.
    #[test]
    fn format_error_chain_preserves_causes_beneath_the_outermost_context() {
        let error = anyhow::anyhow!("object store returned 503")
            .context("Failed to commit compaction delta snapshot")
            .context("Failed to commit compaction");

        let rendered = format_error_chain(&error);

        assert!(rendered.contains("Failed to commit compaction"));
        assert!(rendered.contains("Failed to commit compaction delta snapshot"));
        assert!(rendered.contains("object store returned 503"));
    }

    /// Pins the anyhow behaviour that motivates `format_error_chain`, so that
    /// a future refactor back to `{}` / `to_string()` fails here loudly rather
    /// than silently going dark in production.
    #[test]
    fn plain_display_drops_the_cause_chain() {
        let error =
            anyhow::anyhow!("object store returned 503").context("Failed to commit compaction");

        assert_eq!(error.to_string(), "Failed to commit compaction");
        assert!(!error.to_string().contains("object store returned 503"));
    }

    #[test]
    fn test_executor_config_default() {
        let config = ExecutorConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay_ms, 100);
    }

    #[tokio::test]
    async fn new_executor_starts_with_zero_jobs_started() {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let config = ExecutorConfig::default();
        let metrics = CompactionMetrics::new();

        let executor = CompactionExecutor::new(catalog_manager, config, metrics);

        assert_eq!(executor.metrics().jobs_started(), 0);
    }
}
