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

/// How a failed compaction attempt should be handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryClass {
    /// A concurrent commit won the optimistic-concurrency race. Retrying
    /// re-reads fresh metadata and can succeed.
    Conflict,
    /// A transient infrastructure failure — object store blip, network hiccup,
    /// catalog contention. The identical work can succeed on a later attempt.
    Transient,
    /// A deterministic failure — validation, schema, malformed input. Retrying
    /// burns another full rewrite to arrive at the same error.
    Terminal,
}

impl RetryClass {
    fn is_retryable(self) -> bool {
        matches!(self, RetryClass::Conflict | RetryClass::Transient)
    }

    fn label(self) -> &'static str {
        match self {
            RetryClass::Conflict => "conflict",
            RetryClass::Transient => "transient",
            RetryClass::Terminal => "terminal",
        }
    }
}

/// Substrings that identify a transient failure in errors that carry no type
/// we can match on — the SQL catalog and the iceberg fork both surface network
/// and contention failures as opaque strings, and object store backends render
/// HTTP status text into the message.
///
/// Only positive evidence promotes an error to [`RetryClass::Transient`];
/// anything unrecognized stays terminal and fails fast. A deterministic error
/// whose message happens to contain one of these words costs at most
/// `max_retries` rewrites, which is why the list avoids bare status codes and
/// other strings that also occur in byte counts or file names.
const TRANSIENT_MARKERS: &[&str] = &[
    "timed out",
    "timeout",
    "connection reset",
    "connection refused",
    "connection closed",
    "connection aborted",
    "broken pipe",
    "temporarily unavailable",
    "service unavailable",
    "internal server error",
    "bad gateway",
    "gateway timeout",
    "too many requests",
    "slow down",
    "throttl",
    "database is locked",
    "deadlock",
];

/// Classify a failed compaction attempt (#1052).
///
/// Conflicts are diagnosed by [`is_conflict_error`], which owns that
/// definition. Everything else is transient only on positive evidence — a
/// typed object store or I/O failure known to be recoverable, or a message
/// carrying one of [`TRANSIENT_MARKERS`]. Unrecognized errors stay terminal,
/// so a new deterministic failure mode never silently costs three rewrites.
fn classify_error(error: &anyhow::Error) -> RetryClass {
    if is_conflict_error(error) {
        return RetryClass::Conflict;
    }

    for cause in error.chain() {
        if let Some(store_error) = cause.downcast_ref::<object_store::Error>()
            && object_store_error_is_transient(store_error)
        {
            return RetryClass::Transient;
        }

        if let Some(io_error) = cause.downcast_ref::<std::io::Error>()
            && io_error_is_transient(io_error)
        {
            return RetryClass::Transient;
        }

        let message = cause.to_string().to_lowercase();
        if TRANSIENT_MARKERS
            .iter()
            .any(|marker| message.contains(marker))
        {
            return RetryClass::Transient;
        }
    }

    RetryClass::Terminal
}

/// `Generic` is the backends' catch-all, and in practice it is where network
/// failures and 5xx responses land; `JoinError` is a runtime hiccup, and
/// `Precondition` is a lost conditional-write race that a retry re-reads
/// around. Treating the catch-all as retryable costs at most `max_retries`
/// attempts when a backend files a deterministic 4xx under it. The remaining
/// variants describe the request itself — a missing key, a bad path, denied
/// credentials — and repeat identically on a retry.
fn object_store_error_is_transient(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::Generic { .. }
            | object_store::Error::JoinError { .. }
            | object_store::Error::Precondition { .. }
    )
}

fn io_error_is_transient(error: &std::io::Error) -> bool {
    use std::io::ErrorKind;

    matches!(
        error.kind(),
        ErrorKind::TimedOut
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionRefused
            | ErrorKind::NotConnected
            | ErrorKind::BrokenPipe
            | ErrorKind::Interrupted
            | ErrorKind::WouldBlock
    )
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
        let value_sketch_size = catalog_manager.config().compactor.value_sketch_size;
        let mut rewriter = ParquetRewriter::new(catalog_manager);
        rewriter.set_value_sketch_size(value_sketch_size);

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
            let _table_guard = self
                .table_locks
                .lock(
                    &candidate.tenant_id,
                    &candidate.dataset_id,
                    &candidate.table_name,
                )
                .await;

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

    /// Execute a compaction job, retrying conflicts and transient failures
    async fn execute_job_with_retry(&self, job: CompactionJob) -> Result<CompactionResult> {
        self.metrics.record_job_start(&job_scope(&job));
        self.run_with_retry(&job, |_attempt| self.execute_job(&job))
            .await
    }

    /// Drive a job's attempts under the retry policy: conflicts and transient
    /// infrastructure failures share the backoff budget, deterministic
    /// failures fail on their first attempt rather than paying for another
    /// full rewrite (#1052).
    ///
    /// The attempt is a closure so the policy itself is exercisable without a
    /// live catalog and object store.
    async fn run_with_retry<F, Fut>(
        &self,
        job: &CompactionJob,
        mut run_attempt: F,
    ) -> Result<CompactionResult>
    where
        F: FnMut(u32) -> Fut,
        Fut: Future<Output = Result<CompactionResult>>,
    {
        let mut attempt: u32 = 1;

        loop {
            tracing::debug!(
                job_id = %job.job_id,
                attempt,
                max_retries = self.config.max_retries,
                "Executing compaction job"
            );

            match run_attempt(attempt).await {
                Ok(result) => {
                    if attempt > 1 {
                        tracing::info!(
                            job_id = %job.job_id,
                            attempts = attempt,
                            "Compaction job succeeded after retrying"
                        );
                    }

                    self.metrics.record_job_success(
                        &job_scope(job),
                        result.input_files_count,
                        result.output_files_count,
                        result.bytes_before,
                        result.bytes_after,
                        result.duration,
                    );

                    return Ok(result);
                }
                Err(e) => {
                    let class = classify_error(&e);
                    let retryable = class.is_retryable();
                    if class == RetryClass::Conflict {
                        self.metrics.record_conflict();
                    }

                    if retryable && attempt < self.config.max_retries {
                        let delay_ms = self.config.base_delay_ms * 2_u64.pow(attempt - 1);

                        tracing::info!(
                            job_id = %job.job_id,
                            error_class = class.label(),
                            attempt,
                            max_retries = self.config.max_retries,
                            delay_ms,
                            error = %format_error_chain(&e),
                            "Retrying compaction job after retryable failure"
                        );

                        self.metrics.record_retry();
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        attempt += 1;
                        continue;
                    }

                    if retryable {
                        tracing::error!(
                            job_id = %job.job_id,
                            error_class = class.label(),
                            attempts = attempt,
                            error = %format_error_chain(&e),
                            "Compaction job failed after exhausting retries"
                        );
                    } else {
                        tracing::error!(
                            job_id = %job.job_id,
                            error_class = class.label(),
                            error = %format_error_chain(&e),
                            "Compaction job failed with a terminal error, not retrying"
                        );
                    }

                    self.metrics
                        .record_job_failure(&job_scope(job), class.label());

                    return Ok(CompactionResult {
                        job_id: job.job_id.clone(),
                        status: if class == RetryClass::Conflict {
                            CompactionStatus::Conflict
                        } else {
                            CompactionStatus::Failed
                        },
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

/// The label set a job's metrics are counted under.
fn job_scope(job: &CompactionJob) -> crate::metrics::JobScope {
    crate::metrics::JobScope::new(&job.tenant_id, &job.dataset_id, &job.table_name)
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
    fn snapshot_races_are_classified_as_conflicts() {
        let error =
            anyhow::anyhow!("snapshot changed from 1 to 2").context("Failed to commit compaction");

        assert_eq!(classify_error(&error), RetryClass::Conflict);
    }

    #[test]
    fn object_store_generic_failures_are_transient() {
        let error = anyhow::Error::new(object_store::Error::Generic {
            store: "S3",
            source: "503 Service Unavailable".into(),
        })
        .context("Failed to rewrite partition data");

        assert_eq!(classify_error(&error), RetryClass::Transient);
    }

    #[test]
    fn io_timeouts_are_transient() {
        let error = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "catalog request timed out",
        ))
        .context("Failed to commit compaction");

        assert_eq!(classify_error(&error), RetryClass::Transient);
    }

    /// The catalog and the iceberg fork surface transient failures as
    /// stringly-typed errors buried under `.context(...)` layers, so the
    /// textual fallback has to scan the whole cause chain.
    #[test]
    fn transient_markers_are_detected_beneath_context_layers() {
        let error = anyhow::anyhow!("connection reset by peer")
            .context("Failed to commit compaction delta snapshot")
            .context("Failed to commit compaction");

        assert_eq!(classify_error(&error), RetryClass::Transient);
    }

    /// The row-count guard is a deterministic invariant check: a retry re-runs
    /// the full rewrite only to fail identically.
    #[test]
    fn row_count_mismatch_is_terminal() {
        let error = anyhow::anyhow!(
            "Compaction row count mismatch for acme/prod/traces partition 480000: manifests report 12 live rows but rewrite produced 11"
        )
        .context("compaction failed");

        assert_eq!(classify_error(&error), RetryClass::Terminal);
    }

    #[test]
    fn schema_errors_are_terminal() {
        let error = anyhow::anyhow!("Schema error: field 'span_name' not found in input files");

        assert_eq!(classify_error(&error), RetryClass::Terminal);
    }

    #[test]
    fn object_store_not_found_is_terminal() {
        let error = anyhow::Error::new(object_store::Error::NotFound {
            path: "traces/data/file.parquet".to_string(),
            source: "no such key".into(),
        });

        assert_eq!(classify_error(&error), RetryClass::Terminal);
    }

    #[test]
    fn only_conflict_and_transient_classes_are_retried() {
        assert!(RetryClass::Conflict.is_retryable());
        assert!(RetryClass::Transient.is_retryable());
        assert!(!RetryClass::Terminal.is_retryable());
    }

    fn test_job() -> CompactionJob {
        CompactionJob {
            job_id: "job-under-test".to_string(),
            tenant_id: "acme".to_string(),
            dataset_id: "prod".to_string(),
            table_name: "traces".to_string(),
            partition_id: "480000".to_string(),
            input_files: vec![],
            input_files_count: 4,
            target_file_size_bytes: 128 * 1024 * 1024,
            created_at: Instant::now(),
        }
    }

    fn succeeded(job: &CompactionJob) -> CompactionResult {
        CompactionResult {
            job_id: job.job_id.clone(),
            status: CompactionStatus::Success,
            input_files_count: job.input_files_count,
            output_files_count: 1,
            bytes_before: 4096,
            bytes_after: 2048,
            duration: Duration::from_millis(1),
            error: None,
        }
    }

    async fn test_executor(config: ExecutorConfig) -> CompactionExecutor {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        CompactionExecutor::new(catalog_manager, config, CompactionMetrics::new())
    }

    fn no_backoff() -> ExecutorConfig {
        ExecutorConfig {
            base_delay_ms: 0,
            ..ExecutorConfig::default()
        }
    }

    /// The point of #1052: a rewrite lost to an object store blip is retried
    /// on the same budget conflicts already had, instead of being discarded.
    #[tokio::test]
    async fn transient_failures_are_retried_until_they_succeed() {
        let executor = test_executor(no_backoff()).await;
        let job = test_job();

        let result = executor
            .run_with_retry(&job, |attempt| async move {
                if attempt < 3 {
                    Err(anyhow::anyhow!("connection reset by peer")
                        .context("Failed to commit compaction"))
                } else {
                    Ok(succeeded(&test_job()))
                }
            })
            .await
            .unwrap();

        assert_eq!(result.status, CompactionStatus::Success);
        assert_eq!(executor.metrics().retries_attempted(), 2);
        assert_eq!(executor.metrics().conflicts_detected(), 0);
    }

    /// A transient failure that never clears exhausts the budget and is
    /// reported as a failure, not as contention.
    #[tokio::test]
    async fn persistent_transient_failures_exhaust_the_retry_budget() {
        let executor = test_executor(no_backoff()).await;
        let job = test_job();

        let result = executor
            .run_with_retry(&job, |_attempt| async {
                Err(anyhow::anyhow!("503 Service Unavailable"))
            })
            .await
            .unwrap();

        assert_eq!(result.status, CompactionStatus::Failed);
        assert_eq!(executor.metrics().retries_attempted(), 2);
        assert_eq!(executor.metrics().jobs_failed(), 1);
        assert_eq!(executor.metrics().conflicts_detected(), 0);
    }

    #[tokio::test]
    async fn conflicts_still_retry_and_report_as_conflicts() {
        let executor = test_executor(no_backoff()).await;
        let job = test_job();

        let result = executor
            .run_with_retry(&job, |_attempt| async {
                Err(anyhow::anyhow!("snapshot changed from 1 to 2"))
            })
            .await
            .unwrap();

        assert_eq!(result.status, CompactionStatus::Conflict);
        assert_eq!(executor.metrics().retries_attempted(), 2);
        assert_eq!(executor.metrics().conflicts_detected(), 3);
    }

    /// A terminal failure must not burn the retry budget: the rewrite is the
    /// expensive part and it would fail identically on every attempt.
    #[tokio::test]
    async fn terminal_failures_do_not_consume_retries() {
        let executor = test_executor(ExecutorConfig::default()).await;

        let result = executor
            .execute_candidate(CompactionCandidate {
                tenant_id: "acme".to_string(),
                dataset_id: "prod".to_string(),
                table_name: "traces".to_string(),
                partition_id: "not-a-partition".to_string(),
                stats: crate::planner::PartitionStats {
                    file_count: 4,
                    total_size_bytes: 1024,
                    avg_file_size_bytes: 256,
                },
            })
            .await
            .unwrap();

        assert_eq!(result.status, CompactionStatus::Failed);
        assert_eq!(executor.metrics().retries_attempted(), 0);
        assert_eq!(executor.metrics().jobs_failed(), 1);
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
