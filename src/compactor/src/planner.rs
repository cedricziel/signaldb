//! Compaction planning
//!
//! Analyzes Iceberg tables and identifies the `timestamp_hour` partitions that
//! need compaction, based on the small-file count and average size of the live
//! data files recorded in the current snapshot's manifests.
//!
//! Planning is partition-scoped and only ever selects *closed* partitions —
//! those whose hour has ended and whose lateness allowance has elapsed. The
//! executor rewrites and commits exactly one partition per job, so the unit
//! planned here is the unit mutated there (issue #933).

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use common::config::CompactorConfig;
use std::collections::HashMap;
use std::sync::Arc;

/// Statistics about files in a partition
#[derive(Debug, Clone)]
pub struct PartitionStats {
    /// Total number of files in this partition
    pub file_count: usize,
    /// Total size of all files in bytes
    pub total_size_bytes: u64,
    /// Average file size in bytes
    pub avg_file_size_bytes: u64,
}

/// A partition that is a candidate for compaction
#[derive(Debug, Clone)]
pub struct CompactionCandidate {
    /// Tenant ID
    pub tenant_id: String,
    /// Dataset ID
    pub dataset_id: String,
    /// Table name (e.g., "traces", "logs", "metrics")
    pub table_name: String,
    /// The `timestamp_hour` partition to compact, as hours since the Unix
    /// epoch rendered in decimal. This is both the unit the executor rewrites
    /// and commits, and the lease key — so two jobs on different partitions of
    /// the same table do not serialize against each other.
    pub partition_id: String,
    /// Statistics about files in this partition
    pub stats: PartitionStats,
}

/// Format bytes as MB with 2 decimal places
pub(crate) fn format_mb(bytes: u64) -> String {
    format!("{:.2}", bytes as f64 / (1024.0 * 1024.0))
}

impl CompactionCandidate {
    /// Log this compaction candidate
    pub fn log(&self) {
        tracing::info!(
            "Compaction candidate: tenant={}, dataset={}, table={}, partition={}, files={}, total_size={} MB, avg_size={} MB",
            self.tenant_id,
            self.dataset_id,
            self.table_name,
            self.partition_id,
            self.stats.file_count,
            format_mb(self.stats.total_size_bytes),
            format_mb(self.stats.avg_file_size_bytes)
        );
    }
}

/// Configuration for the compaction planner
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// Minimum number of files to trigger compaction
    pub file_count_threshold: usize,
    /// Maximum input file size in bytes; files at or above this size are
    /// considered already compacted and are not counted as candidates
    pub max_input_file_size_bytes: u64,
    /// Target file size in bytes after compaction
    pub target_file_size_bytes: u64,
    /// Upper bound on the summed size of a partition's eligible input
    /// files. Partitions above it are declined rather than attempted;
    /// `0` disables the guard.
    pub max_partition_input_bytes: u64,
    /// How long after an hour partition ends it stays "open" — i.e. still
    /// accepting late-arriving writes — and is therefore not compacted.
    pub partition_lateness: std::time::Duration,
}

impl From<&CompactorConfig> for PlannerConfig {
    fn from(config: &CompactorConfig) -> Self {
        Self {
            file_count_threshold: config.file_count_threshold,
            max_input_file_size_bytes: config.max_input_file_size_kb * 1024,
            target_file_size_bytes: config.target_file_size_mb * 1024 * 1024,
            max_partition_input_bytes: config.max_partition_input_mb * 1024 * 1024,
            partition_lateness: config.partition_lateness,
        }
    }
}

/// Seconds in an hour partition (the `timestamp_hour` Iceberg transform).
const PARTITION_SECONDS: i64 = 3600;

/// Whether an hour partition is *closed*: its hour has ended and the
/// configured lateness allowance has elapsed, so no further writes are
/// expected into it.
///
/// Compacting an open partition is what made the old whole-table design lose
/// its commit race: the partition being actively appended to is exactly the
/// one whose manifests keep changing underneath the rewrite. Restricting
/// compaction to closed partitions means a compaction job and live ingest
/// never contend for the same files.
pub(crate) fn is_partition_closed(
    partition_hours: i64,
    now_secs: i64,
    lateness: std::time::Duration,
) -> bool {
    let partition_end = (partition_hours + 1) * PARTITION_SECONDS;
    partition_end.saturating_add(lateness.as_secs() as i64) <= now_secs
}

/// Compaction planner that identifies tables and partitions needing compaction
pub struct CompactionPlanner {
    catalog_manager: Arc<CatalogManager>,
    config: PlannerConfig,
    /// Optional counters for files the planner declined to compact. `None`
    /// keeps planning log-only, which is what the unit tests use.
    metrics: Option<crate::metrics::CompactionMetrics>,
}

impl CompactionPlanner {
    /// Create a new compaction planner
    pub fn new(catalog_manager: Arc<CatalogManager>, config: PlannerConfig) -> Self {
        Self {
            catalog_manager,
            config,
            metrics: None,
        }
    }

    /// Record declined-file counters (deferred-open and unclassifiable) into
    /// the shared compaction metrics.
    pub fn with_metrics(mut self, metrics: crate::metrics::CompactionMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Run a planning cycle and return the closed partitions that need
    /// compaction, across every active tenant and dataset.
    pub async fn plan(&self) -> Result<Vec<CompactionCandidate>> {
        tracing::debug!("Starting compaction planning cycle");

        let mut candidates = vec![];

        // Enumerate active tenants through the registry so database-created
        // (admin-API) tenants are compacted alongside config-defined ones.
        let tenants = self.catalog_manager.list_active_tenants().await?;

        tracing::debug!("Found {} active tenants to analyze", tenants.len());

        for tenant_config in &tenants {
            let tenant_id = &tenant_config.id;
            tracing::debug!("Analyzing tenant: {tenant_id}");

            // Iterate through datasets for this tenant
            for dataset_config in &tenant_config.datasets {
                let dataset_id = &dataset_config.id;
                tracing::debug!("  Analyzing dataset: {dataset_id}");

                // Analyze this dataset (non-fatal: log errors and continue)
                match self.analyze_dataset(tenant_id, dataset_id).await {
                    Ok(dataset_candidates) => {
                        candidates.extend(dataset_candidates);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to analyze dataset {tenant_id}/{dataset_id}: {e:?}. Continuing with other datasets."
                        );
                    }
                }
            }
        }

        tracing::debug!(
            "Planning cycle complete: found {} candidates",
            candidates.len()
        );

        Ok(candidates)
    }

    /// Analyze a single dataset and return compaction candidates
    async fn analyze_dataset(
        &self,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<Vec<CompactionCandidate>> {
        let mut candidates = vec![];

        // Build namespace for this tenant/dataset
        let namespace = self
            .catalog_manager
            .build_namespace(tenant_id, dataset_id)
            .context("Failed to build namespace")?;

        // List tables in this namespace
        let catalog = self.catalog_manager.catalog();
        let table_identifiers = catalog
            .list_tabulars(&namespace)
            .await
            .context("Failed to list tables")?;

        tracing::debug!(
            "    Found {} tables in {}/{}",
            table_identifiers.len(),
            tenant_id,
            dataset_id
        );

        for identifier in table_identifiers {
            let table_name = identifier.name();
            tracing::debug!("      Analyzing table: {table_name}");

            // Analyze this table (non-fatal: log errors and continue)
            match self.analyze_table(tenant_id, dataset_id, table_name).await {
                Ok(table_candidates) => {
                    candidates.extend(table_candidates);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to analyze table {tenant_id}/{dataset_id}/{table_name}: {e:?}. Continuing with other tables."
                    );
                }
            }
        }

        Ok(candidates)
    }

    /// Analyze a single table and return compaction candidates
    async fn analyze_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Vec<CompactionCandidate>> {
        // Load table from catalog
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

        let partitions = self.group_files_by_partition(&table).await?;

        let mut candidates = vec![];

        for (partition_id, files) in partitions {
            tracing::debug!("        Partition {partition_id}: {} files", files.len());

            // Evaluate if this partition needs compaction
            if let Some(stats) = self.evaluate_partition(&files) {
                candidates.push(CompactionCandidate {
                    tenant_id: tenant_id.to_string(),
                    dataset_id: dataset_id.to_string(),
                    table_name: table_name.to_string(),
                    partition_id,
                    stats,
                });
            }
        }

        Ok(candidates)
    }

    /// Group the table's live data files by `timestamp_hour` partition.
    ///
    /// Reads the REAL data file set from the current snapshot's manifests
    /// so the planning thresholds operate on actual file counts and sizes
    /// (issue #559 — this used to fabricate synthetic files, which made
    /// every table with a snapshot a candidate on every cycle).
    ///
    /// Files are grouped by the partition value recorded in their manifest
    /// entry, and only *closed* partitions are returned (issue #933). The
    /// executor rewrites and commits exactly one partition per job via a
    /// scoped delta commit, so the candidate's `partition_id` is the
    /// partition the executor actually mutates — which is also the lease
    /// key, so two jobs on different partitions of the same table no longer
    /// serialize against each other.
    ///
    /// Files with no usable partition value are unclassifiable and are
    /// excluded: they are left untouched rather than swept into some other
    /// partition's rewrite, where the delta commit would remove them without
    /// their rows being present in the output.
    ///
    /// A table with no current snapshot yields no partitions at all rather
    /// than an error, so provisioned-but-never-written tables are simply
    /// empty instead of warning on every cycle.
    async fn group_files_by_partition(
        &self,
        table: &iceberg_rust::catalog::tabular::Tabular,
    ) -> Result<HashMap<String, Vec<FileInfo>>> {
        // Extract table from tabular
        let table = match table {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => {
                return Err(anyhow::anyhow!("Expected table but got view"));
            }
        };

        // A table with no current snapshot holds no data files — a freshly
        // provisioned table, or one whose data has all been dropped. Zero
        // candidates, not an error: the caller logs analysis failures at
        // warn level, which would mean one warning per empty table per cycle.
        let Some(snapshot_id) = table.metadata().current_snapshot_id else {
            tracing::debug!(
                "Table {} has no current snapshot; no compaction candidates",
                table.identifier()
            );
            return Ok(HashMap::new());
        };

        tracing::debug!(
            "Reading manifests for table {} (snapshot {})",
            table.identifier(),
            snapshot_id
        );

        let files = crate::iceberg::ManifestReader::new()
            .get_snapshot_files(table)
            .await
            .context("Failed to read data files from manifests")?;

        tracing::debug!(
            "Read {} live data files from manifests for table {}",
            files.len(),
            table.identifier()
        );

        let now_secs = chrono::Utc::now().timestamp();
        let mut partitions: HashMap<String, Vec<FileInfo>> = HashMap::new();
        let mut unclassifiable = 0usize;
        let mut deferred_open = 0usize;

        for file in files {
            let Some(partition_hours) = file.partition_hours else {
                unclassifiable += 1;
                continue;
            };

            if !is_partition_closed(partition_hours, now_secs, self.config.partition_lateness) {
                deferred_open += 1;
                continue;
            }

            partitions
                .entry(partition_hours.to_string())
                .or_default()
                .push(FileInfo {
                    path: file.file_path,
                    size_bytes: file.file_size_bytes,
                    record_count: file.record_count,
                });
        }

        if let Some(metrics) = &self.metrics {
            if unclassifiable > 0 {
                metrics.record_unclassifiable_files(unclassifiable);
            }
            if deferred_open > 0 {
                metrics.record_deferred_open_partition_files(deferred_open);
            }
        }

        if unclassifiable > 0 {
            tracing::warn!(
                table = %table.identifier(),
                unclassifiable_files = unclassifiable,
                "Data files have no usable timestamp_hour partition value; excluding them from compaction"
            );
        }
        if deferred_open > 0 {
            tracing::debug!(
                table = %table.identifier(),
                deferred_files = deferred_open,
                "Deferred data files in still-open partitions"
            );
        }

        Ok(partitions)
    }

    /// Evaluate if a partition needs compaction based on file statistics
    fn evaluate_partition(&self, files: &[FileInfo]) -> Option<PartitionStats> {
        // Only small files are compaction candidates: files at or above the
        // maximum input size are already big enough that re-reading and
        // rewriting them buys nothing (issue #934 — the old logic filtered
        // out files BELOW a minimum size, which excluded exactly the small
        // ingest files compaction exists to merge, so the default config
        // never compacted anything).
        let eligible_files: Vec<_> = files
            .iter()
            .filter(|f| f.size_bytes < self.config.max_input_file_size_bytes)
            .collect();

        let file_count = eligible_files.len();

        // Not enough eligible files to trigger compaction
        if file_count < self.config.file_count_threshold {
            tracing::debug!(
                "Not enough eligible files ({}) after size filtering",
                file_count
            );
            return None;
        }

        // Calculate statistics from eligible files only
        let total_size_bytes: u64 = eligible_files.iter().map(|f| f.size_bytes).sum();
        let avg_file_size_bytes = if file_count > 0 {
            total_size_bytes / file_count as u64
        } else {
            0
        };

        // Decline partitions whose inputs exceed the per-job budget. The
        // rewrite reads and sorts every eligible file at once, so a
        // partition over the cap fails after doing all that work — and the
        // planner, having no memory of the failure, selects it again next
        // cycle (#1053, #1064). Declining is bounded and visible; the
        // partition simply waits for a rewrite that can handle it.
        if self.config.max_partition_input_bytes > 0 {
            let eligible_bytes: u64 = eligible_files.iter().map(|f| f.size_bytes).sum();
            if eligible_bytes > self.config.max_partition_input_bytes {
                tracing::warn!(
                    eligible_files = file_count,
                    eligible_mb = %format_mb(eligible_bytes),
                    cap_mb = %format_mb(self.config.max_partition_input_bytes),
                    "Partition exceeds the compaction input budget and will not be compacted; \
                     raise [compactor] max_partition_input_mb to attempt it anyway"
                );
                if let Some(metrics) = &self.metrics {
                    metrics.record_oversized_partition_skipped();
                }
                return None;
            }
        }

        // Skip if average file size is already close to target
        // (within 20% tolerance)
        let target = self.config.target_file_size_bytes;
        let tolerance = target / 5; // 20%
        if avg_file_size_bytes >= target.saturating_sub(tolerance)
            && avg_file_size_bytes <= target + tolerance
        {
            tracing::debug!(
                "Partition has good average file size ({} MB), skipping",
                format_mb(avg_file_size_bytes)
            );
            return None;
        }

        Some(PartitionStats {
            file_count,
            total_size_bytes,
            avg_file_size_bytes,
        })
    }
}

/// Information about a data file (used internally by planner and executor)
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The partition currently being written to must never be selected: it is
    /// the one whose manifests change under a running rewrite, which is what
    /// made the old whole-table design lose its commit race (issue #933).
    #[test]
    fn open_partition_is_not_closed() {
        let lateness = std::time::Duration::from_secs(600);

        // Partition 100 spans [360000, 363600). Now is inside it.
        assert!(
            !is_partition_closed(100, 361_800, lateness),
            "a partition whose hour is still in progress must not be compacted"
        );

        // The hour has ended but the lateness allowance has not elapsed.
        assert!(
            !is_partition_closed(100, 363_601, lateness),
            "a just-ended partition must stay open for the lateness allowance"
        );
    }

    #[test]
    fn partition_closes_once_lateness_has_elapsed() {
        let lateness = std::time::Duration::from_secs(600);

        // Exactly at the boundary: hour end (363600) + lateness (600).
        assert!(
            is_partition_closed(100, 364_200, lateness),
            "a partition must close exactly at hour end plus lateness"
        );
        assert!(
            is_partition_closed(100, 999_999, lateness),
            "an old partition must be compactable"
        );
    }

    /// With no lateness allowance a partition closes the instant its hour
    /// ends — the setting integration tests rely on.
    #[test]
    fn zero_lateness_closes_partition_at_hour_end() {
        let zero = std::time::Duration::ZERO;
        assert!(!is_partition_closed(100, 363_599, zero));
        assert!(is_partition_closed(100, 363_600, zero));
    }

    /// The planner's declined-file counters are what tell an operator
    /// "nothing to compact" apart from "everything is still open" or
    /// "everything is unclassifiable" (spec: the deferral must be observable).
    #[tokio::test]
    async fn planner_records_deferred_and_unclassifiable_files() {
        let metrics = crate::metrics::CompactionMetrics::new();
        assert_eq!(metrics.deferred_open_partition_files(), 0);
        assert_eq!(metrics.unclassifiable_files(), 0);

        metrics.record_deferred_open_partition_files(3);
        metrics.record_unclassifiable_files(2);

        assert_eq!(metrics.deferred_open_partition_files(), 3);
        assert_eq!(metrics.unclassifiable_files(), 2);
    }

    #[tokio::test]
    async fn planner_without_metrics_still_plans() {
        // The counters are opt-in; a planner built without them must behave
        // identically (the unit tests and the Flight admin path rely on this).
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(
            catalog_manager,
            PlannerConfig::from(&CompactorConfig::default()),
        );
        assert!(planner.metrics.is_none());
        planner.plan().await.expect("planning must succeed");
    }

    #[test]
    fn test_planner_config_from_compactor_config() {
        let compactor_config = CompactorConfig {
            enabled: true,
            tick_interval: std::time::Duration::from_secs(300),
            target_file_size_mb: 128,
            file_count_threshold: 10,
            max_input_file_size_kb: 65536,
            partition_lateness: std::time::Duration::from_secs(600),
            memory_limit_mb: 512,
            target_partitions: 1,
            max_partition_input_mb: 2048,
            retention: Default::default(),
            orphan_cleanup: Default::default(),
            attr_promotion: Default::default(),
            max_candidates_per_cycle: 20,
            max_per_tenant: 5,
            lease_ttl_seconds: 300,
            metrics_addr: "0.0.0.0:9091".to_string(),
        };

        let planner_config = PlannerConfig::from(&compactor_config);

        assert_eq!(planner_config.file_count_threshold, 10);
        assert_eq!(planner_config.max_input_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(planner_config.target_file_size_bytes, 128 * 1024 * 1024);
    }

    /// Regression test for issue #934: under the DEFAULT configuration, a
    /// partition full of small ingest files (tens of KB each) must be
    /// selected for compaction. The old min-size filter excluded exactly
    /// these files, so the default deployment never compacted anything.
    #[tokio::test]
    async fn planner_selects_small_files_under_default_config() {
        let config = PlannerConfig::from(&CompactorConfig::default());

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Typical ingest output: many small files of ~60KB each
        let files: Vec<FileInfo> = (0..800)
            .map(|i| FileInfo {
                path: format!("ingest_file_{i}.parquet"),
                size_bytes: 60 * 1024, // 60KB each
                record_count: 1000,
            })
            .collect();

        let result = planner.evaluate_partition(&files);
        assert!(
            result.is_some(),
            "small ingest files must be compaction candidates under default config"
        );

        let stats = result.unwrap();
        assert_eq!(stats.file_count, 800);
        assert_eq!(stats.total_size_bytes, 800 * 60 * 1024);
    }

    #[tokio::test]
    async fn test_evaluate_partition_below_threshold() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024, // 64MB
            target_file_size_bytes: 128 * 1024 * 1024,   // 128MB
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 0,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create files below threshold
        let files: Vec<FileInfo> = (0..5)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 2 * 1024 * 1024, // 2MB each
                record_count: 10000,
            })
            .collect();

        // Should not trigger compaction (below file count threshold)
        let result = planner.evaluate_partition(&files);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_evaluate_partition_above_threshold() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024, // 64MB
            target_file_size_bytes: 128 * 1024 * 1024,   // 128MB
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 0,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create 15 files (above threshold of 10)
        let files: Vec<FileInfo> = (0..15)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 2 * 1024 * 1024, // 2MB each
                record_count: 10000,
            })
            .collect();

        // Should trigger compaction (above file count threshold)
        let result = planner.evaluate_partition(&files);
        assert!(result.is_some());

        let stats = result.unwrap();
        assert_eq!(stats.file_count, 15);
        assert_eq!(stats.total_size_bytes, 15 * 2 * 1024 * 1024); // 30MB total
        assert_eq!(stats.avg_file_size_bytes, 2 * 1024 * 1024); // 2MB average
    }

    #[tokio::test]
    async fn planner_excludes_files_over_max_input_size() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024, // 64MB maximum
            target_file_size_bytes: 128 * 1024 * 1024,   // 128MB
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 0,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // 8 small files (eligible) plus 7 already-big files (excluded)
        let mut files = vec![];
        for i in 0..8 {
            files.push(FileInfo {
                path: format!("small_file_{i}.parquet"),
                size_bytes: 2 * 1024 * 1024, // 2MB each (below maximum)
                record_count: 10000,
            });
        }
        for i in 0..7 {
            files.push(FileInfo {
                path: format!("big_file_{i}.parquet"),
                size_bytes: 200 * 1024 * 1024, // 200MB each (above maximum)
                record_count: 500000,
            });
        }

        // Should not trigger compaction: big files don't count toward the
        // threshold (only 8 eligible small files, need 10)
        let result = planner.evaluate_partition(&files);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn planner_stats_count_only_files_under_max_input_size() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024, // 64MB maximum
            target_file_size_bytes: 128 * 1024 * 1024,   // 128MB
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 0,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // 12 small files (eligible, above threshold) plus 5 already-big files
        let mut files = vec![];
        for i in 0..12 {
            files.push(FileInfo {
                path: format!("small_file_{i}.parquet"),
                size_bytes: 60 * 1024, // 60KB each
                record_count: 1000,
            });
        }
        for i in 0..5 {
            files.push(FileInfo {
                path: format!("big_file_{i}.parquet"),
                size_bytes: 200 * 1024 * 1024, // 200MB each (above maximum)
                record_count: 500000,
            });
        }

        let result = planner.evaluate_partition(&files);
        assert!(result.is_some());

        // Stats must reflect only the eligible small files
        let stats = result.unwrap();
        assert_eq!(stats.file_count, 12);
        assert_eq!(stats.total_size_bytes, 12 * 60 * 1024);
        assert_eq!(stats.avg_file_size_bytes, 60 * 1024);
    }

    /// A partition whose inputs cannot fit the rewrite's budget is not a
    /// candidate: selecting it means a full read-and-sort that fails every
    /// cycle, forever, while the planner keeps re-selecting it (#1053,
    /// #1064). Skipping is visible and bounded; failing is neither.
    #[tokio::test]
    async fn oversized_partition_is_not_a_candidate() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024,
            target_file_size_bytes: 128 * 1024 * 1024,
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 100 * 1024 * 1024, // 100MB cap
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let metrics = crate::metrics::CompactionMetrics::new();
        let planner = CompactionPlanner::new(catalog_manager, config).with_metrics(metrics.clone());

        // 20 eligible files of 10MB = 200MB total, over the 100MB cap.
        let files: Vec<_> = (0..20)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 10 * 1024 * 1024,
                record_count: 100_000,
            })
            .collect();

        assert!(
            planner.evaluate_partition(&files).is_none(),
            "a partition over max_partition_input_bytes must not be selected"
        );
        assert_eq!(
            metrics.oversized_partitions_skipped(),
            1,
            "skipping must be counted, not silent — an operator has to see \
             which partitions compaction is declining"
        );
    }

    /// The cap is a backstop, not a policy: a partition at or under it is
    /// selected exactly as before.
    #[tokio::test]
    async fn partition_within_the_cap_is_still_a_candidate() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024,
            target_file_size_bytes: 128 * 1024 * 1024,
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 100 * 1024 * 1024,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // 20 files of 1MB = 20MB total, well under the cap.
        let files: Vec<_> = (0..20)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 1024 * 1024,
                record_count: 10_000,
            })
            .collect();

        assert!(planner.evaluate_partition(&files).is_some());
    }

    /// `0` disables the cap, for anyone who would rather have the job fail
    /// than have the partition skipped.
    #[tokio::test]
    async fn a_zero_cap_disables_the_size_guard() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024,
            target_file_size_bytes: 128 * 1024 * 1024,
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 0,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // 300 eligible files of 60MB = ~18GB of input, far past any cap —
        // each still under max_input_file_size_bytes so they all count.
        let files: Vec<_> = (0..300)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 60 * 1024 * 1024,
                record_count: 100_000,
            })
            .collect();

        assert!(planner.evaluate_partition(&files).is_some());
    }

    /// The cap counts only the files the rewrite will actually read.
    /// Already-big files are excluded from compaction, so counting them
    /// toward the budget would skip partitions the rewrite could handle.
    #[tokio::test]
    async fn the_cap_counts_only_eligible_files() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024,
            target_file_size_bytes: 128 * 1024 * 1024,
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 100 * 1024 * 1024,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        let mut files: Vec<_> = (0..12)
            .map(|i| FileInfo {
                path: format!("small_{i}.parquet"),
                size_bytes: 1024 * 1024, // 12MB total eligible
                record_count: 10_000,
            })
            .collect();
        // 1GB of already-compacted files the rewrite will not read.
        files.push(FileInfo {
            path: "big.parquet".to_string(),
            size_bytes: 1024 * 1024 * 1024,
            record_count: 10_000_000,
        });

        assert!(
            planner.evaluate_partition(&files).is_some(),
            "ineligible big files must not count toward the input budget"
        );
    }

    #[tokio::test]
    async fn test_evaluate_partition_skips_optimal_size() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            // Maximum above target so target-sized files stay eligible and
            // the average-size tolerance check is exercised
            max_input_file_size_bytes: 256 * 1024 * 1024, // 256MB
            target_file_size_bytes: 128 * 1024 * 1024,    // 128MB target
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 0,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create 15 files averaging 128MB (within 20% tolerance of target)
        let files: Vec<FileInfo> = (0..15)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 128 * 1024 * 1024, // 128MB each (at target)
                record_count: 100000,
            })
            .collect();

        // Should not trigger compaction (files are already optimal size)
        let result = planner.evaluate_partition(&files);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_evaluate_partition_slightly_below_target() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 256 * 1024 * 1024, // 256MB
            target_file_size_bytes: 128 * 1024 * 1024,    // 128MB target
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 0,
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create 15 files averaging 110MB (within 20% tolerance: 102.4MB - 153.6MB)
        let files: Vec<FileInfo> = (0..15)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 110 * 1024 * 1024, // 110MB each (within tolerance)
                record_count: 100000,
            })
            .collect();

        // Should not trigger compaction (within tolerance)
        let result = planner.evaluate_partition(&files);
        assert!(result.is_none());
    }

    #[test]
    fn test_compactor_config_defaults() {
        let config = CompactorConfig::default();

        assert!(config.enabled); // Compaction is enabled by default
        assert_eq!(config.tick_interval, std::time::Duration::from_secs(300)); // 5 minutes
        assert_eq!(config.target_file_size_mb, 128);
        assert_eq!(config.file_count_threshold, 10);
        // Half the default target output size: files at or above this are
        // "already big" and left alone
        assert_eq!(config.max_input_file_size_kb, 65536); // 64MB
    }

    #[tokio::test]
    async fn plan_enumerates_database_tenants() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024,
            target_file_size_bytes: 128 * 1024 * 1024,
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 0,
        };

        // A tenant that exists only in the database (admin-API created), with
        // no config block.
        let source = Arc::new(common::catalog::Catalog::new_in_memory().await.unwrap());
        source
            .upsert_tenant("gamma", "Gamma", Some("production"), "database")
            .await
            .unwrap();
        source.create_dataset("gamma", "production").await.unwrap();

        let catalog_manager = Arc::new(
            CatalogManager::new_in_memory()
                .await
                .unwrap()
                .with_tenant_source(source),
        );
        // Materialize a table in the database tenant's namespace so the
        // planner has something to analyze there.
        catalog_manager
            .ensure_table("gamma", "production", "traces")
            .await
            .unwrap();

        // The planner enumerates the database tenant (would be skipped if it
        // still read config-only tenants).
        let tenants = catalog_manager.list_active_tenants().await.unwrap();
        assert!(
            tenants.iter().any(|t| t.id == "gamma"),
            "database tenant must be enumerated for planning"
        );

        // A full planning cycle completes over the database tenant.
        let planner = CompactionPlanner::new(catalog_manager, config);
        planner
            .plan()
            .await
            .expect("planning over a database tenant should succeed");
    }

    /// A freshly provisioned table has no snapshot at all. That is zero
    /// compaction candidates, not an error — otherwise every reconciled
    /// dataset warns once per table per cycle (roughly eight warnings per
    /// dataset in steady state).
    #[tokio::test]
    async fn snapshotless_table_yields_no_candidates_without_warning() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            max_input_file_size_bytes: 64 * 1024 * 1024,
            target_file_size_bytes: 128 * 1024 * 1024,
            partition_lateness: std::time::Duration::from_secs(600),
            max_partition_input_bytes: 4 * 1024 * 1024 * 1024,
        };
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());

        // Provision the dataset's tables ahead of any write — none of them
        // has a snapshot.
        catalog_manager
            .ensure_dataset_tables("default", "default")
            .await;

        let planner = CompactionPlanner::new(catalog_manager, config);
        let candidates = planner
            .analyze_table("default", "default", "logs")
            .await
            .expect("a snapshot-less table must not error");
        assert!(candidates.is_empty());
    }
}
