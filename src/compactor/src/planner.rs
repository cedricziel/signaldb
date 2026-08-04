//! Compaction planning module for Phase 1 (dry-run planning only)
//!
//! This module analyzes Iceberg tables and identifies partitions that need compaction
//! based on file count and size thresholds. In Phase 1, it only logs what would be
//! compacted without executing the actual compaction.

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
    /// Partition identifier (currently placeholder, will be real partition in Phase 2)
    pub partition_id: String,
    /// Statistics about files in this partition
    pub stats: PartitionStats,
}

/// Format bytes as MB with 2 decimal places
fn format_mb(bytes: u64) -> String {
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
}

impl From<&CompactorConfig> for PlannerConfig {
    fn from(config: &CompactorConfig) -> Self {
        Self {
            file_count_threshold: config.file_count_threshold,
            max_input_file_size_bytes: config.max_input_file_size_kb * 1024,
            target_file_size_bytes: config.target_file_size_mb * 1024 * 1024,
        }
    }
}

/// Compaction planner that identifies tables and partitions needing compaction
pub struct CompactionPlanner {
    catalog_manager: Arc<CatalogManager>,
    config: PlannerConfig,
}

impl CompactionPlanner {
    /// Create a new compaction planner
    pub fn new(catalog_manager: Arc<CatalogManager>, config: PlannerConfig) -> Self {
        Self {
            catalog_manager,
            config,
        }
    }

    /// Run a planning cycle and return candidates
    ///
    /// Phase 1: Returns empty list as this is dry-run only
    /// Phase 2: Will implement actual table scanning and analysis
    pub async fn plan(&self) -> Result<Vec<CompactionCandidate>> {
        tracing::debug!("Starting compaction planning cycle (Phase 1: dry-run)");

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
    ///
    /// Phase 1: Attempts to list tables from Iceberg catalog
    /// Phase 2: Will add manifest reading and file-level analysis
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
    ///
    /// Phase 1: Loads table and uses placeholder for manifest reading
    /// Phase 2: Will implement actual manifest reading and partition analysis
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

        // Phase 1: Placeholder for reading Iceberg manifests
        // In Phase 2, we'll read actual manifest files and group by partitions
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

    /// Group the table's live data files for planning.
    ///
    /// Reads the REAL data file set from the current snapshot's manifests
    /// so the planning thresholds operate on actual file counts and sizes
    /// (issue #559 — this used to fabricate synthetic files, which made
    /// every table with a snapshot a candidate on every cycle).
    ///
    /// All files are grouped under the single `"all"` key: the executor
    /// rewrites and commits the WHOLE table (a `replace` snapshot), so the
    /// candidate's `partition_id = "all"` deliberately keys the lease at
    /// table granularity — the unit the executor actually mutates.
    /// Partition-scoped planning must not be introduced without also
    /// scoping the rewrite/commit (see issue #559's latent-race note).
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

        // Get current snapshot
        let snapshot_id = table
            .metadata()
            .current_snapshot_id
            .ok_or_else(|| anyhow::anyhow!("Table has no current snapshot"))?;

        tracing::debug!(
            "Reading manifests for table {} (snapshot {})",
            table.identifier(),
            snapshot_id
        );

        let files: Vec<FileInfo> = crate::iceberg::ManifestReader::new()
            .get_snapshot_files(table)
            .await
            .context("Failed to read data files from manifests")?
            .into_iter()
            .map(|file| FileInfo {
                path: file.file_path,
                size_bytes: file.file_size_bytes,
                record_count: file.record_count,
            })
            .collect();

        tracing::debug!(
            "Read {} live data files from manifests for table {}",
            files.len(),
            table.identifier()
        );

        let mut partitions: HashMap<String, Vec<FileInfo>> = HashMap::new();
        if !files.is_empty() {
            partitions.insert("all".to_string(), files);
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

    #[test]
    fn test_planner_config_from_compactor_config() {
        let compactor_config = CompactorConfig {
            enabled: true,
            tick_interval: std::time::Duration::from_secs(300),
            target_file_size_mb: 128,
            file_count_threshold: 10,
            max_input_file_size_kb: 65536,
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

    #[tokio::test]
    async fn test_evaluate_partition_skips_optimal_size() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            // Maximum above target so target-sized files stay eligible and
            // the average-size tolerance check is exercised
            max_input_file_size_bytes: 256 * 1024 * 1024, // 256MB
            target_file_size_bytes: 128 * 1024 * 1024,    // 128MB target
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
}
