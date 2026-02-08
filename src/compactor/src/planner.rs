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
        log::info!(
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
    /// Minimum input file size in bytes
    pub min_input_file_size_bytes: u64,
    /// Maximum files to include in a single compaction job
    pub max_files_per_job: usize,
    /// Target file size in bytes after compaction
    pub target_file_size_bytes: u64,
}

impl From<&CompactorConfig> for PlannerConfig {
    fn from(config: &CompactorConfig) -> Self {
        Self {
            file_count_threshold: config.file_count_threshold,
            min_input_file_size_bytes: config.min_input_file_size_kb * 1024,
            max_files_per_job: config.max_files_per_job,
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
        log::debug!("Starting compaction planning cycle (Phase 1: dry-run)");

        let mut candidates = vec![];

        // Get enabled tenants from configuration
        let tenants = self.catalog_manager.get_enabled_tenants();

        log::debug!("Found {} enabled tenants to analyze", tenants.len());

        for tenant_config in tenants {
            let tenant_id = &tenant_config.id;
            log::debug!("Analyzing tenant: {tenant_id}");

            // Iterate through datasets for this tenant
            for dataset_config in &tenant_config.datasets {
                let dataset_id = &dataset_config.id;
                log::debug!("  Analyzing dataset: {dataset_id}");

                // Analyze this dataset (non-fatal: log errors and continue)
                match self.analyze_dataset(tenant_id, dataset_id).await {
                    Ok(dataset_candidates) => {
                        candidates.extend(dataset_candidates);
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to analyze dataset {tenant_id}/{dataset_id}: {e:?}. Continuing with other datasets."
                        );
                    }
                }
            }
        }

        log::debug!(
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

        log::debug!(
            "    Found {} tables in {}/{}",
            table_identifiers.len(),
            tenant_id,
            dataset_id
        );

        for identifier in table_identifiers {
            let table_name = identifier.name();
            log::debug!("      Analyzing table: {table_name}");

            // Analyze this table (non-fatal: log errors and continue)
            match self.analyze_table(tenant_id, dataset_id, table_name).await {
                Ok(table_candidates) => {
                    candidates.extend(table_candidates);
                }
                Err(e) => {
                    log::warn!(
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
            log::debug!("        Partition {partition_id}: {} files", files.len());

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

    /// Group files by partition (Phase 1 placeholder)
    ///
    /// In Phase 2, this will read Iceberg manifest files and group data files
    /// by their partition values. For Phase 1, we return empty results as this
    /// is dry-run mode only.
    async fn group_files_by_partition(
        &self,
        _table: &iceberg_rust::catalog::tabular::Tabular,
    ) -> Result<HashMap<String, Vec<FileInfo>>> {
        // Phase 1 Placeholder: Return empty result
        // Phase 2 will implement manifest reading like:
        //
        // 1. Get current snapshot ID from table metadata
        // 2. Read manifest list for that snapshot
        // 3. Read each manifest file
        // 4. Extract data file paths and partition specs
        // 5. Group files by partition values
        //
        // Example Phase 2 code:
        // ```
        // let snapshot = table.current_snapshot().context("No current snapshot")?;
        // let manifest_list = snapshot.manifest_list();
        // let mut partitions: HashMap<String, Vec<FileInfo>> = HashMap::new();
        //
        // for manifest in manifest_list.manifests() {
        //     let manifest_file = read_manifest(manifest.manifest_path()).await?;
        //     for entry in manifest_file.entries() {
        //         let partition_key = format_partition_key(&entry.partition());
        //         partitions.entry(partition_key)
        //             .or_default()
        //             .push(FileInfo {
        //                 path: entry.data_file().file_path().to_string(),
        //                 size_bytes: entry.data_file().file_size_in_bytes(),
        //             });
        //     }
        // }
        // ```

        log::debug!("Phase 1: Skipping manifest reading (placeholder)");
        Ok(HashMap::new())
    }

    /// Evaluate if a partition needs compaction based on file statistics
    fn evaluate_partition(&self, files: &[FileInfo]) -> Option<PartitionStats> {
        // Filter out files that are too small to compact first
        let eligible_files: Vec<_> = files
            .iter()
            .filter(|f| f.size_bytes >= self.config.min_input_file_size_bytes)
            .collect();

        let file_count = eligible_files.len();

        // Not enough eligible files to trigger compaction
        if file_count < self.config.file_count_threshold {
            log::debug!(
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
            log::debug!(
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

/// Information about a data file (used internally by planner)
#[derive(Debug, Clone)]
struct FileInfo {
    #[allow(dead_code)]
    path: String,
    size_bytes: u64,
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
            min_input_file_size_kb: 1024,
            max_files_per_job: 50,
        };

        let planner_config = PlannerConfig::from(&compactor_config);

        assert_eq!(planner_config.file_count_threshold, 10);
        assert_eq!(planner_config.min_input_file_size_bytes, 1024 * 1024);
        assert_eq!(planner_config.max_files_per_job, 50);
        assert_eq!(planner_config.target_file_size_bytes, 128 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_evaluate_partition_below_threshold() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            min_input_file_size_bytes: 1024 * 1024, // 1MB
            max_files_per_job: 50,
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create files below threshold
        let files: Vec<FileInfo> = (0..5)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 2 * 1024 * 1024, // 2MB each
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
            min_input_file_size_bytes: 1024 * 1024, // 1MB
            max_files_per_job: 50,
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create 15 files (above threshold of 10)
        let files: Vec<FileInfo> = (0..15)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 2 * 1024 * 1024, // 2MB each
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
    async fn test_evaluate_partition_filters_small_files() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            min_input_file_size_bytes: 1024 * 1024, // 1MB minimum
            max_files_per_job: 50,
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create 15 files, but only 8 are above minimum size
        let mut files = vec![];
        for i in 0..8 {
            files.push(FileInfo {
                path: format!("large_file_{i}.parquet"),
                size_bytes: 2 * 1024 * 1024, // 2MB each (above minimum)
            });
        }
        for i in 0..7 {
            files.push(FileInfo {
                path: format!("small_file_{i}.parquet"),
                size_bytes: 512 * 1024, // 512KB each (below minimum)
            });
        }

        // Should not trigger compaction (only 8 eligible files, need 10)
        let result = planner.evaluate_partition(&files);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_evaluate_partition_skips_optimal_size() {
        let config = PlannerConfig {
            file_count_threshold: 10,
            min_input_file_size_bytes: 1024 * 1024, // 1MB
            max_files_per_job: 50,
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB target
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create 15 files averaging 128MB (within 20% tolerance of target)
        let files: Vec<FileInfo> = (0..15)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 128 * 1024 * 1024, // 128MB each (at target)
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
            min_input_file_size_bytes: 1024 * 1024, // 1MB
            max_files_per_job: 50,
            target_file_size_bytes: 128 * 1024 * 1024, // 128MB target
        };

        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = CompactionPlanner::new(catalog_manager, config);

        // Create 15 files averaging 110MB (within 20% tolerance: 102.4MB - 153.6MB)
        let files: Vec<FileInfo> = (0..15)
            .map(|i| FileInfo {
                path: format!("file_{i}.parquet"),
                size_bytes: 110 * 1024 * 1024, // 110MB each (within tolerance)
            })
            .collect();

        // Should not trigger compaction (within tolerance)
        let result = planner.evaluate_partition(&files);
        assert!(result.is_none());
    }

    #[test]
    fn test_compactor_config_defaults() {
        let config = CompactorConfig::default();

        assert!(!config.enabled); // Disabled by default
        assert_eq!(config.tick_interval, std::time::Duration::from_secs(300)); // 5 minutes
        assert_eq!(config.target_file_size_mb, 128);
        assert_eq!(config.file_count_threshold, 10);
        assert_eq!(config.min_input_file_size_kb, 1024); // 1MB
        assert_eq!(config.max_files_per_job, 50);
    }
}
