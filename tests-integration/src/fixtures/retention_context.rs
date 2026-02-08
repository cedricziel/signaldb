//! Retention test context combining catalog, storage, and configuration
//!
//! Provides a complete test environment for retention enforcement testing.

use super::{CatalogTestContext, StorageTestContext};
use anyhow::Result;
use compactor::metrics::CompactionMetrics;
use compactor::planner::PlannerConfig;
use std::sync::Arc;
use writer::IcebergTableWriter;

/// Complete test context for retention enforcement testing
pub struct RetentionTestContext {
    pub catalog: CatalogTestContext,
    pub storage: StorageTestContext,
    pub planner_config: PlannerConfig,
    pub metrics: CompactionMetrics,
}

/// Configuration for generating test data
#[derive(Debug, Clone)]
pub struct DataGeneratorConfig {
    /// Number of partitions (days/hours) to generate
    pub partition_count: usize,
    /// Files per partition
    pub files_per_partition: usize,
    /// Rows per file
    pub rows_per_file: usize,
    /// Base timestamp (oldest data) in milliseconds
    pub base_timestamp: i64,
    /// Partition granularity
    pub partition_granularity: PartitionGranularity,
}

#[derive(Debug, Clone, Copy)]
pub enum PartitionGranularity {
    Hour,
    Day,
}

impl PartitionGranularity {
    /// Returns the duration in milliseconds
    pub fn to_millis(self) -> i64 {
        match self {
            Self::Hour => 60 * 60 * 1000,
            Self::Day => 24 * 60 * 60 * 1000,
        }
    }
}

impl Default for DataGeneratorConfig {
    fn default() -> Self {
        Self {
            partition_count: 10,
            files_per_partition: 5,
            rows_per_file: 100,
            base_timestamp: 1700000000000, // Nov 14, 2023
            partition_granularity: PartitionGranularity::Day,
        }
    }
}

/// Information about a generated partition
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition_id: String,
    pub timestamp_range: (i64, i64),
    pub file_count: usize,
    pub row_count: usize,
}

/// Information about a table created for testing
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub tenant_id: String,
    pub dataset_id: String,
    pub table_name: String,
    pub partitions: Vec<PartitionInfo>,
}

impl RetentionTestContext {
    /// Creates a new retention test context with in-memory implementations
    pub async fn new_in_memory() -> Result<Self> {
        let catalog = CatalogTestContext::new_in_memory().await?;
        let storage = StorageTestContext::new_in_memory().await?;
        let planner_config = PlannerConfig {
            file_count_threshold: 10,
            min_input_file_size_bytes: 1024 * 1024,
            max_files_per_job: 50,
            target_file_size_bytes: 128 * 1024 * 1024,
        };
        let metrics = CompactionMetrics::new();

        Ok(Self {
            catalog,
            storage,
            planner_config,
            metrics,
        })
    }

    /// Creates a new retention test context (defaults to in-memory)
    pub async fn new() -> Result<Self> {
        Self::new_in_memory().await
    }

    /// Creates a table and populates it with test data
    ///
    /// This is a helper method that creates an Iceberg table writer and
    /// prepares it for data generation. The actual data generation is handled
    /// by the generators module.
    pub async fn create_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<IcebergTableWriter> {
        let writer = IcebergTableWriter::new(
            &self.catalog.catalog_manager,
            self.storage.object_store.clone(),
            tenant_id.to_string(),
            dataset_id.to_string(),
            table_name.to_string(),
        )
        .await?;

        Ok(writer)
    }

    /// Returns a reference to the catalog manager
    pub fn catalog_manager(&self) -> &Arc<common::catalog_manager::CatalogManager> {
        &self.catalog.catalog_manager
    }

    /// Returns a reference to the object store
    pub fn object_store(&self) -> &Arc<dyn object_store::ObjectStore> {
        &self.storage.object_store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_retention_context() -> Result<()> {
        let ctx = RetentionTestContext::new_in_memory().await?;
        assert!(Arc::strong_count(ctx.catalog_manager()) >= 1);
        Ok(())
    }

    #[test]
    fn test_partition_granularity_millis() {
        assert_eq!(PartitionGranularity::Hour.to_millis(), 60 * 60 * 1000);
        assert_eq!(PartitionGranularity::Day.to_millis(), 24 * 60 * 60 * 1000);
    }

    #[test]
    fn test_data_generator_config_default() {
        let config = DataGeneratorConfig::default();
        assert_eq!(config.partition_count, 10);
        assert_eq!(config.files_per_partition, 5);
        assert_eq!(config.rows_per_file, 100);
    }
}
