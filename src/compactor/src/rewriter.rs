//! Parquet file reading, merging, and rewriting
//!
//! Handles the core compaction logic: reading multiple small Parquet files,
//! merging and sorting the data, and writing optimized larger files.

use anyhow::{Context, Result};
use common::CatalogManager;
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;

/// Handles Parquet file merging and rewriting
pub struct ParquetRewriter {
    catalog_manager: Arc<CatalogManager>,
}

impl ParquetRewriter {
    /// Create a new Parquet rewriter
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        Self { catalog_manager }
    }

    /// Compact a partition by reading, merging, and rewriting files
    ///
    /// Returns: (output_file_paths, input_size_bytes, output_size_bytes)
    pub async fn compact_partition(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        partition_id: &str,
        target_file_size_bytes: u64,
    ) -> Result<(Vec<String>, u64, u64)> {
        log::info!(
            "Compacting partition {} for table {}/{}/{}",
            partition_id,
            tenant_id,
            dataset_id,
            table_name
        );

        // Step 1: Load the table to get file information
        let table = self
            .catalog_manager
            .ensure_table(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to load table for compaction")?;

        log::debug!(
            "Loaded table {} for partition {} compaction",
            table.identifier(),
            partition_id
        );

        // Step 2: Read and merge files using DataFusion
        // For Phase 2, we'll use a simplified approach where we read
        // the entire table and filter by partition
        let merged_batches = self
            .read_and_merge_partition(&table, partition_id)
            .await
            .context("Failed to read and merge partition data")?;

        if merged_batches.is_empty() {
            log::info!(
                "No data found in partition {}, skipping compaction",
                partition_id
            );
            return Ok((vec![], 0, 0));
        }

        // Calculate input size (estimate from batches)
        let input_size: u64 = merged_batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        log::debug!(
            "Read and merged {} batches from partition {} ({} bytes)",
            merged_batches.len(),
            partition_id,
            input_size
        );

        // Step 3: Write merged batches to new files
        let output_files = self
            .write_merged_batches(
                tenant_id,
                dataset_id,
                table_name,
                merged_batches,
                target_file_size_bytes,
            )
            .await
            .context("Failed to write merged batches")?;

        // Estimate output size (will be more accurate after actual write)
        let output_size = input_size; // For Phase 2, assume similar size

        log::info!(
            "Compacted partition {} into {} output files",
            partition_id,
            output_files.len()
        );

        Ok((output_files, input_size, output_size))
    }

    /// Read and merge data from a partition
    async fn read_and_merge_partition(
        &self,
        table: &iceberg_rust::table::Table,
        partition_id: &str,
    ) -> Result<Vec<RecordBatch>> {
        log::debug!(
            "Reading partition {} from table {}",
            partition_id,
            table.identifier()
        );

        // Create a DataFusion session context
        let ctx = SessionContext::new();

        // Register the Iceberg table with DataFusion
        let table_name = table.identifier().name();
        let datafusion_table = Arc::new(datafusion_iceberg::DataFusionTable::from(table.clone()));
        ctx.register_table(table_name, datafusion_table)
            .context("Failed to register table with DataFusion")?;

        // For Phase 2, we'll read all data from the table
        // A full implementation would filter by partition values
        let df = ctx
            .table(table_name)
            .await
            .context("Failed to read table")?;

        // Sort the data for optimal query performance
        // For traces table: sort by timestamp, trace_id
        let sorted_df = if table_name == "traces" {
            df.sort(vec![
                col("timestamp").sort(true, true), // ascending, nulls first
                col("trace_id").sort(true, true),
            ])
            .context("Failed to sort data by timestamp and trace_id")?
        } else {
            df // No sorting for other tables yet
        };

        // Collect the data into RecordBatches
        let batches = sorted_df
            .collect()
            .await
            .context("Failed to collect query results")?;

        log::debug!(
            "Collected {} batches from partition {}",
            batches.len(),
            partition_id
        );

        Ok(batches)
    }

    /// Write merged batches to new files
    async fn write_merged_batches(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        batches: Vec<RecordBatch>,
        target_file_size_bytes: u64,
    ) -> Result<Vec<String>> {
        log::debug!(
            "Writing {} merged batches to table {}/{}/{}",
            batches.len(),
            tenant_id,
            dataset_id,
            table_name
        );

        // For Phase 2, we use an in-memory object store for simplicity
        // A full implementation would use the actual storage backend
        let object_store = Arc::new(object_store::memory::InMemory::new());

        // Create an IcebergTableWriter for atomic writes
        let mut writer = writer::IcebergTableWriter::new(
            &self.catalog_manager,
            object_store,
            tenant_id.to_string(),
            dataset_id.to_string(),
            table_name.to_string(),
        )
        .await
        .context("Failed to create Iceberg table writer")?;

        // Begin a transaction for atomic writes
        let transaction_id = writer
            .begin_transaction()
            .await
            .context("Failed to begin transaction")?;

        log::debug!(
            "Started transaction {} for compaction write",
            transaction_id
        );

        // Split batches if they exceed target file size
        let split_batches = self.split_batches_by_size(batches, target_file_size_bytes)?;

        log::debug!(
            "Split batches into {} groups for target size {} bytes",
            split_batches.len(),
            target_file_size_bytes
        );

        // Write each batch group
        for (i, batch) in split_batches.iter().enumerate() {
            log::debug!(
                "Writing batch group {}/{} ({} rows)",
                i + 1,
                split_batches.len(),
                batch.num_rows()
            );

            writer
                .write_batch(batch.clone())
                .await
                .with_context(|| format!("Failed to write batch group {}", i))?;
        }

        // Commit the transaction
        writer
            .commit_transaction(&transaction_id)
            .await
            .context("Failed to commit transaction")?;

        log::info!(
            "Successfully wrote {} batch groups to {}/{}/{}",
            split_batches.len(),
            tenant_id,
            dataset_id,
            table_name
        );

        // Return placeholder file paths (actual paths will be in Iceberg metadata)
        // For Phase 2, we return synthetic paths
        let output_files: Vec<String> = (0..split_batches.len())
            .map(|i| format!("compacted-{}.parquet", i))
            .collect();

        Ok(output_files)
    }

    /// Split batches to target file size
    fn split_batches_by_size(
        &self,
        batches: Vec<RecordBatch>,
        target_size_bytes: u64,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = vec![];
        let mut current_batch_rows = vec![];
        let mut current_size = 0u64;

        for batch in batches {
            let batch_size = batch.get_array_memory_size() as u64;

            // If this batch alone exceeds target size, split it
            if batch_size > target_size_bytes && batch.num_rows() > 1 {
                // Flush current accumulated rows first
                if !current_batch_rows.is_empty() {
                    let merged = self.merge_batches(&current_batch_rows)?;
                    result.push(merged);
                    current_batch_rows.clear();
                    current_size = 0;
                }

                // Split large batch into smaller chunks
                // Use u128 to avoid overflow in multiplication
                let rows_per_chunk = ((batch.num_rows() as u128 * target_size_bytes as u128)
                    / batch_size as u128)
                    .min(usize::MAX as u128) as usize;
                let rows_per_chunk = rows_per_chunk.max(1);

                let mut offset = 0;
                while offset < batch.num_rows() {
                    let length = (batch.num_rows() - offset).min(rows_per_chunk);
                    let slice = batch.slice(offset, length);
                    result.push(slice);
                    offset += length;
                }
            } else if current_size + batch_size > target_size_bytes
                && !current_batch_rows.is_empty()
            {
                // Current accumulation would exceed target, flush it
                let merged = self.merge_batches(&current_batch_rows)?;
                result.push(merged);
                current_batch_rows.clear();
                current_batch_rows.push(batch);
                current_size = batch_size;
            } else {
                // Accumulate this batch
                current_batch_rows.push(batch);
                current_size += batch_size;
            }
        }

        // Flush remaining batches
        if !current_batch_rows.is_empty() {
            let merged = self.merge_batches(&current_batch_rows)?;
            result.push(merged);
        }

        Ok(result)
    }

    /// Merge multiple batches with the same schema into one
    fn merge_batches(&self, batches: &[RecordBatch]) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Err(anyhow::anyhow!("Cannot merge empty batch list"));
        }

        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        // Use DataFusion's concat_batches
        let schema = batches[0].schema();
        datafusion::arrow::compute::concat_batches(&schema, batches)
            .context("Failed to merge batches")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_rewriter_creation() {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let rewriter = ParquetRewriter::new(catalog_manager);

        assert!(std::mem::size_of_val(&rewriter) > 0);
    }

    #[tokio::test]
    async fn test_merge_batches() {
        let catalog_manager = CatalogManager::new_in_memory().await.unwrap();
        let rewriter = ParquetRewriter::new(Arc::new(catalog_manager));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["Charlie", "Diana"])),
            ],
        )
        .unwrap();

        let merged = rewriter.merge_batches(&[batch1, batch2]).unwrap();

        assert_eq!(merged.num_rows(), 4);
        assert_eq!(merged.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_split_batches_by_size() {
        let catalog_manager = CatalogManager::new_in_memory().await.unwrap();
        let rewriter = ParquetRewriter::new(Arc::new(catalog_manager));

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Create a batch with 100 rows
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..100).collect::<Vec<i64>>()))],
        )
        .unwrap();

        let batch_size = batch.get_array_memory_size() as u64;

        // Split with target size smaller than batch
        let target_size = batch_size / 3;
        let split = rewriter
            .split_batches_by_size(vec![batch], target_size)
            .expect("Split should succeed");

        // Should be split into multiple batches
        assert!(split.len() > 1);

        // Total rows should be preserved
        let total_rows: usize = split.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }
}
