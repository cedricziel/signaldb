//! Parquet file reading, merging, and rewriting
//!
//! Handles the core compaction logic: reading a table's live data files,
//! merging and sorting the data, and writing optimized larger Parquet files
//! directly to the table's object store. The atomic snapshot commit that swaps
//! old files for new ones is performed by [`crate::commit::IcebergCommitter`].

use anyhow::{Context, Result};
use common::CatalogManager;
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::*;
use iceberg_rust::spec::manifest::DataFile;
use iceberg_rust::table::Table;
use std::sync::Arc;

/// Result of rewriting a table's data files.
pub struct RewriteOutcome {
    /// Newly written data files (with real paths, sizes, and record counts)
    /// ready to be committed as a replacement snapshot.
    pub new_files: Vec<DataFile>,
    /// Total bytes written across the new files.
    pub output_size_bytes: u64,
    /// Total rows written (for integrity verification against the input).
    pub rows_written: u64,
}

/// Handles Parquet file merging and rewriting
pub struct ParquetRewriter {
    catalog_manager: Arc<CatalogManager>,
}

impl ParquetRewriter {
    /// Create a new Parquet rewriter
    pub fn new(catalog_manager: Arc<CatalogManager>) -> Self {
        Self { catalog_manager }
    }

    /// Load a table with fresh metadata, without creating it if missing.
    ///
    /// Unlike `ensure_table`, this never creates the table: compaction must
    /// only operate on tables that already exist, and always on current
    /// metadata.
    pub async fn load_fresh_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Table> {
        let identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);
        let tabular = self
            .catalog_manager
            .catalog()
            .load_tabular(&identifier)
            .await
            .with_context(|| format!("Failed to load table {identifier} with fresh metadata"))?;
        match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(table) => Ok(table),
            _ => Err(anyhow::anyhow!("Expected table but got view: {identifier}")),
        }
    }

    /// Read, merge, sort, and rewrite the table's data into new Parquet files.
    ///
    /// Reads all live data from `table` (which the caller loaded fresh and
    /// pinned to a snapshot), sorts it for query performance, and writes new
    /// Parquet files directly to the table's object store. No snapshot is
    /// committed here — the caller commits the returned files atomically.
    ///
    /// Returns `None` when the table has no data to compact.
    pub async fn rewrite_table(
        &self,
        table: &Table,
        target_file_size_bytes: u64,
    ) -> Result<Option<RewriteOutcome>> {
        let table_name = table.identifier().name().to_string();

        let merged_batches = self
            .read_and_merge(table)
            .await
            .context("Failed to read and merge table data")?;

        if merged_batches.is_empty() || merged_batches.iter().all(|b| b.num_rows() == 0) {
            tracing::info!(table = %table_name, "No data found, skipping compaction");
            return Ok(None);
        }

        let rows_read: u64 = merged_batches.iter().map(|b| b.num_rows() as u64).sum();

        // Chunk batches toward the target file size so the writer produces
        // reasonably sized files.
        let split_batches = self.split_batches_by_size(merged_batches, target_file_size_bytes)?;

        let batch_stream = futures::stream::iter(
            split_batches
                .into_iter()
                .map(Ok::<_, datafusion::arrow::error::ArrowError>),
        );

        let new_files =
            iceberg_rust::arrow::write::write_parquet_partitioned(table, batch_stream, None)
                .await
                .context("Failed to write compacted Parquet files")?;

        let output_size_bytes: u64 = new_files
            .iter()
            .map(|f| *f.file_size_in_bytes() as u64)
            .sum();
        let rows_written: u64 = new_files.iter().map(|f| *f.record_count() as u64).sum();

        // A rewrite must never lose rows. Fail loudly before the commit if
        // the written files do not account for every row that was read.
        anyhow::ensure!(
            rows_written == rows_read,
            "Compaction row count mismatch for {table_name}: read {rows_read} rows but wrote {rows_written}"
        );

        tracing::info!(
            table = %table_name,
            output_files = new_files.len(),
            output_bytes = output_size_bytes,
            rows = rows_written,
            "Rewrote table data into compacted files"
        );

        Ok(Some(RewriteOutcome {
            new_files,
            output_size_bytes,
            rows_written,
        }))
    }

    /// Get sort columns for a given table type
    ///
    /// Returns a list of (column_name, ascending, nulls_first) tuples
    /// for sorting compacted data. Returns empty vector for unknown tables.
    fn get_sort_columns(table_name: &str) -> Vec<(&str, bool, bool)> {
        match table_name {
            "traces" => vec![("timestamp", true, true), ("trace_id", true, true)],
            "logs" => vec![
                ("timestamp", true, true),
                ("service_name", true, true),
                ("severity_text", true, true),
            ],
            // All 5 metrics types use the same sort pattern
            "metrics_gauge"
            | "metrics_sum"
            | "metrics_histogram"
            | "metrics_exponential_histogram"
            | "metrics_summary" => vec![
                ("timestamp", true, true),
                ("metric_name", true, true),
                ("service_name", true, true),
            ],
            _ => {
                tracing::warn!(
                    "No sort configuration for table {table_name}, data will not be sorted"
                );
                vec![]
            }
        }
    }

    /// Read and merge all live data from the table, sorted for query performance.
    async fn read_and_merge(&self, table: &Table) -> Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();

        let table_name = table.identifier().name().to_string();
        let datafusion_table = Arc::new(datafusion_iceberg::DataFusionTable::from(table.clone()));
        ctx.register_table(&table_name, datafusion_table)
            .context("Failed to register table with DataFusion")?;

        let df = ctx
            .table(&table_name)
            .await
            .context("Failed to read table")?;

        let sort_cols = Self::get_sort_columns(&table_name);
        let sorted_df = if !sort_cols.is_empty() {
            let sort_exprs: Vec<_> = sort_cols
                .into_iter()
                .map(|(col_name, asc, nulls_first)| col(col_name).sort(asc, nulls_first))
                .collect();

            df.sort(sort_exprs)
                .with_context(|| format!("Failed to sort {table_name} table"))?
        } else {
            df
        };

        let batches = sorted_df
            .collect()
            .await
            .context("Failed to collect query results")?;

        tracing::debug!(
            table = %table_name,
            batch_count = batches.len(),
            "Collected table data for rewrite"
        );

        Ok(batches)
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
