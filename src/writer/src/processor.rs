use crate::storage::{IcebergTableWriter, create_iceberg_writer};
use anyhow::Result;
use common::config::Configuration;
use common::wal::{Wal, WalEntry, bytes_to_record_batch};
use datafusion::arrow::array::RecordBatch;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, interval};
use uuid::Uuid;

/// WAL processor that reads entries and writes them to Iceberg tables
/// Replaces the direct Parquet writing approach with transaction-based Iceberg writes
pub struct WalProcessor {
    wal: Arc<Wal>,
    config: Configuration,
    object_store: Arc<dyn ObjectStore>,
    // Cache of table writers per tenant/table combination
    table_writers: HashMap<String, IcebergTableWriter>,
}

impl WalProcessor {
    /// Create a new WAL processor
    pub fn new(wal: Arc<Wal>, config: Configuration, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            wal,
            config,
            object_store,
            table_writers: HashMap::new(),
        }
    }

    /// Start the WAL processing loop
    /// This will continuously process unprocessed WAL entries
    pub async fn start_processing_loop(&mut self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(1)); // Process every second

        loop {
            interval.tick().await;

            if let Err(e) = self.process_pending_entries().await {
                log::error!("Error processing WAL entries: {e}");
                // Continue processing despite errors
            }
        }
    }

    /// Process all pending WAL entries
    pub async fn process_pending_entries(&mut self) -> Result<()> {
        let pending_entries = self.wal.get_unprocessed_entries().await?;

        if pending_entries.is_empty() {
            return Ok(());
        }

        log::debug!("Processing {} pending WAL entries", pending_entries.len());

        // Group entries by tenant and table for batch processing
        let mut grouped_entries: HashMap<(String, String), Vec<(Uuid, RecordBatch)>> =
            HashMap::new();

        for entry in pending_entries {
            // Skip flush operations
            if matches!(entry.operation, common::wal::WalOperation::Flush) {
                continue;
            }

            let (tenant_id, table_name) = self.determine_target_table(&entry)?;
            let batch = self.deserialize_entry_data(&entry).await?;

            grouped_entries
                .entry((tenant_id, table_name))
                .or_default()
                .push((entry.id, batch));
        }

        // Process each group using batch writes
        for ((tenant_id, table_name), entries) in grouped_entries {
            match self
                .process_batch_for_table(&tenant_id, &table_name, entries)
                .await
            {
                Ok(processed_ids) => {
                    // Mark all entries as processed
                    for entry_id in processed_ids {
                        if let Err(e) = self.wal.mark_processed(entry_id).await {
                            log::warn!("Failed to mark WAL entry {entry_id} as processed: {e}");
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to process batch for table {tenant_id}.{table_name}: {e}");
                }
            }
        }

        Ok(())
    }

    /// Process a batch of entries for a specific table
    async fn process_batch_for_table(
        &mut self,
        tenant_id: &str,
        table_name: &str,
        entries: Vec<(Uuid, RecordBatch)>,
    ) -> Result<Vec<Uuid>> {
        let writer_key = format!("{tenant_id}:{table_name}");

        // Get or create table writer
        if !self.table_writers.contains_key(&writer_key) {
            let writer = create_iceberg_writer(
                &self.config,
                self.object_store.clone(),
                tenant_id,
                table_name,
            )
            .await?;
            self.table_writers.insert(writer_key.clone(), writer);
        }

        let writer = self
            .table_writers
            .get_mut(&writer_key)
            .ok_or_else(|| anyhow::anyhow!("Failed to get table writer for {}", writer_key))?;

        // Extract batches and IDs
        let (entry_ids, batches): (Vec<Uuid>, Vec<RecordBatch>) = entries.into_iter().unzip();

        // Write all batches in a single transaction
        writer.write_batches(batches).await?;

        log::debug!(
            "Successfully processed {} entries for table {}.{}",
            entry_ids.len(),
            tenant_id,
            table_name
        );

        Ok(entry_ids)
    }

    /// Determine which tenant and table an entry should go to
    /// Extracts tenant_id and dataset_id from the WalEntry and maps operation type to table name
    /// For metrics, uses target_table from metadata if available, enabling routing to
    /// metrics_exponential_histogram, metrics_summary, etc.
    fn determine_target_table(&self, entry: &WalEntry) -> Result<(String, String)> {
        // Map operation types to appropriate table
        let table_name = match entry.operation {
            common::wal::WalOperation::WriteTraces => "traces".to_string(),
            common::wal::WalOperation::WriteLogs => "logs".to_string(),
            common::wal::WalOperation::WriteMetrics => {
                // Try to extract target_table from metadata
                if let Some(ref metadata_str) = entry.metadata {
                    if let Ok(metadata) = serde_json::from_str::<serde_json::Value>(metadata_str) {
                        if let Some(target_table) = metadata.get("target_table") {
                            if let Some(table_str) = target_table.as_str() {
                                log::debug!(
                                    "Using target_table from metadata: {table_str} for WAL entry {}",
                                    entry.id
                                );
                                table_str.to_string()
                            } else {
                                log::warn!(
                                    "target_table in metadata is not a string, defaulting to metrics_gauge"
                                );
                                "metrics_gauge".to_string()
                            }
                        } else {
                            log::debug!("No target_table in metadata, defaulting to metrics_gauge");
                            "metrics_gauge".to_string()
                        }
                    } else {
                        log::warn!("Failed to parse metadata JSON, defaulting to metrics_gauge");
                        "metrics_gauge".to_string()
                    }
                } else {
                    // No metadata available - use default
                    log::debug!("No metadata available, defaulting to metrics_gauge");
                    "metrics_gauge".to_string()
                }
            }
            common::wal::WalOperation::Flush => {
                return Err(anyhow::anyhow!(
                    "Flush operations should not be processed as table writes"
                ));
            }
        };

        // Extract tenant_id from the entry (already stored during WAL creation)
        Ok((entry.tenant_id.clone(), table_name))
    }

    /// Deserialize WAL entry data back to RecordBatch
    async fn deserialize_entry_data(&self, entry: &WalEntry) -> Result<RecordBatch> {
        let data = self.wal.read_entry_data(entry).await?;
        bytes_to_record_batch(&data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize WAL entry data: {}", e))
    }

    /// Process a single WAL entry (for immediate processing)
    pub async fn process_single_entry(&mut self, entry_id: Uuid) -> Result<()> {
        // Find the entry in the current entries
        let entries = self.wal.get_entries().await?;
        let entry = entries
            .iter()
            .find(|e| e.id == entry_id)
            .ok_or_else(|| anyhow::anyhow!("WAL entry {} not found", entry_id))?
            .clone();

        if entry.processed {
            return Ok(()); // Already processed
        }

        // Skip flush operations
        if matches!(entry.operation, common::wal::WalOperation::Flush) {
            return Ok(());
        }

        let (tenant_id, table_name) = self.determine_target_table(&entry)?;
        let batch = self.deserialize_entry_data(&entry).await?;

        match self
            .process_batch_for_table(&tenant_id, &table_name, vec![(entry_id, batch)])
            .await
        {
            Ok(_) => {
                self.wal.mark_processed(entry_id).await?;
                log::debug!("Successfully processed single WAL entry {entry_id}");
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to process WAL entry {entry_id}: {e}");
                Err(e)
            }
        }
    }

    /// Get statistics about the processor
    pub fn get_stats(&self) -> ProcessorStats {
        ProcessorStats {
            active_writers: self.table_writers.len(),
            writer_keys: self.table_writers.keys().cloned().collect(),
        }
    }

    /// Close all table writers and clean up resources
    pub async fn shutdown(&mut self) -> Result<()> {
        log::info!(
            "Shutting down WAL processor with {} active writers",
            self.table_writers.len()
        );

        // Clear all writers (they should handle cleanup automatically when dropped)
        self.table_writers.clear();

        Ok(())
    }
}

/// Statistics about the WAL processor
#[derive(Debug)]
pub struct ProcessorStats {
    pub active_writers: usize,
    pub writer_keys: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::Configuration;
    use common::wal::{Wal, WalConfig, WalOperation};
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_processor_creation() {
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024, // 1MB
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let config = Configuration::default();
        let object_store = Arc::new(InMemory::new());

        let processor = WalProcessor::new(wal, config, object_store);
        assert_eq!(processor.table_writers.len(), 0);

        let stats = processor.get_stats();
        assert_eq!(stats.active_writers, 0);
    }

    #[tokio::test]
    async fn test_determine_target_table() {
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024, // 1MB
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let config = Configuration::default();
        let object_store = Arc::new(InMemory::new());

        let processor = WalProcessor::new(wal, config, object_store);

        // Test different operation types
        let entry = WalEntry {
            id: uuid::Uuid::new_v4(),
            operation: WalOperation::WriteTraces,
            data_size: 0,
            data_offset: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            processed: false,
            tenant_id: "acme".to_string(),
            dataset_id: "production".to_string(),
            metadata: None,
        };

        let (tenant, table) = processor.determine_target_table(&entry).unwrap();
        assert_eq!(tenant, "acme");
        assert_eq!(table, "traces");

        let entry = WalEntry {
            id: uuid::Uuid::new_v4(),
            operation: WalOperation::WriteLogs,
            data_size: 0,
            data_offset: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            processed: false,
            tenant_id: "globex".to_string(),
            dataset_id: "staging".to_string(),
            metadata: None,
        };

        let (tenant, table) = processor.determine_target_table(&entry).unwrap();
        assert_eq!(tenant, "globex");
        assert_eq!(table, "logs");
    }

    #[tokio::test]
    async fn test_process_pending_entries_empty() {
        let temp_dir = tempdir().unwrap();
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024, // 1MB
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());
        let config = Configuration::default();
        let object_store = Arc::new(InMemory::new());

        let mut processor = WalProcessor::new(wal, config, object_store);

        // Should handle empty entries gracefully
        let result = processor.process_pending_entries().await;
        assert!(result.is_ok());
    }
}
