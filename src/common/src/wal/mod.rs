use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions, create_dir_all};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

/// WAL entry representing a single operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Unique identifier for this entry
    pub id: Uuid,
    /// Timestamp when entry was created
    pub timestamp: u64,
    /// Type of operation
    pub operation: WalOperation,
    /// Size of the data in bytes
    pub data_size: u64,
    /// Offset in the data file where the actual data is stored
    pub data_offset: u64,
    /// Whether this entry has been processed
    pub processed: bool,
}

/// Types of operations that can be logged in WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    /// Write operation for trace data
    WriteTraces,
    /// Write operation for log data
    WriteLogs,
    /// Write operation for metric data
    WriteMetrics,
    /// Flush operation to persistent storage
    Flush,
}

/// WAL segment containing multiple entries
#[derive(Debug)]
pub struct WalSegment {
    /// Segment ID
    pub id: u64,
    /// Path to the segment file
    pub path: PathBuf,
    /// Path to the data file
    pub data_path: PathBuf,
    /// File handle for writing
    file: Option<File>,
    /// Data file handle for writing
    data_file: Option<File>,
    /// Current size of the segment
    pub size: u64,
    /// Current size of the data file
    pub data_size: u64,
    /// Entries in this segment
    pub entries: Vec<WalEntry>,
}

impl WalSegment {
    /// Create a new WAL segment
    pub async fn new(wal_dir: &Path, segment_id: u64) -> Result<Self> {
        create_dir_all(wal_dir).await?;

        let path = wal_dir.join(format!("wal-{segment_id:010}.log"));
        let data_path = wal_dir.join(format!("wal-{segment_id:010}.data"));

        let file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await
                .context("Failed to create WAL segment file")?,
        );

        let data_file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&data_path)
                .await
                .context("Failed to create WAL data file")?,
        );

        Ok(Self {
            id: segment_id,
            path,
            data_path,
            file,
            data_file,
            size: 0,
            data_size: 0,
            entries: Vec::new(),
        })
    }

    /// Load an existing WAL segment from disk
    pub async fn load(wal_dir: &Path, segment_id: u64) -> Result<Self> {
        let path = wal_dir.join(format!("wal-{segment_id:010}.log"));
        let data_path = wal_dir.join(format!("wal-{segment_id:010}.data"));

        if !path.exists() {
            return Self::new(wal_dir, segment_id).await;
        }

        // Read existing entries
        let mut file = File::open(&path).await?;
        let mut entries = Vec::new();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let mut offset = 0;
        while offset < buffer.len() {
            // Read entry length (8 bytes)
            if offset + 8 > buffer.len() {
                break;
            }
            let entry_len = u64::from_le_bytes(
                buffer[offset..offset + 8]
                    .try_into()
                    .context("Failed to read entry length")?,
            );
            offset += 8;

            // Read entry data
            if offset + entry_len as usize > buffer.len() {
                break;
            }
            let entry_data = &buffer[offset..offset + entry_len as usize];
            let entry: WalEntry =
                bincode::deserialize(entry_data).context("Failed to deserialize WAL entry")?;
            entries.push(entry);
            offset += entry_len as usize;
        }

        let size = buffer.len() as u64;
        let data_size = if data_path.exists() {
            tokio::fs::metadata(&data_path).await?.len()
        } else {
            0
        };

        let file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await?,
        );

        let data_file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&data_path)
                .await?,
        );

        Ok(Self {
            id: segment_id,
            path,
            data_path,
            file,
            data_file,
            size,
            data_size,
            entries,
        })
    }

    /// Append an entry to the WAL segment
    pub async fn append(
        &mut self,
        entry_id: Uuid,
        operation: WalOperation,
        data: &[u8],
    ) -> Result<Uuid> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Write data to data file first
        let data_offset = self.data_size;
        if let Some(ref mut data_file) = self.data_file {
            data_file.write_all(data).await?;
            data_file.flush().await?;
        }
        self.data_size += data.len() as u64;

        // Create WAL entry
        let entry = WalEntry {
            id: entry_id,
            timestamp,
            operation,
            data_size: data.len() as u64,
            data_offset,
            processed: false,
        };

        // Serialize entry
        let entry_data = bincode::serialize(&entry).context("Failed to serialize WAL entry")?;

        // Write entry length followed by entry data
        if let Some(ref mut file) = self.file {
            let entry_len = entry_data.len() as u64;
            file.write_all(&entry_len.to_le_bytes()).await?;
            file.write_all(&entry_data).await?;
            file.flush().await?;
        }

        self.size += 8 + entry_data.len() as u64;
        self.entries.push(entry);

        Ok(entry_id)
    }

    /// Read data for a specific entry
    pub async fn read_entry_data(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        let mut data_file = File::open(&self.data_path).await?;
        data_file.seek(SeekFrom::Start(entry.data_offset)).await?;

        let mut buffer = vec![0u8; entry.data_size as usize];
        data_file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    /// Close the segment files
    pub async fn close(&mut self) -> Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush().await?;
        }
        if let Some(mut data_file) = self.data_file.take() {
            data_file.flush().await?;
        }
        Ok(())
    }
}

/// Configuration for the WAL
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory where WAL files are stored
    pub wal_dir: PathBuf,
    /// Maximum size of a single WAL segment in bytes
    pub max_segment_size: u64,
    /// Maximum number of entries in memory buffer before forcing flush
    pub max_buffer_entries: usize,
    /// Maximum time to wait before forcing flush (in seconds)
    pub flush_interval_secs: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_dir: PathBuf::from(".wal"),
            max_segment_size: 64 * 1024 * 1024, // 64MB
            max_buffer_entries: 1000,
            flush_interval_secs: 30,
        }
    }
}

/// Type alias for WAL buffer entries
type WalBuffer = Arc<RwLock<VecDeque<(Uuid, WalOperation, Vec<u8>)>>>;

/// Write-Ahead Log implementation for durability
pub struct Wal {
    config: WalConfig,
    current_segment: Arc<Mutex<WalSegment>>,
    next_segment_id: Arc<Mutex<u64>>,
    buffer: WalBuffer,
    flush_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Wal {
    /// Create a new WAL instance
    pub async fn new(config: WalConfig) -> Result<Self> {
        create_dir_all(&config.wal_dir).await?;

        // Find the latest segment ID
        let mut max_segment_id = 0;
        let mut dir = tokio::fs::read_dir(&config.wal_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("wal-") && name.ends_with(".log") {
                    if let Some(id_str) = name
                        .strip_prefix("wal-")
                        .and_then(|s| s.strip_suffix(".log"))
                    {
                        if let Ok(id) = id_str.parse::<u64>() {
                            max_segment_id = max_segment_id.max(id);
                        }
                    }
                }
            }
        }

        // Load or create current segment
        let current_segment = Arc::new(Mutex::new(
            WalSegment::load(&config.wal_dir, max_segment_id).await?,
        ));

        let wal = Self {
            config: config.clone(),
            current_segment,
            next_segment_id: Arc::new(Mutex::new(max_segment_id + 1)),
            buffer: Arc::new(RwLock::new(VecDeque::new())),
            flush_handle: None,
        };

        Ok(wal)
    }

    /// Start background flush task
    pub fn start_background_flush(&mut self) {
        let buffer = self.buffer.clone();
        let current_segment = self.current_segment.clone();
        let config = self.config.clone();
        let next_segment_id = self.next_segment_id.clone();

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(config.flush_interval_secs));

            loop {
                interval.tick().await;

                let should_flush = {
                    let buffer = buffer.read().await;
                    buffer.len() >= config.max_buffer_entries || !buffer.is_empty()
                };

                if should_flush {
                    if let Err(e) =
                        Self::flush_buffer(&buffer, &current_segment, &config, &next_segment_id)
                            .await
                    {
                        log::error!("Failed to flush WAL buffer: {e}");
                    }
                }
            }
        });

        self.flush_handle = Some(handle);
    }

    /// Add an entry to the WAL
    pub async fn append(&self, operation: WalOperation, data: Vec<u8>) -> Result<Uuid> {
        let entry_id = Uuid::new_v4();

        // Add to buffer first for batching
        {
            let mut buffer = self.buffer.write().await;
            buffer.push_back((entry_id, operation.clone(), data.clone()));
        }

        // Check if we need to flush immediately
        let should_flush = {
            let buffer = self.buffer.read().await;
            buffer.len() >= self.config.max_buffer_entries
        };

        if should_flush {
            Self::flush_buffer(
                &self.buffer,
                &self.current_segment,
                &self.config,
                &self.next_segment_id,
            )
            .await?;
        }

        Ok(entry_id)
    }

    /// Flush buffered entries to WAL
    async fn flush_buffer(
        buffer: &WalBuffer,
        current_segment: &Arc<Mutex<WalSegment>>,
        config: &WalConfig,
        next_segment_id: &Arc<Mutex<u64>>,
    ) -> Result<()> {
        let entries_to_flush = {
            let mut buffer = buffer.write().await;
            let mut entries = Vec::new();
            while let Some(entry) = buffer.pop_front() {
                entries.push(entry);
            }
            entries
        };

        if entries_to_flush.is_empty() {
            return Ok(());
        }

        let mut segment = current_segment.lock().await;

        for (entry_id, operation, data) in entries_to_flush {
            // Check if we need to rotate to a new segment
            if segment.size + data.len() as u64 > config.max_segment_size {
                // Close current segment
                segment.close().await?;

                // Create new segment
                let new_segment_id = {
                    let mut id = next_segment_id.lock().await;
                    let current_id = *id;
                    *id += 1;
                    current_id
                };

                *segment = WalSegment::new(&config.wal_dir, new_segment_id).await?;
            }

            segment.append(entry_id, operation, &data).await?;
        }

        Ok(())
    }

    /// Force flush all buffered entries
    pub async fn flush(&self) -> Result<()> {
        Self::flush_buffer(
            &self.buffer,
            &self.current_segment,
            &self.config,
            &self.next_segment_id,
        )
        .await
    }

    /// Get all entries from WAL for recovery
    pub async fn get_entries(&self) -> Result<Vec<WalEntry>> {
        let segment = self.current_segment.lock().await;
        Ok(segment.entries.clone())
    }

    /// Read data for a specific entry
    pub async fn read_entry_data(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        let segment = self.current_segment.lock().await;
        segment.read_entry_data(entry).await
    }

    /// Shutdown the WAL and cleanup resources
    pub async fn shutdown(mut self) -> Result<()> {
        // Stop background flush task
        if let Some(handle) = self.flush_handle.take() {
            handle.abort();
        }

        // Flush any remaining entries
        self.flush().await?;

        // Close current segment
        let mut segment = self.current_segment.lock().await;
        segment.close().await?;

        Ok(())
    }

    /// Mark a WAL entry as processed
    pub async fn mark_processed(&self, entry_id: Uuid) -> Result<()> {
        let mut segment = self.current_segment.lock().await;

        // Find the entry and mark it as processed
        for entry in &mut segment.entries {
            if entry.id == entry_id {
                entry.processed = true;
                // TODO: Persist the processed state to disk
                log::debug!("Marked WAL entry {entry_id} as processed");
                return Ok(());
            }
        }

        anyhow::bail!("WAL entry {} not found", entry_id)
    }

    /// Get all unprocessed entries
    pub async fn get_unprocessed_entries(&self) -> Result<Vec<WalEntry>> {
        let segment = self.current_segment.lock().await;
        Ok(segment
            .entries
            .iter()
            .filter(|e| !e.processed)
            .cloned()
            .collect())
    }

    /// Remove processed entries older than the retention period
    pub async fn cleanup_processed_entries(&self, retention_secs: u64) -> Result<usize> {
        let mut segment = self.current_segment.lock().await;
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let initial_count = segment.entries.len();

        // Remove entries that are processed and older than retention period
        segment.entries.retain(|entry| {
            if entry.processed {
                let entry_age = current_time - entry.timestamp;
                entry_age < retention_secs
            } else {
                true // Keep unprocessed entries
            }
        });

        let removed_count = initial_count - segment.entries.len();
        if removed_count > 0 {
            log::info!("Cleaned up {removed_count} processed WAL entries");
        }

        Ok(removed_count)
    }
}

/// Utility to convert RecordBatch to bytes for WAL storage
pub fn record_batch_to_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
    use datafusion::arrow::ipc::writer::StreamWriter;

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

/// Utility to convert bytes back to RecordBatch from WAL
pub fn bytes_to_record_batch(bytes: &[u8]) -> Result<RecordBatch> {
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;

    if let Some(batch_result) = reader.next() {
        Ok(batch_result?)
    } else {
        anyhow::bail!("No record batch found in WAL data")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_wal_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024,
            max_buffer_entries: 10,
            flush_interval_secs: 1,
        };

        let mut wal = Wal::new(config).await.unwrap();
        wal.start_background_flush();

        // Create test data
        let test_data = b"test data".to_vec();

        // Append entry
        let _entry_id = wal
            .append(WalOperation::WriteTraces, test_data.clone())
            .await
            .unwrap();

        // Force flush
        wal.flush().await.unwrap();

        // Verify entries
        let entries = wal.get_entries().await.unwrap();
        assert!(!entries.is_empty());

        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_record_batch_serialization() {
        // Create test RecordBatch
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        // Serialize to bytes
        let bytes = record_batch_to_bytes(&batch).unwrap();

        // Deserialize back
        let recovered_batch = bytes_to_record_batch(&bytes).unwrap();

        // Verify
        assert_eq!(batch.num_rows(), recovered_batch.num_rows());
        assert_eq!(batch.num_columns(), recovered_batch.num_columns());
    }
}
