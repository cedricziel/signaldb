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
    /// Tenant ID for multi-tenant isolation
    pub tenant_id: String,
    /// Dataset ID for data partitioning
    pub dataset_id: String,
    /// Optional metadata as JSON string (e.g., FlightMetadata with target_table)
    #[serde(default)]
    pub metadata: Option<String>,
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
    /// Path to the index file (tracks processed entries)
    pub index_path: PathBuf,
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
        let index_path = wal_dir.join(format!("wal-{segment_id:010}.index"));

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
            index_path,
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
        let index_path = wal_dir.join(format!("wal-{segment_id:010}.index"));

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

        // Load processed state from index file if it exists
        if index_path.exists() {
            let processed_ids = Self::load_index(&index_path).await?;
            // Mark entries as processed based on index
            for entry in &mut entries {
                if processed_ids.contains(&entry.id) {
                    entry.processed = true;
                }
            }
            log::debug!(
                "Loaded {} processed entries from index for segment {segment_id}",
                processed_ids.len()
            );
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
            index_path,
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
        tenant_id: &str,
        dataset_id: &str,
        metadata: Option<String>,
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
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            metadata,
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

    /// Load processed entry IDs from the index file
    async fn load_index(index_path: &Path) -> Result<std::collections::HashSet<Uuid>> {
        let mut file = File::open(index_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        if buffer.len() < 8 {
            return Ok(std::collections::HashSet::new());
        }

        // Read count (8 bytes)
        let count = u64::from_le_bytes(
            buffer[0..8]
                .try_into()
                .context("Failed to read processed entry count")?,
        );

        let mut processed_ids = std::collections::HashSet::new();
        let mut offset = 8;

        for _ in 0..count {
            if offset + 16 > buffer.len() {
                break;
            }
            let uuid_bytes: [u8; 16] = buffer[offset..offset + 16]
                .try_into()
                .context("Failed to read UUID")?;
            processed_ids.insert(Uuid::from_bytes(uuid_bytes));
            offset += 16;
        }

        Ok(processed_ids)
    }

    /// Save processed entry IDs to the index file
    pub async fn save_index(&self) -> Result<()> {
        let processed_ids: Vec<Uuid> = self
            .entries
            .iter()
            .filter(|e| e.processed)
            .map(|e| e.id)
            .collect();

        let mut buffer = Vec::new();

        // Write count (8 bytes)
        buffer.extend_from_slice(&(processed_ids.len() as u64).to_le_bytes());

        // Write each UUID (16 bytes each)
        for uuid in processed_ids {
            buffer.extend_from_slice(uuid.as_bytes());
        }

        // Write to file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.index_path)
            .await?;

        file.write_all(&buffer).await?;
        file.flush().await?;

        Ok(())
    }

    /// Check if all entries in this segment are processed
    pub fn is_fully_processed(&self) -> bool {
        !self.entries.is_empty() && self.entries.iter().all(|e| e.processed)
    }

    /// Get the percentage of processed entries (0.0 - 1.0)
    pub fn processed_percentage(&self) -> f64 {
        if self.entries.is_empty() {
            return 0.0;
        }
        let processed_count = self.entries.iter().filter(|e| e.processed).count();
        processed_count as f64 / self.entries.len() as f64
    }

    /// Delete segment files from disk
    pub async fn delete_files(&self) -> Result<()> {
        if self.path.exists() {
            tokio::fs::remove_file(&self.path).await?;
        }
        if self.data_path.exists() {
            tokio::fs::remove_file(&self.data_path).await?;
        }
        if self.index_path.exists() {
            tokio::fs::remove_file(&self.index_path).await?;
        }
        log::info!("Deleted segment files for segment {}", self.id);
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
    /// Tenant ID for multi-tenant isolation (REQUIRED for proper isolation)
    pub tenant_id: String,
    /// Dataset ID for data partitioning (REQUIRED for proper isolation)
    pub dataset_id: String,
    /// How long to keep processed entries before cleanup (in seconds)
    pub retention_secs: u64,
    /// Interval for running cleanup operations (in seconds)
    pub cleanup_interval_secs: u64,
    /// Threshold percentage (0.0-1.0) of processed entries before compacting a segment
    pub compaction_threshold: f64,
}

impl WalConfig {
    /// Create a base WalConfig with default performance settings
    /// This should only be used with for_tenant_dataset() to create tenant-specific configs
    pub fn with_defaults(base_dir: PathBuf) -> Self {
        Self {
            wal_dir: base_dir,
            max_segment_size: 64 * 1024 * 1024, // 64MB
            max_buffer_entries: 1000,
            flush_interval_secs: 30,
            // Default tenant/dataset for single-tenant deployments
            tenant_id: "default".to_string(),
            dataset_id: "default".to_string(),
            retention_secs: 3600,       // 1 hour
            cleanup_interval_secs: 300, // 5 minutes
            compaction_threshold: 0.5,  // 50% processed entries triggers compaction
        }
    }
}

impl WalConfig {
    /// Get the WAL directory path for a specific tenant/dataset/signal combination
    ///
    /// Path structure: `{base_wal_dir}/{tenant}/{dataset}/{signal_type}/`
    /// Example: `.wal/acme/production/traces/`
    pub fn get_wal_path(&self, tenant: &str, dataset: &str, signal_type: &str) -> PathBuf {
        self.wal_dir.join(tenant).join(dataset).join(signal_type)
    }

    /// Create a tenant/dataset-specific WAL configuration
    ///
    /// This creates a new WalConfig with the wal_dir set to the tenant/dataset/signal path
    /// while preserving all other configuration settings
    pub fn for_tenant_dataset(&self, tenant: &str, dataset: &str, signal_type: &str) -> WalConfig {
        WalConfig {
            wal_dir: self.get_wal_path(tenant, dataset, signal_type),
            max_segment_size: self.max_segment_size,
            max_buffer_entries: self.max_buffer_entries,
            flush_interval_secs: self.flush_interval_secs,
            tenant_id: tenant.to_string(),
            dataset_id: dataset.to_string(),
            retention_secs: self.retention_secs,
            cleanup_interval_secs: self.cleanup_interval_secs,
            compaction_threshold: self.compaction_threshold,
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self::with_defaults(PathBuf::from(".wal"))
    }
}

/// Type alias for WAL buffer entries (entry_id, operation, data, optional_metadata)
type WalBuffer = Arc<RwLock<VecDeque<(Uuid, WalOperation, Vec<u8>, Option<String>)>>>;

/// Write-Ahead Log implementation for durability
pub struct Wal {
    config: WalConfig,
    current_segment: Arc<Mutex<WalSegment>>,
    next_segment_id: Arc<Mutex<u64>>,
    buffer: WalBuffer,
    flush_handle: Option<tokio::task::JoinHandle<()>>,
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    /// All segments including current (for cleanup operations)
    segments: Arc<Mutex<Vec<Arc<Mutex<WalSegment>>>>>,
}

impl Wal {
    /// Create a new WAL instance
    ///
    /// IMPORTANT: This WAL instance is strictly scoped to a single tenant/dataset.
    /// The config must have non-empty tenant_id and dataset_id to ensure proper isolation.
    pub async fn new(config: WalConfig) -> Result<Self> {
        // Enforce strict tenant/dataset isolation - reject empty values
        if config.tenant_id.is_empty() {
            return Err(anyhow::anyhow!(
                "WAL configuration requires non-empty tenant_id for proper multi-tenant isolation"
            ));
        }
        if config.dataset_id.is_empty() {
            return Err(anyhow::anyhow!(
                "WAL configuration requires non-empty dataset_id for proper data partitioning"
            ));
        }

        create_dir_all(&config.wal_dir).await?;

        // Find all segment IDs
        let mut segment_ids = Vec::new();
        let mut dir = tokio::fs::read_dir(&config.wal_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("wal-") && name.ends_with(".log") {
                    if let Some(id_str) = name
                        .strip_prefix("wal-")
                        .and_then(|s| s.strip_suffix(".log"))
                    {
                        if let Ok(id) = id_str.parse::<u64>() {
                            segment_ids.push(id);
                        }
                    }
                }
            }
        }

        // Sort segment IDs
        segment_ids.sort_unstable();

        // Determine the latest segment ID
        let max_segment_id = segment_ids.last().copied().unwrap_or(0);

        // Load all segments
        let mut all_segments = Vec::new();
        for segment_id in &segment_ids {
            let segment = Arc::new(Mutex::new(
                WalSegment::load(&config.wal_dir, *segment_id).await?,
            ));
            all_segments.push(segment);
        }

        // Load or create current segment if no segments exist
        let current_segment = if segment_ids.is_empty() {
            let segment = Arc::new(Mutex::new(
                WalSegment::new(&config.wal_dir, max_segment_id).await?,
            ));
            all_segments.push(segment.clone());
            segment
        } else {
            all_segments
                .last()
                .expect("segments list should not be empty")
                .clone()
        };

        let wal = Self {
            config: config.clone(),
            current_segment,
            next_segment_id: Arc::new(Mutex::new(max_segment_id + 1)),
            buffer: Arc::new(RwLock::new(VecDeque::new())),
            flush_handle: None,
            cleanup_handle: None,
            segments: Arc::new(Mutex::new(all_segments)),
        };

        Ok(wal)
    }

    /// Start background flush task
    pub fn start_background_flush(&mut self) {
        let buffer = self.buffer.clone();
        let current_segment = self.current_segment.clone();
        let config = self.config.clone();
        let next_segment_id = self.next_segment_id.clone();
        let segments = self.segments.clone();

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
                    if let Err(e) = Self::flush_buffer(
                        &buffer,
                        &current_segment,
                        &config,
                        &next_segment_id,
                        &segments,
                    )
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
    ///
    /// # Arguments
    /// * `operation` - The type of WAL operation
    /// * `data` - The data to write
    /// * `metadata` - Optional metadata (e.g., JSON-serialized FlightMetadata with target_table)
    pub async fn append(
        &self,
        operation: WalOperation,
        data: Vec<u8>,
        metadata: Option<String>,
    ) -> Result<Uuid> {
        let entry_id = Uuid::new_v4();

        // Add to buffer first for batching
        {
            let mut buffer = self.buffer.write().await;
            buffer.push_back((entry_id, operation.clone(), data.clone(), metadata));
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
                &self.segments,
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
        segments: &Arc<Mutex<Vec<Arc<Mutex<WalSegment>>>>>,
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

        // Verify tenant/dataset isolation at runtime
        debug_assert!(
            !config.tenant_id.is_empty(),
            "WAL instance must be tenant-scoped with non-empty tenant_id"
        );
        debug_assert!(
            !config.dataset_id.is_empty(),
            "WAL instance must be dataset-scoped with non-empty dataset_id"
        );

        // Use the guaranteed per-instance tenant/dataset values
        let tenant_id = &config.tenant_id;
        let dataset_id = &config.dataset_id;

        for (entry_id, operation, data, metadata) in entries_to_flush {
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

                let new_segment = WalSegment::new(&config.wal_dir, new_segment_id).await?;
                *segment = new_segment;

                // Add the new segment to the segments list
                // Note: current_segment is already in segments list, so we update it there too
                drop(segment); // Release lock before acquiring segments lock
                let mut segs = segments.lock().await;
                let new_seg_arc = Arc::new(Mutex::new(
                    WalSegment::new(&config.wal_dir, new_segment_id).await?,
                ));
                segs.push(new_seg_arc);
                drop(segs);

                // Re-acquire the current segment lock
                segment = current_segment.lock().await;
            }

            segment
                .append(entry_id, operation, &data, tenant_id, dataset_id, metadata)
                .await?;
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
            &self.segments,
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
        // Stop background tasks
        if let Some(handle) = self.flush_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
        }

        // Flush any remaining entries
        self.flush().await?;

        // Close all segments
        let segments = self.segments.lock().await;
        for segment_arc in segments.iter() {
            let mut segment = segment_arc.lock().await;
            segment.close().await?;
        }

        Ok(())
    }

    /// Mark a WAL entry as processed and persist the state to disk
    pub async fn mark_processed(&self, entry_id: Uuid) -> Result<()> {
        // Search all segments, not just current
        let segments = self.segments.lock().await;

        for segment_arc in segments.iter() {
            let mut segment = segment_arc.lock().await;

            // Find the entry in this segment
            for entry in &mut segment.entries {
                if entry.id == entry_id {
                    entry.processed = true;

                    // Persist the processed state to disk
                    segment.save_index().await?;

                    log::debug!("Marked WAL entry {entry_id} as processed and persisted to index");
                    return Ok(());
                }
            }
        }

        anyhow::bail!(
            "WAL entry {entry_id} not found in any of {} segments",
            segments.len()
        )
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

    /// Delete fully-processed old segments
    async fn delete_fully_processed_segments(&self) -> Result<usize> {
        let mut segments = self.segments.lock().await;
        let mut deleted_count = 0;

        // Find segments to delete (all processed, not the current segment)
        let mut segments_to_delete = Vec::new();
        for (i, segment_arc) in segments.iter().enumerate() {
            // Skip the current segment (should be the last one)
            if i == segments.len() - 1 {
                continue;
            }

            let segment = segment_arc.lock().await;
            if segment.is_fully_processed() {
                segments_to_delete.push(i);
            }
        }

        // Delete segments in reverse order to maintain indices
        for &index in segments_to_delete.iter().rev() {
            let segment_arc = segments.remove(index);
            let segment = segment_arc.lock().await;
            segment.delete_files().await?;
            deleted_count += 1;
        }

        if deleted_count > 0 {
            log::info!("Deleted {deleted_count} fully-processed WAL segments");
        }

        Ok(deleted_count)
    }

    /// Compact a segment by rewriting it without processed entries
    async fn compact_segment(
        segment_arc: &Arc<Mutex<WalSegment>>,
        config: &WalConfig,
    ) -> Result<bool> {
        let mut segment = segment_arc.lock().await;

        // Check if compaction is needed
        let processed_pct = segment.processed_percentage();
        if processed_pct < config.compaction_threshold {
            return Ok(false);
        }

        // Store original entry count before filtering
        let original_entry_count = segment.entries.len();

        log::info!(
            "Compacting segment {} ({:.1}% processed)",
            segment.id,
            processed_pct * 100.0
        );

        // Collect unprocessed entries
        let unprocessed_entries: Vec<_> = segment
            .entries
            .iter()
            .filter(|e| !e.processed)
            .cloned()
            .collect();

        if unprocessed_entries.is_empty() {
            // All entries processed, segment can be deleted (handled by delete_fully_processed_segments)
            return Ok(false);
        }

        // Read data for unprocessed entries
        let mut entries_with_data = Vec::new();
        for entry in &unprocessed_entries {
            let data = segment.read_entry_data(entry).await?;
            entries_with_data.push((entry.clone(), data));
        }

        // Close current segment files
        segment.close().await?;

        // Create temporary new segment
        let temp_segment_id = segment.id;

        // Delete old files
        if segment.path.exists() {
            tokio::fs::remove_file(&segment.path).await?;
        }
        if segment.data_path.exists() {
            tokio::fs::remove_file(&segment.data_path).await?;
        }
        if segment.index_path.exists() {
            tokio::fs::remove_file(&segment.index_path).await?;
        }

        // Create new compacted segment
        let mut new_segment = WalSegment::new(&config.wal_dir, temp_segment_id).await?;

        // Write unprocessed entries to new segment
        for (entry, data) in entries_with_data {
            new_segment
                .append(
                    entry.id,
                    entry.operation,
                    &data,
                    &entry.tenant_id,
                    &entry.dataset_id,
                    entry.metadata,
                )
                .await?;
        }

        new_segment.close().await?;

        // Replace old segment with compacted one
        *segment = new_segment;

        log::info!(
            "Compacted segment {} from {} to {} entries",
            temp_segment_id,
            original_entry_count,
            unprocessed_entries.len()
        );

        Ok(true)
    }

    /// Run cleanup: delete fully-processed segments and compact others
    async fn cleanup(&self) -> Result<()> {
        // Delete fully-processed old segments
        self.delete_fully_processed_segments().await?;

        // Compact segments that exceed the threshold
        let segments = self.segments.lock().await;
        for (i, segment_arc) in segments.iter().enumerate() {
            // Skip the current segment (last one)
            if i == segments.len() - 1 {
                continue;
            }

            Self::compact_segment(segment_arc, &self.config).await?;
        }

        Ok(())
    }

    /// Start background cleanup task
    pub fn start_background_cleanup(&mut self) {
        let config = self.config.clone();
        let buffer = self.buffer.clone();
        let current_segment = self.current_segment.clone();
        let next_segment_id = self.next_segment_id.clone();
        let segments = self.segments.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                config.cleanup_interval_secs,
            ));

            loop {
                interval.tick().await;

                // Create a temporary Wal instance for cleanup (reuses existing segments)
                let wal = Wal {
                    config: config.clone(),
                    current_segment: current_segment.clone(),
                    next_segment_id: next_segment_id.clone(),
                    buffer: buffer.clone(),
                    flush_handle: None,
                    cleanup_handle: None,
                    segments: segments.clone(),
                };

                if let Err(e) = wal.cleanup().await {
                    log::error!("Failed to run WAL cleanup: {e}");
                }
            }
        });

        self.cleanup_handle = Some(handle);
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
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };

        let mut wal = Wal::new(config).await.unwrap();
        wal.start_background_flush();

        // Create test data
        let test_data = b"test data".to_vec();

        // Append entry
        let _entry_id = wal
            .append(WalOperation::WriteTraces, test_data.clone(), None)
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
