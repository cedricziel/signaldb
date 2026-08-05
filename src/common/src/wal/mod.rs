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
    /// Write operation for profile data
    WriteProfiles,
    /// Flush operation to persistent storage
    Flush,
}

impl WalOperation {
    /// Stable signal name for telemetry (log fields, metric labels) and the
    /// acceptor's `signal` dimension. Matches the ingest vocabulary
    /// (`traces`/`logs`/`metrics`/`profiles`) so WAL diagnostics correlate
    /// with the pipeline that produced the entry.
    pub fn signal(&self) -> &'static str {
        match self {
            WalOperation::WriteTraces => "traces",
            WalOperation::WriteLogs => "logs",
            WalOperation::WriteMetrics => "metrics",
            WalOperation::WriteProfiles => "profiles",
            WalOperation::Flush => "flush",
        }
    }
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

        // Opened for writing without O_APPEND: `append` seeks to the entry's
        // authoritative offset before writing (see `append`), so O_APPEND —
        // which forces every write to the physical EOF regardless of the
        // recorded offset — must not be set (issue #865).
        let file = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .await
                .context("Failed to create WAL segment file")?,
        );

        let data_file = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
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

        // Reopened for writing without O_APPEND, matching `new` — appends seek
        // to the tracked offset (issue #865). `data_size`/`size` are reseeded
        // from the on-disk lengths below, so writes resume at the true EOF.
        let file = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .await?,
        );

        let data_file = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
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
        // A clock before the epoch yields timestamp 0 rather than a panic
        // on the hot write path.
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Write the payload to the data file at its authoritative offset.
        //
        // `data_offset` is recorded on the entry and later drives the read
        // seek, so the write must land at exactly that offset. We seek there
        // and overwrite rather than trusting O_APPEND to place bytes at the
        // physical EOF. This keeps the recorded offset and the physical write
        // location identical even after a short write: a partial-then-errored
        // write never advances `self.data_size`, so the next append seeks back
        // to the same offset and overwrites the debris instead of landing
        // past it. With O_APPEND a single short write permanently shifted every
        // subsequent entry, corrupting the Arrow framing (issue #865).
        let data_offset = self.data_size;
        if let Some(ref mut data_file) = self.data_file {
            data_file.seek(SeekFrom::Start(data_offset)).await?;
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

        // Write entry length followed by entry data at the log's authoritative
        // offset (`self.size`), for the same reason as the data file above: a
        // short log write leaves `self.size` unadvanced so the next append
        // overwrites the partial record rather than appending past it, which
        // would otherwise wedge recovery with a mid-file garbage record.
        if let Some(ref mut file) = self.file {
            let entry_len = entry_data.len() as u64;
            file.seek(SeekFrom::Start(self.size)).await?;
            file.write_all(&entry_len.to_le_bytes()).await?;
            file.write_all(&entry_data).await?;
            file.flush().await?;
        }

        self.size += 8 + entry_data.len() as u64;
        self.entries.push(entry);

        Ok(entry_id)
    }

    /// Read data for a specific entry.
    ///
    /// Validates the entry's `[data_offset, data_offset + data_size)` range
    /// against the actual data-file length before reading. A range that runs
    /// past the file (truncated/partial write, stale offset bookkeeping, or a
    /// corrupt index) yields a clear, attributable bounds error here rather
    /// than an opaque `read_exact` "failed to fill whole buffer" or, worse,
    /// in-bounds garbage that the Arrow reader later rejects as
    /// `RangeOutOfBounds`. The caller's dead-letter path then records which
    /// tenant/dataset/signal was affected.
    pub async fn read_entry_data(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        let data_len = tokio::fs::metadata(&self.data_path)
            .await
            .with_context(|| format!("Failed to stat WAL data file {}", self.data_path.display()))?
            .len();
        let end = entry
            .data_offset
            .checked_add(entry.data_size)
            .with_context(|| {
                format!(
                    "WAL entry {} data range overflows u64 (offset={}, size={})",
                    entry.id, entry.data_offset, entry.data_size
                )
            })?;
        if end > data_len {
            anyhow::bail!(
                "WAL entry {} data out of bounds: [{}, {}) exceeds data file length {} (segment {})",
                entry.id,
                entry.data_offset,
                end,
                data_len,
                self.id
            );
        }

        let mut data_file = File::open(&self.data_path).await?;
        data_file.seek(SeekFrom::Start(entry.data_offset)).await?;

        let mut buffer = vec![0u8; entry.data_size as usize];
        data_file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    /// Durably persist appended entries and data to disk (fsync)
    ///
    /// The data file is synced before the entry log: an entry record that
    /// survives a crash must never point at data that did not.
    pub async fn sync(&self) -> Result<()> {
        if let Some(ref data_file) = self.data_file {
            data_file
                .sync_all()
                .await
                .context("Failed to fsync WAL data file")?;
        }
        if let Some(ref file) = self.file {
            file.sync_all()
                .await
                .context("Failed to fsync WAL segment file")?;
        }
        Ok(())
    }

    /// Close the segment files, syncing them to disk first
    pub async fn close(&mut self) -> Result<()> {
        if let Some(mut data_file) = self.data_file.take() {
            data_file.flush().await?;
            data_file
                .sync_all()
                .await
                .context("Failed to fsync WAL data file on close")?;
        }
        if let Some(mut file) = self.file.take() {
            file.flush().await?;
            file.sync_all()
                .await
                .context("Failed to fsync WAL segment file on close")?;
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
        // Sync the index so processed-state survives power loss; losing it
        // would only cause reprocessing (at-least-once), but it is cheap
        // relative to how rarely the index is rewritten.
        file.sync_all()
            .await
            .context("Failed to fsync WAL index file")?;

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
    /// Stable identity of this WAL directory, persisted in `writer.id`.
    /// Survives restarts so downstream consumers can key idempotency
    /// markers to the WAL whose entries they process.
    writer_id: String,
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

        let writer_id = Self::load_or_create_writer_id(&config.wal_dir).await?;

        // Find all segment IDs
        let mut segment_ids = Vec::new();
        let mut dir = tokio::fs::read_dir(&config.wal_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            if let Some(name) = entry.file_name().to_str()
                && name.starts_with("wal-")
                && name.ends_with(".log")
                && let Some(id_str) = name
                    .strip_prefix("wal-")
                    .and_then(|s| s.strip_suffix(".log"))
                && let Ok(id) = id_str.parse::<u64>()
            {
                segment_ids.push(id);
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
                .ok_or_else(|| {
                    anyhow::anyhow!("WAL segment list empty despite discovered segment ids")
                })?
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
            writer_id,
        };

        Ok(wal)
    }

    /// Stable identity of this WAL directory (see the `writer_id` field).
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Move a poison entry aside: persist its raw payload to
    /// `<wal_dir>/dead-letter/<entry_id>.bin` (fsynced) and mark the
    /// entry processed so it stops blocking the processing loop. The
    /// data is preserved for manual inspection/replay rather than lost.
    ///
    /// Returns the dead-letter file path.
    pub async fn dead_letter(&self, entry_id: Uuid) -> Result<PathBuf> {
        let entry = self
            .get_entries()
            .await?
            .into_iter()
            .find(|e| e.id == entry_id)
            .ok_or_else(|| anyhow::anyhow!("WAL entry {entry_id} not found"))?;
        let data = self.read_entry_data(&entry).await?;

        let dir = self.config.wal_dir.join("dead-letter");
        create_dir_all(&dir).await?;
        let path = dir.join(format!("{}.bin", entry_id.simple()));
        let mut file = File::create(&path)
            .await
            .with_context(|| format!("Failed to create dead-letter file {}", path.display()))?;
        file.write_all(&data).await?;
        file.flush().await?;
        file.sync_all()
            .await
            .context("Failed to fsync dead-letter file")?;

        self.mark_processed(entry_id).await?;
        Ok(path)
    }

    /// Load the persisted writer id from `writer.id`, creating (and
    /// fsyncing) it on first use. The id shares the WAL directory's
    /// lifetime: if the directory is wiped, the entries the id guarded
    /// are gone with it, so a fresh id is correct.
    async fn load_or_create_writer_id(wal_dir: &Path) -> Result<String> {
        let path = wal_dir.join("writer.id");
        match tokio::fs::read_to_string(&path).await {
            Ok(contents) => {
                let id = contents.trim().to_string();
                if !id.is_empty() {
                    return Ok(id);
                }
                // Empty file (e.g. crash between create and write): regenerate
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).context("Failed to read WAL writer.id");
            }
        }

        let id = Uuid::new_v4().simple().to_string();
        let mut file = File::create(&path)
            .await
            .context("Failed to create WAL writer.id")?;
        file.write_all(id.as_bytes()).await?;
        file.flush().await?;
        file.sync_all()
            .await
            .context("Failed to fsync WAL writer.id")?;
        Ok(id)
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

                if should_flush
                    && let Err(e) = Self::flush_buffer(
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
        });

        self.flush_handle = Some(handle);
    }

    /// Add an entry to the WAL
    ///
    /// # Arguments
    /// * `operation` - The type of WAL operation
    /// * `data` - The data to write
    /// * `metadata` - Optional metadata (e.g., JSON-serialized FlightMetadata with target_table)
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(signaldb.wal.operation = ?operation, signaldb.wal.data_size = data.len())
    )]
    pub async fn append(
        &self,
        operation: WalOperation,
        data: Vec<u8>,
        metadata: Option<String>,
    ) -> Result<Uuid> {
        let entry_id = Uuid::new_v4();

        {
            let metrics = crate::self_monitoring::app_metrics();
            let attrs = [opentelemetry::KeyValue::new(
                "operation",
                format!("{operation:?}"),
            )];
            metrics.wal_entries_written.add(1, &attrs);
            metrics.wal_entries_pending.add(1, &[]);
        }

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
            // Rotate when EITHER the entry-log or the payload data file would
            // exceed the segment size cap. The data file holds the payloads and
            // grows far faster than the log (which stores only fixed-size entry
            // metadata), so gating rotation on the log size alone lets the data
            // file grow without bound — on hive it reached multiple GB in a
            // single never-rotated segment, marching toward the 2^32 offset
            // limit and giving any single write-path desync an unbounded blast
            // radius (issue #865). Capping the data file too keeps segments
            // small, bounds recovery cost, and periodically starts a fresh
            // (offset-aligned) segment.
            if segment.size + data.len() as u64 > config.max_segment_size
                || segment.data_size + data.len() as u64 > config.max_segment_size
            {
                // Close current segment
                segment.close().await?;

                // Create new segment
                let new_segment_id = {
                    let mut id = next_segment_id.lock().await;
                    let current_id = *id;
                    *id += 1;
                    current_id
                };

                // The current_segment Arc is also an element of the segments
                // list, so swapping its contents in place would overwrite the
                // list's only reference to the old segment. Move the old
                // segment out and re-insert it as a sealed segment just
                // before the current one (cleanup relies on current being
                // last).
                let new_segment = WalSegment::new(&config.wal_dir, new_segment_id).await?;
                let old_segment = std::mem::replace(&mut *segment, new_segment);

                drop(segment); // Release lock before acquiring segments lock
                let mut segs = segments.lock().await;
                let insert_at = segs.len().saturating_sub(1);
                segs.insert(insert_at, Arc::new(Mutex::new(old_segment)));
                drop(segs);

                // Re-acquire the current segment lock
                segment = current_segment.lock().await;
            }

            segment
                .append(entry_id, operation, &data, tenant_id, dataset_id, metadata)
                .await?;
        }

        // fsync once per flushed batch: callers treat a successful flush as
        // a durability guarantee (the acceptor ACKs OTLP exports on it), so
        // the entries must survive power loss, not just reach the page cache.
        segment.sync().await?;

        Ok(())
    }

    /// Force flush all buffered entries
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn flush(&self) -> Result<()> {
        let start = std::time::Instant::now();
        let result = Self::flush_buffer(
            &self.buffer,
            &self.current_segment,
            &self.config,
            &self.next_segment_id,
            &self.segments,
        )
        .await;
        crate::self_monitoring::app_metrics()
            .wal_flush_duration
            .record(start.elapsed().as_secs_f64(), &[]);
        result
    }

    /// Get all entries from WAL for recovery, across all segments
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get_entries(&self) -> Result<Vec<WalEntry>> {
        let segments = self.segments.lock().await;
        let mut entries = Vec::new();
        for segment_arc in segments.iter() {
            let segment = segment_arc.lock().await;
            entries.extend(segment.entries.iter().cloned());
        }
        Ok(entries)
    }

    /// Read data for a specific entry
    ///
    /// Locates the segment that contains the entry (offsets are relative to
    /// each segment's own data file) and reads from there.
    pub async fn read_entry_data(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        let segments = self.segments.lock().await;
        for segment_arc in segments.iter() {
            let segment = segment_arc.lock().await;
            if segment.entries.iter().any(|e| e.id == entry.id) {
                return segment.read_entry_data(entry).await;
            }
        }
        anyhow::bail!(
            "WAL entry {} not found in any of {} segments",
            entry.id,
            segments.len()
        )
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
    #[tracing::instrument(level = "debug", skip_all, fields(signaldb.wal.entry_id = %entry_id))]
    pub async fn mark_processed(&self, entry_id: Uuid) -> Result<()> {
        self.mark_processed_many(std::slice::from_ref(&entry_id))
            .await
    }

    /// Mark a batch of WAL entries as processed and persist the state to disk.
    ///
    /// Mutates all matching entries in memory across segments, then persists
    /// each *affected* segment's index exactly once — instead of one full
    /// index rewrite + fsync per entry, which made per-entry marking O(n²)
    /// in entries per segment (issue #943). The index is an at-least-once
    /// optimization (losing it only causes reprocessing), so batching the
    /// fsyncs does not weaken durability.
    ///
    /// Ids not found in any segment surface as an error after all found ids
    /// have been marked and persisted, matching `mark_processed`'s contract
    /// for unknown ids; marks are idempotent, so the partial progress is
    /// safe for callers that retry.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(signaldb.wal.entry_count = entry_ids.len())
    )]
    pub async fn mark_processed_many(&self, entry_ids: &[Uuid]) -> Result<()> {
        if entry_ids.is_empty() {
            return Ok(());
        }

        let mut remaining: std::collections::HashSet<Uuid> = entry_ids.iter().copied().collect();
        // Count only unprocessed -> processed transitions so repeated calls
        // don't skew the metrics.
        let mut newly_processed: i64 = 0;

        // Search all segments, not just current
        let segments = self.segments.lock().await;
        let segment_count = segments.len();

        for segment_arc in segments.iter() {
            if remaining.is_empty() {
                break;
            }

            let mut segment = segment_arc.lock().await;

            let mut segment_dirty = false;
            for entry in &mut segment.entries {
                if remaining.remove(&entry.id) {
                    if !entry.processed {
                        entry.processed = true;
                        newly_processed += 1;
                        segment_dirty = true;
                    }
                    if remaining.is_empty() {
                        break;
                    }
                }
            }

            // Persist the processed state to disk once per affected segment
            if segment_dirty {
                segment.save_index().await?;
            }
        }
        drop(segments);

        if newly_processed > 0 {
            let metrics = crate::self_monitoring::app_metrics();
            metrics
                .wal_entries_processed
                .add(newly_processed as u64, &[]);
            metrics.wal_entries_pending.add(-newly_processed, &[]);
        }

        if !remaining.is_empty() {
            anyhow::bail!(
                "{} of {} WAL entries not found in any of {segment_count} segments: {remaining:?}",
                remaining.len(),
                entry_ids.len()
            );
        }

        log::debug!(
            "Marked {} WAL entries as processed and persisted indexes",
            entry_ids.len()
        );
        Ok(())
    }

    /// Get all unprocessed entries, across all segments
    pub async fn get_unprocessed_entries(&self) -> Result<Vec<WalEntry>> {
        let segments = self.segments.lock().await;
        let mut entries = Vec::new();
        for segment_arc in segments.iter() {
            let segment = segment_arc.lock().await;
            entries.extend(segment.entries.iter().filter(|e| !e.processed).cloned());
        }
        Ok(entries)
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
        let writer_id = self.writer_id.clone();

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
                    writer_id: writer_id.clone(),
                };

                if let Err(e) = wal.cleanup().await {
                    log::error!("Failed to run WAL cleanup: {e}");
                }
            }
        });

        self.cleanup_handle = Some(handle);
    }
}

/// Utility to convert RecordBatch to bytes for WAL storage.
///
/// Buffers are zstd-compressed (#945): WAL payloads are dominated by
/// repeated JSON attribute strings, so higher-ratio zstd wins over lz4 on
/// this durability (not latency) path. Compression is recorded per IPC
/// message, so [`bytes_to_record_batch`] transparently reads both these
/// and legacy uncompressed segments.
pub fn record_batch_to_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
    use datafusion::arrow::ipc::CompressionType;
    use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

    let options = IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new_with_options(&mut buffer, &batch.schema(), options)?;
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

    #[test]
    fn wal_operation_signal_names_are_stable() {
        // These strings are used as telemetry attribute values (log fields,
        // metric labels) and as the acceptor's `signal` dimension, so they
        // must stay stable and match the ingest vocabulary.
        assert_eq!(WalOperation::WriteTraces.signal(), "traces");
        assert_eq!(WalOperation::WriteLogs.signal(), "logs");
        assert_eq!(WalOperation::WriteMetrics.signal(), "metrics");
        assert_eq!(WalOperation::WriteProfiles.signal(), "profiles");
        assert_eq!(WalOperation::Flush.signal(), "flush");
    }

    #[tokio::test]
    async fn read_entry_data_rejects_out_of_bounds_range() {
        // A WAL entry whose data_offset+data_size exceeds the data file must
        // fail with a clear, attributable bounds error before any bytes are
        // handed to the Arrow reader — otherwise the reader surfaces an opaque
        // "failed to fill whole buffer" / RangeOutOfBounds and the caller
        // cannot tell corruption from a genuine read fault.
        let temp_dir = TempDir::new().unwrap();
        let mut segment = WalSegment::new(temp_dir.path(), 0).await.unwrap();

        let payload = record_batch_to_bytes(&make_batch()).unwrap();
        let id = Uuid::new_v4();
        segment
            .append(id, WalOperation::WriteTraces, &payload, "t", "d", None)
            .await
            .unwrap();

        // Craft an entry pointing past the end of the data file.
        let bogus = WalEntry {
            id,
            timestamp: 0,
            operation: WalOperation::WriteTraces,
            data_size: payload.len() as u64,
            data_offset: payload.len() as u64 + 4096,
            processed: false,
            tenant_id: "t".to_string(),
            dataset_id: "d".to_string(),
            metadata: None,
        };

        let err = segment
            .read_entry_data(&bogus)
            .await
            .expect_err("out-of-bounds read must error");
        let msg = err.to_string();
        assert!(
            msg.contains("out of bounds"),
            "expected a bounds error, got: {msg}"
        );

        // A valid entry still reads back correctly.
        let good = WalEntry {
            data_offset: 0,
            ..bogus.clone()
        };
        let data = segment.read_entry_data(&good).await.unwrap();
        assert_eq!(data, payload);
    }

    #[tokio::test]
    async fn append_stays_consistent_after_a_partial_data_write() {
        // Regression for the LIVE hive WAL corruption (issue #865). The data
        // file is offset-addressed: each entry records `data_offset =
        // self.data_size`. But `append` writes the payload with the data file
        // opened O_APPEND and advances `self.data_size` only *after* a fully
        // successful `write_all`, with no truncate-on-error. If a data write
        // ever lands some bytes and then errors (ENOSPC / interrupted syscall
        // under disk pressure — hive is a TrueNAS box), those bytes are
        // durably at the physical EOF but unaccounted for. From then on every
        // subsequent entry records the stale, smaller offset while O_APPEND
        // places its bytes *past* the orphan debris — so every following read
        // is shifted by a constant, which is exactly the fixed-offset Arrow
        // framing errors flooding the writer on hive.
        //
        // #868's concurrency guard never induces a short write, so it stays
        // green; this test targets that gap head-on. It reproduces the state a
        // partial write leaves — orphan bytes at EOF, counter not advanced —
        // by writing through the same handle `append` uses, then asserts the
        // next entry still round-trips byte-identical.
        let temp_dir = TempDir::new().unwrap();
        let mut segment = WalSegment::new(temp_dir.path(), 0).await.unwrap();

        // A healthy first entry.
        let first = record_batch_to_bytes(&make_batch_val(1)).unwrap();
        let first_id = Uuid::new_v4();
        segment
            .append(first_id, WalOperation::WriteTraces, &first, "t", "d", None)
            .await
            .unwrap();

        // Simulate the aftermath of a partial write that errored *inside*
        // append's write_all, before `self.data_size` advanced: bytes durably
        // at EOF, counter unchanged. We inject them through the very handle a
        // real partial write would use.
        let orphan = b"PARTIAL-WRITE-DEBRIS";
        {
            let data_file = segment.data_file.as_mut().unwrap();
            data_file.write_all(orphan).await.unwrap();
            data_file.flush().await.unwrap();
        }
        // `self.data_size` deliberately NOT advanced — mirrors the real bug.

        // The next healthy entry. Its recorded offset is `self.data_size`, but
        // under O_APPEND its bytes land after the orphan debris.
        let second = record_batch_to_bytes(&make_batch_val(2)).unwrap();
        let second_id = Uuid::new_v4();
        segment
            .append(
                second_id,
                WalOperation::WriteTraces,
                &second,
                "t",
                "d",
                None,
            )
            .await
            .unwrap();

        // Must read back byte-identical. Today it does not: the read seeks to
        // the stale offset and returns orphan + shifted bytes, which the Arrow
        // reader then rejects with the #865 framing errors.
        let second_entry = segment
            .entries
            .iter()
            .find(|e| e.id == second_id)
            .cloned()
            .unwrap();
        let read_back = segment.read_entry_data(&second_entry).await.unwrap();
        assert_eq!(
            read_back, second,
            "entry read back corrupted after a partial data write: the WAL \
             desynced its offset counter from the physical data file"
        );

        // The first entry must remain intact regardless.
        let first_entry = segment
            .entries
            .iter()
            .find(|e| e.id == first_id)
            .cloned()
            .unwrap();
        let first_read = segment.read_entry_data(&first_entry).await.unwrap();
        assert_eq!(first_read, first, "first entry corrupted");
    }

    fn make_batch() -> RecordBatch {
        make_batch_val(1)
    }

    fn make_batch_val(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        // Vary row count with the value so distinct entries have distinct
        // encoded lengths — a stronger check that offsets don't cross wires.
        let rows: Vec<i64> = (0..=(v % 7)).map(|i| v + i).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(rows))]).unwrap()
    }

    #[tokio::test]
    async fn concurrent_appends_round_trip_every_entry_byte_identical() {
        // Reproduction guard for the hive WAL corruption (issue #865): under
        // concurrent appends that force segment rotation + interleaved
        // flushes, every entry must read back byte-identical and still
        // deserialize. If offset bookkeeping crosses wires, an entry reads
        // back mismatched or Arrow-undeserializable bytes.
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 4096, // small: forces frequent rotation
            max_buffer_entries: 4,  // small: forces frequent flushes
            flush_interval_secs: 1,
            tenant_id: "t".to_string(),
            dataset_id: "d".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(config).await.unwrap());

        // Shared map of committed (id -> bytes), populated by writers as each
        // append returns and sampled by concurrent readers — mirrors the
        // WalProcessor reading entries while do_put appends and rotates.
        let committed: Arc<tokio::sync::Mutex<std::collections::HashMap<Uuid, Vec<u8>>>> =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let mut readers = Vec::new();
        for _ in 0..3 {
            let wal = wal.clone();
            let committed = committed.clone();
            let stop = stop.clone();
            readers.push(tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let entries = wal.get_entries().await.unwrap_or_default();
                    let want = committed.lock().await;
                    for entry in &entries {
                        if let Some(expected) = want.get(&entry.id) {
                            // A committed entry, if readable, must be exact.
                            if let Ok(got) = wal.read_entry_data(entry).await {
                                assert_eq!(
                                    &got, expected,
                                    "concurrent read of {} returned corrupted bytes",
                                    entry.id
                                );
                                bytes_to_record_batch(&got)
                                    .expect("concurrently-read payload must deserialize");
                            }
                        }
                    }
                    drop(want);
                    tokio::task::yield_now().await;
                }
            }));
        }

        let mut writers = Vec::new();
        for w in 0..8u64 {
            let wal = wal.clone();
            let committed = committed.clone();
            writers.push(tokio::spawn(async move {
                let mut written = Vec::new();
                for i in 0..50u64 {
                    let batch = make_batch_val((w * 1000 + i) as i64);
                    let bytes = record_batch_to_bytes(&batch).unwrap();
                    let id = wal
                        .append(WalOperation::WriteTraces, bytes.clone(), None)
                        .await
                        .unwrap();
                    // Publish immediately so readers race the write.
                    committed.lock().await.insert(id, bytes.clone());
                    written.push((id, bytes));
                }
                written
            }));
        }

        let mut expected = Vec::new();
        for h in writers {
            expected.extend(h.await.unwrap());
        }
        wal.flush().await.unwrap();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for r in readers {
            r.await.unwrap();
        }

        let entries = wal.get_entries().await.unwrap();
        assert_eq!(entries.len(), expected.len(), "entry count mismatch");
        for (id, want) in &expected {
            let entry = entries
                .iter()
                .find(|e| e.id == *id)
                .unwrap_or_else(|| panic!("entry {id} missing"));
            let got = wal.read_entry_data(entry).await.unwrap();
            assert_eq!(&got, want, "payload for {id} read back corrupted");
            bytes_to_record_batch(&got).expect("payload must deserialize");
        }
    }

    #[tokio::test]
    async fn dead_letter_preserves_payload_and_marks_processed() {
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
        let wal = Wal::new(config).await.unwrap();

        let payload = b"poison payload".to_vec();
        let entry_id = wal
            .append(WalOperation::WriteTraces, payload.clone(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        let path = wal.dead_letter(entry_id).await.unwrap();
        let preserved = tokio::fs::read(&path).await.unwrap();
        assert_eq!(preserved, payload, "payload must be preserved verbatim");

        // The entry no longer blocks processing.
        let unprocessed = wal.get_unprocessed_entries().await.unwrap();
        assert!(
            unprocessed.is_empty(),
            "dead-lettered entry must be marked processed"
        );
    }

    #[tokio::test]
    async fn rotates_when_data_file_exceeds_cap_even_if_log_stays_small() {
        // Regression for issue #865. Rotation was gated only on the entry-log
        // size; the payload data file grows far faster (the log stores only
        // fixed-size metadata), so a workload of small-metadata/large-payload
        // entries never rotated and the data file grew to multiple GB in one
        // segment on hive. Here the ~1 KiB payloads blow past the 4 KiB cap on
        // the data file while the log accrues only ~100 bytes/entry, so ONLY
        // the data-size check can force rotation.
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 4096,
            max_buffer_entries: 1,
            flush_interval_secs: 1,
            tenant_id: "t".to_string(),
            dataset_id: "d".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Wal::new(config).await.unwrap();

        let payload = vec![0xABu8; 1024];
        for _ in 0..8 {
            wal.append(WalOperation::WriteTraces, payload.clone(), None)
                .await
                .unwrap();
            wal.flush().await.unwrap();
        }

        // The 8 KiB of payload must have spilled into more than one segment.
        let mut data_files = 0usize;
        let mut rd = tokio::fs::read_dir(temp_dir.path()).await.unwrap();
        while let Some(e) = rd.next_entry().await.unwrap() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("wal-") && name.ends_with(".data") {
                data_files += 1;
            }
        }
        assert!(
            data_files >= 2,
            "a data file exceeding max_segment_size must force rotation; found {data_files} segment(s)"
        );

        // Rotation must not lose or corrupt any entry.
        let entries = wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(entries.len(), 8, "all entries must survive rotation");
        for e in &entries {
            let got = wal.read_entry_data(e).await.unwrap();
            assert_eq!(got, payload, "entry corrupted across data-size rotation");
        }
    }

    #[tokio::test]
    async fn writer_id_is_stable_across_reopen() {
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

        let wal = Wal::new(config.clone()).await.unwrap();
        let first_id = wal.writer_id().to_string();
        assert!(!first_id.is_empty());
        drop(wal);

        let reopened = Wal::new(config.clone()).await.unwrap();
        assert_eq!(reopened.writer_id(), first_id);
        drop(reopened);

        // A different WAL directory gets a different identity
        let other_dir = TempDir::new().unwrap();
        let other = Wal::new(WalConfig {
            wal_dir: other_dir.path().to_path_buf(),
            ..config
        })
        .await
        .unwrap();
        assert_ne!(other.writer_id(), first_id);
    }

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
    async fn segment_rotation_preserves_sealed_segment_entries() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            // Small enough that every entry triggers a rotation
            max_segment_size: 64,
            max_buffer_entries: 100,
            flush_interval_secs: 3600,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };

        let wal = Wal::new(config).await.unwrap();

        // Append three entries with distinct payloads, flushing each so
        // rotation happens between them
        let mut ids = Vec::new();
        for i in 0..3 {
            let payload = format!("payload-{i}-{}", "x".repeat(100)).into_bytes();
            let id = wal
                .append(WalOperation::WriteTraces, payload, None)
                .await
                .unwrap();
            wal.flush().await.unwrap();
            ids.push(id);
        }

        // All entries must be visible, not just those in the current segment
        let entries = wal.get_entries().await.unwrap();
        assert_eq!(entries.len(), 3, "rotation must not lose sealed entries");

        let unprocessed = wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(unprocessed.len(), 3);

        // Data must be readable from sealed segments with correct contents
        for (i, id) in ids.iter().enumerate() {
            let entry = entries.iter().find(|e| e.id == *id).unwrap();
            let data = wal.read_entry_data(entry).await.unwrap();
            let expected = format!("payload-{i}-{}", "x".repeat(100)).into_bytes();
            assert_eq!(data, expected, "entry {i} data must round-trip");
        }

        // Entries in sealed segments must be markable as processed
        for id in &ids {
            wal.mark_processed(*id).await.unwrap();
        }
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn mark_processed_many_marks_all_ids_across_segments_and_persists() {
        // Batch marking must cover entries living in *different* segments
        // (sealed + current) and persist each affected segment's index so the
        // processed state survives a reload — the whole point of the batch
        // API is one save_index per affected segment instead of one per
        // entry (issue #943).
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            // Small enough that every entry triggers a rotation
            max_segment_size: 64,
            max_buffer_entries: 100,
            flush_interval_secs: 3600,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };

        let wal = Wal::new(config.clone()).await.unwrap();

        let mut ids = Vec::new();
        for i in 0..4 {
            let payload = format!("payload-{i}-{}", "x".repeat(100)).into_bytes();
            let id = wal
                .append(WalOperation::WriteTraces, payload, None)
                .await
                .unwrap();
            wal.flush().await.unwrap();
            ids.push(id);
        }

        // One call marks everything, across all segments.
        wal.mark_processed_many(&ids).await.unwrap();
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "all entries must be marked processed after one batch call"
        );
        wal.shutdown().await.unwrap();

        // The processed state must have been persisted to each segment's
        // index, so a reload sees no unprocessed entries.
        let reopened = Wal::new(config).await.unwrap();
        assert!(
            reopened.get_unprocessed_entries().await.unwrap().is_empty(),
            "batch-marked processed state must survive a reload"
        );
    }

    #[tokio::test]
    async fn mark_processed_many_marks_known_ids_and_errors_on_unknown() {
        // mark_processed bails on an unknown id; the batch variant preserves
        // that contract — but marks are idempotent at-least-once state, so
        // the known ids in the batch must still be marked (and persisted)
        // before the error is reported.
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024,
            max_buffer_entries: 10,
            flush_interval_secs: 3600,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Wal::new(config).await.unwrap();

        let a = wal
            .append(WalOperation::WriteTraces, b"a".to_vec(), None)
            .await
            .unwrap();
        let b = wal
            .append(WalOperation::WriteTraces, b"b".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        let unknown = Uuid::new_v4();
        let err = wal
            .mark_processed_many(&[a, unknown, b])
            .await
            .expect_err("an unknown id in the batch must surface an error");
        assert!(
            err.to_string().contains("not found"),
            "error must identify the missing entries, got: {err}"
        );

        // The known ids were still marked.
        assert!(
            wal.get_unprocessed_entries().await.unwrap().is_empty(),
            "known ids in the batch must be marked despite the unknown id"
        );
    }

    #[tokio::test]
    async fn mark_processed_many_with_empty_slice_is_a_noop() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024,
            max_buffer_entries: 10,
            flush_interval_secs: 3600,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Wal::new(config).await.unwrap();

        let id = wal
            .append(WalOperation::WriteTraces, b"x".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        wal.mark_processed_many(&[]).await.unwrap();
        let unprocessed = wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(unprocessed.len(), 1, "empty batch must not mark anything");
        assert_eq!(unprocessed[0].id, id);
    }

    #[tokio::test]
    async fn mark_processed_single_still_marks_and_errors_on_unknown() {
        // mark_processed is now a thin wrapper over mark_processed_many;
        // its observable behavior must not change: marks a known entry
        // (persisted), errors for an unknown one.
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024,
            max_buffer_entries: 10,
            flush_interval_secs: 3600,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Wal::new(config.clone()).await.unwrap();

        let id = wal
            .append(WalOperation::WriteTraces, b"x".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        wal.mark_processed(id).await.unwrap();
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());

        // Marking again is idempotent.
        wal.mark_processed(id).await.unwrap();

        wal.mark_processed(Uuid::new_v4())
            .await
            .expect_err("unknown id must error");

        wal.shutdown().await.unwrap();

        // Persisted across reload.
        let reopened = Wal::new(config).await.unwrap();
        assert!(reopened.get_unprocessed_entries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn reload_after_rotation_sees_all_unprocessed_entries() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 64,
            max_buffer_entries: 100,
            flush_interval_secs: 3600,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };

        let wal = Wal::new(config.clone()).await.unwrap();
        for i in 0..3 {
            let payload = format!("payload-{i}-{}", "x".repeat(100)).into_bytes();
            wal.append(WalOperation::WriteTraces, payload, None)
                .await
                .unwrap();
            wal.flush().await.unwrap();
        }
        wal.shutdown().await.unwrap();

        // Re-open the WAL from disk: replay must surface entries from all
        // segments, not only the current one
        let reopened = Wal::new(config).await.unwrap();
        let unprocessed = reopened.get_unprocessed_entries().await.unwrap();
        assert_eq!(unprocessed.len(), 3);

        for entry in &unprocessed {
            let data = reopened.read_entry_data(entry).await.unwrap();
            assert!(!data.is_empty());
        }
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

    /// A batch shaped like real WAL payloads: highly repetitive JSON strings.
    fn repetitive_string_batch() -> RecordBatch {
        use datafusion::arrow::array::StringArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "resource_json",
            DataType::Utf8,
            false,
        )]));
        let resource = "{\"service.name\":\"checkout\",\"deployment.environment\":\"prod\"}";
        let array = Arc::new(StringArray::from(vec![resource; 2048]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn record_batch_round_trips_with_full_data_equality() {
        let batch = repetitive_string_batch();
        let bytes = record_batch_to_bytes(&batch).unwrap();
        let recovered = bytes_to_record_batch(&bytes).unwrap();
        assert_eq!(recovered, batch);
    }

    #[test]
    fn bytes_to_record_batch_reads_legacy_uncompressed_payloads() {
        use datafusion::arrow::ipc::writer::StreamWriter;

        // Encode exactly like the pre-compression WAL writer did:
        // StreamWriter with default (uncompressed) options.
        let batch = repetitive_string_batch();
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let recovered = bytes_to_record_batch(&buffer).unwrap();
        assert_eq!(recovered, batch);
    }

    #[test]
    fn record_batch_to_bytes_compresses_repetitive_payloads() {
        use datafusion::arrow::ipc::writer::StreamWriter;

        let batch = repetitive_string_batch();
        let compressed = record_batch_to_bytes(&batch).unwrap();

        // Reference: default (uncompressed) IPC encoding of the same batch.
        let mut uncompressed = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut uncompressed, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        assert!(
            compressed.len() < uncompressed.len(),
            "zstd-compressed WAL payload ({} bytes) should be smaller than the \
             uncompressed encoding ({} bytes) for repetitive data",
            compressed.len(),
            uncompressed.len()
        );
    }
}
