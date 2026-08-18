pub mod framing;
pub mod manager;

use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use framing::{
    DATA_RECORD_HEADER_LEN, LOG_RECORD_HEADER_LEN, SEGMENT_HEADER_LEN, SegmentFormat,
    data_record_header, detect_segment_format, log_record_header, parse_log_record_header,
    segment_header, validate_data_record,
};
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
    /// `entry.id -> index into entries`, maintained alongside `entries`.
    ///
    /// Entry ids are already uniformly random (UUIDv4), so SipHash's
    /// collision resistance buys nothing here — `FxHashMap` trades it for
    /// speed. This index turns entry lookups (`mark_processed_many`,
    /// `Wal::read_entry_data`) from an O(entries-in-segment) linear scan
    /// into an O(1) lookup; without it those scans dominated CPU on hosts
    /// with large, not-yet-compacted segments (issue #1112).
    entry_index: rustc_hash::FxHashMap<Uuid, usize>,
    /// On-disk layout of this segment (see [`framing`]). Only [`SegmentFormat::V1`]
    /// segments accept appends; legacy segments are read-only survivors that
    /// compaction rewrites into v1.
    format: SegmentFormat,
}

impl WalSegment {
    /// The `.log`/`.data`/`.index` file paths for a segment id in `wal_dir`.
    fn segment_paths(wal_dir: &Path, segment_id: u64) -> (PathBuf, PathBuf, PathBuf) {
        (
            wal_dir.join(format!("wal-{segment_id:010}.log")),
            wal_dir.join(format!("wal-{segment_id:010}.data")),
            wal_dir.join(format!("wal-{segment_id:010}.index")),
        )
    }

    /// Create a new WAL segment
    pub async fn new(wal_dir: &Path, segment_id: u64) -> Result<Self> {
        create_dir_all(wal_dir).await?;

        let (path, data_path, index_path) = Self::segment_paths(wal_dir, segment_id);

        // Opened for writing without O_APPEND: `append` seeks to the entry's
        // authoritative offset before writing (see `append`), so O_APPEND —
        // which forces every write to the physical EOF regardless of the
        // recorded offset — must not be set (issue #865).
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .await
            .context("Failed to create WAL segment file")?;
        // Every v1 segment opens with the format header so a later `load`
        // can tell it apart from a legacy (unframed) segment.
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&segment_header()).await?;
        file.flush().await?;

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
            file: Some(file),
            data_file,
            size: SEGMENT_HEADER_LEN as u64,
            data_size: 0,
            entries: Vec::new(),
            entry_index: rustc_hash::FxHashMap::default(),
            format: SegmentFormat::V1,
        })
    }

    /// On-disk layout of this segment.
    pub fn format(&self) -> SegmentFormat {
        self.format
    }

    /// Load an existing WAL segment from disk
    pub async fn load(wal_dir: &Path, segment_id: u64) -> Result<Self> {
        let (path, data_path, index_path) = Self::segment_paths(wal_dir, segment_id);

        if !path.exists() {
            return Self::new(wal_dir, segment_id).await;
        }

        // Read existing entries
        let mut file = File::open(&path).await?;
        let mut entries = Vec::new();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let format = detect_segment_format(&buffer)
            .with_context(|| format!("Failed to classify WAL segment {}", path.display()))?;
        if format == SegmentFormat::Legacy {
            tracing::warn!(
                segment_id,
                path = %path.display(),
                "WAL segment uses the legacy unframed format (pre-#946); it is read on \
                 the legacy layout, sealed against new appends, and rewritten into the \
                 framed format on compaction"
            );
        }
        let mut is_empty_v1 = false;
        let mut offset = match format {
            SegmentFormat::Legacy => 0,
            SegmentFormat::V1 if buffer.is_empty() => {
                is_empty_v1 = true;
                0
            }
            SegmentFormat::V1 => SEGMENT_HEADER_LEN,
        };
        let record_header_len = match format {
            SegmentFormat::Legacy => 8,
            SegmentFormat::V1 => LOG_RECORD_HEADER_LEN,
        };
        while offset < buffer.len() {
            if offset + record_header_len > buffer.len() {
                tracing::warn!(
                    segment_id,
                    offset,
                    trailing_bytes = buffer.len() - offset,
                    "WAL segment tail truncated (length prefix incomplete); \
                     dropping the torn record, all prior entries preserved"
                );
                break;
            }
            // v1 records carry a CRC of the payload; legacy records only a
            // u64 length. `expected_crc = None` means "nothing to verify".
            let (entry_len, expected_crc) = match format {
                SegmentFormat::Legacy => (
                    u64::from_le_bytes(
                        buffer[offset..offset + 8]
                            .try_into()
                            .context("Failed to read entry length")?,
                    ) as usize,
                    None,
                ),
                SegmentFormat::V1 => {
                    let (len, crc) = parse_log_record_header(&buffer[offset..])
                        .context("Failed to read entry record header")?;
                    (len, Some(crc))
                }
            };
            offset += record_header_len;

            // Read entry data
            if offset + entry_len > buffer.len() {
                tracing::warn!(
                    segment_id,
                    offset,
                    entry_len,
                    trailing_bytes = buffer.len() - offset,
                    "WAL segment tail truncated (entry payload incomplete); \
                     dropping the torn record, all prior entries preserved"
                );
                break;
            }
            let entry_data = &buffer[offset..offset + entry_len];
            let decoded = match expected_crc {
                Some(expected) if framing::checksum(entry_data) != expected => {
                    Err(anyhow::anyhow!(
                        "entry record CRC mismatch (stored {expected:#010x}, computed {:#010x})",
                        framing::checksum(entry_data)
                    ))
                }
                _ => bincode::deserialize::<WalEntry>(entry_data)
                    .map_err(|e| anyhow::anyhow!("entry record failed to deserialize: {e}")),
            };
            match decoded {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    // Framing is intact (the length prefix matched a
                    // readable byte range) but the payload itself does not
                    // decode — content corruption rather than a truncated
                    // write. Unlike the bounds checks above, `offset +=
                    // entry_len` still resyncs exactly onto the next
                    // record, so skip-and-continue preserves every entry
                    // around this one. A `break` here would silently
                    // discard the rest of the segment on every single
                    // restart (issue #1033).
                    tracing::error!(
                        segment_id,
                        offset,
                        entry_len,
                        error = %e,
                        "WAL entry record corrupt during replay; \
                         skipping and quarantining it, replay continues"
                    );
                    crate::self_monitoring::app_metrics()
                        .wal_corrupt_entries
                        .add(1, &[opentelemetry::KeyValue::new("record", "log")]);
                    if let Err(quarantine_err) =
                        Self::quarantine_corrupt_entry(wal_dir, segment_id, offset, entry_data)
                            .await
                    {
                        // A failed quarantine must never abort replay — that
                        // would recreate the exact crash loop this handling
                        // exists to fix. The bytes are simply lost; the
                        // error above already named the segment and offset.
                        tracing::error!(
                            segment_id,
                            offset,
                            error = %quarantine_err,
                            "Failed to quarantine corrupt WAL entry bytes; \
                             continuing replay without them"
                        );
                    }
                }
            }
            offset += entry_len;
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
            tracing::debug!(
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

        let entry_index = entries
            .iter()
            .enumerate()
            .map(|(idx, entry)| (entry.id, idx))
            .collect();

        // Reopened for writing without O_APPEND, matching `new` — appends seek
        // to the tracked offset (issue #865). `data_size`/`size` are reseeded
        // from the on-disk lengths below, so writes resume at the true EOF.
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .await?;
        // An empty v1 file (crash between create and header write) gets its
        // header now so the next `load` classifies it correctly.
        let size = if is_empty_v1 {
            file.seek(SeekFrom::Start(0)).await?;
            file.write_all(&segment_header()).await?;
            file.flush().await?;
            SEGMENT_HEADER_LEN as u64
        } else {
            size
        };

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
            file: Some(file),
            data_file,
            size,
            data_size,
            entries,
            entry_index,
            format,
        })
    }

    /// Preserve the raw bytes of a WAL log entry that failed to deserialize
    /// during replay, mirroring the dead-letter convention used elsewhere in
    /// this file ([`Wal::dead_letter`], [`Wal::dead_letter_rejected`],
    /// [`Wal::dead_letter_unreadable`]) so operators find every retired
    /// entry — payload or, as here, metadata-record — under one directory.
    ///
    /// This differs from those in one way: it is a `WalSegment` associated
    /// function with only `wal_dir` and `segment_id` in scope, not a `Wal`
    /// method. `load()` runs before any `Wal` exists (segments are loaded to
    /// *construct* one), so there is no `&self`, no tenant/dataset, and no
    /// entry id to key the marker on — the corrupt bytes are all that
    /// survived. The file is named
    /// `<wal_dir>/dead-letter/segment-<segment_id>-offset-<offset>.corrupt.bin`,
    /// where `offset` is the byte offset of the entry's payload within the
    /// segment's `.log` file (after its 8-byte length prefix), which is
    /// enough to locate the damage by hand with `hexdump`.
    ///
    /// Errors are the caller's to swallow: a failed quarantine (disk full,
    /// permissions) must never abort replay, or this recreates the exact
    /// crash loop issue #1033 exists to fix.
    async fn quarantine_corrupt_entry(
        wal_dir: &Path,
        segment_id: u64,
        offset: usize,
        entry_data: &[u8],
    ) -> Result<PathBuf> {
        let dir = wal_dir.join("dead-letter");
        create_dir_all(&dir).await?;
        let path = dir.join(format!("segment-{segment_id}-offset-{offset}.corrupt.bin"));

        let mut file = File::create(&path)
            .await
            .with_context(|| format!("Failed to create quarantine file {}", path.display()))?;
        file.write_all(entry_data).await?;
        file.flush().await?;
        file.sync_all()
            .await
            .context("Failed to fsync quarantined WAL entry")?;

        Ok(path)
    }

    /// This segment's own copy of `entry_id`, resolved through `entry_index`
    /// (O(1), the lookup #1112 introduced) rather than by scanning entries.
    ///
    /// This is the authoritative copy: compaction rewrites a segment and
    /// moves surviving entries to new offsets, so a caller's older clone of
    /// the same entry can carry a stale `data_offset`.
    fn entry_by_id(&self, entry_id: &Uuid) -> Option<&WalEntry> {
        self.entries.get(*self.entry_index.get(entry_id)?)
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
        self.append_at(
            entry_id,
            unix_now_secs(),
            operation,
            data,
            tenant_id,
            dataset_id,
            metadata,
        )
        .await
    }

    /// Append an entry carrying an explicit creation `timestamp`.
    ///
    /// Only compaction ([`Wal::cleanup`]) uses this: a rewritten segment must
    /// re-append its surviving entries with the timestamps they were first
    /// written with. Stamping them "now" would reset the entry age the
    /// acceptor's retry loop gates on, so every cleanup pass would postpone
    /// the retry of the entries it just rewrote.
    #[allow(clippy::too_many_arguments)]
    async fn append_at(
        &mut self,
        entry_id: Uuid,
        timestamp: u64,
        operation: WalOperation,
        data: &[u8],
        tenant_id: &str,
        dataset_id: &str,
        metadata: Option<String>,
    ) -> Result<Uuid> {
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
        if self.format != SegmentFormat::V1 {
            anyhow::bail!(
                "WAL segment {} uses the legacy unframed format and is read-only",
                self.id
            );
        }
        // A closed segment has had its handles taken. Writing through the
        // `if let Some(..)` arms below would then skip the bytes but still
        // advance the offsets and record the entry — durability claimed for
        // data never written, and every later offset pointing past a hole.
        // Refuse instead: the caller (rotation, compaction, idle eviction)
        // must open a fresh segment.
        if self.file.is_none() || self.data_file.is_none() {
            anyhow::bail!(
                "WAL segment {} is closed and cannot accept appends",
                self.id
            );
        }
        let data_offset = self.data_size;
        let data_header = data_record_header(data)?;
        if let Some(ref mut data_file) = self.data_file {
            data_file.seek(SeekFrom::Start(data_offset)).await?;
            data_file.write_all(&data_header).await?;
            data_file.write_all(data).await?;
            data_file.flush().await?;
        }
        self.data_size += (DATA_RECORD_HEADER_LEN + data.len()) as u64;

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
        let log_header = log_record_header(&entry_data)?;
        if let Some(ref mut file) = self.file {
            file.seek(SeekFrom::Start(self.size)).await?;
            file.write_all(&log_header).await?;
            file.write_all(&entry_data).await?;
            file.flush().await?;
        }

        self.size += (LOG_RECORD_HEADER_LEN + entry_data.len()) as u64;
        self.entries.push(entry);
        self.entry_index.insert(entry_id, self.entries.len() - 1);

        Ok(entry_id)
    }

    /// Read data for a specific entry.
    ///
    /// Validates the entry's byte range against the actual data-file length
    /// before reading. A range that runs past the file (truncated/partial
    /// write, stale offset bookkeeping, or a corrupt index) yields a clear,
    /// attributable bounds error here rather than an opaque `read_exact`
    /// "failed to fill whole buffer" or, worse, in-bounds garbage that the
    /// Arrow reader later rejects as `RangeOutOfBounds`.
    ///
    /// On a v1 segment the record header is then verified (magic, length,
    /// payload CRC). A record that fails is corruption at rest — a bit flip
    /// or torn write that survived the bounds check — so the raw bytes are
    /// quarantined to `<wal_dir>/dead-letter/<entry_id>.corrupt.bin`, a
    /// structured warning names the tenant/dataset/signal/offset,
    /// `signaldb.wal.corrupt_entries{record="data"}` is incremented, and an
    /// error is returned. Neighbouring records are unaffected: the caller
    /// retires this entry (see [`Wal::dead_letter_unreadable`]) and moves on.
    pub async fn read_entry_data(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        let data_len = tokio::fs::metadata(&self.data_path)
            .await
            .with_context(|| format!("Failed to stat WAL data file {}", self.data_path.display()))?
            .len();
        let record_len = match self.format {
            SegmentFormat::Legacy => entry.data_size,
            SegmentFormat::V1 => entry
                .data_size
                .checked_add(DATA_RECORD_HEADER_LEN as u64)
                .with_context(|| format!("WAL entry {} data size overflows u64", entry.id))?,
        };
        let end = entry.data_offset.checked_add(record_len).with_context(|| {
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

        let mut buffer = vec![0u8; record_len as usize];
        data_file.read_exact(&mut buffer).await?;

        if self.format == SegmentFormat::Legacy {
            return Ok(buffer);
        }

        match validate_data_record(&buffer, entry.data_size) {
            Ok(payload) => Ok(payload.to_vec()),
            Err(e) => {
                tracing::warn!(
                    entry_id = %entry.id,
                    tenant_id = %entry.tenant_id,
                    dataset_id = %entry.dataset_id,
                    signal = entry.operation.signal(),
                    segment_id = self.id,
                    data_offset = entry.data_offset,
                    data_size = entry.data_size,
                    error = %e,
                    "WAL data record failed integrity check; quarantining its bytes, \
                     neighbouring records remain readable"
                );
                crate::self_monitoring::app_metrics()
                    .wal_corrupt_entries
                    .add(
                        1,
                        &[
                            opentelemetry::KeyValue::new("record", "data"),
                            opentelemetry::KeyValue::new("signal", entry.operation.signal()),
                        ],
                    );
                if let Some(wal_dir) = self.path.parent()
                    && let Err(quarantine_err) =
                        Self::quarantine_corrupt_payload(wal_dir, entry.id, &buffer).await
                {
                    tracing::error!(
                        entry_id = %entry.id,
                        error = %quarantine_err,
                        "Failed to quarantine corrupt WAL data record bytes"
                    );
                }
                Err(anyhow::anyhow!(
                    "WAL entry {} data record corrupt at offset {} (segment {}): {e}",
                    entry.id,
                    entry.data_offset,
                    self.id
                ))
            }
        }
    }

    /// Preserve the raw bytes (header included) of a `.data` record that
    /// failed its integrity check, as
    /// `<wal_dir>/dead-letter/<entry_id>.corrupt.bin`. Unlike
    /// [`Self::quarantine_corrupt_entry`] the entry id *is* known here — the
    /// `.log` record decoded fine, only its payload is damaged — so the file
    /// is keyed by it like every other dead-letter artefact.
    async fn quarantine_corrupt_payload(
        wal_dir: &Path,
        entry_id: Uuid,
        record: &[u8],
    ) -> Result<PathBuf> {
        let dir = wal_dir.join("dead-letter");
        create_dir_all(&dir).await?;
        let path = dir.join(format!("{}.corrupt.bin", entry_id.simple()));
        let mut file = File::create(&path)
            .await
            .with_context(|| format!("Failed to create quarantine file {}", path.display()))?;
        file.write_all(record).await?;
        file.flush().await?;
        file.sync_all()
            .await
            .context("Failed to fsync quarantined WAL data record")?;
        Ok(path)
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
        tracing::info!("Deleted segment files for segment {}", self.id);
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
    ///
    /// A [`crate::wal::manager::WalManager`] sweep covers every cached WAL at
    /// once, so it paces itself by the **smallest** `cleanup_interval_secs`
    /// among its four per-signal configs. Raising this for one signal alone
    /// therefore has no effect; lowering it speeds up the sweep for all of
    /// them.
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
            tenant_id: tenant.to_string(),
            dataset_id: dataset.to_string(),
            ..self.clone()
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

/// Seconds since the Unix epoch. A clock before the epoch yields 0 rather
/// than a panic — this is called on the write path.
pub fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// WAL directory identities (`writer.id`) whose recovered backlog has already
/// been seeded into `signaldb.wal.entries_pending` by this process.
///
/// Process-global because the gauge is: see [`Wal::claim_pending_seed`].
static PENDING_SEED_CLAIMS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<String>>,
> = std::sync::LazyLock::new(Default::default);

/// Write-Ahead Log implementation for durability
pub struct Wal {
    config: WalConfig,
    current_segment: Arc<Mutex<WalSegment>>,
    next_segment_id: Arc<Mutex<u64>>,
    buffer: WalBuffer,
    /// Handle to the background flush task, behind a lock because
    /// [`Wal::close`] must stop it through a shared `Arc<Wal>` — the only
    /// shape the manager ever hands out.
    ///
    /// Aborting is not optional: the task holds clones of `buffer`,
    /// `current_segment` and `segments`, so dropping the `Arc<Wal>` alone
    /// neither stops the timer nor releases the segments' file descriptors.
    flush_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// All segments including current (for cleanup operations)
    segments: Arc<Mutex<Vec<Arc<Mutex<WalSegment>>>>>,
    /// Unix seconds of the last append, for idle eviction. Monotonic clocks
    /// are not used because the value is only ever compared against `now` on
    /// the same process.
    last_append: std::sync::atomic::AtomicU64,
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

        // A legacy (unframed) segment is read-only: if the newest segment on
        // disk is one, rotate onto a fresh framed segment so appends have
        // somewhere to go. Its pending entries stay replayable and it is
        // reclaimed like any sealed segment once processed.
        let last_is_legacy = match all_segments.last() {
            Some(segment) => segment.lock().await.format() == SegmentFormat::Legacy,
            None => false,
        };
        let mut next_id = max_segment_id + 1;

        // Load or create current segment if no segments exist
        let current_segment = if segment_ids.is_empty() {
            let segment = Arc::new(Mutex::new(
                WalSegment::new(&config.wal_dir, max_segment_id).await?,
            ));
            all_segments.push(segment.clone());
            segment
        } else if last_is_legacy {
            let segment = Arc::new(Mutex::new(WalSegment::new(&config.wal_dir, next_id).await?));
            next_id += 1;
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

        // Seed the pending gauge with the backlog recovered from disk.
        //
        // `signaldb.wal.entries_pending` is an UpDownCounter: `append`
        // increments as an entry enters the pending set, `mark_processed_many`
        // decrements as it leaves. The pending set outlives the process, but
        // the counter does not — entries already on disk were incremented by a
        // previous process, so without this their eventual processing
        // decrements with no matching increment and the gauge drifts negative
        // (a pending count below zero is impossible by construction, which
        // makes the metric useless as a backlog alarm).
        //
        // Seeding is once per WAL directory per process, keyed on the
        // directory's stable `writer_id`. Only the first open can encounter
        // entries this process did not count: anything still pending at a
        // later open was either seeded by that first open or incremented by an
        // `append` here, so seeding again would double-count it.
        let mut recovered_pending: usize = 0;
        if Self::claim_pending_seed(&writer_id) {
            for segment_arc in &all_segments {
                let segment = segment_arc.lock().await;
                recovered_pending += segment.entries.iter().filter(|e| !e.processed).count();
            }
        }
        if recovered_pending > 0 {
            tracing::info!(
                signaldb.wal.recovered_pending = recovered_pending as i64,
                "Recovered unprocessed WAL entries from disk"
            );
            crate::self_monitoring::app_metrics()
                .wal_entries_pending
                .add(recovered_pending as i64, &[]);
        }

        let wal = Self {
            config: config.clone(),
            current_segment,
            next_segment_id: Arc::new(Mutex::new(next_id)),
            buffer: Arc::new(RwLock::new(VecDeque::new())),
            flush_handle: std::sync::Mutex::new(None),
            segments: Arc::new(Mutex::new(all_segments)),
            last_append: std::sync::atomic::AtomicU64::new(unix_now_secs()),
            writer_id,
        };

        Ok(wal)
    }

    /// Claim the one-time pending-gauge seed for a WAL directory identity,
    /// returning whether this caller won it.
    ///
    /// `signaldb.wal.entries_pending` is process-global while a WAL directory
    /// may be opened more than once per process (`WalManager::clear_cache`
    /// drops cached WALs and they are rebuilt on next access). Only the first
    /// open can see entries this process never counted, so the seed is claimed
    /// once per identity: a later open finds entries that were either seeded
    /// by the first open or incremented by an `append` here, and seeding them
    /// again would double-count.
    fn claim_pending_seed(writer_id: &str) -> bool {
        PENDING_SEED_CLAIMS
            .lock()
            .map(|mut claimed| claimed.insert(writer_id.to_string()))
            // A poisoned lock means another thread panicked mid-claim. Skipping
            // the seed under-counts; it never double-counts.
            .unwrap_or(false)
    }

    /// Forget every claimed pending-gauge seed, so the next `Wal::new` seeds as
    /// if it were the first open in a fresh process.
    ///
    /// Test-only: exists so a single test process can exercise the restart path
    /// that seeding is *for*, which is otherwise unreachable without actually
    /// restarting.
    #[cfg(any(test, feature = "testing"))]
    pub fn reset_pending_seed_claims_for_test() {
        if let Ok(mut claimed) = PENDING_SEED_CLAIMS.lock() {
            claimed.clear();
        }
    }

    /// Stable identity of this WAL directory (see the `writer_id` field).
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Retire an entry whose payload cannot be read at all.
    ///
    /// [`Self::dead_letter`] re-reads the payload before preserving it, which
    /// is exactly the operation that fails when an entry's recorded byte range
    /// is unreadable (truncated segment, offset past EOF). Using it there would
    /// leave the entry pending forever, pinning its segment against
    /// reclamation — the failure this path exists to avoid.
    ///
    /// There is nothing to preserve, so record what *is* known — the entry's
    /// identity, byte range, and why the read failed — as
    /// `<wal_dir>/dead-letter/<entry_id>.unreadable.json` (fsynced), then mark
    /// the entry processed. The marker is deliberately not a `.bin`: it must
    /// not be mistaken for a replayable payload.
    ///
    /// Returns the marker file path.
    pub async fn dead_letter_unreadable(&self, entry_id: Uuid, reason: &str) -> Result<PathBuf> {
        let entry = self.find_entry(entry_id).await?;

        let dir = self.dead_letter_dir().await?;
        let path = dir.join(format!("{}.unreadable.json", entry_id.simple()));

        let marker = serde_json::json!({
            "entry_id": entry_id.to_string(),
            "tenant_id": entry.tenant_id,
            "dataset_id": entry.dataset_id,
            "operation": format!("{:?}", entry.operation),
            "timestamp": entry.timestamp,
            "data_offset": entry.data_offset,
            "data_size": entry.data_size,
            "reason": reason,
            "note": "payload was unreadable; no bytes could be preserved",
        });
        let body = serde_json::to_vec_pretty(&marker)?;
        Self::write_fsynced(&path, &body, "unreadable-entry marker").await?;

        self.mark_processed(entry_id).await?;
        Ok(path)
    }

    /// Move a poison entry aside: persist its raw payload to
    /// `<wal_dir>/dead-letter/<entry_id>.bin` (fsynced) and mark the
    /// entry processed so it stops blocking the processing loop. The
    /// data is preserved for manual inspection/replay rather than lost.
    ///
    /// Returns the dead-letter file path.
    pub async fn dead_letter(&self, entry_id: Uuid) -> Result<PathBuf> {
        let entry = self.find_entry(entry_id).await?;
        let data = self.read_entry_data(&entry).await?;

        let dir = self.dead_letter_dir().await?;
        let path = dir.join(format!("{}.bin", entry_id.simple()));
        Self::write_fsynced(&path, &data, "dead-letter file").await?;

        self.mark_processed(entry_id).await?;
        Ok(path)
    }

    /// Retire an entry the writer refuses to accept.
    ///
    /// Unlike [`Self::dead_letter`], which retires a payload that could not be
    /// parsed, the payload here is intact — it simply cannot be shaped into
    /// its target table. It stays replayable once the rejection cause is
    /// fixed, so the raw bytes are preserved exactly as `dead_letter` does and
    /// a `<entry_id>.rejected.json` marker records why alongside them. Without
    /// the marker the directory mixes salvageable batches with unparseable
    /// ones under identical `.bin` names, and an operator cannot tell which is
    /// safe to replay.
    ///
    /// Returns the path of the preserved payload.
    pub async fn dead_letter_rejected(&self, entry_id: Uuid, reason: &str) -> Result<PathBuf> {
        let entry = self.find_entry(entry_id).await?;
        let data = self.read_entry_data(&entry).await?;

        let dir = self.dead_letter_dir().await?;
        let path = dir.join(format!("{}.bin", entry_id.simple()));
        Self::write_fsynced(&path, &data, "dead-letter file").await?;

        let marker_path = path.with_extension("rejected.json");
        let marker = serde_json::json!({
            "entry_id": entry_id.to_string(),
            "tenant_id": entry.tenant_id,
            "dataset_id": entry.dataset_id,
            "operation": format!("{:?}", entry.operation),
            "timestamp": entry.timestamp,
            "reason": reason,
            "note": "writer rejected this batch; payload is intact and replayable once the cause is fixed",
        });
        let body = serde_json::to_vec_pretty(&marker)?;
        Self::write_fsynced(&marker_path, &body, "rejection marker").await?;

        self.mark_processed(entry_id).await?;
        Ok(path)
    }

    /// Look up a WAL entry by id, for the dead-letter paths above.
    async fn find_entry(&self, entry_id: Uuid) -> Result<WalEntry> {
        self.get_entries()
            .await?
            .into_iter()
            .find(|e| e.id == entry_id)
            .ok_or_else(|| anyhow::anyhow!("WAL entry {entry_id} not found"))
    }

    /// The dead-letter directory, created if missing.
    async fn dead_letter_dir(&self) -> Result<PathBuf> {
        let dir = self.config.wal_dir.join("dead-letter");
        create_dir_all(&dir).await?;
        Ok(dir)
    }

    /// Write `data` to `path`, flushing and fsyncing before returning.
    /// `what` names the file in the create/fsync error context (e.g.
    /// "dead-letter file", "rejection marker").
    async fn write_fsynced(path: &Path, data: &[u8], what: &str) -> Result<()> {
        let mut file = File::create(path)
            .await
            .with_context(|| format!("Failed to create {what} {}", path.display()))?;
        file.write_all(data).await?;
        file.flush().await?;
        file.sync_all()
            .await
            .with_context(|| format!("Failed to fsync {what}"))?;
        Ok(())
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
                    tracing::error!("Failed to flush WAL buffer: {e}");
                }
            }
        });

        *self
            .flush_handle
            .lock()
            .expect("flush handle mutex poisoned") = Some(handle);
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
        fields(signaldb.wal.operation = ?operation, signaldb.wal.data_size = data.len() as i64)
    )]
    pub async fn append(
        &self,
        operation: WalOperation,
        data: Vec<u8>,
        metadata: Option<String>,
    ) -> Result<Uuid> {
        let entry_id = Uuid::new_v4();
        self.last_append
            .store(unix_now_secs(), std::sync::atomic::Ordering::Relaxed);

        {
            let metrics = crate::self_monitoring::app_metrics();
            let attrs = [opentelemetry::KeyValue::new(
                "operation",
                format!("{operation:?}"),
            )];
            metrics.wal_entries_written.add(1, &attrs);
            metrics.wal_entries_pending.add(1, &[]);
        }

        // Add to buffer first for batching, checking the flush threshold
        // under the same write-lock acquisition rather than re-locking to
        // read the length back.
        let should_flush = {
            let mut buffer = self.buffer.write().await;
            buffer.push_back((entry_id, operation, data, metadata));
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

    /// How long since this WAL last accepted an append.
    ///
    /// Drives idle eviction. A WAL created and never written to counts as idle
    /// from its creation, which is what makes a discovered-but-quiet WAL from a
    /// previous run evictable once its backlog is drained.
    pub fn idle_for(&self) -> std::time::Duration {
        let last = self.last_append.load(std::sync::atomic::Ordering::Relaxed);
        std::time::Duration::from_secs(unix_now_secs().saturating_sub(last))
    }

    /// Number of segments this WAL currently holds, including the open one.
    ///
    /// Sealed segments are only reclaimed by [`Self::cleanup`], so this is the
    /// direct read on whether reclamation is keeping up.
    pub async fn segment_count(&self) -> usize {
        self.segments.lock().await.len()
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
    ///
    /// The read uses the segment's **own** copy of the entry, not the caller's:
    /// [`Self::cleanup`] compacts sealed segments, which moves surviving
    /// entries to new offsets, so a consumer that listed entries before a
    /// compaction still holds stale offsets. Honouring those verbatim would
    /// read another entry's bytes — the offset-desync class behind the hive
    /// WAL corruption (#865/#883). Only the id is taken from the caller.
    pub async fn read_entry_data(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        let segments = self.segments.lock().await;
        for segment_arc in segments.iter() {
            let segment = segment_arc.lock().await;
            if let Some(authoritative) = segment.entry_by_id(&entry.id) {
                return segment.read_entry_data(authoritative).await;
            }
        }
        anyhow::bail!(
            "WAL entry {} not found in any of {} segments",
            entry.id,
            segments.len()
        )
    }

    /// Stop this WAL's background flush task, flush what is buffered, and
    /// close every segment's file handles.
    ///
    /// Takes `&self` so it is callable through the `Arc<Wal>` the manager
    /// hands out — that is the whole point. Aborting the flush task is what
    /// actually releases the resources: it holds clones of `buffer`,
    /// `current_segment` and `segments`, so dropping the last `Arc<Wal>`
    /// without this leaves the timer running and the descriptors open.
    ///
    /// After this the WAL is inert: its segments refuse appends
    /// ([`WalSegment::append`] fails rather than silently dropping bytes), so
    /// a caller holding a stale clone gets an error, never silent data loss.
    /// Reads still work.
    pub async fn close(&self) -> Result<()> {
        if let Some(handle) = self
            .flush_handle
            .lock()
            .expect("flush handle mutex poisoned")
            .take()
        {
            handle.abort();
        }

        // Flush after aborting, so the abort cannot land mid-write and the
        // buffered entries are made durable exactly once, here.
        self.flush().await?;

        let segments = self.segments.lock().await;
        for segment_arc in segments.iter() {
            let mut segment = segment_arc.lock().await;
            segment.close().await?;
        }

        Ok(())
    }

    /// Shutdown the WAL and cleanup resources
    pub async fn shutdown(self) -> Result<()> {
        self.close().await
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
        fields(signaldb.wal.entry_count = entry_ids.len() as i64)
    )]
    pub async fn mark_processed_many(&self, entry_ids: &[Uuid]) -> Result<()> {
        if entry_ids.is_empty() {
            return Ok(());
        }

        // Count only unprocessed -> processed transitions so repeated calls
        // don't skew the metrics.
        let mut newly_processed: i64 = 0;
        let mut remaining: rustc_hash::FxHashSet<Uuid> = entry_ids.iter().copied().collect();

        // Search all segments, not just current. Each id is looked up via
        // `entry_index` (O(1)) instead of scanning every entry in the
        // segment — with large, not-yet-compacted segments the scan used to
        // dominate CPU (issue #1112).
        let segments = self.segments.lock().await;
        let segment_count = segments.len();

        for segment_arc in segments.iter() {
            if remaining.is_empty() {
                break;
            }

            let mut segment = segment_arc.lock().await;

            let mut segment_dirty = false;
            let found_ids: Vec<Uuid> = remaining
                .iter()
                .copied()
                .filter(|id| segment.entry_index.contains_key(id))
                .collect();

            for id in found_ids {
                remaining.remove(&id);
                let idx = segment.entry_index[&id];
                let entry = &mut segment.entries[idx];
                if !entry.processed {
                    entry.processed = true;
                    newly_processed += 1;
                    segment_dirty = true;
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

        tracing::debug!(
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
            tracing::info!("Deleted {deleted_count} fully-processed WAL segments");
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

        tracing::info!(
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

        // Write unprocessed entries to new segment, each keeping the
        // timestamp it was originally written with.
        for (entry, data) in entries_with_data {
            new_segment
                .append_at(
                    entry.id,
                    entry.timestamp,
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

        tracing::info!(
            "Compacted segment {} from {} to {} entries",
            temp_segment_id,
            original_entry_count,
            unprocessed_entries.len()
        );

        Ok(true)
    }

    /// Reclaim WAL disk: delete sealed segments whose entries are all
    /// processed, and compact the rest past `compaction_threshold`.
    ///
    /// **Call this from the same task that drains the WAL, between passes.**
    /// Compaction rewrites a segment and moves its surviving entries to new
    /// offsets. [`Self::read_entry_data`] re-resolves offsets so a stale entry
    /// copy still reads correctly, but running cleanup concurrently with a
    /// drain would still have it rewriting segments under a consumer that is
    /// mid-pass. [`crate::wal::manager::WalManager::cleanup_all`] is the
    /// sweep both services call at the end of a pass.
    pub async fn cleanup(&self) -> Result<()> {
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
}

/// Fixtures shared by the WAL tests in this module and in
/// [`crate::wal::manager`], which exercise the same seal-a-few-segments setup.
#[cfg(test)]
pub(crate) mod test_support {
    use super::WalConfig;
    use std::path::Path;

    /// A WAL config tuned so a handful of small appends seal several segments,
    /// which is what makes segment reclamation observable in a fast test.
    pub(crate) fn rotating_config(dir: &Path) -> WalConfig {
        let mut config = WalConfig::with_defaults(dir.to_path_buf());
        config.max_segment_size = 1024;
        config.max_buffer_entries = 1;
        config.flush_interval_secs = 3600;
        config
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
    use super::test_support::rotating_config;
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
    async fn a_closed_segment_refuses_appends_instead_of_dropping_them() {
        // `close()` takes the file handles out of the segment. Appending after
        // that used to be silently lossy: the write was skipped because the
        // handle was `None`, but the offsets still advanced and the entry was
        // still recorded — so the WAL claimed durability for bytes that were
        // never written, and every later offset pointed past a hole. Idle
        // eviction closes segments while the manager may still hand out the
        // instance, so this has to fail loudly.
        let temp_dir = TempDir::new().unwrap();
        let mut segment = WalSegment::new(temp_dir.path(), 0).await.unwrap();

        let payload = record_batch_to_bytes(&make_batch()).unwrap();
        segment
            .append(
                Uuid::new_v4(),
                WalOperation::WriteTraces,
                &payload,
                "t",
                "d",
                None,
            )
            .await
            .unwrap();
        let entries_before = segment.entries.len();

        segment.close().await.unwrap();

        let err = segment
            .append(
                Uuid::new_v4(),
                WalOperation::WriteTraces,
                &payload,
                "t",
                "d",
                None,
            )
            .await
            .expect_err("appending to a closed segment must fail, not silently drop the payload");
        assert!(
            err.to_string().contains("closed"),
            "expected a closed-segment error, got: {err}"
        );
        assert_eq!(
            segment.entries.len(),
            entries_before,
            "a refused append must not record an entry"
        );
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

    /// Flip one byte inside the payload of `entry` in the segment's `.data`
    /// file, leaving its record header intact.
    async fn flip_payload_byte(data_path: &Path, entry: &WalEntry) {
        let mut bytes = tokio::fs::read(data_path).await.unwrap();
        let idx = entry.data_offset as usize + DATA_RECORD_HEADER_LEN + 2;
        bytes[idx] ^= 0x01;
        tokio::fs::write(data_path, &bytes).await.unwrap();
    }

    #[tokio::test]
    async fn framed_records_round_trip_through_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());
        let payloads: Vec<Vec<u8>> = (0..5)
            .map(|i| record_batch_to_bytes(&make_batch_val(i)).unwrap())
            .collect();

        let wal = Wal::new(config.clone()).await.unwrap();
        let mut ids = Vec::new();
        for p in &payloads {
            ids.push(
                wal.append(WalOperation::WriteTraces, p.clone(), None)
                    .await
                    .unwrap(),
            );
        }
        wal.flush().await.unwrap();
        drop(wal);

        let log = tokio::fs::read(temp_dir.path().join("wal-0000000000.log"))
            .await
            .unwrap();
        assert_eq!(&log[..SEGMENT_HEADER_LEN], &segment_header());

        let reopened = Wal::new(config).await.unwrap();
        for (id, p) in ids.iter().zip(&payloads) {
            let entry = reopened
                .get_entries()
                .await
                .unwrap()
                .into_iter()
                .find(|e| e.id == *id)
                .unwrap();
            assert_eq!(entry.data_size, p.len() as u64);
            assert_eq!(&reopened.read_entry_data(&entry).await.unwrap(), p);
        }
    }

    #[tokio::test]
    async fn flipped_payload_byte_is_detected_quarantined_and_neighbours_still_read() {
        // The #946 promise: a bit flip inside one record's payload is caught
        // by the record CRC, attributed to that entry, and does not disturb
        // the entries around it.
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());
        let wal = Wal::new(config.clone()).await.unwrap();

        let payloads: Vec<Vec<u8>> = (0..3)
            .map(|i| record_batch_to_bytes(&make_batch_val(i)).unwrap())
            .collect();
        let mut ids = Vec::new();
        for p in &payloads {
            ids.push(
                wal.append(WalOperation::WriteTraces, p.clone(), None)
                    .await
                    .unwrap(),
            );
        }
        wal.flush().await.unwrap();
        let entries = wal.get_entries().await.unwrap();
        let victim = entries.iter().find(|e| e.id == ids[1]).unwrap().clone();

        flip_payload_byte(&temp_dir.path().join("wal-0000000000.data"), &victim).await;

        let err = wal
            .read_entry_data(&victim)
            .await
            .expect_err("a flipped payload byte must fail the CRC");
        assert!(
            err.to_string().contains("CRC mismatch"),
            "expected a CRC error, got: {err}"
        );

        // Bytes preserved for forensics, keyed by entry id.
        let quarantined = temp_dir
            .path()
            .join("dead-letter")
            .join(format!("{}.corrupt.bin", victim.id.simple()));
        let bytes = tokio::fs::read(&quarantined).await.unwrap();
        assert_eq!(bytes.len(), DATA_RECORD_HEADER_LEN + payloads[1].len());

        // Neighbours are unaffected.
        for i in [0usize, 2] {
            let e = entries.iter().find(|e| e.id == ids[i]).unwrap();
            assert_eq!(&wal.read_entry_data(e).await.unwrap(), &payloads[i]);
        }

        // Also after a reopen: the log records are intact, only the payload
        // is damaged, so the entry is still listed and still refuses to read.
        drop(wal);
        let reopened = Wal::new(config).await.unwrap();
        let entries = reopened.get_entries().await.unwrap();
        assert_eq!(entries.len(), 3);
        assert!(reopened.read_entry_data(&victim).await.is_err());
        let last = entries.iter().find(|e| e.id == ids[2]).unwrap();
        assert_eq!(&reopened.read_entry_data(last).await.unwrap(), &payloads[2]);
    }

    #[tokio::test]
    async fn truncated_data_tail_fails_only_the_torn_record() {
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());
        let wal = Wal::new(config.clone()).await.unwrap();
        let first = record_batch_to_bytes(&make_batch_val(1)).unwrap();
        let second = record_batch_to_bytes(&make_batch_val(2)).unwrap();
        let first_id = wal
            .append(WalOperation::WriteTraces, first.clone(), None)
            .await
            .unwrap();
        let second_id = wal
            .append(WalOperation::WriteTraces, second.clone(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        drop(wal);

        let data_path = temp_dir.path().join("wal-0000000000.data");
        let mut bytes = tokio::fs::read(&data_path).await.unwrap();
        let torn = bytes.len() - 5;
        bytes.truncate(torn);
        tokio::fs::write(&data_path, &bytes).await.unwrap();

        let reopened = Wal::new(config).await.unwrap();
        let entries = reopened.get_entries().await.unwrap();
        let first_entry = entries.iter().find(|e| e.id == first_id).unwrap();
        let second_entry = entries.iter().find(|e| e.id == second_id).unwrap();
        assert_eq!(reopened.read_entry_data(first_entry).await.unwrap(), first);
        let err = reopened.read_entry_data(second_entry).await.unwrap_err();
        assert!(err.to_string().contains("out of bounds"), "{err}");
    }

    #[tokio::test]
    async fn corrupt_log_record_crc_is_skipped_and_neighbours_survive() {
        // A single flipped bit in a `.log` record payload no longer has to
        // wait for bincode to choke on it: the record CRC catches it, and
        // replay resyncs onto the next record exactly as before.
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());
        let wal = Wal::new(config.clone()).await.unwrap();
        let mut ids = Vec::new();
        for i in 0..3u8 {
            ids.push(
                wal.append(WalOperation::WriteTraces, vec![i; 16], None)
                    .await
                    .unwrap(),
            );
        }
        wal.flush().await.unwrap();
        drop(wal);

        let log_path = temp_dir.path().join("wal-0000000000.log");
        let mut buffer = tokio::fs::read(&log_path).await.unwrap();
        // Second record: skip file header, first record header + payload.
        let (first_len, _) = parse_log_record_header(&buffer[SEGMENT_HEADER_LEN..]).unwrap();
        let second_payload =
            SEGMENT_HEADER_LEN + LOG_RECORD_HEADER_LEN + first_len + LOG_RECORD_HEADER_LEN;
        buffer[second_payload + 20] ^= 0x01;
        tokio::fs::write(&log_path, &buffer).await.unwrap();

        let reopened = Wal::new(config).await.unwrap();
        let surviving: Vec<Uuid> = reopened
            .get_entries()
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect();
        assert_eq!(surviving, vec![ids[0], ids[2]]);
    }

    /// Hand-write a segment in the pre-#946 layout: `.log` = `[u64 len][bincode]`
    /// records with no file header, `.data` = raw concatenated payloads.
    async fn write_legacy_segment(dir: &Path, payloads: &[Vec<u8>]) -> Vec<Uuid> {
        let mut log = Vec::new();
        let mut data = Vec::new();
        let mut ids = Vec::new();
        for p in payloads {
            let entry = WalEntry {
                id: Uuid::new_v4(),
                timestamp: 0,
                operation: WalOperation::WriteTraces,
                data_size: p.len() as u64,
                data_offset: data.len() as u64,
                processed: false,
                tenant_id: "test-tenant".to_string(),
                dataset_id: "test-dataset".to_string(),
                metadata: None,
            };
            ids.push(entry.id);
            data.extend_from_slice(p);
            let bytes = bincode::serialize(&entry).unwrap();
            log.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            log.extend_from_slice(&bytes);
        }
        tokio::fs::write(dir.join("wal-0000000000.log"), &log)
            .await
            .unwrap();
        tokio::fs::write(dir.join("wal-0000000000.data"), &data)
            .await
            .unwrap();
        ids
    }

    #[tokio::test]
    async fn legacy_unframed_segment_is_readable_sealed_and_rotated_past() {
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());
        let payloads = vec![b"legacy one".to_vec(), b"legacy two".to_vec()];
        let legacy_ids = write_legacy_segment(temp_dir.path(), &payloads).await;

        let wal = Wal::new(config.clone()).await.unwrap();

        // Legacy entries are listed and read on the legacy layout.
        let entries = wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(entries.len(), 2);
        for (entry, payload) in entries.iter().zip(&payloads) {
            assert_eq!(
                entry.id,
                legacy_ids[entries.iter().position(|e| e.id == entry.id).unwrap()]
            );
            assert_eq!(&wal.read_entry_data(entry).await.unwrap(), payload);
        }

        // New appends land in a fresh framed segment, not the legacy one.
        let new_id = wal
            .append(WalOperation::WriteTraces, b"framed".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        assert!(temp_dir.path().join("wal-0000000001.log").exists());
        let legacy_log = tokio::fs::read(temp_dir.path().join("wal-0000000000.log"))
            .await
            .unwrap();
        assert_ne!(
            &legacy_log[..4],
            &framing::SEGMENT_MAGIC,
            "legacy log must stay untouched"
        );
        {
            let segments = wal.segments.lock().await;
            assert_eq!(segments[0].lock().await.format(), SegmentFormat::Legacy);
            assert_eq!(segments[1].lock().await.format(), SegmentFormat::V1);
        }

        // Everything is still readable after a reopen, and the reopen does
        // not keep rotating (the current segment is already framed).
        drop(wal);
        let reopened = Wal::new(config).await.unwrap();
        assert!(!temp_dir.path().join("wal-0000000002.log").exists());
        let entries = reopened.get_unprocessed_entries().await.unwrap();
        assert_eq!(entries.len(), 3);
        let new_entry = entries.iter().find(|e| e.id == new_id).unwrap();
        assert_eq!(
            reopened.read_entry_data(new_entry).await.unwrap(),
            b"framed"
        );
        let legacy_entry = entries.iter().find(|e| e.id == legacy_ids[1]).unwrap();
        assert_eq!(
            reopened.read_entry_data(legacy_entry).await.unwrap(),
            payloads[1]
        );
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
    async fn dead_letter_rejected_records_the_reason_beside_the_payload() {
        // A rejected batch is intact and replayable once the rejection cause
        // is fixed, unlike a truncated record. An operator sweeping the
        // dead-letter directory must be able to tell the two apart without
        // guessing, so the payload is preserved AND annotated (#1060).
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

        let payload = b"perfectly good batch the writer refuses".to_vec();
        let entry_id = wal
            .append(WalOperation::WriteMetrics, payload.clone(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();

        let reason = "Flight do_put failed: Column 'value' is declared as non-nullable";
        let path = wal.dead_letter_rejected(entry_id, reason).await.unwrap();

        let preserved = tokio::fs::read(&path).await.unwrap();
        assert_eq!(preserved, payload, "payload must be preserved verbatim");

        let marker_path = path.with_extension("rejected.json");
        let marker: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&marker_path).await.unwrap()).unwrap();
        assert_eq!(marker["entry_id"], entry_id.to_string());
        assert_eq!(marker["reason"], reason);
        assert_eq!(marker["tenant_id"], "test-tenant");

        let unprocessed = wal.get_unprocessed_entries().await.unwrap();
        assert!(
            unprocessed.is_empty(),
            "rejected entry must be marked processed so it stops pinning its segment"
        );
    }

    /// Corrupt the payload bytes of the `target_index`-th entry (0-based, in
    /// append order) in a single-segment WAL log file on disk, leaving the
    /// entry's record header — and every other entry — untouched.
    /// Overwrites with `0xFF` rather than flipping a byte or two: a small
    /// flip can still deserialize into garbage bincode, which would not
    /// exercise the deserialize-failure path this helper exists to trigger.
    ///
    /// Returns `(entry_payload_offset, entry_payload_len)` of the corrupted
    /// entry — the byte range inside the `.log` file, after its record
    /// header — so callers can assert on the quarantine file name/contents.
    async fn corrupt_nth_log_entry_payload(log_path: &Path, target_index: usize) -> (usize, usize) {
        let mut buffer = tokio::fs::read(log_path).await.unwrap();

        // Walk the file exactly like `WalSegment::load` does, to find the
        // byte range of the target entry's payload.
        let mut offset = SEGMENT_HEADER_LEN;
        let mut index = 0usize;
        loop {
            assert!(
                offset + LOG_RECORD_HEADER_LEN <= buffer.len(),
                "ran out of entries before reaching index {target_index}"
            );
            let (entry_len, _) = parse_log_record_header(&buffer[offset..]).unwrap();
            offset += LOG_RECORD_HEADER_LEN;
            assert!(offset + entry_len <= buffer.len(), "entry runs past EOF");

            if index == target_index {
                let payload_offset = offset;
                for byte in &mut buffer[payload_offset..payload_offset + entry_len] {
                    *byte = 0xFF;
                }
                tokio::fs::write(log_path, &buffer).await.unwrap();
                return (payload_offset, entry_len);
            }

            offset += entry_len;
            index += 1;
        }
    }

    fn wal_test_config(wal_dir: PathBuf) -> WalConfig {
        WalConfig {
            wal_dir,
            max_segment_size: 64 * 1024 * 1024,
            max_buffer_entries: 10,
            flush_interval_secs: 1,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        }
    }

    #[tokio::test]
    async fn wal_load_skips_content_corrupted_entry_and_keeps_neighbours() {
        // Regression for issue #1033: a single content-corrupted WAL entry
        // (framing intact, payload undeserializable) must not abort replay.
        // Framing is intact, so `offset += entry_len` resyncs exactly onto
        // the next record — skip-and-continue must preserve every entry
        // around the corrupt one rather than discarding the whole tail.
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());

        let wal = Wal::new(config.clone()).await.unwrap();
        let first = b"entry one payload".to_vec();
        let second = b"entry two payload (will be corrupted)".to_vec();
        let third = b"entry three payload".to_vec();
        let first_id = wal
            .append(WalOperation::WriteTraces, first.clone(), None)
            .await
            .unwrap();
        let second_id = wal
            .append(WalOperation::WriteTraces, second.clone(), None)
            .await
            .unwrap();
        let third_id = wal
            .append(WalOperation::WriteTraces, third.clone(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        drop(wal);

        let log_path = temp_dir.path().join("wal-0000000000.log");
        corrupt_nth_log_entry_payload(&log_path, 1).await;

        // Reopening must succeed, not error out on the corrupted entry.
        let reopened = Wal::new(config).await.expect(
            "load() must skip a content-corrupted entry instead of aborting the whole replay",
        );

        let entries = reopened.get_entries().await.unwrap();
        let ids: std::collections::HashSet<Uuid> = entries.iter().map(|e| e.id).collect();
        assert!(
            ids.contains(&first_id),
            "entry before the corrupt one must survive"
        );
        assert!(
            ids.contains(&third_id),
            "entry after the corrupt one must survive"
        );
        assert!(
            !ids.contains(&second_id),
            "the corrupted entry itself must not appear in the replayed set"
        );

        let first_entry = entries.iter().find(|e| e.id == first_id).unwrap();
        let third_entry = entries.iter().find(|e| e.id == third_id).unwrap();
        assert_eq!(reopened.read_entry_data(first_entry).await.unwrap(), first);
        assert_eq!(reopened.read_entry_data(third_entry).await.unwrap(), third);
    }

    #[tokio::test]
    async fn wal_load_quarantines_corrupt_entry_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());

        let wal = Wal::new(config.clone()).await.unwrap();
        wal.append(WalOperation::WriteTraces, b"first".to_vec(), None)
            .await
            .unwrap();
        wal.append(
            WalOperation::WriteTraces,
            b"second (corrupted)".to_vec(),
            None,
        )
        .await
        .unwrap();
        wal.append(WalOperation::WriteTraces, b"third".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        drop(wal);

        let log_path = temp_dir.path().join("wal-0000000000.log");
        let (payload_offset, payload_len) = corrupt_nth_log_entry_payload(&log_path, 1).await;
        let corrupted_bytes = {
            let buffer = tokio::fs::read(&log_path).await.unwrap();
            buffer[payload_offset..payload_offset + payload_len].to_vec()
        };

        Wal::new(config).await.unwrap();

        let dead_letter_dir = temp_dir.path().join("dead-letter");
        let mut quarantine_files = Vec::new();
        let mut rd = tokio::fs::read_dir(&dead_letter_dir).await.unwrap();
        while let Some(e) = rd.next_entry().await.unwrap() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("segment-") && name.ends_with(".corrupt.bin") {
                quarantine_files.push(e.path());
            }
        }
        assert_eq!(
            quarantine_files.len(),
            1,
            "expected exactly one quarantined corrupt entry, found {quarantine_files:?}"
        );

        let quarantined = tokio::fs::read(&quarantine_files[0]).await.unwrap();
        assert_eq!(
            quarantined, corrupted_bytes,
            "quarantined bytes must match the raw corrupt entry bytes on disk"
        );
    }

    #[tokio::test]
    async fn wal_reopen_after_corruption_is_idempotent() {
        // The actual crash-loop regression (#1033): a corrupted entry must
        // not turn every subsequent restart into a fatal error. Reopening
        // twice must both succeed.
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());

        let wal = Wal::new(config.clone()).await.unwrap();
        wal.append(WalOperation::WriteTraces, b"first".to_vec(), None)
            .await
            .unwrap();
        wal.append(
            WalOperation::WriteTraces,
            b"second (corrupted)".to_vec(),
            None,
        )
        .await
        .unwrap();
        wal.append(WalOperation::WriteTraces, b"third".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        drop(wal);

        let log_path = temp_dir.path().join("wal-0000000000.log");
        corrupt_nth_log_entry_payload(&log_path, 1).await;

        Wal::new(config.clone())
            .await
            .expect("first reopen after corruption must succeed");
        Wal::new(config)
            .await
            .expect("second reopen after corruption must ALSO succeed (this is the crash loop)");
    }

    #[tokio::test]
    async fn wal_load_survives_torn_tail() {
        // Characterization test: a truncated tail (short read, e.g. a crash
        // mid-write) is a different failure mode from content corruption and
        // is already handled by the bounds check breaking out of the replay
        // loop. This locks in that existing behavior.
        let temp_dir = TempDir::new().unwrap();
        let config = wal_test_config(temp_dir.path().to_path_buf());

        let wal = Wal::new(config.clone()).await.unwrap();
        let first = b"entry one payload".to_vec();
        let second = b"entry two payload".to_vec();
        let first_id = wal
            .append(WalOperation::WriteTraces, first.clone(), None)
            .await
            .unwrap();
        let second_id = wal
            .append(WalOperation::WriteTraces, second.clone(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        drop(wal);

        let log_path = temp_dir.path().join("wal-0000000000.log");
        let mut buffer = tokio::fs::read(&log_path).await.unwrap();
        // Truncate mid-payload of the last entry.
        let torn_len = buffer.len() - 3;
        buffer.truncate(torn_len);
        tokio::fs::write(&log_path, &buffer).await.unwrap();

        let reopened = Wal::new(config)
            .await
            .expect("a torn tail must not abort replay");
        let entries = reopened.get_entries().await.unwrap();
        let ids: std::collections::HashSet<Uuid> = entries.iter().map(|e| e.id).collect();
        assert!(
            ids.contains(&first_id),
            "entry before the tear must survive"
        );
        assert!(
            !ids.contains(&second_id),
            "the torn entry must not appear (it was never fully written)"
        );

        let first_entry = entries.iter().find(|e| e.id == first_id).unwrap();
        assert_eq!(reopened.read_entry_data(first_entry).await.unwrap(), first);
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
    async fn mark_processed_many_and_read_entry_data_scale_with_batch_not_segment_size() {
        // mark_processed_many and Wal::read_entry_data used to locate an
        // entry by hashing/comparing against every entry in a segment
        // (issue #1112). That made them O(entries-in-segment) instead of
        // O(ids-looked-up), which dominated CPU once a segment held many
        // already-processed entries. This pins the *correctness* of the
        // O(1) `entry_index` lookup that replaced the scan: entries appended
        // early and late in a large single segment must still be found,
        // marked, and readable.
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            // Large enough that all entries land in one segment.
            max_segment_size: 10 * 1024 * 1024,
            max_buffer_entries: 1,
            flush_interval_secs: 3600,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Wal::new(config).await.unwrap();

        let mut ids = Vec::new();
        for i in 0..500 {
            let id = wal
                .append(
                    WalOperation::WriteTraces,
                    format!("payload-{i}").into_bytes(),
                    None,
                )
                .await
                .unwrap();
            ids.push(id);
        }
        wal.flush().await.unwrap();

        // Mark only the first and last entries processed; everything else
        // (in between, all sharing the one segment) must stay untouched.
        let first = ids[0];
        let last = *ids.last().unwrap();
        wal.mark_processed_many(&[first, last]).await.unwrap();

        let unprocessed = wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(unprocessed.len(), 498);
        assert!(unprocessed.iter().all(|e| e.id != first && e.id != last));

        // An entry appended late into the segment must still be readable via
        // the entry-id -> segment lookup.
        let late_entry = unprocessed
            .iter()
            .find(|e| e.id == ids[499 - 2])
            .expect("entry appended near the end of the segment must be present");
        let data = wal.read_entry_data(late_entry).await.unwrap();
        assert_eq!(data, format!("payload-{}", 499 - 2).into_bytes());
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

    /// Append `count` Arrow payloads, flushing each so segments seal as the
    /// size cap is crossed. Returns the entry ids in append order.
    async fn append_rotating(wal: &Wal, count: usize) -> Vec<Uuid> {
        let mut ids = Vec::new();
        for i in 0..count {
            let bytes = record_batch_to_bytes(&make_batch_val(i as i64)).unwrap();
            ids.push(
                wal.append(WalOperation::WriteTraces, bytes, None)
                    .await
                    .unwrap(),
            );
            wal.flush().await.unwrap();
        }
        ids
    }

    #[tokio::test]
    async fn cleanup_deletes_sealed_segments_whose_entries_are_all_processed() {
        // `Wal::cleanup` is the only path that reclaims WAL disk, and it had
        // no caller in any service (#1305): sealed segments accumulated
        // forever and were re-read into memory at every start.
        let temp_dir = TempDir::new().unwrap();
        let wal = Wal::new(rotating_config(temp_dir.path())).await.unwrap();

        let ids = append_rotating(&wal, 12).await;
        let sealed_before = wal.segment_count().await;
        assert!(
            sealed_before > 1,
            "test needs several segments, got {sealed_before}"
        );

        // Everything is processed, so every sealed segment is reclaimable.
        for id in &ids {
            wal.mark_processed(*id).await.unwrap();
        }

        wal.cleanup().await.unwrap();

        assert_eq!(
            wal.segment_count().await,
            1,
            "only the current segment should survive a full cleanup"
        );
        let on_disk = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext == "log" || ext == "data" || ext == "index")
            })
            .count();
        assert_eq!(
            on_disk, 3,
            "deleted segments must not leave .log/.data/.index files behind"
        );
    }

    #[tokio::test]
    async fn compaction_preserves_unprocessed_payloads_and_their_timestamps() {
        // Compaction rewrites a sealed segment without its processed entries.
        // The survivors must read back byte-identical, and keep their original
        // timestamps: the acceptor's retry loop gates on entry age, so a
        // refreshed timestamp would silently postpone a retry every time
        // cleanup ran.
        let temp_dir = TempDir::new().unwrap();
        let wal = Wal::new(rotating_config(temp_dir.path())).await.unwrap();

        let ids = append_rotating(&wal, 12).await;
        let before = wal.get_entries().await.unwrap();
        let mut payloads: std::collections::HashMap<Uuid, Vec<u8>> = Default::default();
        for entry in &before {
            payloads.insert(entry.id, wal.read_entry_data(entry).await.unwrap());
        }
        let timestamps: std::collections::HashMap<Uuid, u64> =
            before.iter().map(|e| (e.id, e.timestamp)).collect();

        // Process every second entry: sealed segments cross the 50% threshold
        // without becoming fully reclaimable.
        for id in ids.iter().step_by(2) {
            wal.mark_processed(*id).await.unwrap();
        }

        // WAL timestamps have second granularity, so a compaction that ran in
        // the same wall-clock second as the appends would re-stamp entries
        // without the assertion below noticing. Cross a second boundary so a
        // re-stamped entry is provably distinguishable from a preserved one.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        wal.cleanup().await.unwrap();

        let survivors = wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(
            survivors.len(),
            ids.len() - ids.iter().step_by(2).count(),
            "compaction must keep every unprocessed entry"
        );
        for entry in &survivors {
            assert_eq!(
                wal.read_entry_data(entry).await.unwrap(),
                payloads[&entry.id],
                "payload for {} changed across compaction",
                entry.id
            );
            assert_eq!(
                entry.timestamp, timestamps[&entry.id],
                "compaction reset the timestamp of {}",
                entry.id
            );
        }
    }

    #[tokio::test]
    async fn read_entry_data_resolves_offsets_against_the_live_segment() {
        // Compaction moves surviving entries to new offsets. A consumer that
        // listed entries before the compaction still holds the old offsets —
        // reading those verbatim is the offset-desync class that corrupted
        // hive's WAL (#865/#883), so the read must resolve the entry's
        // location from the segment itself, not from the caller's copy.
        let temp_dir = TempDir::new().unwrap();
        let wal = Wal::new(rotating_config(temp_dir.path())).await.unwrap();

        let ids = append_rotating(&wal, 12).await;
        let stale: Vec<WalEntry> = wal.get_entries().await.unwrap();
        let mut payloads: std::collections::HashMap<Uuid, Vec<u8>> = Default::default();
        for entry in &stale {
            payloads.insert(entry.id, wal.read_entry_data(entry).await.unwrap());
        }

        for id in ids.iter().step_by(2) {
            wal.mark_processed(*id).await.unwrap();
        }
        wal.cleanup().await.unwrap();

        let survivors: std::collections::HashSet<Uuid> = wal
            .get_unprocessed_entries()
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect();

        // Read using the PRE-compaction entry copies.
        for entry in stale.iter().filter(|e| survivors.contains(&e.id)) {
            assert_eq!(
                wal.read_entry_data(entry).await.unwrap(),
                payloads[&entry.id],
                "stale entry copy for {} read the wrong bytes after compaction",
                entry.id
            );
        }
    }
}
