//! # WAL record framing (format v1)
//!
//! Every byte range the WAL writes is self-describing and checksummed, so a
//! torn or bit-flipped record is *attributable* (the read names the entry,
//! tenant, dataset, signal, and offset) and *skippable* (the reader steps over
//! it and keeps serving the neighbours) instead of surfacing as an opaque
//! Arrow parse error that poisons the whole segment (issues #865/#883/#946).
//!
//! ## Layout
//!
//! `.log` file (entry records):
//!
//! ```text
//! [segment header: magic "SDBW" | u32 version]        8 bytes, once per file
//! [u32 payload_len | u32 crc32(payload) | payload]     per record
//! ```
//!
//! `.data` file (entry payloads, addressed by `WalEntry::data_offset`):
//!
//! ```text
//! [magic "SDBR" | u32 payload_len | u32 crc32(payload) | payload]   per record
//! ```
//!
//! All integers are little-endian. `WalEntry::data_offset` points at the
//! *record header*, and `WalEntry::data_size` is the payload length (header
//! excluded), so a reader cross-checks the header's length against the entry
//! it is resolving before trusting the checksum.
//!
//! ## Legacy segments
//!
//! Segments written before this format have no segment header and no record
//! headers (`.log` = `[u64 len][bincode]`, `.data` = raw payloads). They are
//! detected by the missing magic, read on their legacy layout, and never
//! appended to: the WAL rotates onto a fresh v1 segment on open, and
//! compaction rewrites survivors into v1.

use anyhow::{Result, bail};

/// Magic bytes opening every v1 `.log` file.
pub const SEGMENT_MAGIC: [u8; 4] = *b"SDBW";
/// Format version recorded in the segment header.
pub const SEGMENT_FORMAT_VERSION: u32 = 1;
/// Length of the `.log` segment header.
pub const SEGMENT_HEADER_LEN: usize = 8;

/// Length of a `.log` record header (`u32 len | u32 crc`).
pub const LOG_RECORD_HEADER_LEN: usize = 8;

/// Magic bytes opening every v1 `.data` record.
pub const DATA_RECORD_MAGIC: [u8; 4] = *b"SDBR";
/// Length of a `.data` record header (`magic | u32 len | u32 crc`).
pub const DATA_RECORD_HEADER_LEN: usize = 12;

/// CRC-32 (IEEE) of `payload`, as stored in record headers.
pub fn checksum(payload: &[u8]) -> u32 {
    crc32fast::hash(payload)
}

/// The 8-byte header opening a v1 `.log` file.
pub fn segment_header() -> [u8; SEGMENT_HEADER_LEN] {
    let mut header = [0u8; SEGMENT_HEADER_LEN];
    header[..4].copy_from_slice(&SEGMENT_MAGIC);
    header[4..].copy_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
    header
}

/// On-disk layout of a segment, decided from the first bytes of its `.log`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentFormat {
    /// Pre-framing layout: no headers, no checksums.
    Legacy,
    /// Framed and checksummed (this module).
    V1,
}

/// Classify a `.log` file by its leading bytes.
///
/// An empty file counts as v1: `WalSegment::new` creates the file and writes
/// the header in two steps, so a crash between them leaves an empty file that
/// simply gets its header on the next open. A file that starts with the magic
/// but names an unknown version is an error — reading it on either layout
/// would silently misinterpret every record.
pub fn detect_segment_format(log_bytes: &[u8]) -> Result<SegmentFormat> {
    if log_bytes.is_empty() {
        return Ok(SegmentFormat::V1);
    }
    if log_bytes.len() >= SEGMENT_HEADER_LEN && log_bytes[..4] == SEGMENT_MAGIC {
        let version = u32::from_le_bytes([log_bytes[4], log_bytes[5], log_bytes[6], log_bytes[7]]);
        if version != SEGMENT_FORMAT_VERSION {
            bail!(
                "unsupported WAL segment format version {version} (this build reads version \
                 {SEGMENT_FORMAT_VERSION})"
            );
        }
        return Ok(SegmentFormat::V1);
    }
    Ok(SegmentFormat::Legacy)
}

/// Encode a `.log` record header for `payload`.
pub fn log_record_header(payload: &[u8]) -> Result<[u8; LOG_RECORD_HEADER_LEN]> {
    let len = u32::try_from(payload.len())
        .map_err(|_| anyhow::anyhow!("WAL log record too large: {} bytes", payload.len()))?;
    let mut header = [0u8; LOG_RECORD_HEADER_LEN];
    header[..4].copy_from_slice(&len.to_le_bytes());
    header[4..].copy_from_slice(&checksum(payload).to_le_bytes());
    Ok(header)
}

/// Decode a `.log` record header into `(payload_len, crc)`.
pub fn parse_log_record_header(header: &[u8]) -> Option<(usize, u32)> {
    if header.len() < LOG_RECORD_HEADER_LEN {
        return None;
    }
    let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    Some((len, crc))
}

/// Encode a `.data` record header for `payload`.
pub fn data_record_header(payload: &[u8]) -> Result<[u8; DATA_RECORD_HEADER_LEN]> {
    let len = u32::try_from(payload.len())
        .map_err(|_| anyhow::anyhow!("WAL data record too large: {} bytes", payload.len()))?;
    let mut header = [0u8; DATA_RECORD_HEADER_LEN];
    header[..4].copy_from_slice(&DATA_RECORD_MAGIC);
    header[4..8].copy_from_slice(&len.to_le_bytes());
    header[8..].copy_from_slice(&checksum(payload).to_le_bytes());
    Ok(header)
}

/// Why a framed `.data` record failed validation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DataRecordError {
    #[error("record header magic mismatch (found {found:02x?}, expected {expected:02x?})")]
    BadMagic { found: [u8; 4], expected: [u8; 4] },
    #[error("record header length {header_len} does not match entry data_size {entry_len}")]
    LengthMismatch { header_len: u64, entry_len: u64 },
    #[error("payload CRC mismatch (stored {stored:#010x}, computed {computed:#010x})")]
    ChecksumMismatch { stored: u32, computed: u32 },
}

/// Validate a framed `.data` record (`header ++ payload`) against the payload
/// length the entry claims, returning the payload slice on success.
pub fn validate_data_record(record: &[u8], expected_len: u64) -> Result<&[u8], DataRecordError> {
    let mut found = [0u8; 4];
    let head = &record[..record.len().min(4)];
    found[..head.len()].copy_from_slice(head);
    if record.len() < DATA_RECORD_HEADER_LEN || found != DATA_RECORD_MAGIC {
        return Err(DataRecordError::BadMagic {
            found,
            expected: DATA_RECORD_MAGIC,
        });
    }
    let (header, payload) = record.split_at(DATA_RECORD_HEADER_LEN);
    let header_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as u64;
    if header_len != expected_len || payload.len() as u64 != expected_len {
        return Err(DataRecordError::LengthMismatch {
            header_len,
            entry_len: expected_len,
        });
    }
    let stored = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let computed = checksum(payload);
    if stored != computed {
        return Err(DataRecordError::ChecksumMismatch { stored, computed });
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_header_round_trips_and_detects_legacy() {
        assert_eq!(
            detect_segment_format(&segment_header()).unwrap(),
            SegmentFormat::V1
        );
        assert_eq!(detect_segment_format(&[]).unwrap(), SegmentFormat::V1);
        // A legacy `.log` starts with a u64 LE length prefix of a small
        // bincode record: low bytes non-zero, high bytes zero — never the magic.
        let legacy = 90u64.to_le_bytes();
        assert_eq!(
            detect_segment_format(&legacy).unwrap(),
            SegmentFormat::Legacy
        );
    }

    #[test]
    fn unknown_segment_version_is_refused() {
        let mut header = segment_header();
        header[4..].copy_from_slice(&99u32.to_le_bytes());
        let err = detect_segment_format(&header).unwrap_err();
        assert!(err.to_string().contains("version 99"), "{err}");
    }

    #[test]
    fn data_record_validates_and_rejects_each_corruption() {
        let payload = b"hello wal".to_vec();
        let mut record = data_record_header(&payload).unwrap().to_vec();
        record.extend_from_slice(&payload);

        assert_eq!(
            validate_data_record(&record, payload.len() as u64).unwrap(),
            payload.as_slice()
        );

        let mut bad_magic = record.clone();
        bad_magic[0] ^= 0xFF;
        assert!(matches!(
            validate_data_record(&bad_magic, payload.len() as u64),
            Err(DataRecordError::BadMagic { .. })
        ));

        assert!(matches!(
            validate_data_record(&record, payload.len() as u64 + 1),
            Err(DataRecordError::LengthMismatch { .. })
        ));

        let mut flipped = record.clone();
        flipped[DATA_RECORD_HEADER_LEN + 3] ^= 0x01;
        assert!(matches!(
            validate_data_record(&flipped, payload.len() as u64),
            Err(DataRecordError::ChecksumMismatch { .. })
        ));

        // Shorter than a header: cannot even be a record.
        assert!(matches!(
            validate_data_record(&record[..5], payload.len() as u64),
            Err(DataRecordError::BadMagic { .. })
        ));
    }

    #[test]
    fn log_record_header_round_trips() {
        let payload = b"entry".to_vec();
        let header = log_record_header(&payload).unwrap();
        let (len, crc) = parse_log_record_header(&header).unwrap();
        assert_eq!(len, payload.len());
        assert_eq!(crc, checksum(&payload));
        assert!(parse_log_record_header(&header[..3]).is_none());
    }
}
