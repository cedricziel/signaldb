use crate::error::{HeraclitusError, Result};
use bytes::Buf;
use std::collections::HashMap;
use std::io::Cursor;

/// Kafka RecordBatch v2 format (magic byte = 2)
/// This is the format used in Kafka 0.11.0+
#[derive(Debug)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub length: i32,
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub key: String,
    pub value: Vec<u8>,
}

/// Compression types supported by Kafka
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionType {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl RecordBatch {
    /// Parse a RecordBatch from bytes
    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(data);

        // Check if we have at least the header
        if data.len() < 61 {
            // Minimum size for batch header
            return Err(HeraclitusError::Protocol(
                "Insufficient data for RecordBatch header".to_string(),
            ));
        }

        let base_offset = cursor.get_i64();
        let batch_length = cursor.get_i32();
        let partition_leader_epoch = cursor.get_i32();
        let magic = cursor.get_i8();

        // Verify magic byte
        if magic != 2 {
            return Err(HeraclitusError::Protocol(format!(
                "Unsupported magic byte: {magic}. Only v2 (magic=2) is supported"
            )));
        }

        let crc = cursor.get_u32();
        let attributes = cursor.get_i16();
        let last_offset_delta = cursor.get_i32();
        let base_timestamp = cursor.get_i64();
        let max_timestamp = cursor.get_i64();
        let producer_id = cursor.get_i64();
        let producer_epoch = cursor.get_i16();
        let base_sequence = cursor.get_i32();
        let record_count = cursor.get_i32();

        // Parse compression type from attributes
        let compression_type = Self::extract_compression_type(attributes);

        // Read records
        let records_data = if compression_type != CompressionType::None {
            // If compressed, decompress the remaining data
            let compressed_data = &data[cursor.position() as usize..];
            Self::decompress(compressed_data, compression_type)?
        } else {
            // Uncompressed - just use remaining data
            data[cursor.position() as usize..].to_vec()
        };

        let records = Self::parse_records(&records_data, record_count)?;

        // Verify CRC (optional for now)
        // TODO: Implement CRC32C verification

        Ok(RecordBatch {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }

    /// Extract compression type from attributes
    fn extract_compression_type(attributes: i16) -> CompressionType {
        match attributes & 0x07 {
            0 => CompressionType::None,
            1 => CompressionType::Gzip,
            2 => CompressionType::Snappy,
            3 => CompressionType::Lz4,
            4 => CompressionType::Zstd,
            _ => CompressionType::None, // Default to none for unknown
        }
    }

    /// Decompress data based on compression type
    fn decompress(data: &[u8], compression_type: CompressionType) -> Result<Vec<u8>> {
        match compression_type {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Gzip => {
                use flate2::read::GzDecoder;
                use std::io::Read;

                let mut decoder = GzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).map_err(|e| {
                    HeraclitusError::Protocol(format!("Gzip decompression failed: {e}"))
                })?;
                Ok(decompressed)
            }
            _ => Err(HeraclitusError::Protocol(format!(
                "Compression type {compression_type:?} not yet supported"
            ))),
        }
    }

    /// Parse individual records from the batch
    fn parse_records(data: &[u8], expected_count: i32) -> Result<Vec<Record>> {
        let mut cursor = Cursor::new(data);
        let mut records = Vec::with_capacity(expected_count as usize);

        for _ in 0..expected_count {
            if cursor.remaining() == 0 {
                break;
            }

            let record = Self::parse_record(&mut cursor)?;
            records.push(record);
        }

        Ok(records)
    }

    /// Parse a single record
    fn parse_record(cursor: &mut Cursor<&[u8]>) -> Result<Record> {
        // Read variable-length integer for record length
        let length = Self::read_varint(cursor)?;
        let attributes = cursor.get_i8();
        let timestamp_delta = Self::read_varlong(cursor)?;
        let offset_delta = Self::read_varint(cursor)?;

        // Read key
        let key_length = Self::read_varint(cursor)?;
        let key = if key_length < 0 {
            None
        } else {
            let mut key_data = vec![0u8; key_length as usize];
            cursor.copy_to_slice(&mut key_data);
            Some(key_data)
        };

        // Read value
        let value_length = Self::read_varint(cursor)?;
        if value_length < 0 {
            return Err(HeraclitusError::Protocol(
                "Null value not supported in records".to_string(),
            ));
        }
        let mut value = vec![0u8; value_length as usize];
        cursor.copy_to_slice(&mut value);

        // Read headers
        let header_count = Self::read_varint(cursor)?;
        let mut headers = Vec::with_capacity(header_count as usize);

        for _ in 0..header_count {
            let header = Self::parse_header(cursor)?;
            headers.push(header);
        }

        Ok(Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }

    /// Parse a record header
    fn parse_header(cursor: &mut Cursor<&[u8]>) -> Result<RecordHeader> {
        let key_length = Self::read_varint(cursor)?;
        if key_length < 0 {
            return Err(HeraclitusError::Protocol(
                "Null header key not allowed".to_string(),
            ));
        }

        let mut key_data = vec![0u8; key_length as usize];
        cursor.copy_to_slice(&mut key_data);
        let key = String::from_utf8(key_data)
            .map_err(|e| HeraclitusError::Protocol(format!("Invalid header key: {e}")))?;

        let value_length = Self::read_varint(cursor)?;
        let value = if value_length < 0 {
            Vec::new()
        } else {
            let mut value_data = vec![0u8; value_length as usize];
            cursor.copy_to_slice(&mut value_data);
            value_data
        };

        Ok(RecordHeader { key, value })
    }

    /// Read a variable-length integer (varint) from the cursor
    fn read_varint(cursor: &mut Cursor<&[u8]>) -> Result<i32> {
        let mut value = 0i32;
        let mut shift = 0;

        loop {
            if !cursor.has_remaining() {
                return Err(HeraclitusError::Protocol(
                    "Unexpected end of data reading varint".to_string(),
                ));
            }

            let byte = cursor.get_u8();
            value |= ((byte & 0x7F) as i32) << shift;

            if byte & 0x80 == 0 {
                // If MSB is 0, this is the last byte
                break;
            }

            shift += 7;
            if shift > 28 {
                return Err(HeraclitusError::Protocol("Varint is too large".to_string()));
            }
        }

        // Zigzag decode
        Ok((value >> 1) ^ -(value & 1))
    }

    /// Read a variable-length long (varlong) from the cursor
    fn read_varlong(cursor: &mut Cursor<&[u8]>) -> Result<i64> {
        let mut value = 0i64;
        let mut shift = 0;

        loop {
            if !cursor.has_remaining() {
                return Err(HeraclitusError::Protocol(
                    "Unexpected end of data reading varlong".to_string(),
                ));
            }

            let byte = cursor.get_u8();
            value |= ((byte & 0x7F) as i64) << shift;

            if byte & 0x80 == 0 {
                // If MSB is 0, this is the last byte
                break;
            }

            shift += 7;
            if shift > 63 {
                return Err(HeraclitusError::Protocol(
                    "Varlong is too large".to_string(),
                ));
            }
        }

        // Zigzag decode
        Ok((value >> 1) ^ -(value & 1))
    }

    /// Get the actual timestamp for a record
    pub fn record_timestamp(&self, record: &Record) -> i64 {
        self.base_timestamp + record.timestamp_delta
    }

    /// Get the actual offset for a record
    pub fn record_offset(&self, record: &Record) -> i64 {
        self.base_offset + record.offset_delta as i64
    }

    /// Convert headers to HashMap for easier use
    pub fn headers_map(record: &Record) -> HashMap<String, Vec<u8>> {
        record
            .headers
            .iter()
            .map(|h| (h.key.clone(), h.value.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        // Test positive number
        let data = [0x08]; // 4 in zigzag encoding
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(RecordBatch::read_varint(&mut cursor).unwrap(), 4);

        // Test negative number
        let data = [0x07]; // -4 in zigzag encoding
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(RecordBatch::read_varint(&mut cursor).unwrap(), -4);

        // Test multi-byte varint
        let data = [0xAC, 0x02]; // 150 in zigzag encoding
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(RecordBatch::read_varint(&mut cursor).unwrap(), 150);
    }

    #[test]
    fn test_compression_type_extraction() {
        assert_eq!(
            RecordBatch::extract_compression_type(0),
            CompressionType::None
        );
        assert_eq!(
            RecordBatch::extract_compression_type(1),
            CompressionType::Gzip
        );
        assert_eq!(
            RecordBatch::extract_compression_type(2),
            CompressionType::Snappy
        );
        assert_eq!(
            RecordBatch::extract_compression_type(3),
            CompressionType::Lz4
        );
        assert_eq!(
            RecordBatch::extract_compression_type(4),
            CompressionType::Zstd
        );

        // Test with other attribute bits set
        assert_eq!(
            RecordBatch::extract_compression_type(0x18), // 0b00011000, compression = 0
            CompressionType::None
        );
        assert_eq!(
            RecordBatch::extract_compression_type(0x19), // 0b00011001, compression = 1
            CompressionType::Gzip
        );
    }

    #[test]
    fn test_zigzag_decode() {
        // Test varint zigzag encoding/decoding
        let data = [0x00]; // 0
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(RecordBatch::read_varint(&mut cursor).unwrap(), 0);

        let data = [0x01]; // -1
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(RecordBatch::read_varint(&mut cursor).unwrap(), -1);

        let data = [0x02]; // 1
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(RecordBatch::read_varint(&mut cursor).unwrap(), 1);

        let data = [0x03]; // -2
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(RecordBatch::read_varint(&mut cursor).unwrap(), -2);
    }
}
