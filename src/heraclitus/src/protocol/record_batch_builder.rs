use crate::error::{HeraclitusError, Result};
use crate::storage::KafkaMessage;
use bytes::{BufMut, BytesMut};

/// Builder for creating Kafka RecordBatch v2 format
pub struct RecordBatchBuilder {
    messages: Vec<KafkaMessage>,
    compression_type: CompressionType,
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

impl RecordBatchBuilder {
    /// Create a new RecordBatchBuilder
    pub fn new(compression_type: CompressionType) -> Self {
        Self {
            messages: Vec::new(),
            compression_type,
        }
    }

    /// Build an empty RecordBatch v2 format with proper headers
    /// This is needed for Fetch responses when there are no messages
    pub fn build_empty_batch(base_offset: i64, partition_leader_epoch: i32) -> Vec<u8> {
        let mut batch = BytesMut::new();

        // Base offset
        batch.put_i64(base_offset);

        // Batch length (61 bytes for minimal batch with no records)
        // 4 (partition_leader_epoch) + 1 (magic) + 4 (crc) + 2 (attributes) +
        // 4 (last_offset_delta) + 8 (base_timestamp) + 8 (max_timestamp) +
        // 8 (producer_id) + 2 (producer_epoch) + 4 (base_sequence) +
        // 4 (record_count) = 49 bytes
        batch.put_i32(49);

        // Partition leader epoch
        batch.put_i32(partition_leader_epoch);

        // Magic byte (always 2 for v2)
        batch.put_i8(2);

        // CRC placeholder (will calculate below)
        let crc_pos = batch.len();
        batch.put_u32(0);

        // Attributes (0 = no compression, CreateTime)
        batch.put_i16(0);

        // Last offset delta (0 for empty batch)
        batch.put_i32(0);

        // Base timestamp (current time)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        batch.put_i64(timestamp);

        // Max timestamp (same as base for empty batch)
        batch.put_i64(timestamp);

        // Producer ID (-1 for non-transactional)
        batch.put_i64(-1);

        // Producer epoch (-1 for non-transactional)
        batch.put_i16(-1);

        // Base sequence (-1 for non-transactional)
        batch.put_i32(-1);

        // Record count (0 for empty batch)
        batch.put_i32(0);

        // No records data

        // Calculate and update CRC32C (from attributes to end)
        let crc_data = &batch[crc_pos + 4..]; // Skip the CRC field itself
        let crc = crc32c::crc32c(crc_data);
        batch[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

        batch.to_vec()
    }

    /// Add a message to the batch
    pub fn add_message(&mut self, message: KafkaMessage) {
        self.messages.push(message);
    }

    /// Add multiple messages to the batch
    pub fn add_messages(&mut self, messages: Vec<KafkaMessage>) {
        self.messages.extend(messages);
    }

    /// Build the RecordBatch v2 format
    pub fn build(mut self) -> Result<Vec<u8>> {
        if self.messages.is_empty() {
            return Ok(Vec::new());
        }

        // Sort messages by offset to ensure correct offset deltas
        self.messages.sort_by_key(|m| m.offset);

        let base_offset = self.messages[0].offset;
        let last_offset_delta = (self.messages.last().unwrap().offset - base_offset) as i32;

        // Find min and max timestamps
        let base_timestamp = self.messages.iter().map(|m| m.timestamp).min().unwrap_or(0);
        let max_timestamp = self.messages.iter().map(|m| m.timestamp).max().unwrap_or(0);

        // Get producer info from first message (if any)
        let producer_id = self.messages[0].producer_id.unwrap_or(-1);
        let producer_epoch = self.messages[0].producer_epoch.unwrap_or(-1);
        let base_sequence = self.messages[0].sequence.unwrap_or(-1);

        // Build records data
        let records_data = self.build_records(&self.messages, base_offset, base_timestamp)?;

        // Compress if needed
        let compressed_records = self.compress_records(records_data)?;

        // Build the batch header
        let mut batch = BytesMut::new();

        // Base offset
        batch.put_i64(base_offset);

        // We'll write batch length later
        let batch_length_pos = batch.len();
        batch.put_i32(0); // Placeholder

        // Partition leader epoch (we'll use -1 for now)
        batch.put_i32(-1);

        // Magic byte (always 2 for v2)
        batch.put_i8(2);

        // We'll calculate CRC later
        let crc_pos = batch.len();
        batch.put_u32(0); // Placeholder

        // Attributes
        let attributes = self.build_attributes();
        batch.put_i16(attributes);

        // Last offset delta
        batch.put_i32(last_offset_delta);

        // Base timestamp
        batch.put_i64(base_timestamp);

        // Max timestamp
        batch.put_i64(max_timestamp);

        // Producer ID
        batch.put_i64(producer_id);

        // Producer epoch
        batch.put_i16(producer_epoch);

        // Base sequence
        batch.put_i32(base_sequence);

        // Record count
        batch.put_i32(self.messages.len() as i32);

        // Add compressed records
        batch.extend_from_slice(&compressed_records);

        // Update batch length (excluding offset and length fields)
        let batch_length = (batch.len() - 12) as i32; // 8 bytes offset + 4 bytes length
        batch[batch_length_pos..batch_length_pos + 4].copy_from_slice(&batch_length.to_be_bytes());

        // Calculate and update CRC32C (from attributes to end)
        // CRC is calculated from the byte after the CRC field to the end of the batch
        let crc_data = &batch[crc_pos + 4..]; // Skip everything up to and including CRC field
        let crc = crc32c::crc32c(crc_data);
        batch[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

        Ok(batch.to_vec())
    }

    /// Build the records data in v2 format
    fn build_records(
        &self,
        messages: &[KafkaMessage],
        base_offset: i64,
        base_timestamp: i64,
    ) -> Result<Vec<u8>> {
        let mut records = BytesMut::new();

        for message in messages.iter() {
            let offset_delta = (message.offset - base_offset) as i32;
            let timestamp_delta = message.timestamp - base_timestamp;

            // Build the record
            let record_data = self.build_record(message, offset_delta, timestamp_delta)?;

            // Write varint length
            Self::write_varint(&mut records, record_data.len() as i32);

            // Write record data
            records.extend_from_slice(&record_data);
        }

        Ok(records.to_vec())
    }

    /// Build a single record
    fn build_record(
        &self,
        message: &KafkaMessage,
        offset_delta: i32,
        timestamp_delta: i64,
    ) -> Result<Vec<u8>> {
        let mut record = BytesMut::new();

        // Attributes (no compression at record level in v2)
        record.put_i8(0);

        // Timestamp delta (varlong)
        Self::write_varlong(&mut record, timestamp_delta);

        // Offset delta (varint)
        Self::write_varint(&mut record, offset_delta);

        // Key
        if let Some(key) = &message.key {
            Self::write_varint(&mut record, key.len() as i32);
            record.extend_from_slice(key);
        } else {
            Self::write_varint(&mut record, -1);
        }

        // Value
        Self::write_varint(&mut record, message.value.len() as i32);
        record.extend_from_slice(&message.value);

        // Headers
        Self::write_varint(&mut record, message.headers.len() as i32);
        for (key, value) in &message.headers {
            // Header key
            Self::write_varint(&mut record, key.len() as i32);
            record.extend_from_slice(key.as_bytes());

            // Header value
            Self::write_varint(&mut record, value.len() as i32);
            record.extend_from_slice(value);
        }

        Ok(record.to_vec())
    }

    /// Write a variable-length integer (varint)
    fn write_varint(buf: &mut BytesMut, value: i32) {
        // Zigzag encode
        let encoded = ((value << 1) ^ (value >> 31)) as u32;
        Self::write_unsigned_varint(buf, encoded);
    }

    /// Write a variable-length long (varlong)
    fn write_varlong(buf: &mut BytesMut, value: i64) {
        // Zigzag encode
        let encoded = ((value << 1) ^ (value >> 63)) as u64;
        Self::write_unsigned_varlong(buf, encoded);
    }

    /// Write an unsigned variable-length integer
    fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
        while value > 0x7F {
            buf.put_u8((value as u8) | 0x80);
            value >>= 7;
        }
        buf.put_u8(value as u8);
    }

    /// Write an unsigned variable-length long
    fn write_unsigned_varlong(buf: &mut BytesMut, mut value: u64) {
        while value > 0x7F {
            buf.put_u8((value as u8) | 0x80);
            value >>= 7;
        }
        buf.put_u8(value as u8);
    }

    /// Build attributes field
    fn build_attributes(&self) -> i16 {
        let mut attributes = 0i16;

        // Compression type (bits 0-2)
        attributes |= (self.compression_type as i16) & 0x07;

        // Timestamp type (bit 3) - 0 for CreateTime, 1 for LogAppendTime
        // We use CreateTime
        attributes |= 0 << 3;

        // Transactional (bit 4) - 0 for non-transactional
        attributes |= 0 << 4;

        // Control batch (bit 5) - 0 for normal batch
        attributes |= 0 << 5;

        attributes
    }

    /// Compress records if needed
    fn compress_records(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        match self.compression_type {
            CompressionType::None => Ok(data),
            CompressionType::Gzip => {
                use flate2::Compression;
                use flate2::write::GzEncoder;
                use std::io::Write;

                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(&data).map_err(|e| {
                    HeraclitusError::Protocol(format!("Gzip compression failed: {e}"))
                })?;
                encoder
                    .finish()
                    .map_err(|e| HeraclitusError::Protocol(format!("Gzip compression failed: {e}")))
            }
            CompressionType::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                encoder.compress_vec(&data).map_err(|e| {
                    HeraclitusError::Protocol(format!("Snappy compression failed: {e}"))
                })
            }
            CompressionType::Lz4 => {
                // Kafka uses LZ4 block format without the frame header
                lz4::block::compress(&data, None, true)
                    .map_err(|e| HeraclitusError::Protocol(format!("LZ4 compression failed: {e}")))
            }
            CompressionType::Zstd => {
                zstd::encode_all(data.as_slice(), 3) // Use compression level 3 (Kafka default)
                    .map_err(|e| HeraclitusError::Protocol(format!("Zstd compression failed: {e}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_write_varint() {
        let mut buf = BytesMut::new();

        // Test positive number
        RecordBatchBuilder::write_varint(&mut buf, 4);
        assert_eq!(&buf[..], &[0x08]); // zigzag(4) = 8

        buf.clear();

        // Test negative number
        RecordBatchBuilder::write_varint(&mut buf, -4);
        assert_eq!(&buf[..], &[0x07]); // zigzag(-4) = 7

        buf.clear();

        // Test larger number
        RecordBatchBuilder::write_varint(&mut buf, 150);
        assert_eq!(&buf[..], &[0xAC, 0x02]); // zigzag(150) = 300
    }

    #[test]
    fn test_build_empty_batch() {
        let builder = RecordBatchBuilder::new(CompressionType::None);
        let result = builder.build().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_build_single_message() {
        let mut builder = RecordBatchBuilder::new(CompressionType::None);

        let message = KafkaMessage {
            topic: "test".to_string(),
            partition: 0,
            offset: 100,
            timestamp: 1234567890,
            key: Some(b"key".to_vec()),
            value: b"value".to_vec(),
            headers: HashMap::new(),
            producer_id: None,
            producer_epoch: None,
            sequence: None,
        };

        builder.add_message(message);
        let batch = builder.build().unwrap();

        // Verify batch starts with base offset
        assert_eq!(&batch[0..8], &100i64.to_be_bytes());

        // Verify magic byte at correct position
        assert_eq!(batch[16], 2); // Magic byte = 2
    }
}
