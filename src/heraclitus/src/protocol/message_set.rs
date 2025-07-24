use crate::error::{HeraclitusError, Result};
use bytes::Buf;
use std::io::Cursor;

/// Kafka MessageSet format (magic byte 0 and 1)
/// This is the legacy format used before RecordBatch v2
#[derive(Debug)]
pub struct MessageSet {
    pub messages: Vec<Message>,
}

#[derive(Debug)]
pub struct Message {
    pub offset: i64,
    pub size: i32,
    #[allow(dead_code)]
    pub crc: u32,
    #[allow(dead_code)]
    pub magic: i8,
    pub attributes: i8,
    pub timestamp: Option<i64>, // Only present if magic >= 1
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

impl MessageSet {
    /// Parse a MessageSet from bytes
    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(data);
        let mut messages = Vec::new();

        while cursor.remaining() >= 12 {
            // MessageSet entry: [Offset MessageSize Message]
            let offset = cursor.get_i64();
            let size = cursor.get_i32();

            if size < 0 {
                // Null message, skip
                continue;
            }

            if cursor.remaining() < size as usize {
                // Not enough data for this message
                break;
            }

            // Parse the message
            let message_data =
                &data[cursor.position() as usize..cursor.position() as usize + size as usize];
            let message = Self::parse_message(offset, size, message_data)?;
            cursor.advance(size as usize);

            messages.push(message);
        }

        Ok(MessageSet { messages })
    }

    /// Parse a single message
    fn parse_message(offset: i64, size: i32, data: &[u8]) -> Result<Message> {
        let mut cursor = Cursor::new(data);

        // Minimum message size check
        if data.len() < 5 {
            return Err(HeraclitusError::Protocol(
                "Message too small for v0/v1 format".to_string(),
            ));
        }

        let crc = cursor.get_u32();
        let magic = cursor.get_i8();

        match magic {
            0 => Self::parse_v0_message(offset, size, crc, magic, &mut cursor),
            1 => Self::parse_v1_message(offset, size, crc, magic, &mut cursor),
            _ => Err(HeraclitusError::Protocol(format!(
                "Unsupported message magic byte: {magic}"
            ))),
        }
    }

    /// Parse v0 message format
    fn parse_v0_message(
        offset: i64,
        size: i32,
        crc: u32,
        magic: i8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<Message> {
        // v0: 1 byte magic, 1 byte attributes, then key/value
        let attributes = cursor.get_i8();

        // Read key
        let key_size = cursor.get_i32();
        let key = if key_size < 0 {
            None
        } else {
            let mut key_data = vec![0u8; key_size as usize];
            cursor.copy_to_slice(&mut key_data);
            Some(key_data)
        };

        // Read value
        let value_size = cursor.get_i32();
        if value_size < 0 {
            return Err(HeraclitusError::Protocol(
                "Null value not supported in v0 messages".to_string(),
            ));
        }
        let mut value = vec![0u8; value_size as usize];
        cursor.copy_to_slice(&mut value);

        Ok(Message {
            offset,
            size,
            crc,
            magic,
            attributes,
            timestamp: None,
            key,
            value,
        })
    }

    /// Parse v1 message format
    fn parse_v1_message(
        offset: i64,
        size: i32,
        crc: u32,
        magic: i8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<Message> {
        // v1: 1 byte magic, 1 byte attributes, 8 byte timestamp, then key/value
        let attributes = cursor.get_i8();
        let timestamp = cursor.get_i64();

        // Read key
        let key_size = cursor.get_i32();
        let key = if key_size < 0 {
            None
        } else {
            let mut key_data = vec![0u8; key_size as usize];
            cursor.copy_to_slice(&mut key_data);
            Some(key_data)
        };

        // Read value
        let value_size = cursor.get_i32();
        if value_size < 0 {
            return Err(HeraclitusError::Protocol(
                "Null value not supported in v1 messages".to_string(),
            ));
        }
        let mut value = vec![0u8; value_size as usize];
        cursor.copy_to_slice(&mut value);

        Ok(Message {
            offset,
            size,
            crc,
            magic,
            attributes,
            timestamp: Some(timestamp),
            key,
            value,
        })
    }

    /// Check if data might be a MessageSet (magic byte 0 or 1)
    #[allow(dead_code)]
    pub fn is_message_set(data: &[u8]) -> bool {
        if data.len() < 17 {
            // Too small to be a valid MessageSet entry
            return false;
        }

        // Check if the magic byte at position 16 (after offset + size + crc) is 0 or 1
        data[16] == 0 || data[16] == 1
    }

    /// Convert MessageSet to RecordBatch format for internal processing
    pub fn to_record_batch(&self) -> Result<crate::protocol::record_batch::RecordBatch> {
        use crate::protocol::record_batch::{Record, RecordBatch};

        if self.messages.is_empty() {
            return Err(HeraclitusError::Protocol(
                "Cannot convert empty MessageSet to RecordBatch".to_string(),
            ));
        }

        // Use first message for base values
        let first_msg = &self.messages[0];
        let base_offset = first_msg.offset;
        let base_timestamp = first_msg.timestamp.unwrap_or_else(|| {
            // Generate timestamp if not present
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        });

        // Convert messages to records
        let mut records = Vec::new();
        let mut max_timestamp = base_timestamp;
        let mut last_offset_delta = 0;

        for msg in &self.messages {
            let offset_delta = (msg.offset - base_offset) as i32;
            last_offset_delta = offset_delta;

            let timestamp = msg.timestamp.unwrap_or(base_timestamp);
            let timestamp_delta = timestamp - base_timestamp;
            max_timestamp = max_timestamp.max(timestamp);

            records.push(Record {
                length: msg.size,
                attributes: 0, // RecordBatch v2 attributes are different
                timestamp_delta,
                offset_delta,
                key: msg.key.clone(),
                value: msg.value.clone(),
                headers: Vec::new(), // v0/v1 don't have headers
            });
        }

        // Calculate batch length (approximate - actual encoding may differ)
        let batch_length = 49
            + records
                .iter()
                .map(|r| {
                    5 + // varint overhead estimate
            r.key.as_ref().map(|k| k.len()).unwrap_or(0) +
            r.value.len()
                })
                .sum::<usize>();

        Ok(RecordBatch {
            base_offset,
            batch_length: batch_length as i32,
            partition_leader_epoch: -1, // Not available in old format
            magic: 2,                   // Convert to v2
            crc: 0,                     // Will be recalculated if needed
            attributes: first_msg.attributes as i16,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id: -1, // Not available in old format
            producer_epoch: -1,
            base_sequence: -1,
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_message_set() {
        // Valid v0 MessageSet data pattern
        let mut data = vec![0u8; 17];
        data[16] = 0; // magic byte 0
        assert!(MessageSet::is_message_set(&data));

        data[16] = 1; // magic byte 1
        assert!(MessageSet::is_message_set(&data));

        data[16] = 2; // magic byte 2 (RecordBatch)
        assert!(!MessageSet::is_message_set(&data));

        // Too small
        let small_data = vec![0u8; 16];
        assert!(!MessageSet::is_message_set(&small_data));
    }

    #[test]
    fn test_parse_v0_message() {
        // Construct a minimal v0 message
        let mut data = Vec::new();

        // Offset
        data.extend_from_slice(&0i64.to_be_bytes());
        // Size (calculated below)
        let size_pos = data.len();
        data.extend_from_slice(&0i32.to_be_bytes());

        let msg_start = data.len();

        // CRC (dummy)
        data.extend_from_slice(&0u32.to_be_bytes());
        // Magic byte 0
        data.push(0);
        // Attributes
        data.push(0);
        // Key size (-1 for null)
        data.extend_from_slice(&(-1i32).to_be_bytes());
        // Value size
        data.extend_from_slice(&5i32.to_be_bytes());
        // Value
        data.extend_from_slice(b"hello");

        // Update size
        let msg_size = (data.len() - msg_start) as i32;
        data[size_pos..size_pos + 4].copy_from_slice(&msg_size.to_be_bytes());

        let message_set = MessageSet::parse(&data).unwrap();
        assert_eq!(message_set.messages.len(), 1);

        let msg = &message_set.messages[0];
        assert_eq!(msg.magic, 0);
        assert_eq!(msg.value, b"hello");
        assert!(msg.key.is_none());
        assert!(msg.timestamp.is_none());
    }
}
