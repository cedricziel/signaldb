use crate::error::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

/// Kafka OffsetFetch Request (API Key 9)
#[derive(Debug)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    pub topics: Option<Vec<OffsetFetchTopic>>, // None means fetch all topics
    #[allow(dead_code)]
    pub require_stable: bool, // v7+
}

#[derive(Debug)]
pub struct OffsetFetchTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

/// Kafka OffsetFetch Response
#[derive(Debug)]
pub struct OffsetFetchResponse {
    pub throttle_time_ms: i32, // v3+
    pub topics: Vec<OffsetFetchResponseTopic>,
    pub error_code: i16, // v2+, for group-level errors
}

#[derive(Debug)]
pub struct OffsetFetchResponseTopic {
    pub name: String,
    pub partitions: Vec<OffsetFetchResponsePartition>,
}

#[derive(Debug)]
pub struct OffsetFetchResponsePartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_leader_epoch: i32, // v5+
    pub metadata: Option<String>,
    pub error_code: i16,
}

impl OffsetFetchRequest {
    /// Parse an OffsetFetch request from bytes
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        // group_id: string
        let group_id = crate::protocol::kafka_protocol::read_string(cursor)?;

        // topics: nullable array (null means fetch all)
        let topics = if api_version <= 1 {
            // In v0-v1, topics is not nullable and empty array means all topics
            let topic_count = cursor.get_i32();
            if topic_count == 0 {
                None // Empty array means all topics in v0-v1
            } else {
                let mut topics = Vec::with_capacity(topic_count as usize);
                for _ in 0..topic_count {
                    topics.push(Self::parse_topic(cursor)?);
                }
                Some(topics)
            }
        } else {
            // In v2+, topics can be null (represented as -1 length)
            let topic_count = cursor.get_i32();
            if topic_count == -1 {
                None // Null array means all topics
            } else {
                let mut topics = Vec::with_capacity(topic_count as usize);
                for _ in 0..topic_count {
                    topics.push(Self::parse_topic(cursor)?);
                }
                Some(topics)
            }
        };

        // require_stable: boolean (v7+)
        let require_stable = if api_version >= 7 {
            cursor.get_u8() != 0
        } else {
            false
        };

        // Handle tagged fields for newer versions
        if api_version >= 6 {
            // Read tagged fields (empty for now)
            let _num_tagged_fields = read_unsigned_varint(cursor)?;
        }

        Ok(OffsetFetchRequest {
            group_id,
            topics,
            require_stable,
        })
    }

    fn parse_topic(cursor: &mut Cursor<&[u8]>) -> Result<OffsetFetchTopic> {
        // name: string
        let name = crate::protocol::kafka_protocol::read_string(cursor)?;

        // partition_indexes: [int32]
        let partition_count = cursor.get_i32();
        let mut partition_indexes = Vec::with_capacity(partition_count as usize);

        for _ in 0..partition_count {
            partition_indexes.push(cursor.get_i32());
        }

        Ok(OffsetFetchTopic {
            name,
            partition_indexes,
        })
    }
}

impl OffsetFetchResponse {
    /// Create a new OffsetFetch response
    pub fn new(topics: Vec<OffsetFetchResponseTopic>) -> Self {
        Self {
            throttle_time_ms: 0,
            topics,
            error_code: 0, // Success
        }
    }

    /// Create an error response
    #[allow(dead_code)]
    pub fn error(error_code: i16) -> Self {
        Self {
            throttle_time_ms: 0,
            topics: Vec::new(),
            error_code,
        }
    }

    /// Encode the response to bytes
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // throttle_time_ms: int32 (v3+)
        if api_version >= 3 {
            buf.put_i32(self.throttle_time_ms);
        }

        // topics: [topic]
        buf.put_i32(self.topics.len() as i32);

        for topic in &self.topics {
            // name: string
            crate::protocol::kafka_protocol::write_string(&mut buf, &topic.name);

            // partitions: [partition]
            buf.put_i32(topic.partitions.len() as i32);

            for partition in &topic.partitions {
                // partition_index: int32
                buf.put_i32(partition.partition_index);

                // committed_offset: int64
                buf.put_i64(partition.committed_offset);

                // committed_leader_epoch: int32 (v5+)
                if api_version >= 5 {
                    buf.put_i32(partition.committed_leader_epoch);
                }

                // metadata: nullable string
                crate::protocol::kafka_protocol::write_nullable_string(
                    &mut buf,
                    partition.metadata.as_deref(),
                );

                // error_code: int16
                buf.put_i16(partition.error_code);
            }
        }

        // error_code: int16 (v2+, group-level error)
        if api_version >= 2 {
            buf.put_i16(self.error_code);
        }

        // Handle tagged fields for newer versions
        if api_version >= 6 {
            // Write empty tagged fields
            write_unsigned_varint(&mut buf, 0);
        }

        Ok(buf.to_vec())
    }
}

impl OffsetFetchResponsePartition {
    /// Create a successful partition response
    pub fn success(partition_index: i32, committed_offset: i64, metadata: Option<String>) -> Self {
        Self {
            partition_index,
            committed_offset,
            committed_leader_epoch: -1, // Unknown
            metadata,
            error_code: 0, // Success
        }
    }

    /// Create a partition response with no committed offset
    pub fn no_offset(partition_index: i32) -> Self {
        Self {
            partition_index,
            committed_offset: -1, // No committed offset
            committed_leader_epoch: -1,
            metadata: None,
            error_code: 0, // Success (but no offset available)
        }
    }

    /// Create an error partition response
    #[allow(dead_code)]
    pub fn error(partition_index: i32, error_code: i16) -> Self {
        Self {
            partition_index,
            committed_offset: -1,
            committed_leader_epoch: -1,
            metadata: None,
            error_code,
        }
    }
}

fn read_unsigned_varint(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value = 0u32;
    let mut i = 0;

    loop {
        if i > 4 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Varint is too long".to_string(),
            ));
        }

        let b = cursor.get_u8();
        value |= ((b & 0x7F) as u32) << (i * 7);

        if (b & 0x80) == 0 {
            break;
        }

        i += 1;
    }

    Ok(value)
}

fn write_unsigned_varint(buffer: &mut BytesMut, mut value: u32) {
    while (value & 0xFFFFFF80) != 0 {
        buffer.put_u8(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    buffer.put_u8((value & 0x7F) as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_fetch_request_parse_v0() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // topics: 1 topic
        buf.put_i32(1);

        // topic name: "test-topic"
        buf.put_i16(10);
        buf.put_slice(b"test-topic");

        // partition_indexes: 1 partition
        buf.put_i32(1);
        buf.put_i32(0); // partition 0

        let mut cursor = Cursor::new(&buf[..]);
        let request = OffsetFetchRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert!(request.topics.is_some());
        let topics = request.topics.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "test-topic");
        assert_eq!(topics[0].partition_indexes, vec![0]);
        assert!(!request.require_stable);
    }

    #[test]
    fn test_offset_fetch_request_parse_all_topics() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // topics: empty array (means all topics in v0-v1)
        buf.put_i32(0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = OffsetFetchRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert!(request.topics.is_none()); // None means all topics
    }

    #[test]
    fn test_offset_fetch_response_encode() {
        let response = OffsetFetchResponse::new(vec![OffsetFetchResponseTopic {
            name: "test-topic".to_string(),
            partitions: vec![
                OffsetFetchResponsePartition::success(0, 100, Some("meta1".to_string())),
                OffsetFetchResponsePartition::no_offset(1),
            ],
        }]);

        let encoded = response.encode(0).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // No throttle_time_ms in v0
        // topics count
        assert_eq!(cursor.get_i32(), 1);

        // topic name length
        assert_eq!(cursor.get_i16(), 10);
        let mut topic_name = vec![0u8; 10];
        cursor.copy_to_slice(&mut topic_name);
        assert_eq!(&topic_name, b"test-topic");

        // partitions count
        assert_eq!(cursor.get_i32(), 2);

        // partition 0
        assert_eq!(cursor.get_i32(), 0); // partition_index
        assert_eq!(cursor.get_i64(), 100); // committed_offset
        // No committed_leader_epoch in v0
        // metadata: "meta1"
        assert_eq!(cursor.get_i16(), 5);
        let mut metadata = vec![0u8; 5];
        cursor.copy_to_slice(&mut metadata);
        assert_eq!(&metadata, b"meta1");
        assert_eq!(cursor.get_i16(), 0); // error_code

        // partition 1
        assert_eq!(cursor.get_i32(), 1); // partition_index
        assert_eq!(cursor.get_i64(), -1); // committed_offset (no offset)
        // metadata: null
        assert_eq!(cursor.get_i16(), -1);
        assert_eq!(cursor.get_i16(), 0); // error_code

        // No group-level error_code in v0
    }

    #[test]
    fn test_offset_fetch_response_encode_v2() {
        let response = OffsetFetchResponse::error(16); // NOT_COORDINATOR

        let encoded = response.encode(2).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // No throttle_time_ms in v2
        // topics count
        assert_eq!(cursor.get_i32(), 0);

        // group-level error_code (v2+)
        assert_eq!(cursor.get_i16(), 16);
    }
}
