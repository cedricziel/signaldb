use crate::error::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

/// Kafka OffsetCommit Request (API Key 8)
#[derive(Debug)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32, // v1+
    pub member_id: String,  // v1+
    #[allow(dead_code)]
    pub retention_time_ms: i64, // v2-v4, removed in v5+
    pub topics: Vec<OffsetCommitTopic>,
}

#[derive(Debug)]
pub struct OffsetCommitTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartition>,
}

#[derive(Debug)]
pub struct OffsetCommitPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    #[allow(dead_code)]
    pub committed_leader_epoch: i32, // v6+
    pub metadata: Option<String>,
}

/// Kafka OffsetCommit Response
#[derive(Debug)]
pub struct OffsetCommitResponse {
    pub throttle_time_ms: i32, // v3+
    pub topics: Vec<OffsetCommitResponseTopic>,
}

#[derive(Debug)]
pub struct OffsetCommitResponseTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitResponsePartition>,
}

#[derive(Debug)]
pub struct OffsetCommitResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
}

impl OffsetCommitRequest {
    /// Parse an OffsetCommit request from bytes
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        // group_id: string
        let group_id = crate::protocol::kafka_protocol::read_string(cursor)?;

        // generation_id: int32 (v1+)
        let generation_id = if api_version >= 1 {
            cursor.get_i32()
        } else {
            -1 // No generation in v0
        };

        // member_id: string (v1+)
        let member_id = if api_version >= 1 {
            crate::protocol::kafka_protocol::read_string(cursor)?
        } else {
            String::new() // No member_id in v0
        };

        // retention_time_ms: int64 (v2-v4)
        let retention_time_ms = if (2..=4).contains(&api_version) {
            cursor.get_i64()
        } else {
            -1 // Default retention
        };

        // topics: [topic]
        let topic_count = cursor.get_i32();
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            // name: string
            let name = crate::protocol::kafka_protocol::read_string(cursor)?;

            // partitions: [partition]
            let partition_count = cursor.get_i32();
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                // partition_index: int32
                let partition_index = cursor.get_i32();

                // committed_offset: int64
                let committed_offset = cursor.get_i64();

                // committed_leader_epoch: int32 (v6+)
                let committed_leader_epoch = if api_version >= 6 {
                    cursor.get_i32()
                } else {
                    -1
                };

                // metadata: nullable string
                let metadata = crate::protocol::kafka_protocol::read_nullable_string(cursor)?;

                partitions.push(OffsetCommitPartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch,
                    metadata,
                });
            }

            topics.push(OffsetCommitTopic { name, partitions });
        }

        // Handle tagged fields for newer versions
        if api_version >= 8 {
            // Read tagged fields (empty for now)
            let _num_tagged_fields = read_unsigned_varint(cursor)?;
        }

        Ok(OffsetCommitRequest {
            group_id,
            generation_id,
            member_id,
            retention_time_ms,
            topics,
        })
    }
}

impl OffsetCommitResponse {
    /// Create a new OffsetCommit response
    pub fn new(topics: Vec<OffsetCommitResponseTopic>) -> Self {
        Self {
            throttle_time_ms: 0,
            topics,
        }
    }

    /// Create an error response for all partitions
    pub fn error_all(topics: &[OffsetCommitTopic], error_code: i16) -> Self {
        let response_topics = topics
            .iter()
            .map(|topic| OffsetCommitResponseTopic {
                name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|p| OffsetCommitResponsePartition {
                        partition_index: p.partition_index,
                        error_code,
                    })
                    .collect(),
            })
            .collect();

        Self::new(response_topics)
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

                // error_code: int16
                buf.put_i16(partition.error_code);
            }
        }

        // Handle tagged fields for newer versions
        if api_version >= 8 {
            // Write empty tagged fields
            write_unsigned_varint(&mut buf, 0);
        }

        Ok(buf.to_vec())
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
    fn test_offset_commit_request_parse_v0() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // topics: 1 topic
        buf.put_i32(1);

        // topic name: "test-topic"
        buf.put_i16(10);
        buf.put_slice(b"test-topic");

        // partitions: 1 partition
        buf.put_i32(1);

        // partition_index: 0
        buf.put_i32(0);
        // committed_offset: 100
        buf.put_i64(100);
        // metadata: "test-metadata"
        buf.put_i16(13);
        buf.put_slice(b"test-metadata");

        let mut cursor = Cursor::new(&buf[..]);
        let request = OffsetCommitRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.generation_id, -1); // No generation in v0
        assert_eq!(request.member_id, ""); // No member_id in v0
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name, "test-topic");
        assert_eq!(request.topics[0].partitions.len(), 1);
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[0].committed_offset, 100);
        assert_eq!(
            request.topics[0].partitions[0].metadata,
            Some("test-metadata".to_string())
        );
    }

    #[test]
    fn test_offset_commit_response_encode() {
        let response = OffsetCommitResponse::new(vec![OffsetCommitResponseTopic {
            name: "test-topic".to_string(),
            partitions: vec![
                OffsetCommitResponsePartition {
                    partition_index: 0,
                    error_code: 0,
                },
                OffsetCommitResponsePartition {
                    partition_index: 1,
                    error_code: 0,
                },
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
        assert_eq!(cursor.get_i16(), 0); // error_code

        // partition 1
        assert_eq!(cursor.get_i32(), 1); // partition_index
        assert_eq!(cursor.get_i16(), 0); // error_code
    }
}
