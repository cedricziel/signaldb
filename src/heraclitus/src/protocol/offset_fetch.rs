use crate::error::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
use tracing::debug;

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
        let is_compact = api_version >= 6;

        debug!(
            "Parsing OffsetFetch request v{}, is_compact={}",
            api_version, is_compact
        );

        // group_id: string/compact string
        let group_id = if is_compact {
            debug!(
                "Reading compact group_id string, cursor position: {}",
                cursor.position()
            );
            let group_id = read_compact_string(cursor)?;
            debug!(
                "Read group_id: '{}', cursor position: {}",
                group_id,
                cursor.position()
            );
            group_id
        } else {
            crate::protocol::kafka_protocol::read_string(cursor)?
        };

        // topics: nullable array (null means fetch all)
        let topics = if api_version <= 1 {
            // In v0-v1, topics is not nullable and empty array means all topics
            let topic_count = cursor.get_i32();
            if topic_count == 0 {
                None // Empty array means all topics in v0-v1
            } else {
                let mut topics = Vec::with_capacity(topic_count as usize);
                for _ in 0..topic_count {
                    topics.push(Self::parse_topic(cursor, api_version)?);
                }
                Some(topics)
            }
        } else if is_compact {
            // In v6+, use compact array
            debug!(
                "Reading compact topics array, cursor position: {}",
                cursor.position()
            );
            let topic_count_raw = read_unsigned_varint(cursor)?;
            let topic_count = topic_count_raw as i32 - 1;
            debug!(
                "Compact topics count: {} (raw={}), cursor position: {}",
                topic_count,
                topic_count_raw,
                cursor.position()
            );
            if topic_count == -1 {
                debug!("Null topics array - all topics requested");
                None // Null array means all topics
            } else {
                let mut topics = Vec::with_capacity(topic_count as usize);
                for i in 0..topic_count {
                    debug!("Parsing topic {} of {}", i + 1, topic_count);
                    topics.push(Self::parse_topic(cursor, api_version)?);
                }
                Some(topics)
            }
        } else {
            // In v2-v5, topics can be null (represented as -1 length)
            let topic_count = cursor.get_i32();
            if topic_count == -1 {
                None // Null array means all topics
            } else {
                let mut topics = Vec::with_capacity(topic_count as usize);
                for _ in 0..topic_count {
                    topics.push(Self::parse_topic(cursor, api_version)?);
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
        if is_compact {
            // Read tagged fields (empty for now)
            let _num_tagged_fields = read_unsigned_varint(cursor)?;
        }

        Ok(OffsetFetchRequest {
            group_id,
            topics,
            require_stable,
        })
    }

    fn parse_topic(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<OffsetFetchTopic> {
        let is_compact = api_version >= 6;

        debug!("Parsing topic, cursor position: {}", cursor.position());

        // name: string/compact string
        let name = if is_compact {
            debug!(
                "Reading compact topic name, cursor position: {}",
                cursor.position()
            );
            let name = read_compact_string(cursor)?;
            debug!(
                "Read topic name: '{}', cursor position: {}",
                name,
                cursor.position()
            );
            name
        } else {
            crate::protocol::kafka_protocol::read_string(cursor)?
        };

        // partition_indexes: [int32]/compact array
        let partition_count = if is_compact {
            read_unsigned_varint(cursor)? as i32 - 1
        } else {
            cursor.get_i32()
        };

        let mut partition_indexes = Vec::with_capacity(partition_count as usize);
        for _ in 0..partition_count {
            partition_indexes.push(cursor.get_i32());
        }

        // Tagged fields for compact versions
        if is_compact {
            let _num_tagged_fields = read_unsigned_varint(cursor)?;
        }

        Ok(OffsetFetchTopic {
            name,
            partition_indexes,
        })
    }
}

impl OffsetFetchResponse {
    /// Encode the response to bytes
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        let is_compact = api_version >= 6;

        // throttle_time_ms: int32 (v3+)
        if api_version >= 3 {
            buf.put_i32(self.throttle_time_ms);
        }

        // topics: [topic]
        if is_compact {
            // Compact array length (actual length + 1)
            write_unsigned_varint(&mut buf, (self.topics.len() + 1) as u32);
        } else {
            buf.put_i32(self.topics.len() as i32);
        }

        for topic in &self.topics {
            // name: string/compact string
            if is_compact {
                write_compact_string(&mut buf, &topic.name);
            } else {
                crate::protocol::kafka_protocol::write_string(&mut buf, &topic.name);
            }

            // partitions: [partition]
            if is_compact {
                // Compact array length (actual length + 1)
                write_unsigned_varint(&mut buf, (topic.partitions.len() + 1) as u32);
            } else {
                buf.put_i32(topic.partitions.len() as i32);
            }

            for partition in &topic.partitions {
                // partition_index: int32
                buf.put_i32(partition.partition_index);

                // committed_offset: int64
                buf.put_i64(partition.committed_offset);

                // committed_leader_epoch: int32 (v5+)
                if api_version >= 5 {
                    buf.put_i32(partition.committed_leader_epoch);
                }

                // metadata: nullable string/compact nullable string
                if is_compact {
                    write_compact_nullable_string(&mut buf, partition.metadata.as_deref());
                } else {
                    crate::protocol::kafka_protocol::write_nullable_string(
                        &mut buf,
                        partition.metadata.as_deref(),
                    );
                }

                // error_code: int16
                buf.put_i16(partition.error_code);

                // Tagged fields for partitions in compact versions
                if is_compact {
                    write_unsigned_varint(&mut buf, 0);
                }
            }

            // Tagged fields for topics in compact versions
            if is_compact {
                write_unsigned_varint(&mut buf, 0);
            }
        }

        // error_code: int16 (v2+, group-level error)
        if api_version >= 2 {
            buf.put_i16(self.error_code);
        }

        // Handle tagged fields for newer versions
        if is_compact {
            // Write empty tagged fields
            write_unsigned_varint(&mut buf, 0);
        }

        Ok(buf.to_vec())
    }
}

impl OffsetFetchResponsePartition {}

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

fn read_compact_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let len = read_unsigned_varint(cursor)? as usize;
    debug!(
        "read_compact_string: length={}, cursor pos={}",
        len,
        cursor.position()
    );

    if len == 0 {
        // In compact protocol, 0 means null - for non-nullable strings, return empty string
        debug!("read_compact_string: null string (len=0), returning empty");
        return Ok(String::new());
    }

    let actual_len = len - 1;
    if actual_len == 0 {
        debug!("read_compact_string: empty string (len=1), returning empty");
        return Ok(String::new());
    }

    if cursor.remaining() < actual_len {
        return Err(crate::error::HeraclitusError::Protocol(format!(
            "Not enough bytes for compact string: needed {}, got {} at position {}",
            actual_len,
            cursor.remaining(),
            cursor.position()
        )));
    }

    let mut bytes = vec![0u8; actual_len];
    cursor.copy_to_slice(&mut bytes);

    let result = String::from_utf8(bytes).map_err(|e| {
        crate::error::HeraclitusError::Protocol(format!("Invalid UTF-8 in compact string: {e}"))
    })?;

    debug!(
        "read_compact_string: successfully read '{}', cursor pos={}",
        result,
        cursor.position()
    );
    Ok(result)
}

fn write_compact_string(buffer: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    write_unsigned_varint(buffer, (bytes.len() + 1) as u32);
    buffer.put_slice(bytes);
}

fn write_compact_nullable_string(buffer: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(str) => write_compact_string(buffer, str),
        None => write_unsigned_varint(buffer, 0), // Null is represented as 0
    }
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
        let response = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetFetchResponseTopic {
                name: "test-topic".to_string(),
                partitions: vec![
                    OffsetFetchResponsePartition {
                        partition_index: 0,
                        committed_offset: 100,
                        committed_leader_epoch: -1,
                        metadata: Some("meta1".to_string()),
                        error_code: 0,
                    },
                    OffsetFetchResponsePartition {
                        partition_index: 1,
                        committed_offset: -1,
                        committed_leader_epoch: -1,
                        metadata: None,
                        error_code: 0,
                    },
                ],
            }],
            error_code: 0,
        };

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
        let response = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: Vec::new(),
            error_code: 16, // NOT_COORDINATOR
        };

        let encoded = response.encode(2).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // No throttle_time_ms in v2
        // topics count
        assert_eq!(cursor.get_i32(), 0);

        // group-level error_code (v2+)
        assert_eq!(cursor.get_i16(), 16);
    }
}
