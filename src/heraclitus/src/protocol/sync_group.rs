use crate::error::Result;
use crate::protocol::kafka_protocol::*;
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use std::io::Cursor;

/// Kafka SyncGroup Request (API Key 14)
#[derive(Debug)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_assignments: HashMap<String, Vec<u8>>, // member_id -> assignment bytes
}

/// Kafka SyncGroup Response
#[derive(Debug)]
pub struct SyncGroupResponse {
    pub throttle_time_ms: i32, // v1+
    pub error_code: i16,
    pub member_assignment: Vec<u8>,
}

impl SyncGroupRequest {
    /// Parse a SyncGroup request from bytes
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        let is_compact = api_version >= 4; // SyncGroup uses flexible versions from v4+

        // group_id: string
        let group_id = if is_compact {
            read_compact_string(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!("Failed to read group_id: {e}"))
            })?
        } else {
            read_string(cursor)?
        };

        // generation_id: int32
        let generation_id = cursor.get_i32();

        // member_id: string
        let member_id = if is_compact {
            read_compact_string(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!("Failed to read member_id: {e}"))
            })?
        } else {
            read_string(cursor)?
        };

        // group_assignments: [member_id, member_assignment]
        let assignment_count = if is_compact {
            let len = read_unsigned_varint(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!(
                    "Failed to read assignment count: {e}"
                ))
            })?;
            if len == 0 {
                return Err(crate::error::HeraclitusError::Protocol(
                    "Null assignment array not expected".to_string(),
                ));
            }
            (len - 1) as i32
        } else {
            cursor.get_i32()
        };

        let mut group_assignments = HashMap::new();
        for _ in 0..assignment_count {
            let member_id = if is_compact {
                read_compact_string(cursor).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Failed to read assignment member_id: {e}"
                    ))
                })?
            } else {
                read_string(cursor)?
            };

            let assignment = if is_compact {
                read_compact_bytes(cursor).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Failed to read assignment bytes: {e}"
                    ))
                })?
            } else {
                read_bytes(cursor)?
            };

            group_assignments.insert(member_id, assignment);
        }

        // Handle tagged fields for newer versions
        if is_compact {
            // Read tagged fields (empty for now)
            let _num_tagged_fields = read_unsigned_varint(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!(
                    "Failed to read tagged fields: {e}"
                ))
            })?;
        }

        Ok(SyncGroupRequest {
            group_id,
            generation_id,
            member_id,
            group_assignments,
        })
    }
}

impl SyncGroupResponse {
    /// Create a successful SyncGroup response
    pub fn new(member_assignment: Vec<u8>) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0, // NONE
            member_assignment,
        }
    }

    /// Create an error response
    pub fn error(error_code: i16) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            member_assignment: Vec::new(),
        }
    }

    /// Encode the response to bytes
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        let is_compact = api_version >= 4; // SyncGroup uses flexible versions from v4+

        // throttle_time_ms: int32 (v1+)
        if api_version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // error_code: int16
        buf.put_i16(self.error_code);

        // member_assignment: bytes
        if is_compact {
            write_compact_bytes(&mut buf, &self.member_assignment);
        } else {
            write_bytes(&mut buf, &self.member_assignment);
        }

        // Handle tagged fields for newer versions
        if is_compact {
            // Write empty tagged fields
            write_unsigned_varint(&mut buf, 0);
        }

        Ok(buf.to_vec())
    }
}

/// Consumer protocol assignment - the standard format for partition assignments
#[derive(Debug)]
pub struct ConsumerProtocolAssignment {
    pub version: i16,
    pub topic_partitions: HashMap<String, Vec<i32>>, // topic -> partitions
    pub user_data: Vec<u8>,
}

impl ConsumerProtocolAssignment {
    pub fn new(topic_partitions: HashMap<String, Vec<i32>>) -> Self {
        Self {
            version: 0,
            topic_partitions,
            user_data: Vec::new(),
        }
    }

    /// Encode to the standard consumer protocol format
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();

        // version: int16
        buf.put_i16(self.version);

        // topic_partitions: [topic, [partition]]
        buf.put_i32(self.topic_partitions.len() as i32);

        for (topic, partitions) in &self.topic_partitions {
            // topic: string
            write_string(&mut buf, topic);

            // partitions: [int32]
            buf.put_i32(partitions.len() as i32);
            for &partition in partitions {
                buf.put_i32(partition);
            }
        }

        // user_data: bytes
        write_bytes(&mut buf, &self.user_data);

        buf.to_vec()
    }

    /// Decode from bytes
    #[allow(dead_code)]
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        // version: int16
        let version = cursor.get_i16();

        // topic_partitions: [topic, [partition]]
        let topic_count = cursor.get_i32();
        let mut topic_partitions = HashMap::new();

        for _ in 0..topic_count {
            let topic = read_string(&mut cursor)?;

            let partition_count = cursor.get_i32();
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                partitions.push(cursor.get_i32());
            }

            topic_partitions.insert(topic, partitions);
        }

        // user_data: bytes
        let user_data = read_bytes(&mut cursor)?;

        Ok(ConsumerProtocolAssignment {
            version,
            topic_partitions,
            user_data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_group_request_parse_v0() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // generation_id: 1
        buf.put_i32(1);

        // member_id: "member-1"
        buf.put_i16(8);
        buf.put_slice(b"member-1");

        // group_assignments: 1 assignment
        buf.put_i32(1);

        // member_id: "member-1"
        buf.put_i16(8);
        buf.put_slice(b"member-1");

        // assignment: empty
        buf.put_i32(0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = SyncGroupRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.generation_id, 1);
        assert_eq!(request.member_id, "member-1");
        assert_eq!(request.group_assignments.len(), 1);
        assert!(request.group_assignments.contains_key("member-1"));
    }

    #[test]
    fn test_sync_group_response_encode() {
        let assignment = vec![1, 2, 3, 4];
        let response = SyncGroupResponse::new(assignment.clone());

        let encoded = response.encode(0).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // No throttle_time_ms in v0
        assert_eq!(cursor.get_i16(), 0); // error_code

        // assignment
        let assignment_len = cursor.get_i32() as usize;
        let mut assignment_bytes = vec![0u8; assignment_len];
        cursor.copy_to_slice(&mut assignment_bytes);
        assert_eq!(assignment_bytes, assignment);
    }

    #[test]
    fn test_consumer_protocol_assignment() {
        let mut topic_partitions = HashMap::new();
        topic_partitions.insert("topic1".to_string(), vec![0, 1, 2]);
        topic_partitions.insert("topic2".to_string(), vec![3, 4]);

        let assignment = ConsumerProtocolAssignment::new(topic_partitions);
        let encoded = assignment.encode();
        let decoded = ConsumerProtocolAssignment::decode(&encoded).unwrap();

        assert_eq!(decoded.version, 0);
        assert_eq!(decoded.topic_partitions.len(), 2);
        assert_eq!(
            decoded.topic_partitions.get("topic1").unwrap(),
            &vec![0, 1, 2]
        );
        assert_eq!(decoded.topic_partitions.get("topic2").unwrap(), &vec![3, 4]);
    }

    #[test]
    fn test_sync_group_request_parse_v4_compact() {
        let mut buf = BytesMut::new();

        // group_id: "test-group" (compact string)
        write_compact_string(&mut buf, "test-group");

        // generation_id: 2
        buf.put_i32(2);

        // member_id: "member-2" (compact string)
        write_compact_string(&mut buf, "member-2");

        // group_assignments: 1 assignment (compact array)
        write_unsigned_varint(&mut buf, 2); // 1 + 1 for compact array

        // member_id: "member-2" (compact string)
        write_compact_string(&mut buf, "member-2");

        // assignment: 4 bytes (compact bytes)
        write_compact_bytes(&mut buf, &[1, 2, 3, 4]);

        // Tagged fields (empty)
        write_unsigned_varint(&mut buf, 0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = SyncGroupRequest::parse(&mut cursor, 4).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.generation_id, 2);
        assert_eq!(request.member_id, "member-2");
        assert_eq!(request.group_assignments.len(), 1);
        assert_eq!(
            request.group_assignments.get("member-2").unwrap(),
            &vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn test_sync_group_response_encode_v4_compact() {
        let assignment = vec![5, 6, 7, 8];
        let response = SyncGroupResponse::new(assignment.clone());

        let encoded = response.encode(4).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // throttle_time_ms
        assert_eq!(cursor.get_i32(), 0);

        // error_code
        assert_eq!(cursor.get_i16(), 0);

        // assignment (compact bytes)
        let assignment_bytes = read_compact_bytes(&mut cursor).unwrap();
        assert_eq!(assignment_bytes, assignment);

        // Tagged fields
        let tagged_fields_len = read_unsigned_varint(&mut cursor).unwrap();
        assert_eq!(tagged_fields_len, 0);
    }
}
