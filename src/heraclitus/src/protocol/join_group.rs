use crate::error::Result;
use crate::protocol::{
    encoder::{KafkaResponse, ProtocolEncoder},
    kafka_protocol::*,
};
use bytes::Buf;
use std::io::Cursor;

/// Kafka JoinGroup Request (API Key 11)
#[derive(Debug)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32, // v1+
    pub member_id: String,
    pub _group_instance_id: Option<String>, // v5+
    pub protocol_type: String,
    pub protocols: Vec<GroupProtocol>,
}

#[derive(Debug, Clone)]
pub struct GroupProtocol {
    pub name: String,
    pub metadata: Vec<u8>,
}

/// Kafka JoinGroup Response
#[derive(Debug)]
pub struct JoinGroupResponse {
    pub throttle_time_ms: i32, // v2+
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_type: Option<String>, // v7+
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinGroupMember>,
}

#[derive(Debug, Clone)]
pub struct JoinGroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>, // v5+
    pub metadata: Vec<u8>,
}

impl JoinGroupRequest {
    /// Parse a JoinGroup request from bytes
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        let is_compact = api_version >= 6; // JoinGroup uses flexible versions from v6+

        // group_id: string
        let group_id = if is_compact {
            read_compact_string(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!("Failed to read group_id: {e}"))
            })?
        } else {
            read_string(cursor)?
        };

        // session_timeout_ms: int32
        let session_timeout_ms = cursor.get_i32();

        // rebalance_timeout_ms: int32 (v1+)
        let rebalance_timeout_ms = if api_version >= 1 {
            cursor.get_i32()
        } else {
            session_timeout_ms // Default to session timeout
        };

        // member_id: string
        let member_id = if is_compact {
            read_compact_string(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!("Failed to read member_id: {e}"))
            })?
        } else {
            read_string(cursor)?
        };

        // group_instance_id: nullable_string (v5+)
        let group_instance_id = if api_version >= 5 {
            if is_compact {
                read_compact_nullable_string(cursor).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Failed to read group_instance_id: {e}"
                    ))
                })?
            } else {
                read_nullable_string(cursor)?
            }
        } else {
            None
        };

        // protocol_type: string
        let protocol_type = if is_compact {
            read_compact_string(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!(
                    "Failed to read protocol_type: {e}"
                ))
            })?
        } else {
            read_string(cursor)?
        };

        // protocols: [string, bytes]
        let protocol_count = if is_compact {
            let len = read_unsigned_varint(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!(
                    "Failed to read protocol count: {e}"
                ))
            })?;
            if len == 0 {
                return Err(crate::error::HeraclitusError::Protocol(
                    "Null protocol array not expected".to_string(),
                ));
            }
            (len - 1) as usize
        } else {
            cursor.get_i32() as usize
        };

        let mut protocols = Vec::with_capacity(protocol_count);

        for _ in 0..protocol_count {
            let name = if is_compact {
                read_compact_string(cursor).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Failed to read protocol name: {e}"
                    ))
                })?
            } else {
                read_string(cursor)?
            };

            let metadata = if is_compact {
                read_compact_bytes(cursor).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Failed to read protocol metadata: {e}"
                    ))
                })?
            } else {
                read_bytes(cursor)?
            };

            protocols.push(GroupProtocol { name, metadata });
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

        Ok(JoinGroupRequest {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            _group_instance_id: group_instance_id,
            protocol_type,
            protocols,
        })
    }
}

impl JoinGroupResponse {
    /// Legacy encode method - delegates to centralized encoder
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let encoder = ProtocolEncoder::new(11, api_version); // JoinGroup API key = 11
        self.encode_with_encoder(&encoder)
    }

    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        let mut buf = encoder.create_buffer();

        // throttle_time_ms: int32 (v2+)
        if encoder.api_version() >= 2 {
            encoder.write_i32(&mut buf, self.throttle_time_ms);
        }

        // error_code: int16
        encoder.write_i16(&mut buf, self.error_code);

        // generation_id: int32
        encoder.write_i32(&mut buf, self.generation_id);

        // protocol_type: string (v7+)
        if encoder.api_version() >= 7 {
            encoder.write_nullable_string(&mut buf, self.protocol_type.as_deref());
        }

        // protocol_name: string
        encoder.write_nullable_string(&mut buf, self.protocol_name.as_deref());

        // leader: string
        encoder.write_string(&mut buf, &self.leader);

        // member_id: string
        encoder.write_string(&mut buf, &self.member_id);

        // members: [member_id, group_instance_id, metadata]
        encoder.write_array_len(&mut buf, self.members.len());

        for member in &self.members {
            // member_id: string
            encoder.write_string(&mut buf, &member.member_id);

            // group_instance_id: nullable_string (v5+)
            if encoder.api_version() >= 5 {
                encoder.write_nullable_string(&mut buf, member.group_instance_id.as_deref());
            }

            // metadata: bytes
            encoder.write_bytes(&mut buf, &member.metadata);

            // Tagged fields for members in flexible versions
            encoder.write_tagged_fields(&mut buf);
        }

        // Top-level tagged fields for flexible versions
        encoder.write_tagged_fields(&mut buf);

        Ok(buf.to_vec())
    }
}

impl KafkaResponse for JoinGroupResponse {
    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        self.encode_with_encoder(encoder)
    }

    fn api_key(&self) -> i16 {
        11 // JoinGroup API key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_join_group_request_parse_v0() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // session_timeout_ms: 30000
        buf.put_i32(30000);

        // member_id: "" (empty for new member)
        buf.put_i16(0);

        // protocol_type: "consumer"
        buf.put_i16(8);
        buf.put_slice(b"consumer");

        // protocols: 1 protocol
        buf.put_i32(1);

        // protocol name: "range"
        buf.put_i16(5);
        buf.put_slice(b"range");

        // protocol metadata: empty
        buf.put_i32(0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = JoinGroupRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.session_timeout_ms, 30000);
        assert_eq!(request.rebalance_timeout_ms, 30000); // Same as session in v0
        assert_eq!(request.member_id, "");
        assert_eq!(request.protocol_type, "consumer");
        assert_eq!(request.protocols.len(), 1);
        assert_eq!(request.protocols[0].name, "range");
    }

    #[test]
    fn test_join_group_response_encode_leader() {
        let members = vec![
            JoinGroupMember {
                member_id: "member-1".to_string(),
                group_instance_id: None,
                metadata: vec![1, 2, 3],
            },
            JoinGroupMember {
                member_id: "member-2".to_string(),
                group_instance_id: None,
                metadata: vec![4, 5, 6],
            },
        ];

        let response = JoinGroupResponse {
            throttle_time_ms: 0,
            error_code: 0,
            generation_id: 1,
            protocol_type: None,
            protocol_name: Some("range".to_string()),
            leader: "member-1".to_string(),
            member_id: "member-1".to_string(),
            members,
        };

        let encoded = response.encode(0).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // No throttle_time_ms in v0
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i32(), 1); // generation_id

        // protocol_name
        let proto_len = cursor.get_i16() as usize;
        let mut proto_bytes = vec![0u8; proto_len];
        cursor.copy_to_slice(&mut proto_bytes);
        assert_eq!(String::from_utf8(proto_bytes).unwrap(), "range");

        // leader
        let leader_len = cursor.get_i16() as usize;
        let mut leader_bytes = vec![0u8; leader_len];
        cursor.copy_to_slice(&mut leader_bytes);
        assert_eq!(String::from_utf8(leader_bytes).unwrap(), "member-1");

        // member_id
        let member_len = cursor.get_i16() as usize;
        let mut member_bytes = vec![0u8; member_len];
        cursor.copy_to_slice(&mut member_bytes);
        assert_eq!(String::from_utf8(member_bytes).unwrap(), "member-1");

        // members array
        assert_eq!(cursor.get_i32(), 2); // 2 members
    }

    #[test]
    fn test_join_group_response_encode_follower() {
        let response = JoinGroupResponse {
            throttle_time_ms: 0,
            error_code: 0,
            generation_id: 1,
            protocol_type: None,
            protocol_name: Some("range".to_string()),
            leader: "member-1".to_string(),
            member_id: "member-2".to_string(),
            members: Vec::new(),
        };

        let encoded = response.encode(0).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // Skip to members array
        cursor.get_i16(); // error_code
        cursor.get_i32(); // generation_id
        let proto_len = cursor.get_i16() as usize;
        cursor.advance(proto_len); // protocol_name
        let leader_len = cursor.get_i16() as usize;
        cursor.advance(leader_len); // leader
        let member_len = cursor.get_i16() as usize;
        cursor.advance(member_len); // member_id

        // members array should be empty for followers
        assert_eq!(cursor.get_i32(), 0);
    }

    #[test]
    fn test_join_group_request_parse_v6_compact() {
        let mut buf = BytesMut::new();

        // group_id: "test-group" (compact string)
        write_compact_string(&mut buf, "test-group");

        // session_timeout_ms: 30000
        buf.put_i32(30000);

        // rebalance_timeout_ms: 45000
        buf.put_i32(45000);

        // member_id: "member-1" (compact string)
        write_compact_string(&mut buf, "member-1");

        // group_instance_id: Some("instance-1") (compact nullable string)
        write_compact_nullable_string(&mut buf, Some("instance-1"));

        // protocol_type: "consumer" (compact string)
        write_compact_string(&mut buf, "consumer");

        // protocols: 1 protocol (compact array)
        write_unsigned_varint(&mut buf, 2); // 1 + 1 for compact array

        // protocol name: "range" (compact string)
        write_compact_string(&mut buf, "range");

        // protocol metadata: [1, 2, 3] (compact bytes)
        write_compact_bytes(&mut buf, &[1, 2, 3]);

        // Tagged fields (empty)
        write_unsigned_varint(&mut buf, 0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = JoinGroupRequest::parse(&mut cursor, 6).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.session_timeout_ms, 30000);
        assert_eq!(request.rebalance_timeout_ms, 45000);
        assert_eq!(request.member_id, "member-1");
        assert_eq!(request._group_instance_id, Some("instance-1".to_string()));
        assert_eq!(request.protocol_type, "consumer");
        assert_eq!(request.protocols.len(), 1);
        assert_eq!(request.protocols[0].name, "range");
        assert_eq!(request.protocols[0].metadata, vec![1, 2, 3]);
    }

    #[test]
    fn test_join_group_response_encode_v6_compact() {
        let members = vec![
            JoinGroupMember {
                member_id: "member-1".to_string(),
                group_instance_id: Some("instance-1".to_string()),
                metadata: vec![1, 2, 3],
            },
            JoinGroupMember {
                member_id: "member-2".to_string(),
                group_instance_id: None,
                metadata: vec![4, 5, 6],
            },
        ];

        let response = JoinGroupResponse {
            throttle_time_ms: 0,
            error_code: 0,
            generation_id: 1,
            protocol_type: None,
            protocol_name: Some("range".to_string()),
            leader: "member-1".to_string(),
            member_id: "member-1".to_string(),
            members,
        };

        let encoded = response.encode(6).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // throttle_time_ms
        assert_eq!(cursor.get_i32(), 0);

        // error_code
        assert_eq!(cursor.get_i16(), 0);

        // generation_id
        assert_eq!(cursor.get_i32(), 1);

        // protocol_name (compact nullable string)
        let protocol_name = read_compact_nullable_string(&mut cursor).unwrap();
        assert_eq!(protocol_name, Some("range".to_string()));

        // leader (compact string)
        let leader = read_compact_string(&mut cursor).unwrap();
        assert_eq!(leader, "member-1");

        // member_id (compact string)
        let member_id = read_compact_string(&mut cursor).unwrap();
        assert_eq!(member_id, "member-1");

        // members array (compact array)
        let member_count = read_unsigned_varint(&mut cursor).unwrap();
        assert_eq!(member_count, 3); // 2 + 1

        // Member 1
        let member1_id = read_compact_string(&mut cursor).unwrap();
        assert_eq!(member1_id, "member-1");
        let instance1_id = read_compact_nullable_string(&mut cursor).unwrap();
        assert_eq!(instance1_id, Some("instance-1".to_string()));
        let metadata1 = read_compact_bytes(&mut cursor).unwrap();
        assert_eq!(metadata1, vec![1, 2, 3]);

        // Member 2
        let member2_id = read_compact_string(&mut cursor).unwrap();
        assert_eq!(member2_id, "member-2");
        let instance2_id = read_compact_nullable_string(&mut cursor).unwrap();
        assert_eq!(instance2_id, None);
        let metadata2 = read_compact_bytes(&mut cursor).unwrap();
        assert_eq!(metadata2, vec![4, 5, 6]);

        // Tagged fields
        let tagged_fields_len = read_unsigned_varint(&mut cursor).unwrap();
        assert_eq!(tagged_fields_len, 0);
    }
}
