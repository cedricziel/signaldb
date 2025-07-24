use crate::error::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

/// Kafka JoinGroup Request (API Key 11)
#[derive(Debug)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32, // v1+
    pub member_id: String,
    pub group_instance_id: Option<String>, // v5+
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
        // group_id: string
        let group_id = crate::protocol::kafka_protocol::read_string(cursor)?;

        // session_timeout_ms: int32
        let session_timeout_ms = cursor.get_i32();

        // rebalance_timeout_ms: int32 (v1+)
        let rebalance_timeout_ms = if api_version >= 1 {
            cursor.get_i32()
        } else {
            session_timeout_ms // Default to session timeout
        };

        // member_id: string
        let member_id = crate::protocol::kafka_protocol::read_string(cursor)?;

        // group_instance_id: nullable_string (v5+)
        let group_instance_id = if api_version >= 5 {
            crate::protocol::kafka_protocol::read_nullable_string(cursor)?
        } else {
            None
        };

        // protocol_type: string
        let protocol_type = crate::protocol::kafka_protocol::read_string(cursor)?;

        // protocols: [string, bytes]
        let protocol_count = cursor.get_i32() as usize;
        let mut protocols = Vec::with_capacity(protocol_count);

        for _ in 0..protocol_count {
            let name = crate::protocol::kafka_protocol::read_string(cursor)?;
            let metadata = crate::protocol::kafka_protocol::read_bytes(cursor)?;
            protocols.push(GroupProtocol { name, metadata });
        }

        Ok(JoinGroupRequest {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
        })
    }
}

impl JoinGroupResponse {
    /// Create a new JoinGroup response for a leader
    pub fn new_leader(
        generation_id: i32,
        protocol_name: String,
        leader_id: String,
        member_id: String,
        members: Vec<JoinGroupMember>,
    ) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0, // NONE
            generation_id,
            protocol_type: None,
            protocol_name: Some(protocol_name),
            leader: leader_id,
            member_id,
            members,
        }
    }

    /// Create a new JoinGroup response for a follower
    pub fn new_follower(
        generation_id: i32,
        protocol_name: String,
        leader_id: String,
        member_id: String,
    ) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0, // NONE
            generation_id,
            protocol_type: None,
            protocol_name: Some(protocol_name),
            leader: leader_id,
            member_id,
            members: Vec::new(), // Followers don't get member list
        }
    }

    /// Create an error response
    #[allow(dead_code)]
    pub fn error(error_code: i16) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            generation_id: -1,
            protocol_type: None,
            protocol_name: None,
            leader: String::new(),
            member_id: String::new(),
            members: Vec::new(),
        }
    }

    /// Encode the response to bytes
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // throttle_time_ms: int32 (v2+)
        if api_version >= 2 {
            buf.put_i32(self.throttle_time_ms);
        }

        // error_code: int16
        buf.put_i16(self.error_code);

        // generation_id: int32
        buf.put_i32(self.generation_id);

        // protocol_type: string (v7+)
        if api_version >= 7 {
            crate::protocol::kafka_protocol::write_nullable_string(
                &mut buf,
                self.protocol_type.as_deref(),
            );
        }

        // protocol_name: string
        crate::protocol::kafka_protocol::write_nullable_string(
            &mut buf,
            self.protocol_name.as_deref(),
        );

        // leader: string
        crate::protocol::kafka_protocol::write_string(&mut buf, &self.leader);

        // member_id: string
        crate::protocol::kafka_protocol::write_string(&mut buf, &self.member_id);

        // members: [member_id, group_instance_id, metadata]
        buf.put_i32(self.members.len() as i32);

        for member in &self.members {
            // member_id: string
            crate::protocol::kafka_protocol::write_string(&mut buf, &member.member_id);

            // group_instance_id: nullable_string (v5+)
            if api_version >= 5 {
                crate::protocol::kafka_protocol::write_nullable_string(
                    &mut buf,
                    member.group_instance_id.as_deref(),
                );
            }

            // metadata: bytes
            crate::protocol::kafka_protocol::write_bytes(&mut buf, &member.metadata);
        }

        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let response = JoinGroupResponse::new_leader(
            1,
            "range".to_string(),
            "member-1".to_string(),
            "member-1".to_string(),
            members,
        );

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
        let response = JoinGroupResponse::new_follower(
            1,
            "range".to_string(),
            "member-1".to_string(),
            "member-2".to_string(),
        );

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
}
