use crate::error::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

/// Kafka LeaveGroup Request (API Key 13)
#[derive(Debug)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
    #[allow(dead_code)]
    pub members: Option<Vec<LeaveGroupMember>>, // v3+
}

#[derive(Debug)]
pub struct LeaveGroupMember {
    pub member_id: String,
    #[allow(dead_code)]
    pub group_instance_id: Option<String>, // v3+
}

/// Kafka LeaveGroup Response
#[derive(Debug)]
pub struct LeaveGroupResponse {
    pub throttle_time_ms: i32, // v1+
    pub error_code: i16,
    #[allow(dead_code)]
    pub members: Option<Vec<LeaveGroupMemberResponse>>, // v3+
}

#[derive(Debug)]
pub struct LeaveGroupMemberResponse {
    pub member_id: String,
    #[allow(dead_code)]
    pub group_instance_id: Option<String>, // v3+
    pub error_code: i16,
}

impl LeaveGroupRequest {
    /// Parse a LeaveGroup request from bytes
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        // group_id: string
        let group_id = crate::protocol::kafka_protocol::read_string(cursor)?;

        if api_version <= 2 {
            // v0-v2: single member_id
            let member_id = crate::protocol::kafka_protocol::read_string(cursor)?;

            Ok(LeaveGroupRequest {
                group_id,
                member_id,
                members: None,
            })
        } else {
            // v3+: array of members (for batch leaving)
            let member_count = cursor.get_i32();
            let mut members = Vec::with_capacity(member_count as usize);

            for _ in 0..member_count {
                let member_id = crate::protocol::kafka_protocol::read_string(cursor)?;
                let group_instance_id = if api_version >= 4 {
                    crate::protocol::kafka_protocol::read_nullable_string(cursor)?
                } else {
                    None
                };

                members.push(LeaveGroupMember {
                    member_id,
                    group_instance_id,
                });
            }

            // For backward compatibility, set member_id to first member if available
            let member_id = members
                .first()
                .map(|m| m.member_id.clone())
                .unwrap_or_default();

            Ok(LeaveGroupRequest {
                group_id,
                member_id,
                members: Some(members),
            })
        }
    }
}

impl LeaveGroupResponse {
    /// Create a successful response
    pub fn success() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0, // SUCCESS
            members: None,
        }
    }

    /// Create an error response
    pub fn error(error_code: i16) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            members: None,
        }
    }

    /// Create a response for v3+ with member results
    #[allow(dead_code)]
    pub fn with_members(members: Vec<LeaveGroupMemberResponse>) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            members: Some(members),
        }
    }

    /// Encode the response to bytes
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // throttle_time_ms: int32 (v1+)
        if api_version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // error_code: int16
        buf.put_i16(self.error_code);

        // members: [member] (v3+)
        if api_version >= 3 {
            if let Some(ref members) = self.members {
                buf.put_i32(members.len() as i32);

                for member in members {
                    // member_id: string
                    crate::protocol::kafka_protocol::write_string(&mut buf, &member.member_id);

                    // group_instance_id: nullable string (v4+)
                    if api_version >= 4 {
                        crate::protocol::kafka_protocol::write_nullable_string(
                            &mut buf,
                            member.group_instance_id.as_deref(),
                        );
                    }

                    // error_code: int16
                    buf.put_i16(member.error_code);
                }
            } else {
                // Empty array
                buf.put_i32(0);
            }
        }

        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leave_group_request_parse_v0() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // member_id: "test-member"
        buf.put_i16(11);
        buf.put_slice(b"test-member");

        let mut cursor = Cursor::new(&buf[..]);
        let request = LeaveGroupRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.member_id, "test-member");
        assert!(request.members.is_none());
    }

    #[test]
    fn test_leave_group_request_parse_v3() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // members: 1 member
        buf.put_i32(1);

        // member_id: "test-member"
        buf.put_i16(11);
        buf.put_slice(b"test-member");

        let mut cursor = Cursor::new(&buf[..]);
        let request = LeaveGroupRequest::parse(&mut cursor, 3).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.member_id, "test-member");
        assert!(request.members.is_some());
        let members = request.members.unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].member_id, "test-member");
    }

    #[test]
    fn test_leave_group_response_encode_v0() {
        let response = LeaveGroupResponse::success();
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // No throttle_time_ms in v0
        // error_code: int16
        assert_eq!(cursor.get_i16(), 0);

        // No members array in v0
    }

    #[test]
    fn test_leave_group_response_encode_v1() {
        let response = LeaveGroupResponse::error(16); // NOT_COORDINATOR
        let encoded = response.encode(1).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // throttle_time_ms: int32
        assert_eq!(cursor.get_i32(), 0);

        // error_code: int16
        assert_eq!(cursor.get_i16(), 16);
    }

    #[test]
    fn test_leave_group_response_encode_v3() {
        let members = vec![LeaveGroupMemberResponse {
            member_id: "test-member".to_string(),
            group_instance_id: None,
            error_code: 0,
        }];

        let response = LeaveGroupResponse::with_members(members);
        let encoded = response.encode(3).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // throttle_time_ms: int32
        assert_eq!(cursor.get_i32(), 0);

        // error_code: int16
        assert_eq!(cursor.get_i16(), 0);

        // members count
        assert_eq!(cursor.get_i32(), 1);

        // member_id length
        assert_eq!(cursor.get_i16(), 11);
        let mut member_id_bytes = vec![0u8; 11];
        cursor.copy_to_slice(&mut member_id_bytes);
        assert_eq!(&member_id_bytes, b"test-member");

        // member error_code
        assert_eq!(cursor.get_i16(), 0);
    }
}
