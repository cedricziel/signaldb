use crate::error::Result;
use crate::protocol::kafka_protocol::*;
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
        let is_compact = api_version >= 4; // LeaveGroup uses flexible versions from v4+

        // group_id: string
        let group_id = if is_compact {
            read_compact_string(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!("Failed to read group_id: {e}"))
            })?
        } else {
            read_string(cursor)?
        };

        if api_version <= 2 {
            // v0-v2: single member_id
            let member_id = read_string(cursor)?;

            Ok(LeaveGroupRequest {
                group_id,
                member_id,
                members: None,
            })
        } else {
            // v3+: array of members (for batch leaving)
            let member_count = if is_compact {
                let len = read_unsigned_varint(cursor).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Failed to read member count: {e}"
                    ))
                })?;
                if len == 0 {
                    return Err(crate::error::HeraclitusError::Protocol(
                        "Null member array not expected".to_string(),
                    ));
                }
                (len - 1) as i32
            } else {
                cursor.get_i32()
            };

            let mut members = Vec::with_capacity(member_count as usize);

            for _ in 0..member_count {
                let member_id = if is_compact {
                    read_compact_string(cursor).map_err(|e| {
                        crate::error::HeraclitusError::Protocol(format!(
                            "Failed to read member_id: {e}"
                        ))
                    })?
                } else {
                    read_string(cursor)?
                };

                let group_instance_id = if api_version >= 4 {
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

                members.push(LeaveGroupMember {
                    member_id,
                    group_instance_id,
                });
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
        let is_compact = api_version >= 4; // LeaveGroup uses flexible versions from v4+

        // throttle_time_ms: int32 (v1+)
        if api_version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // error_code: int16
        buf.put_i16(self.error_code);

        // members: [member] (v3+)
        if api_version >= 3 {
            if let Some(ref members) = self.members {
                if is_compact {
                    // Compact arrays use length + 1
                    write_unsigned_varint(&mut buf, (members.len() + 1) as u32);
                } else {
                    buf.put_i32(members.len() as i32);
                }

                for member in members {
                    // member_id: string
                    if is_compact {
                        write_compact_string(&mut buf, &member.member_id);
                    } else {
                        write_string(&mut buf, &member.member_id);
                    }

                    // group_instance_id: nullable string (v4+)
                    if api_version >= 4 {
                        if is_compact {
                            write_compact_nullable_string(
                                &mut buf,
                                member.group_instance_id.as_deref(),
                            );
                        } else {
                            write_nullable_string(&mut buf, member.group_instance_id.as_deref());
                        }
                    }

                    // error_code: int16
                    buf.put_i16(member.error_code);
                }
            } else {
                // Empty array
                if is_compact {
                    write_unsigned_varint(&mut buf, 1); // 0 + 1 for empty array
                } else {
                    buf.put_i32(0);
                }
            }
        }

        // Handle tagged fields for newer versions
        if is_compact {
            // Write empty tagged fields
            write_unsigned_varint(&mut buf, 0);
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

    #[test]
    fn test_leave_group_request_parse_v4_compact() {
        let mut buf = BytesMut::new();

        // group_id: "test-group" (compact string)
        write_compact_string(&mut buf, "test-group");

        // members: 2 members (compact array)
        write_unsigned_varint(&mut buf, 3); // 2 + 1 for compact array

        // member 1
        write_compact_string(&mut buf, "member-1");
        write_compact_nullable_string(&mut buf, Some("instance-1"));

        // member 2
        write_compact_string(&mut buf, "member-2");
        write_compact_nullable_string(&mut buf, None);

        // Tagged fields (empty)
        write_unsigned_varint(&mut buf, 0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = LeaveGroupRequest::parse(&mut cursor, 4).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.member_id, "member-1"); // First member
        assert!(request.members.is_some());
        let members = request.members.unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].member_id, "member-1");
        assert_eq!(members[0].group_instance_id, Some("instance-1".to_string()));
        assert_eq!(members[1].member_id, "member-2");
        assert_eq!(members[1].group_instance_id, None);
    }

    #[test]
    fn test_leave_group_response_encode_v4_compact() {
        let members = vec![
            LeaveGroupMemberResponse {
                member_id: "member-1".to_string(),
                group_instance_id: Some("instance-1".to_string()),
                error_code: 0,
            },
            LeaveGroupMemberResponse {
                member_id: "member-2".to_string(),
                group_instance_id: None,
                error_code: 0,
            },
        ];

        let response = LeaveGroupResponse::with_members(members);
        let encoded = response.encode(4).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // throttle_time_ms
        assert_eq!(cursor.get_i32(), 0);

        // error_code
        assert_eq!(cursor.get_i16(), 0);

        // members count (compact array)
        let member_count = read_unsigned_varint(&mut cursor).unwrap();
        assert_eq!(member_count, 3); // 2 + 1

        // member 1
        let member_id = read_compact_string(&mut cursor).unwrap();
        assert_eq!(member_id, "member-1");
        let instance_id = read_compact_nullable_string(&mut cursor).unwrap();
        assert_eq!(instance_id, Some("instance-1".to_string()));
        assert_eq!(cursor.get_i16(), 0); // error_code

        // member 2
        let member_id = read_compact_string(&mut cursor).unwrap();
        assert_eq!(member_id, "member-2");
        let instance_id = read_compact_nullable_string(&mut cursor).unwrap();
        assert_eq!(instance_id, None);
        assert_eq!(cursor.get_i16(), 0); // error_code

        // Tagged fields
        let tagged_fields_len = read_unsigned_varint(&mut cursor).unwrap();
        assert_eq!(tagged_fields_len, 0);
    }
}
