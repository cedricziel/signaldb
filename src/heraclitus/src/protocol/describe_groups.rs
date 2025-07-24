use crate::error::Result;
use crate::protocol::kafka_protocol::*;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

fn read_array_len(buf: &mut Cursor<&[u8]>) -> Result<i32> {
    if buf.remaining() < 4 {
        return Err(crate::error::HeraclitusError::Protocol(
            "Not enough bytes for array length".to_string(),
        ));
    }
    Ok(buf.get_i32())
}

fn write_array_len(buf: &mut BytesMut, len: i32) {
    buf.put_i32(len);
}

fn read_bool(buf: &mut Cursor<&[u8]>) -> Result<bool> {
    if buf.remaining() < 1 {
        return Err(crate::error::HeraclitusError::Protocol(
            "Not enough bytes for boolean".to_string(),
        ));
    }
    Ok(buf.get_u8() != 0)
}

#[derive(Debug, Clone)]
pub struct DescribeGroupsRequest {
    pub group_ids: Vec<String>,
    #[allow(dead_code)]
    pub include_authorized_operations: bool, // v3+
}

impl DescribeGroupsRequest {
    pub fn parse(buf: &mut Cursor<&[u8]>, version: i16) -> Result<Self> {
        // Array of group IDs
        let group_count = read_array_len(buf)?;
        let mut group_ids = Vec::with_capacity(group_count as usize);

        for _ in 0..group_count {
            group_ids.push(read_string(buf)?);
        }

        // Include authorized operations (v3+)
        let include_authorized_operations = if version >= 3 { read_bool(buf)? } else { false };

        Ok(Self {
            group_ids,
            include_authorized_operations,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DescribeGroupsResponse {
    pub throttle_time_ms: i32, // v1+
    pub groups: Vec<GroupDescription>,
}

#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub error_code: i16,
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<MemberDescription>,
    pub authorized_operations: i32, // v3+
}

#[derive(Debug, Clone)]
pub struct MemberDescription {
    pub member_id: String,
    pub group_instance_id: Option<String>, // v4+
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Vec<u8>,
    pub member_assignment: Vec<u8>,
}

impl DescribeGroupsResponse {
    pub fn new(groups: Vec<GroupDescription>) -> Self {
        Self {
            throttle_time_ms: 0,
            groups,
        }
    }

    pub fn encode(&self, version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // Throttle time (v1+)
        if version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Groups array
        write_array_len(&mut buf, self.groups.len() as i32);

        for group in &self.groups {
            // Error code
            buf.put_i16(group.error_code);

            // Group ID
            write_string(&mut buf, &group.group_id);

            // State
            write_string(&mut buf, &group.state);

            // Protocol type
            write_string(&mut buf, &group.protocol_type);

            // Protocol data
            write_string(&mut buf, &group.protocol_data);

            // Members array
            write_array_len(&mut buf, group.members.len() as i32);

            for member in &group.members {
                // Member ID
                write_string(&mut buf, &member.member_id);

                // Group instance ID (v4+)
                if version >= 4 {
                    write_nullable_string(&mut buf, member.group_instance_id.as_deref());
                }

                // Client ID
                write_string(&mut buf, &member.client_id);

                // Client host
                write_string(&mut buf, &member.client_host);

                // Member metadata
                write_bytes(&mut buf, &member.member_metadata);

                // Member assignment
                write_bytes(&mut buf, &member.member_assignment);
            }

            // Authorized operations (v3+)
            if version >= 3 {
                buf.put_i32(group.authorized_operations);
            }
        }

        Ok(buf.to_vec())
    }
}

// Helper to determine group state from members
pub fn determine_group_state(group: &crate::state::ConsumerGroupState) -> &'static str {
    if group.members.is_empty() {
        "Empty"
    } else if group.leader.is_some() {
        "Stable"
    } else {
        "PreparingRebalance"
    }
}
