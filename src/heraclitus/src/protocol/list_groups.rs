use crate::error::Result;
use crate::protocol::kafka_protocol::*;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

#[derive(Debug, Clone)]
pub struct ListGroupsRequest {
    pub states_filter: Option<Vec<String>>, // v4+
}

impl ListGroupsRequest {
    pub fn parse(buf: &mut Cursor<&[u8]>, version: i16) -> Result<Self> {
        // States filter (v4+)
        let states_filter = if version >= 4 {
            // Check if we have enough bytes for array length
            if buf.remaining() < 4 {
                return Err(crate::error::HeraclitusError::Protocol(
                    "Not enough bytes for states filter array length".to_string(),
                ));
            }

            let len = buf.get_i32();
            if len < 0 {
                None // Null array
            } else {
                let mut states = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    states.push(read_string(buf)?);
                }
                Some(states)
            }
        } else {
            None
        };

        Ok(Self { states_filter })
    }
}

#[derive(Debug, Clone)]
pub struct ListGroupsResponse {
    pub throttle_time_ms: i32, // v1+
    pub error_code: i16,
    pub groups: Vec<ListedGroup>,
}

#[derive(Debug, Clone)]
pub struct ListedGroup {
    pub group_id: String,
    pub protocol_type: String,
    pub group_state: String, // v4+
}

impl ListGroupsResponse {
    pub fn new(groups: Vec<ListedGroup>) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: ERROR_NONE,
            groups,
        }
    }

    pub fn encode(&self, version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // Throttle time (v1+)
        if version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Error code
        buf.put_i16(self.error_code);

        // Groups array
        buf.put_i32(self.groups.len() as i32);

        for group in &self.groups {
            // Group ID
            write_string(&mut buf, &group.group_id);

            // Protocol type
            write_string(&mut buf, &group.protocol_type);

            // Group state (v4+)
            if version >= 4 {
                write_string(&mut buf, &group.group_state);
            }
        }

        Ok(buf.to_vec())
    }
}
