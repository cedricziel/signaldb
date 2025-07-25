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
        let is_compact = version >= 3; // ListGroups uses flexible versions from v3+

        // States filter (v4+)
        let states_filter = if version >= 4 {
            if is_compact {
                // Compact array
                let len = read_unsigned_varint(buf)?;
                if len == 0 {
                    None // Null array in compact format
                } else {
                    let actual_len = len - 1; // Compact arrays use length + 1
                    let mut states = Vec::with_capacity(actual_len as usize);
                    for _ in 0..actual_len {
                        states.push(read_compact_string(buf)?);
                    }
                    Some(states)
                }
            } else {
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
            }
        } else {
            None
        };

        // Tagged fields (v3+)
        if is_compact {
            let _num_tagged_fields = read_unsigned_varint(buf)?;
            // Skip tagged fields for now
        }

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
        let is_compact = version >= 3; // ListGroups uses flexible versions from v3+

        // Throttle time (v1+)
        if version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Error code
        buf.put_i16(self.error_code);

        // Groups array
        if is_compact {
            // Compact arrays use length + 1
            write_unsigned_varint(&mut buf, (self.groups.len() + 1) as u32);
        } else {
            buf.put_i32(self.groups.len() as i32);
        }

        for group in &self.groups {
            // Group ID
            if is_compact {
                write_compact_string(&mut buf, &group.group_id);
            } else {
                write_string(&mut buf, &group.group_id);
            }

            // Protocol type
            if is_compact {
                write_compact_string(&mut buf, &group.protocol_type);
            } else {
                write_string(&mut buf, &group.protocol_type);
            }

            // Group state (v4+)
            if version >= 4 {
                if is_compact {
                    write_compact_string(&mut buf, &group.group_state);
                } else {
                    write_string(&mut buf, &group.group_state);
                }
            }
        }

        // Tagged fields (v3+)
        if is_compact {
            // No tagged fields for now
            write_unsigned_varint(&mut buf, 0);
        }

        Ok(buf.to_vec())
    }
}

// Helper functions for compact encoding
fn read_unsigned_varint(buf: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value = 0u32;
    let mut i = 0;
    loop {
        if i > 4 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Varint too long".to_string(),
            ));
        }
        if buf.remaining() < 1 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Not enough bytes for varint".to_string(),
            ));
        }
        let byte = buf.get_u8();
        value |= ((byte & 0x7F) as u32) << (i * 7);
        if byte & 0x80 == 0 {
            break;
        }
        i += 1;
    }
    Ok(value)
}

fn read_compact_string(buf: &mut Cursor<&[u8]>) -> Result<String> {
    let len = read_unsigned_varint(buf)?;
    if len == 0 {
        return Err(crate::error::HeraclitusError::Protocol(
            "Null string not expected here".to_string(),
        ));
    }

    let actual_len = (len - 1) as usize;
    if actual_len == 0 {
        return Ok(String::new());
    }

    if buf.remaining() < actual_len {
        return Err(crate::error::HeraclitusError::Protocol(
            "Not enough bytes for compact string data".to_string(),
        ));
    }

    let mut bytes = vec![0u8; actual_len];
    buf.copy_to_slice(&mut bytes);

    String::from_utf8(bytes).map_err(|e| {
        crate::error::HeraclitusError::Protocol(format!("Invalid UTF-8 in compact string: {e}"))
    })
}

fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

fn write_compact_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    // Compact strings use length + 1 (0 means null, 1 means empty string)
    write_unsigned_varint(buf, (bytes.len() + 1) as u32);
    buf.put_slice(bytes);
}
