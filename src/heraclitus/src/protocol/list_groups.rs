use crate::error::Result;
use crate::protocol::{
    encoder::{KafkaResponse, ProtocolEncoder},
    kafka_protocol::*,
};
use bytes::Buf;
use std::io::Cursor;

#[derive(Debug, Clone)]
pub struct ListGroupsRequest {
    #[allow(dead_code)] // Part of Kafka protocol spec, parsed but not used yet
    pub states_filter: Option<Vec<String>>, // v4+
}

impl ListGroupsRequest {
    pub fn parse(buf: &mut Cursor<&[u8]>, version: i16) -> Result<Self> {
        let is_compact = version >= 3; // ListGroups uses flexible versions from v3+

        // States filter (v4+)
        let states_filter = if version >= 4 {
            if is_compact {
                // Compact array
                let len = read_unsigned_varint(buf).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!("Failed to read varint: {e}"))
                })?;
                if len == 0 {
                    None // Null array in compact format
                } else {
                    let actual_len = len - 1; // Compact arrays use length + 1
                    let mut states = Vec::with_capacity(actual_len as usize);
                    for _ in 0..actual_len {
                        states.push(read_compact_string(buf).map_err(|e| {
                            crate::error::HeraclitusError::Protocol(format!(
                                "Failed to read compact string: {e}"
                            ))
                        })?);
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
            let _num_tagged_fields = read_unsigned_varint(buf).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!(
                    "Failed to read tagged fields: {e}"
                ))
            })?;
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
    /// Legacy encode method - delegates to centralized encoder
    pub fn encode(&self, version: i16) -> Result<Vec<u8>> {
        let encoder = ProtocolEncoder::new(16, version); // ListGroups API key = 16
        self.encode_with_encoder(&encoder)
    }

    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        let mut buf = encoder.create_buffer();

        // Throttle time (v1+)
        if encoder.api_version() >= 1 {
            encoder.write_i32(&mut buf, self.throttle_time_ms);
        }

        // Error code
        encoder.write_i16(&mut buf, self.error_code);

        // Groups array
        encoder.write_array_len(&mut buf, self.groups.len());

        for group in &self.groups {
            // Group ID
            encoder.write_string(&mut buf, &group.group_id);

            // Protocol type
            encoder.write_string(&mut buf, &group.protocol_type);

            // Group state (v4+)
            if encoder.api_version() >= 4 {
                encoder.write_string(&mut buf, &group.group_state);
            }

            // Tagged fields for groups in flexible versions
            encoder.write_tagged_fields(&mut buf);
        }

        // Top-level tagged fields for flexible versions
        encoder.write_tagged_fields(&mut buf);

        Ok(buf.to_vec())
    }
}

impl KafkaResponse for ListGroupsResponse {
    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        self.encode_with_encoder(encoder)
    }

    fn api_key(&self) -> i16 {
        16 // ListGroups API key
    }
}
