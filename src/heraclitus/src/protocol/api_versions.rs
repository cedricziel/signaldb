use crate::error::Result;
use crate::protocol::{
    encoder::{KafkaResponse, ProtocolEncoder},
    kafka_protocol::read_unsigned_varint as read_varint,
};
use bytes::Buf;
use std::io::Cursor;

#[derive(Debug)]
pub struct ApiVersionsRequest {
    // In version 0-2, this field doesn't exist
    // In version 3+, this is the client software name
    #[allow(dead_code)]
    pub client_software_name: Option<String>,
    #[allow(dead_code)]
    pub client_software_version: Option<String>,
}

impl ApiVersionsRequest {
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        let (client_software_name, client_software_version) = if api_version >= 3 {
            // Check if we have any bytes left to read
            if cursor.remaining() == 0 {
                // No request body - this is valid for ApiVersions requests
                (None, None)
            } else {
                // Try to read the fields, but be lenient about format issues
                let mut name = None;
                let mut version = None;

                // Try to read client software name
                if cursor.remaining() > 0 {
                    match read_varint(cursor) {
                        Ok(name_len) => {
                            if name_len > 0 {
                                let actual_len = (name_len - 1) as usize;
                                if cursor.remaining() >= actual_len {
                                    let mut name_bytes = vec![0u8; actual_len];
                                    cursor.copy_to_slice(&mut name_bytes);
                                    if let Ok(name_str) = String::from_utf8(name_bytes) {
                                        name = Some(name_str);
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            // If we can't read the name, skip the rest
                            return Ok(Self {
                                client_software_name: None,
                                client_software_version: None,
                            });
                        }
                    }
                }

                // Try to read client software version
                if cursor.remaining() > 0 {
                    match read_varint(cursor) {
                        Ok(version_len) => {
                            if version_len > 0 {
                                let actual_len = (version_len - 1) as usize;
                                if cursor.remaining() >= actual_len {
                                    let mut version_bytes = vec![0u8; actual_len];
                                    cursor.copy_to_slice(&mut version_bytes);
                                    if let Ok(version_str) = String::from_utf8(version_bytes) {
                                        version = Some(version_str);
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            // Version parsing failed, but that's okay
                        }
                    }
                }

                // Try to read tagged fields if there are any bytes left
                if cursor.remaining() > 0 {
                    let _ = read_varint(cursor); // Ignore tagged fields for now
                }

                (name, version)
            }
        } else {
            (None, None)
        };

        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    /// Legacy encode method - will be replaced by centralized encoder
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let encoder = ProtocolEncoder::new(18, api_version);
        self.encode_with_encoder(&encoder)
    }

    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        let mut buf = encoder.create_buffer();

        // Error code
        encoder.write_i16(&mut buf, self.error_code);

        // API versions array
        encoder.write_array_len(&mut buf, self.api_versions.len());

        for api in &self.api_versions {
            encoder.write_i16(&mut buf, api.api_key);
            encoder.write_i16(&mut buf, api.min_version);
            encoder.write_i16(&mut buf, api.max_version);

            // Tagged fields for each API (only in flexible versions)
            encoder.write_tagged_fields(&mut buf);
        }

        // Throttle time (v1+)
        if encoder.api_version() >= 1 {
            encoder.write_i32(&mut buf, self.throttle_time_ms);
        }

        // Tagged fields (only in flexible versions)
        encoder.write_tagged_fields(&mut buf);

        Ok(buf.to_vec())
    }
}

impl KafkaResponse for ApiVersionsResponse {
    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        self.encode_with_encoder(encoder)
    }

    fn api_key(&self) -> i16 {
        18 // ApiVersions API key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_v3_encoding() {
        // Create a simple response with just one API
        let response = ApiVersionsResponse {
            error_code: 0,
            api_versions: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 3,
            }],
            throttle_time_ms: 0,
        };

        // Encode as v3 (should use compact format)
        let encoded = response.encode(3).unwrap();

        println!("Encoded v3 response: {encoded:02x?}");

        // Expected format for v3:
        // - Error code: 2 bytes (0x0000)
        // - Compact array length: 1 byte (0x02 for 1 element)
        // - API key: 2 bytes (0x0012)
        // - Min version: 2 bytes (0x0000)
        // - Max version: 2 bytes (0x0003)
        // - Tagged fields: 1 byte (0x00)
        // - Throttle time: 4 bytes (0x00000000)
        // - Tagged fields: 1 byte (0x00)

        assert_eq!(encoded[0..2], [0x00, 0x00], "Error code should be 0");
        assert_eq!(encoded[2], 0x02, "Compact array length should be 2 (1+1)");
        assert_eq!(encoded[3..5], [0x00, 0x12], "API key should be 18");
        assert_eq!(encoded[5..7], [0x00, 0x00], "Min version should be 0");
        assert_eq!(encoded[7..9], [0x00, 0x03], "Max version should be 3");
        assert_eq!(encoded[9], 0x00, "Tagged fields should be empty");
        assert_eq!(
            encoded[10..14],
            [0x00, 0x00, 0x00, 0x00],
            "Throttle time should be 0"
        );
        assert_eq!(encoded[14], 0x00, "Final tagged fields should be empty");

        println!("✓ v3 encoding is correct");
    }

    #[test]
    fn test_api_versions_from_logs() {
        // Test the actual response we're sending based on logs
        let response_hex = "0000009200000001000000140000000000090000010000000c000002000000070000030000000c000008000000080000090000000800000a0000000400000b0000000700000c0000000400000d0000000400000e0000000500000f0000000500001000000004000011000000010000120000000300001300000007000014000000060000160000000400002400000002000000000000";
        // Convert hex string to bytes manually
        let response_bytes: Vec<u8> = (0..response_hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&response_hex[i..i + 2], 16).unwrap())
            .collect();

        // Skip the first 4 bytes (frame size) and next 4 bytes (correlation ID)
        let body = &response_bytes[8..];

        println!("Response body: {body:02x?}");

        // This appears to be v0/v1/v2 format (non-compact)
        // Error code (2 bytes)
        assert_eq!(&body[0..2], &[0x00, 0x00]);

        // Array length (4 bytes) - should be 20 (0x14)
        assert_eq!(&body[2..6], &[0x00, 0x00, 0x00, 0x14]);

        println!("Response is using non-compact format even for v3!");
    }
}
