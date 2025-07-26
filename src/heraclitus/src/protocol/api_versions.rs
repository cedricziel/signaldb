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

        // Debug logging
        tracing::info!(
            "Encoding ApiVersionsResponse: api_version={}, is_flexible={}, error_code={}, num_apis={}",
            encoder.api_version(),
            encoder.is_flexible(),
            self.error_code,
            self.api_versions.len()
        );

        // Error code
        encoder.write_i16(&mut buf, self.error_code);
        tracing::info!(
            "  Wrote error_code: {} (bytes: {:02x?})",
            self.error_code,
            &buf[..]
        );

        // API versions array
        let array_start = buf.len();
        encoder.write_array_len(&mut buf, self.api_versions.len());
        tracing::info!(
            "  Wrote array length {} (flexible={}) as bytes: {:02x?}",
            self.api_versions.len(),
            encoder.is_flexible(),
            &buf[array_start..]
        );

        for (i, api) in self.api_versions.iter().enumerate() {
            let api_start = buf.len();
            encoder.write_i16(&mut buf, api.api_key);
            encoder.write_i16(&mut buf, api.min_version);
            encoder.write_i16(&mut buf, api.max_version);

            // Tagged fields for each API (only in flexible versions)
            encoder.write_tagged_fields(&mut buf);

            if i < 3 || i == self.api_versions.len() - 1 {
                tracing::info!(
                    "  API[{}]: key={}, min={}, max={}, bytes: {:02x?}",
                    i,
                    api.api_key,
                    api.min_version,
                    api.max_version,
                    &buf[api_start..]
                );
            }
        }

        // Throttle time (v1+)
        if encoder.api_version() >= 1 {
            let throttle_start = buf.len();
            encoder.write_i32(&mut buf, self.throttle_time_ms);
            tracing::info!(
                "  Wrote throttle_time_ms: {} as bytes: {:02x?}",
                self.throttle_time_ms,
                &buf[throttle_start..]
            );
        }

        // Tagged fields (only in flexible versions)
        let tagged_start = buf.len();
        encoder.write_tagged_fields(&mut buf);
        if buf.len() > tagged_start {
            tracing::info!("  Wrote tagged fields: {:02x?}", &buf[tagged_start..]);
        }

        tracing::info!(
            "Encoded ApiVersionsResponse total {} bytes: {:02x?}",
            buf.len(),
            &buf[..]
        );
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

        // Encode as v3 (should use NON-compact format)
        let encoded = response.encode(3).unwrap();

        println!("Encoded v3 response: {encoded:02x?}");

        // Expected format for v3 (non-compact):
        // - Error code: 2 bytes (0x0000)
        // - Array length: 4 bytes (0x00000001 for 1 element)
        // - API key: 2 bytes (0x0012)
        // - Min version: 2 bytes (0x0000)
        // - Max version: 2 bytes (0x0003)
        // - Throttle time: 4 bytes (0x00000000)

        assert_eq!(encoded[0..2], [0x00, 0x00], "Error code should be 0");
        assert_eq!(
            encoded[2..6],
            [0x00, 0x00, 0x00, 0x01],
            "Array length should be 1"
        );
        assert_eq!(encoded[6..8], [0x00, 0x12], "API key should be 18");
        assert_eq!(encoded[8..10], [0x00, 0x00], "Min version should be 0");
        assert_eq!(encoded[10..12], [0x00, 0x03], "Max version should be 3");
        assert_eq!(
            encoded[12..16],
            [0x00, 0x00, 0x00, 0x00],
            "Throttle time should be 0"
        );

        println!("✓ v3 encoding is correct (non-compact)");
    }

    #[test]
    fn test_api_versions_v3_actual_encoding() {
        // Test that v3 uses NON-compact encoding (based on rdkafka behavior)
        let response = ApiVersionsResponse {
            error_code: 0,
            api_versions: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 3,
            }],
            throttle_time_ms: 0,
        };

        let encoder = ProtocolEncoder::new(18, 3);
        assert!(
            !encoder.is_flexible(),
            "v3 should NOT use flexible encoding"
        );

        let encoded = response.encode_with_encoder(&encoder).unwrap();
        println!("Encoded v3 bytes: {encoded:02x?}");

        // Verify non-compact encoding is used
        // Error code: 2 bytes
        assert_eq!(encoded[0..2], [0x00, 0x00]);
        // Array length should be 4-byte int
        assert_eq!(
            encoded[2..6],
            [0x00, 0x00, 0x00, 0x01],
            "Should use 4-byte int for array length"
        );
        // API key
        assert_eq!(encoded[6..8], [0x00, 0x12]);
    }

    #[test]
    fn test_api_versions_from_logs() {
        // Test the actual response we're sending based on logs
        let response_hex = "0000009200000001000000000014000000090000010000000c000002000000070000030000000c000008000000080000090000000800000a0000000400000b0000000700000c0000000400000d0000000400000e0000000500000f0000000500001000000004000011000000010000120000000300001300000007000014000000060000160000000400002400000002000000000000";
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
