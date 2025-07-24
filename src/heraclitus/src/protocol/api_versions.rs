use bytes::{Buf, BufMut};
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
    pub fn parse(
        cursor: &mut Cursor<&[u8]>,
        api_version: i16,
    ) -> Result<Self, crate::error::HeraclitusError> {
        let (client_software_name, client_software_version) = if api_version >= 3 {
            // Read compact string for client software name
            let name_len = read_unsigned_varint(cursor)?;
            let name = if name_len > 0 {
                let mut name_bytes = vec![0u8; (name_len - 1) as usize];
                cursor.copy_to_slice(&mut name_bytes);
                Some(String::from_utf8(name_bytes).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Invalid UTF-8 in client name: {e}"
                    ))
                })?)
            } else {
                None
            };

            // Read compact string for client software version
            let version_len = read_unsigned_varint(cursor)?;
            let version = if version_len > 0 {
                let mut version_bytes = vec![0u8; (version_len - 1) as usize];
                cursor.copy_to_slice(&mut version_bytes);
                Some(String::from_utf8(version_bytes).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Invalid UTF-8 in client version: {e}"
                    ))
                })?)
            } else {
                None
            };

            (name, version)
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
    pub fn new() -> Self {
        // Define which API versions we support
        let api_versions = vec![
            ApiVersion {
                api_key: 0,
                min_version: 0,
                max_version: 3,
            }, // Produce
            ApiVersion {
                api_key: 1,
                min_version: 0,
                max_version: 0,
            }, // Fetch
            ApiVersion {
                api_key: 2,
                min_version: 0,
                max_version: 1,
            }, // ListOffsets
            ApiVersion {
                api_key: 3,
                min_version: 0,
                max_version: 0,
            }, // Metadata
            ApiVersion {
                api_key: 10,
                min_version: 0,
                max_version: 4,
            }, // FindCoordinator
            ApiVersion {
                api_key: 11,
                min_version: 0,
                max_version: 7,
            }, // JoinGroup
            ApiVersion {
                api_key: 12,
                min_version: 0,
                max_version: 4,
            }, // Heartbeat
            ApiVersion {
                api_key: 14,
                min_version: 0,
                max_version: 5,
            }, // SyncGroup
            ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 3,
            }, // ApiVersions
               // Add more supported APIs as we implement them
        ];

        Self {
            error_code: 0,
            api_versions,
            throttle_time_ms: 0,
        }
    }

    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>, crate::error::HeraclitusError> {
        let mut buffer = Vec::new();

        // Error code
        buffer.put_i16(self.error_code);

        if api_version <= 2 {
            // Non-compact array format
            buffer.put_i32(self.api_versions.len() as i32);

            for api in &self.api_versions {
                buffer.put_i16(api.api_key);
                buffer.put_i16(api.min_version);
                buffer.put_i16(api.max_version);
            }

            // Throttle time (v1+)
            if api_version >= 1 {
                buffer.put_i32(self.throttle_time_ms);
            }
        } else {
            // Compact array format (v3+)
            write_unsigned_varint(&mut buffer, (self.api_versions.len() + 1) as u32);

            for api in &self.api_versions {
                buffer.put_i16(api.api_key);
                buffer.put_i16(api.min_version);
                buffer.put_i16(api.max_version);

                // Tagged fields (empty)
                write_unsigned_varint(&mut buffer, 0);
            }

            // Throttle time
            buffer.put_i32(self.throttle_time_ms);

            // Tagged fields (empty)
            write_unsigned_varint(&mut buffer, 0);
        }

        Ok(buffer)
    }
}

fn read_unsigned_varint(cursor: &mut Cursor<&[u8]>) -> Result<u32, crate::error::HeraclitusError> {
    let mut value = 0u32;
    let mut i = 0;

    loop {
        if i > 4 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Varint is too long".to_string(),
            ));
        }

        let b = cursor.get_u8();
        value |= ((b & 0x7F) as u32) << (i * 7);

        if (b & 0x80) == 0 {
            break;
        }

        i += 1;
    }

    Ok(value)
}

fn write_unsigned_varint(buffer: &mut Vec<u8>, mut value: u32) {
    while (value & 0xFFFFFF80) != 0 {
        buffer.push(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    buffer.push((value & 0x7F) as u8);
}
