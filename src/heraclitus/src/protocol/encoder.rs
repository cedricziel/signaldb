use crate::error::Result;
use crate::protocol::kafka_protocol::{
    uses_flexible_version_for_api, write_unsigned_varint, write_unsigned_varint_to_vec,
};
use bytes::{BufMut, BytesMut};

/// Centralized Kafka protocol encoder that handles compact vs non-compact encoding
/// based on API version and flexible version requirements
#[derive(Debug, Clone)]
pub struct ProtocolEncoder {
    api_version: i16,
    is_flexible: bool,
}

impl ProtocolEncoder {
    /// Create a new encoder for a specific API and version
    pub fn new(api_key: i16, api_version: i16) -> Self {
        let is_flexible = uses_flexible_version_for_api(api_key, api_version);
        Self {
            api_version,
            is_flexible,
        }
    }

    /// Returns true if this encoder uses flexible (compact) encoding
    pub fn is_flexible(&self) -> bool {
        self.is_flexible
    }

    /// Returns the API version this encoder is configured for
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    /// Write an array length using the appropriate encoding format
    /// - Flexible: varint length + 1 (0 = null, 1 = empty, 2+ = length + 1)
    /// - Non-flexible: 4-byte signed integer length (-1 = null, 0+ = length)
    pub fn write_array_len(&self, buf: &mut BytesMut, length: usize) {
        if self.is_flexible {
            // Compact format: length + 1 as varint
            write_unsigned_varint(buf, (length + 1) as u32);
        } else {
            // Non-compact format: length as 4-byte signed integer
            buf.put_i32(length as i32);
        }
    }

    /// Write an array length to Vec<u8> using the appropriate encoding format
    pub fn write_array_len_to_vec(&self, buf: &mut Vec<u8>, length: usize) {
        if self.is_flexible {
            // Compact format: length + 1 as varint
            write_unsigned_varint_to_vec(buf, (length + 1) as u32);
        } else {
            // Non-compact format: length as 4-byte signed integer
            buf.extend_from_slice(&(length as i32).to_be_bytes());
        }
    }

    /// Write a nullable array length (for null arrays)
    pub fn write_nullable_array_len(&self, buf: &mut BytesMut, length: Option<usize>) {
        match length {
            Some(len) => self.write_array_len(buf, len),
            None => {
                if self.is_flexible {
                    write_unsigned_varint(buf, 0); // 0 = null in compact format
                } else {
                    buf.put_i32(-1); // -1 = null in non-compact format
                }
            }
        }
    }

    /// Write a string using the appropriate encoding format
    /// - Flexible: varint length + 1, then string bytes (0 = null, 1 = empty string)
    /// - Non-flexible: 2-byte signed length, then string bytes (-1 = null)
    pub fn write_string(&self, buf: &mut BytesMut, s: &str) {
        let bytes = s.as_bytes();
        if self.is_flexible {
            // Compact format: length + 1 as varint, then bytes
            write_unsigned_varint(buf, (bytes.len() + 1) as u32);
            buf.put_slice(bytes);
        } else {
            // Non-compact format: length as 2-byte signed integer, then bytes
            buf.put_i16(bytes.len() as i16);
            buf.put_slice(bytes);
        }
    }

    /// Write a nullable string using the appropriate encoding format
    pub fn write_nullable_string(&self, buf: &mut BytesMut, s: Option<&str>) {
        match s {
            Some(string) => self.write_string(buf, string),
            None => {
                if self.is_flexible {
                    write_unsigned_varint(buf, 0); // 0 = null in compact format
                } else {
                    buf.put_i16(-1); // -1 = null in non-compact format
                }
            }
        }
    }

    /// Write bytes using the appropriate encoding format
    /// - Flexible: varint length + 1, then byte data (0 = null, 1 = empty bytes)
    /// - Non-flexible: 4-byte signed length, then byte data (-1 = null)
    pub fn write_bytes(&self, buf: &mut BytesMut, bytes: &[u8]) {
        if self.is_flexible {
            // Compact format: length + 1 as varint, then bytes
            write_unsigned_varint(buf, (bytes.len() + 1) as u32);
            buf.put_slice(bytes);
        } else {
            // Non-compact format: length as 4-byte signed integer, then bytes
            buf.put_i32(bytes.len() as i32);
            buf.put_slice(bytes);
        }
    }

    /// Write nullable bytes using the appropriate encoding format
    pub fn write_nullable_bytes(&self, buf: &mut BytesMut, bytes: Option<&[u8]>) {
        match bytes {
            Some(data) => self.write_bytes(buf, data),
            None => {
                if self.is_flexible {
                    write_unsigned_varint(buf, 0); // 0 = null in compact format
                } else {
                    buf.put_i32(-1); // -1 = null in non-compact format
                }
            }
        }
    }

    /// Write tagged fields (only for flexible versions)
    /// For non-flexible versions, this is a no-op
    pub fn write_tagged_fields(&self, buf: &mut BytesMut) {
        if self.is_flexible {
            // Empty tagged fields = 0
            write_unsigned_varint(buf, 0);
        }
        // Non-flexible versions don't have tagged fields
    }

    /// Write tagged fields to Vec<u8> (only for flexible versions)
    pub fn write_tagged_fields_to_vec(&self, buf: &mut Vec<u8>) {
        if self.is_flexible {
            // Empty tagged fields = 0
            write_unsigned_varint_to_vec(buf, 0);
        }
        // Non-flexible versions don't have tagged fields
    }

    /// Write a boolean value using the appropriate encoding
    pub fn write_bool(&self, buf: &mut BytesMut, value: bool) {
        buf.put_u8(if value { 1 } else { 0 });
    }

    /// Write an int8 value
    pub fn write_i8(&self, buf: &mut BytesMut, value: i8) {
        buf.put_i8(value);
    }

    /// Write an int16 value
    pub fn write_i16(&self, buf: &mut BytesMut, value: i16) {
        buf.put_i16(value);
    }

    /// Write an int32 value
    pub fn write_i32(&self, buf: &mut BytesMut, value: i32) {
        buf.put_i32(value);
    }

    /// Write an int64 value
    pub fn write_i64(&self, buf: &mut BytesMut, value: i64) {
        buf.put_i64(value);
    }

    /// Create a new buffer for encoding
    pub fn create_buffer(&self) -> BytesMut {
        BytesMut::new()
    }

    /// Create a new Vec<u8> buffer for encoding
    pub fn create_vec_buffer(&self) -> Vec<u8> {
        Vec::new()
    }
}

/// Trait for Kafka protocol responses that can be encoded
pub trait KafkaResponse {
    /// Encode this response using the provided encoder
    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>>;

    /// Get the API key for this response type
    fn api_key(&self) -> i16;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_flexible_detection() {
        // ApiVersions v4+ uses flexible encoding
        let encoder_v2 = ProtocolEncoder::new(18, 2);
        assert!(!encoder_v2.is_flexible());

        let encoder_v3 = ProtocolEncoder::new(18, 3);
        assert!(!encoder_v3.is_flexible());

        let encoder_v4 = ProtocolEncoder::new(18, 4);
        assert!(encoder_v4.is_flexible());

        // Metadata v9+ uses flexible encoding
        let encoder_v8 = ProtocolEncoder::new(3, 8);
        assert!(!encoder_v8.is_flexible());

        let encoder_v9 = ProtocolEncoder::new(3, 9);
        assert!(encoder_v9.is_flexible());
    }

    #[test]
    fn test_array_length_encoding() {
        // Non-flexible encoding
        let encoder = ProtocolEncoder::new(18, 2);
        let mut buf = BytesMut::new();
        encoder.write_array_len(&mut buf, 5);
        assert_eq!(buf.to_vec(), vec![0x00, 0x00, 0x00, 0x05]); // 4-byte big-endian

        // Flexible encoding
        let encoder = ProtocolEncoder::new(18, 4);
        let mut buf = BytesMut::new();
        encoder.write_array_len(&mut buf, 5);
        assert_eq!(buf.to_vec(), vec![0x06]); // varint 6 = 5 + 1
    }

    #[test]
    fn test_string_encoding() {
        let test_str = "hello";

        // Non-flexible encoding
        let encoder = ProtocolEncoder::new(18, 2);
        let mut buf = BytesMut::new();
        encoder.write_string(&mut buf, test_str);
        assert_eq!(buf.to_vec(), vec![0x00, 0x05, b'h', b'e', b'l', b'l', b'o']);

        // Flexible encoding
        let encoder = ProtocolEncoder::new(18, 4);
        let mut buf = BytesMut::new();
        encoder.write_string(&mut buf, test_str);
        assert_eq!(buf.to_vec(), vec![0x06, b'h', b'e', b'l', b'l', b'o']); // varint 6 = 5 + 1
    }

    #[test]
    fn test_nullable_string_encoding() {
        // Non-flexible null string
        let encoder = ProtocolEncoder::new(18, 2);
        let mut buf = BytesMut::new();
        encoder.write_nullable_string(&mut buf, None);
        assert_eq!(buf.to_vec(), vec![0xff, 0xff]); // -1 as 2-byte signed

        // Flexible null string
        let encoder = ProtocolEncoder::new(18, 4);
        let mut buf = BytesMut::new();
        encoder.write_nullable_string(&mut buf, None);
        assert_eq!(buf.to_vec(), vec![0x00]); // varint 0 = null
    }

    #[test]
    fn test_tagged_fields() {
        // Non-flexible: no tagged fields written
        let encoder = ProtocolEncoder::new(18, 2);
        let mut buf = BytesMut::new();
        encoder.write_tagged_fields(&mut buf);
        assert_eq!(buf.to_vec(), Vec::<u8>::new()); // Empty

        // Flexible: empty tagged fields
        let encoder = ProtocolEncoder::new(18, 4);
        let mut buf = BytesMut::new();
        encoder.write_tagged_fields(&mut buf);
        assert_eq!(buf.to_vec(), vec![0x00]); // varint 0 = no tagged fields
    }
}
