use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

/// Determine if an API uses flexible versions and requires compact encoding
pub fn uses_flexible_version_for_api(api_key: i16, api_version: i16) -> bool {
    match api_key {
        // ApiVersions uses flexible versions from v3+
        18 => api_version >= 3,
        // Metadata uses flexible versions from v9+
        3 => api_version >= 9,
        // CreateTopics uses flexible versions from v5+
        19 => api_version >= 5,
        // InitProducerId uses flexible versions from v2+
        22 => api_version >= 2,
        // OffsetCommit uses flexible versions from v8+
        8 => api_version >= 8,
        // OffsetFetch uses flexible versions from v6+
        9 => api_version >= 6,
        // Heartbeat uses flexible versions from v4+
        12 => api_version >= 4,
        // JoinGroup uses flexible versions from v6+
        11 => api_version >= 6,
        // SyncGroup uses flexible versions from v4+
        14 => api_version >= 4,
        // LeaveGroup uses flexible versions from v4+
        13 => api_version >= 4,
        // ListGroups uses flexible versions from v3+
        16 => api_version >= 3,
        // DescribeGroups uses flexible versions from v5+
        15 => api_version >= 5,
        // ListOffsets uses flexible versions from v4+
        2 => api_version >= 4,
        // Produce uses flexible versions from v9+
        0 => api_version >= 9,
        // Fetch uses flexible versions from v12+
        1 => api_version >= 12,
        _ => false,
    }
}

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

pub fn read_request_header(buf: &mut Cursor<&[u8]>) -> Result<RequestHeader, std::io::Error> {
    if buf.remaining() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for request header",
        ));
    }

    let api_key = buf.get_i16();
    let api_version = buf.get_i16();
    let correlation_id = buf.get_i32();

    // Read client_id - use compact format for flexible API versions
    let client_id = if uses_flexible_version_for_api(api_key, api_version) {
        read_compact_nullable_string(buf)?
    } else {
        read_nullable_string(buf)?
    };

    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

pub fn write_response_header(buf: &mut BytesMut, correlation_id: i32) {
    buf.put_i32(correlation_id);
}

/// Write response header for flexible versions (v1)
/// This includes the correlation ID and tagged fields
pub fn write_response_header_v1(buf: &mut BytesMut, correlation_id: i32) {
    buf.put_i32(correlation_id);
    // Write empty tagged fields (0 = no tagged fields)
    write_unsigned_varint(buf, 0);
}

/// Write an unsigned variable-length integer (varint)
pub fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// Write an unsigned variable-length integer (varint) to Vec<u8>
pub fn write_unsigned_varint_to_vec(buffer: &mut Vec<u8>, mut value: u32) {
    let original_value = value;
    let start_len = buffer.len();
    while (value & 0xFFFFFF80) != 0 {
        buffer.push(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    buffer.push((value & 0x7F) as u8);

    tracing::info!(
        "write_unsigned_varint: value={}, wrote {} bytes: {:02x?}",
        original_value,
        buffer.len() - start_len,
        &buffer[start_len..]
    );
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn read_string(buf: &mut Cursor<&[u8]>) -> Result<String, std::io::Error> {
    if buf.remaining() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string length",
        ));
    }

    let len = buf.get_i16();
    if len < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Negative string length",
        ));
    }

    let len = len as usize;
    if buf.remaining() < len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string data",
        ));
    }

    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);

    String::from_utf8(bytes)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 string"))
}

pub fn read_nullable_string(buf: &mut Cursor<&[u8]>) -> Result<Option<String>, std::io::Error> {
    if buf.remaining() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string length",
        ));
    }

    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }

    let len = len as usize;
    if buf.remaining() < len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string data",
        ));
    }

    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);

    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 string"))
}

/// Read bytes from buffer
pub fn read_bytes(buf: &mut Cursor<&[u8]>) -> Result<Vec<u8>, std::io::Error> {
    if buf.remaining() < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for byte array length",
        ));
    }

    let len = buf.get_i32();
    if len < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Negative byte array length",
        ));
    }

    let len = len as usize;
    if buf.remaining() < len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for byte array data",
        ));
    }

    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(bytes)
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn write_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_i16(bytes.len() as i16);
    buf.put_slice(bytes);
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn write_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(s) => write_string(buf, s),
        None => buf.put_i16(-1),
    }
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn write_bytes(buf: &mut BytesMut, bytes: &[u8]) {
    buf.put_i32(bytes.len() as i32);
    buf.put_slice(bytes);
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn write_nullable_bytes(buf: &mut BytesMut, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => write_bytes(buf, bytes),
        None => buf.put_i32(-1),
    }
}

// API Keys
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_PRODUCE: i16 = 0;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_FETCH: i16 = 1;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_LIST_OFFSETS: i16 = 2;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_METADATA: i16 = 3;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_OFFSET_COMMIT: i16 = 8;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_OFFSET_FETCH: i16 = 9;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_JOIN_GROUP: i16 = 11;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_HEARTBEAT: i16 = 12;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_LEAVE_GROUP: i16 = 13;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_SYNC_GROUP: i16 = 14;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_DESCRIBE_GROUPS: i16 = 15;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_LIST_GROUPS: i16 = 16;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_DELETE_TOPICS: i16 = 20;

// Error codes
pub const ERROR_NONE: i16 = 0;
pub const ERROR_UNKNOWN: i16 = -1;
pub const ERROR_INVALID_REQUEST: i16 = 42;
#[allow(dead_code)]
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
pub const ERROR_TOPIC_NOT_FOUND: i16 = 3;
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3; // Same as TOPIC_NOT_FOUND
#[allow(dead_code)]
pub const ERROR_INVALID_REQUIRED_ACKS: i16 = 21;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const ERROR_OFFSET_NOT_AVAILABLE: i16 = 78;
pub const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;
pub const ERROR_ILLEGAL_GENERATION: i16 = 22;
#[allow(dead_code)]
pub const ERROR_REBALANCE_IN_PROGRESS: i16 = 27;
#[allow(dead_code)]
pub const ERROR_SESSION_TIMEOUT: i16 = 10;
pub const ERROR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const ERROR_NOT_COORDINATOR: i16 = 16;
#[allow(dead_code)]
pub const ERROR_GROUP_ID_NOT_FOUND: i16 = 69;
pub const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const ERROR_SASL_AUTHENTICATION_FAILED: i16 = 58;
pub const ERROR_TOPIC_ALREADY_EXISTS: i16 = 36;
pub const ERROR_INVALID_TOPIC_EXCEPTION: i16 = 17;
pub const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;

// Compact encoding helper functions

/// Read an unsigned varint from buffer
pub fn read_unsigned_varint(buf: &mut Cursor<&[u8]>) -> Result<u32, std::io::Error> {
    let mut value = 0u32;
    let mut i = 0;
    loop {
        if i > 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Varint too long",
            ));
        }
        if buf.remaining() < 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Not enough bytes for varint",
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

/// Read a signed varint from buffer (zigzag encoded)
#[allow(dead_code)]
pub fn read_signed_varint(buf: &mut Cursor<&[u8]>) -> Result<i32, std::io::Error> {
    let unsigned = read_unsigned_varint(buf)?;
    Ok(((unsigned >> 1) as i32) ^ -((unsigned & 1) as i32))
}

/// Read a compact string from buffer
pub fn read_compact_string(buf: &mut Cursor<&[u8]>) -> Result<String, std::io::Error> {
    let len = read_unsigned_varint(buf)?;
    if len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Null string not expected here",
        ));
    }

    let actual_len = (len - 1) as usize;
    if actual_len == 0 {
        return Ok(String::new());
    }

    if buf.remaining() < actual_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for compact string data",
        ));
    }

    let mut bytes = vec![0u8; actual_len];
    buf.copy_to_slice(&mut bytes);

    String::from_utf8(bytes).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid UTF-8: {e}"),
        )
    })
}

/// Read a nullable compact string from buffer
pub fn read_compact_nullable_string(
    buf: &mut Cursor<&[u8]>,
) -> Result<Option<String>, std::io::Error> {
    let len = read_unsigned_varint(buf)?;
    if len == 0 {
        return Ok(None);
    }

    let actual_len = (len - 1) as usize;
    if actual_len == 0 {
        return Ok(Some(String::new()));
    }

    if buf.remaining() < actual_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for compact string data",
        ));
    }

    let mut bytes = vec![0u8; actual_len];
    buf.copy_to_slice(&mut bytes);

    String::from_utf8(bytes).map(Some).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid UTF-8: {e}"),
        )
    })
}

/// Read compact bytes from buffer
pub fn read_compact_bytes(buf: &mut Cursor<&[u8]>) -> Result<Vec<u8>, std::io::Error> {
    let len = read_unsigned_varint(buf)?;
    if len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Null bytes not expected here",
        ));
    }

    let actual_len = (len - 1) as usize;
    if actual_len == 0 {
        return Ok(Vec::new());
    }

    if buf.remaining() < actual_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for compact bytes data",
        ));
    }

    let mut bytes = vec![0u8; actual_len];
    buf.copy_to_slice(&mut bytes);
    Ok(bytes)
}

/// Read nullable compact bytes from buffer
#[allow(dead_code)]
pub fn read_compact_nullable_bytes(
    buf: &mut Cursor<&[u8]>,
) -> Result<Option<Vec<u8>>, std::io::Error> {
    let len = read_unsigned_varint(buf)?;
    if len == 0 {
        return Ok(None);
    }

    let actual_len = (len - 1) as usize;
    if actual_len == 0 {
        return Ok(Some(Vec::new()));
    }

    if buf.remaining() < actual_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for compact bytes data",
        ));
    }

    let mut bytes = vec![0u8; actual_len];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(bytes))
}

/// Write a compact string to buffer
pub fn write_compact_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    // Compact strings use length + 1 (0 means null, 1 means empty string)
    write_unsigned_varint(buf, (bytes.len() + 1) as u32);
    buf.put_slice(bytes);
}

/// Write a nullable compact string to buffer
pub fn write_compact_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(s) => write_compact_string(buf, s),
        None => write_unsigned_varint(buf, 0),
    }
}

/// Write compact bytes to buffer
pub fn write_compact_bytes(buf: &mut BytesMut, bytes: &[u8]) {
    // Compact bytes use length + 1 (0 means null)
    write_unsigned_varint(buf, (bytes.len() + 1) as u32);
    buf.put_slice(bytes);
}

/// Write nullable compact bytes to buffer
#[allow(dead_code)]
pub fn write_compact_nullable_bytes(buf: &mut BytesMut, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => write_compact_bytes(buf, bytes),
        None => write_unsigned_varint(buf, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::io::Cursor;

    #[test]
    fn test_response_header_v1_format() {
        // Test writing response header v1
        let mut buf = BytesMut::new();
        let correlation_id = 12345;

        write_response_header_v1(&mut buf, correlation_id);

        // Verify the written data
        let mut cursor = Cursor::new(&buf[..]);

        // First 4 bytes should be the correlation ID
        let read_correlation_id = cursor.get_i32();
        assert_eq!(read_correlation_id, correlation_id);

        // Next should be the tagged fields (0 = no tagged fields)
        let tagged_field_byte = cursor.get_u8();
        assert_eq!(tagged_field_byte, 0);

        // Total size should be 5 bytes (4 for correlation ID + 1 for empty tagged fields)
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn test_unsigned_varint_encoding() {
        // Test various varint encodings
        let test_cases = vec![
            (0u32, vec![0x00]),
            (1u32, vec![0x01]),
            (127u32, vec![0x7F]),
            (128u32, vec![0x80, 0x01]),
            (300u32, vec![0xAC, 0x02]),
            (16383u32, vec![0xFF, 0x7F]),
            (16384u32, vec![0x80, 0x80, 0x01]),
        ];

        for (value, expected_bytes) in test_cases {
            let mut buf = BytesMut::new();
            write_unsigned_varint(&mut buf, value);
            assert_eq!(buf.to_vec(), expected_bytes, "Failed for value {value}");
        }
    }

    #[test]
    fn test_read_request_header() {
        let mut buf = BytesMut::new();
        buf.put_i16(3); // API key (metadata)
        buf.put_i16(0); // API version
        buf.put_i32(123); // Correlation ID
        buf.put_i16(11); // Client ID length
        buf.put_slice(b"test-client"); // Client ID

        let mut cursor = Cursor::new(&buf[..]);
        let header = read_request_header(&mut cursor).unwrap();

        assert_eq!(header.api_key, 3);
        assert_eq!(header.api_version, 0);
        assert_eq!(header.correlation_id, 123);
        assert_eq!(header.client_id, Some("test-client".to_string()));
    }

    #[test]
    fn test_read_request_header_null_client_id() {
        let mut buf = BytesMut::new();
        buf.put_i16(1); // API key
        buf.put_i16(0); // API version
        buf.put_i32(456); // Correlation ID
        buf.put_i16(-1); // Null client ID

        let mut cursor = Cursor::new(&buf[..]);
        let header = read_request_header(&mut cursor).unwrap();

        assert_eq!(header.api_key, 1);
        assert_eq!(header.api_version, 0);
        assert_eq!(header.correlation_id, 456);
        assert_eq!(header.client_id, None);
    }

    #[test]
    fn test_read_request_header_insufficient_data() {
        let mut buf = BytesMut::new();
        buf.put_i16(1); // API key
        buf.put_i16(0); // API version
        // Missing correlation ID and client ID

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_request_header(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_response_header() {
        let mut buf = BytesMut::new();
        write_response_header(&mut buf, 789);

        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(cursor.get_i32(), 789);
    }

    #[test]
    fn test_read_write_string() {
        let test_str = "hello kafka";
        let mut buf = BytesMut::new();
        write_string(&mut buf, test_str);

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_string(&mut cursor).unwrap();
        assert_eq!(result, test_str);
    }

    #[test]
    fn test_read_write_nullable_string() {
        // Test with Some value
        let mut buf = BytesMut::new();
        write_nullable_string(&mut buf, Some("test"));

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_nullable_string(&mut cursor).unwrap();
        assert_eq!(result, Some("test".to_string()));

        // Test with None
        let mut buf = BytesMut::new();
        write_nullable_string(&mut buf, None);

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_nullable_string(&mut cursor).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_write_bytes() {
        let test_bytes = b"binary data";
        let mut buf = BytesMut::new();
        write_bytes(&mut buf, test_bytes);

        let mut cursor = Cursor::new(&buf[..]);
        let len = cursor.get_i32();
        assert_eq!(len, test_bytes.len() as i32);

        let mut data = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut data);
        assert_eq!(&data[..], test_bytes);
    }

    #[test]
    fn test_write_nullable_bytes() {
        // Test with Some value
        let mut buf = BytesMut::new();
        write_nullable_bytes(&mut buf, Some(b"data"));

        let mut cursor = Cursor::new(&buf[..]);
        let len = cursor.get_i32();
        assert_eq!(len, 4);

        // Test with None
        let mut buf = BytesMut::new();
        write_nullable_bytes(&mut buf, None);

        let mut cursor = Cursor::new(&buf[..]);
        let len = cursor.get_i32();
        assert_eq!(len, -1);
    }

    #[test]
    fn test_signed_varint_encoding() {
        // Test zigzag encoding/decoding
        let test_cases = vec![
            (0i32, 0u32),
            (-1i32, 1u32),
            (1i32, 2u32),
            (-2i32, 3u32),
            (2i32, 4u32),
            (i32::MAX, 4294967294u32),
            (i32::MIN, 4294967295u32),
        ];

        for (signed, unsigned) in test_cases {
            let mut buf = BytesMut::new();
            write_unsigned_varint(&mut buf, unsigned);

            let mut cursor = Cursor::new(&buf[..]);
            let decoded = read_signed_varint(&mut cursor).unwrap();
            assert_eq!(decoded, signed, "Failed for signed value {signed}");
        }
    }

    #[test]
    fn test_compact_string_encoding() {
        // Test normal string
        let mut buf = BytesMut::new();
        write_compact_string(&mut buf, "hello");

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_string(&mut cursor).unwrap();
        assert_eq!(result, "hello");

        // Test empty string
        let mut buf = BytesMut::new();
        write_compact_string(&mut buf, "");

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_string(&mut cursor).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_compact_nullable_string_encoding() {
        // Test Some value
        let mut buf = BytesMut::new();
        write_compact_nullable_string(&mut buf, Some("test"));

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_nullable_string(&mut cursor).unwrap();
        assert_eq!(result, Some("test".to_string()));

        // Test None
        let mut buf = BytesMut::new();
        write_compact_nullable_string(&mut buf, None);

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_nullable_string(&mut cursor).unwrap();
        assert_eq!(result, None);

        // Test empty string
        let mut buf = BytesMut::new();
        write_compact_nullable_string(&mut buf, Some(""));

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_nullable_string(&mut cursor).unwrap();
        assert_eq!(result, Some(String::new()));
    }

    #[test]
    fn test_compact_bytes_encoding() {
        // Test normal bytes
        let mut buf = BytesMut::new();
        write_compact_bytes(&mut buf, b"binary");

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_bytes(&mut cursor).unwrap();
        assert_eq!(result, b"binary");

        // Test empty bytes
        let mut buf = BytesMut::new();
        write_compact_bytes(&mut buf, b"");

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_bytes(&mut cursor).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_compact_nullable_bytes_encoding() {
        // Test Some value
        let mut buf = BytesMut::new();
        write_compact_nullable_bytes(&mut buf, Some(b"data"));

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_nullable_bytes(&mut cursor).unwrap();
        assert_eq!(result, Some(b"data".to_vec()));

        // Test None
        let mut buf = BytesMut::new();
        write_compact_nullable_bytes(&mut buf, None);

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_compact_nullable_bytes(&mut cursor).unwrap();
        assert_eq!(result, None);
    }
}
