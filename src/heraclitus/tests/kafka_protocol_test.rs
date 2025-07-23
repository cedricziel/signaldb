#[cfg(test)]
mod protocol_tests {
    use bytes::{Buf, BufMut, BytesMut};
    use std::io::Cursor;

    #[test]
    fn test_kafka_frame_format() {
        // Test that we can construct and parse a basic Kafka frame
        let mut frame = BytesMut::new();

        // Frame: length prefix + data
        let data = b"test data";
        frame.put_i32(data.len() as i32);
        frame.put_slice(data);

        // Parse it back
        let mut cursor = Cursor::new(&frame[..]);
        let length = cursor.get_i32() as usize;
        assert_eq!(length, data.len());

        let mut read_data = vec![0u8; length];
        cursor.copy_to_slice(&mut read_data);
        assert_eq!(&read_data[..], data);
    }

    #[test]
    fn test_metadata_request_encoding() {
        // Test encoding a metadata request
        let mut request = BytesMut::new();

        // Request header
        request.put_i16(3); // Metadata API
        request.put_i16(0); // Version 0
        request.put_i32(123); // Correlation ID
        request.put_i16(4); // Client ID length
        request.put_slice(b"test"); // Client ID

        // Request body - all topics (null array)
        request.put_i32(-1);

        // Verify size and basic structure
        assert!(request.len() > 14); // Header is at least 14 bytes
    }

    #[test]
    fn test_error_response_format() {
        // Test error response format
        let mut response = BytesMut::new();

        // Response header
        response.put_i32(456); // Correlation ID

        // Error code
        response.put_i16(35); // UNSUPPORTED_VERSION

        assert_eq!(response.len(), 6); // 4 bytes correlation + 2 bytes error
    }
}
