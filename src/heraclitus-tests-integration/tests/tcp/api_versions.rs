use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_api_versions_request() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-api-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    // Test 1: Send API versions request (v0)
    let response = send_api_versions_request(&mut stream, 0).await?;

    // Parse the response
    let mut cursor = Cursor::new(&response[..]);
    Buf::advance(&mut cursor, 4); // Skip correlation ID

    let error_code = cursor.get_i16();
    println!("API versions response error code: {error_code}");
    assert_eq!(error_code, 0, "API versions request should succeed");

    let api_count = cursor.get_i32();
    println!("Number of supported APIs: {api_count}");
    assert!(api_count > 0, "Should support at least one API");

    // Parse API entries
    for i in 0..api_count {
        let api_key = cursor.get_i16();
        let min_version = cursor.get_i16();
        let max_version = cursor.get_i16();
        println!("API {i}: key={api_key}, min_version={min_version}, max_version={max_version}");
    }

    // Test 2: Send API versions request (v3) with client info
    let response = send_api_versions_request_v3(&mut stream).await?;

    // Parse the response
    let mut cursor = Cursor::new(&response[..]);
    Buf::advance(&mut cursor, 4); // Skip correlation ID

    let error_code = cursor.get_i16();
    println!("API versions v3 response error code: {error_code}");
    assert_eq!(error_code, 0, "API versions v3 request should succeed");

    Ok(())
}

async fn send_api_versions_request(stream: &mut TcpStream, version: i16) -> Result<Vec<u8>> {
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(18); // ApiVersions API
    request.put_i16(version); // Version
    request.put_i32(1); // Correlation ID
    request.put_i16(-1); // No client ID

    // Request body is empty for v0-2

    // Send request with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read response
    read_response(stream).await
}

async fn send_api_versions_request_v3(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(18); // ApiVersions API
    request.put_i16(3); // Version 3
    request.put_i32(2); // Correlation ID
    request.put_i16(-1); // No client ID

    // Request body for v3
    // Client software name (compact string)
    let name = "heraclitus-test";
    write_compact_string(&mut request, Some(name));

    // Client software version (compact string)
    let version = "0.1.0";
    write_compact_string(&mut request, Some(version));

    // Tagged fields (empty)
    write_unsigned_varint(&mut request, 0);

    // Send request with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read response
    read_response(stream).await
}

async fn read_response(stream: &mut TcpStream) -> Result<Vec<u8>> {
    // Read length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = i32::from_be_bytes(len_buf) as usize;

    // Read response body
    let mut response = vec![0u8; len];
    stream.read_exact(&mut response).await?;

    Ok(response)
}

fn write_compact_string(buffer: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(s) => {
            let bytes = s.as_bytes();
            write_unsigned_varint(buffer, (bytes.len() + 1) as u32);
            buffer.extend_from_slice(bytes);
        }
        None => {
            write_unsigned_varint(buffer, 0);
        }
    }
}

fn write_unsigned_varint(buffer: &mut BytesMut, mut value: u32) {
    while (value & 0xFFFFFF80) != 0 {
        buffer.put_u8(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    buffer.put_u8((value & 0x7F) as u8);
}
