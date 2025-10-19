use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use kafka_protocol::messages::{ApiVersionsRequest, RequestHeader};
use kafka_protocol::protocol::{Encodable, StrBytes, encode_request_header_into_buffer};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_multiple_requests_with_delays() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-debug-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    println!("✓ Heraclitus started on port {}", kafka_port);

    // Connect to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;
    println!("✓ Connected to Heraclitus");

    // Test 1: Send first API versions request
    println!("\n=== Sending first API versions request ===");
    send_api_versions_request(&mut stream, 1).await?;
    println!("✓ First request sent");

    let response1 = read_response(&mut stream).await?;
    println!("✓ First response received: {} bytes", response1.len());

    // Parse first response
    let mut cursor = Cursor::new(&response1[..]);
    Buf::advance(&mut cursor, 4); // Skip correlation ID
    let error_code = cursor.get_i16();
    println!("  Error code: {}", error_code);
    assert_eq!(error_code, 0);

    // Removed delay - send second request immediately
    println!("\n=== Sending second request immediately ===");

    // Test 2: Send API versions v3 request (with client info)
    println!("\n=== Sending API versions v3 request ===");
    send_api_versions_request_v3(&mut stream, 2).await?;
    println!("✓ v3 request sent");

    let response2 = match read_response(&mut stream).await {
        Ok(response) => {
            println!("✓ v3 response received: {} bytes", response.len());
            response
        }
        Err(e) => {
            println!("✗ Failed to read v3 response: {}", e);
            println!("\n=== Heraclitus Server Logs (Last 50 lines) ===");
            _heraclitus.print_tail(50);
            println!("\n=== Searching for errors/failures ===");
            let mut errors = _heraclitus.grep_all("error");
            errors.extend(_heraclitus.grep_all("ERROR"));
            errors.extend(_heraclitus.grep_all("Failed"));
            for error_line in errors {
                println!("  {}", error_line);
            }
            return Err(e);
        }
    };

    // Parse second response
    let mut cursor = Cursor::new(&response2[..]);
    Buf::advance(&mut cursor, 4); // Skip correlation ID
    let error_code = cursor.get_i16();
    println!("  Error code: {}", error_code);
    assert_eq!(error_code, 0);

    println!("\n✓ Test passed!");
    Ok(())
}

async fn send_api_versions_request(stream: &mut TcpStream, correlation_id: i32) -> Result<()> {
    // Create request header using builder pattern
    let header = RequestHeader::default()
        .with_request_api_key(18) // ApiVersions
        .with_request_api_version(0)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    // Create request body (empty for v0)
    let request = ApiVersionsRequest::default();

    // Encode to buffer
    let mut request_buf = BytesMut::new();

    // Encode header (this uses the correct header version based on API version)
    encode_request_header_into_buffer(&mut request_buf, &header)?;

    // Encode body (empty for v0)
    request.encode(&mut request_buf, 0)?;

    // Send request with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request_buf.len() as i32);
    frame.extend_from_slice(&request_buf);

    println!("  Sending {} bytes...", frame.len());
    stream.write_all(&frame).await?;
    stream.flush().await?;
    println!("  Flushed");

    Ok(())
}

async fn send_api_versions_request_v3(stream: &mut TcpStream, correlation_id: i32) -> Result<()> {
    // Create request header using builder pattern
    let header = RequestHeader::default()
        .with_request_api_key(18) // ApiVersions
        .with_request_api_version(3)
        .with_correlation_id(correlation_id)
        .with_client_id(None);

    // Create request body using builder pattern
    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("heraclitus-test"))
        .with_client_software_version(StrBytes::from_static_str("0.1.0"));

    // Encode to buffer
    let mut request_buf = BytesMut::new();

    // Encode header (this uses the correct header version based on API version)
    encode_request_header_into_buffer(&mut request_buf, &header)?;

    // Encode body
    request.encode(&mut request_buf, 3)?;

    // Send request with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request_buf.len() as i32);
    frame.extend_from_slice(&request_buf);

    println!("  Sending {} bytes...", frame.len());
    stream.write_all(&frame).await?;
    stream.flush().await?;
    println!("  Flushed");

    Ok(())
}

async fn read_response(stream: &mut TcpStream) -> Result<Vec<u8>> {
    println!("  Reading length prefix...");
    // Read length prefix
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {
            let len = i32::from_be_bytes(len_buf) as usize;
            println!("  Response length: {} bytes", len);

            // Read response body
            let mut response = vec![0u8; len];
            stream.read_exact(&mut response).await?;
            println!("  Response body read");

            Ok(response)
        }
        Err(e) => {
            println!("  ERROR reading length prefix: {}", e);
            Err(e.into())
        }
    }
}
