// Basic Kafka functionality integration tests

use super::helpers::HeraclitusTestContext;
use anyhow::Result;
use bytes::BytesMut;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, MetadataRequest, RequestHeader};
use kafka_protocol::protocol::Encodable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Test API versions request/response
#[tokio::test]
async fn test_api_versions_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Create API versions request using kafka-protocol
    let mut buf = BytesMut::new();

    // Create request header
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ApiVersions as i16)
        .with_request_api_version(0)
        .with_correlation_id(42)
        .with_client_id(None);

    // Encode header (version 1 for API versions v0)
    header.encode(&mut buf, 1)?;

    // Create and encode request body (empty for v0)
    let request = ApiVersionsRequest::default();
    request.encode(&mut buf, 0)?;

    // Create frame with length prefix
    let mut frame = BytesMut::new();
    frame.extend_from_slice(&(buf.len() as i32).to_be_bytes());
    frame.extend_from_slice(&buf);

    // Send request
    stream.write_all(&frame).await?;

    // Read response
    let mut length_buf = [0u8; 4];
    stream.read_exact(&mut length_buf).await?;
    let response_length = i32::from_be_bytes(length_buf) as usize;

    let mut response_buf = vec![0u8; response_length];
    stream.read_exact(&mut response_buf).await?;

    // Basic validation
    assert!(
        response_length > 4,
        "Response should be longer than just correlation ID"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 42, "Correlation ID should match request");

    println!("✓ API versions request/response working correctly");
    Ok(())
}

/// Test metadata request/response
#[tokio::test]
async fn test_metadata_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Create metadata request using kafka-protocol
    let mut buf = BytesMut::new();

    // Create request header
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(1) // Use version 1 which is simpler
        .with_correlation_id(43)
        .with_client_id(None);

    // Encode header (version 1 for metadata v1)
    header.encode(&mut buf, 1)?;

    // Create and encode request body
    let request = MetadataRequest::default().with_topics(None); // None means all topics
    request.encode(&mut buf, 1)?;

    // Create frame with length prefix
    let mut frame = BytesMut::new();
    frame.extend_from_slice(&(buf.len() as i32).to_be_bytes());
    frame.extend_from_slice(&buf);

    // Send request
    stream.write_all(&frame).await?;

    // Read response
    let mut length_buf = [0u8; 4];
    stream.read_exact(&mut length_buf).await?;
    let response_length = i32::from_be_bytes(length_buf) as usize;

    let mut response_buf = vec![0u8; response_length];
    stream.read_exact(&mut response_buf).await?;

    // Basic validation
    assert!(response_length > 10, "Response should contain metadata");

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 43, "Correlation ID should match request");

    println!("✓ Metadata request/response working correctly");
    Ok(())
}

/// Test multiple sequential requests on same connection
#[tokio::test]
async fn test_connection_reuse() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Send multiple API versions requests
    for i in 0..3 {
        let mut buf = BytesMut::new();

        // Create request header
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(0)
            .with_correlation_id(100 + i)
            .with_client_id(None);

        // Encode header (version 1)
        header.encode(&mut buf, 1)?;

        // Create and encode request body
        let request = ApiVersionsRequest::default();
        request.encode(&mut buf, 0)?;

        // Create frame with length prefix
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&(buf.len() as i32).to_be_bytes());
        frame.extend_from_slice(&buf);

        // Send request
        stream.write_all(&frame).await?;

        // Read response
        let mut length_buf = [0u8; 4];
        stream.read_exact(&mut length_buf).await?;
        let response_length = i32::from_be_bytes(length_buf) as usize;

        let mut response_buf = vec![0u8; response_length];
        stream.read_exact(&mut response_buf).await?;

        // Validate correlation ID
        let correlation_id = i32::from_be_bytes([
            response_buf[0],
            response_buf[1],
            response_buf[2],
            response_buf[3],
        ]);
        assert_eq!(
            correlation_id,
            100 + i,
            "Correlation ID should match request {i}"
        );
    }

    println!("✓ Connection reuse working correctly");
    Ok(())
}

/// Test graceful error handling
#[tokio::test]
async fn test_error_handling() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Send request with unsupported API version
    let mut buf = BytesMut::new();

    // Create request header with unsupported version
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(999) // Unsupported version
        .with_correlation_id(999)
        .with_client_id(None);

    // Encode header with version 1
    header.encode(&mut buf, 1)?;

    // Create frame with length prefix
    let mut frame = BytesMut::new();
    frame.extend_from_slice(&(buf.len() as i32).to_be_bytes());
    frame.extend_from_slice(&buf);

    // Send request
    stream.write_all(&frame).await?;

    // Try to read response - connection might close or return error
    let mut length_buf = [0u8; 4];
    match stream.read_exact(&mut length_buf).await {
        Ok(_) => {
            let response_length = i32::from_be_bytes(length_buf) as usize;
            if response_length > 0 && response_length < 1000 {
                let mut response_buf = vec![0u8; response_length];
                if stream.read_exact(&mut response_buf).await.is_ok() {
                    println!("✓ Server returned error response gracefully");
                }
            }
        }
        Err(_) => {
            println!("✓ Server closed connection on invalid request (acceptable)");
        }
    }

    Ok(())
}
