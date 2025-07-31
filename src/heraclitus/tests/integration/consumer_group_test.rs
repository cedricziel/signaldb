// Consumer group functionality integration tests

use super::helpers::HeraclitusTestContext;
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Test join group protocol
#[tokio::test]
async fn test_join_group_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Send FindCoordinator request (API key 10, version 2)
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(10); // API key for FindCoordinator
    request.put_i16(2); // API version
    request.put_i32(50); // Correlation ID
    request.put_i16(0); // Client ID length (empty)

    // Request body
    request.put_i16(5); // Key length
    request.put_slice(b"group"); // Key
    request.put_i8(0); // Key type (group)

    // Wrap with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

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
        response_length > 10,
        "Response should contain coordinator info"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 50, "Correlation ID should match request");

    println!("✓ FindCoordinator request/response working correctly");
    Ok(())
}

/// Test consumer group membership
#[tokio::test]
async fn test_group_membership() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Send JoinGroup request (API key 11, version 5)
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(11); // API key for JoinGroup
    request.put_i16(5); // API version
    request.put_i32(60); // Correlation ID
    request.put_i16(0); // Client ID length (empty)

    // Request body
    request.put_i16(10); // Group ID length
    request.put_slice(b"test-group"); // Group ID
    request.put_i32(30000); // Session timeout
    request.put_i32(3000); // Rebalance timeout
    request.put_i16(6); // Member ID length
    request.put_slice(b"member"); // Member ID
    request.put_i16(0); // Group instance ID (null)
    request.put_i16(8); // Protocol type length
    request.put_slice(b"consumer"); // Protocol type
    request.put_i32(0); // Number of protocols (empty for now)

    // Wrap with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

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
        response_length > 10,
        "Response should contain join group response"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 60, "Correlation ID should match request");

    println!("✓ JoinGroup request/response working correctly");
    Ok(())
}

/// Test heartbeat mechanism
#[tokio::test]
async fn test_heartbeat_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Send Heartbeat request (API key 12, version 3)
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(12); // API key for Heartbeat
    request.put_i16(3); // API version
    request.put_i32(70); // Correlation ID
    request.put_i16(0); // Client ID length (empty)

    // Request body
    request.put_i16(10); // Group ID length
    request.put_slice(b"test-group"); // Group ID
    request.put_i32(1); // Generation ID
    request.put_i16(8); // Member ID length
    request.put_slice(b"member-1"); // Member ID
    request.put_i16(0); // Group instance ID (null)

    // Wrap with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

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
        response_length >= 6,
        "Response should contain heartbeat response"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 70, "Correlation ID should match request");

    println!("✓ Heartbeat request/response working correctly");
    Ok(())
}

/// Test offset commit and fetch
#[tokio::test]
async fn test_offset_management() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Send OffsetCommit request (API key 8, version 7)
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(8); // API key for OffsetCommit
    request.put_i16(7); // API version
    request.put_i32(80); // Correlation ID
    request.put_i16(0); // Client ID length (empty)

    // Request body
    request.put_i16(10); // Group ID length
    request.put_slice(b"test-group"); // Group ID
    request.put_i32(1); // Generation ID
    request.put_i16(8); // Member ID length
    request.put_slice(b"member-1"); // Member ID
    request.put_i16(0); // Group instance ID (null)
    request.put_i32(1); // Number of topics

    // Topic data
    request.put_i16(10); // Topic name length
    request.put_slice(b"test-topic"); // Topic name
    request.put_i32(1); // Number of partitions

    // Partition data
    request.put_i32(0); // Partition index
    request.put_i64(100); // Committed offset
    request.put_i32(-1); // Committed leader epoch
    request.put_i16(0); // Metadata (null)

    // Wrap with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

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
        response_length > 10,
        "Response should contain offset commit response"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 80, "Correlation ID should match request");

    println!("✓ OffsetCommit request/response working correctly");

    // Now test OffsetFetch
    let mut stream2 = TcpStream::connect(&context.kafka_addr()).await?;

    // Send OffsetFetch request (API key 9, version 5)
    let mut fetch_request = BytesMut::new();

    // Request header
    fetch_request.put_i16(9); // API key for OffsetFetch
    fetch_request.put_i16(5); // API version
    fetch_request.put_i32(81); // Correlation ID
    fetch_request.put_i16(0); // Client ID length (empty)

    // Request body
    fetch_request.put_i16(10); // Group ID length
    fetch_request.put_slice(b"test-group"); // Group ID
    fetch_request.put_i32(1); // Number of topics

    // Topic data
    fetch_request.put_i16(10); // Topic name length
    fetch_request.put_slice(b"test-topic"); // Topic name
    fetch_request.put_i32(1); // Number of partitions
    fetch_request.put_i32(0); // Partition index

    // Wrap with length prefix
    let mut fetch_frame = BytesMut::new();
    fetch_frame.put_i32(fetch_request.len() as i32);
    fetch_frame.extend_from_slice(&fetch_request);

    // Send request
    stream2.write_all(&fetch_frame).await?;

    // Read response
    let mut fetch_length_buf = [0u8; 4];
    stream2.read_exact(&mut fetch_length_buf).await?;
    let fetch_response_length = i32::from_be_bytes(fetch_length_buf) as usize;

    let mut fetch_response_buf = vec![0u8; fetch_response_length];
    stream2.read_exact(&mut fetch_response_buf).await?;

    // Basic validation
    assert!(
        fetch_response_length > 10,
        "Response should contain offset fetch response"
    );

    let fetch_correlation_id = i32::from_be_bytes([
        fetch_response_buf[0],
        fetch_response_buf[1],
        fetch_response_buf[2],
        fetch_response_buf[3],
    ]);
    assert_eq!(
        fetch_correlation_id, 81,
        "Correlation ID should match request"
    );

    println!("✓ OffsetFetch request/response working correctly");
    Ok(())
}
