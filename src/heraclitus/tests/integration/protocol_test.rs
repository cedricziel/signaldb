// Kafka protocol integration tests

use super::helpers::HeraclitusTestContext;
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::{
    ApiKey, CreateTopicsRequest, RequestHeader, create_topics_request::CreatableTopic,
};
use kafka_protocol::protocol::Encodable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Test produce protocol with various configurations
#[tokio::test]
async fn test_produce_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Create produce request (API key 0, version 8)
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(0); // API key for Produce
    request.put_i16(8); // API version
    request.put_i32(90); // Correlation ID
    request.put_i16(0); // Client ID length (empty)

    // Request body
    request.put_i16(-1); // Transactional ID (null)
    request.put_i16(1); // Acks
    request.put_i32(30000); // Timeout
    request.put_i32(1); // Number of topics

    // Topic data
    request.put_i16(13); // Topic name length
    request.put_slice(b"protocol-test"); // Topic name
    request.put_i32(1); // Number of partitions

    // Partition data
    request.put_i32(0); // Partition index

    // Create a simple record batch
    let mut record_batch = BytesMut::new();

    // Record batch header
    record_batch.put_i64(0); // Base offset
    record_batch.put_i32(0); // Batch length (placeholder)
    record_batch.put_i32(0); // Partition leader epoch
    record_batch.put_i8(2); // Magic byte (v2)
    record_batch.put_i32(0); // CRC (placeholder)
    record_batch.put_i16(0); // Attributes
    record_batch.put_i32(0); // Last offset delta
    record_batch.put_i64(0); // First timestamp
    record_batch.put_i64(0); // Max timestamp
    record_batch.put_i64(-1); // Producer ID
    record_batch.put_i16(-1); // Producer epoch
    record_batch.put_i32(-1); // Base sequence
    record_batch.put_i32(1); // Number of records

    // Simple record
    record_batch.put_i8(0); // Length (varint)
    record_batch.put_i8(0); // Attributes
    record_batch.put_i8(0); // Timestamp delta (varint)
    record_batch.put_i8(0); // Offset delta (varint)
    record_batch.put_i8(4); // Key length (varint) - "test"
    record_batch.put_slice(b"test"); // Key
    record_batch.put_i8(12); // Value length (varint) - "test message"
    record_batch.put_slice(b"test message"); // Value
    record_batch.put_i8(0); // Headers array length (varint)

    // Update batch length
    let batch_len = record_batch.len() as i32 - 12; // Exclude offset and length fields
    record_batch[8..12].copy_from_slice(&batch_len.to_be_bytes());

    // Add record batch to request
    request.put_i32(record_batch.len() as i32);
    request.extend_from_slice(&record_batch);

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
        "Response should contain produce response"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 90, "Correlation ID should match request");

    println!("✓ Produce request/response working correctly");
    Ok(())
}

/// Test fetch protocol with various configurations
#[tokio::test]
async fn test_fetch_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // First produce a message
    produce_test_message(&mut stream, "fetch-test", "test-key", "test-value").await?;

    // Create fetch request (API key 1, version 11)
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(1); // API key for Fetch
    request.put_i16(11); // API version
    request.put_i32(91); // Correlation ID
    request.put_i16(0); // Client ID length (empty)

    // Request body
    request.put_i32(-1); // Replica ID
    request.put_i32(100); // Max wait time
    request.put_i32(1); // Min bytes
    request.put_i32(1048576); // Max bytes
    request.put_i8(0); // Isolation level
    request.put_i32(0); // Session ID
    request.put_i32(-1); // Session epoch
    request.put_i32(1); // Number of topics

    // Topic data
    request.put_i16(10); // Topic name length
    request.put_slice(b"fetch-test"); // Topic name
    request.put_i32(1); // Number of partitions

    // Partition data
    request.put_i32(0); // Partition index
    request.put_i16(-1); // Current leader epoch
    request.put_i64(0); // Fetch offset
    request.put_i64(-1); // Log start offset
    request.put_i32(1048576); // Partition max bytes

    request.put_i32(0); // Forgotten topics count
    request.put_i16(0); // Rack ID (null)

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
        response_length > 20,
        "Response should contain fetch response"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 91, "Correlation ID should match request");

    println!("✓ Fetch request/response working correctly");
    Ok(())
}

/// Test list offsets protocol
#[tokio::test]
async fn test_list_offsets_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Create ListOffsets request (API key 2, version 5)
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(2); // API key for ListOffsets
    request.put_i16(5); // API version
    request.put_i32(92); // Correlation ID
    request.put_i16(0); // Client ID length (empty)

    // Request body
    request.put_i32(-1); // Replica ID
    request.put_i8(0); // Isolation level
    request.put_i32(1); // Number of topics

    // Topic data
    request.put_i16(11); // Topic name length
    request.put_slice(b"offset-test"); // Topic name
    request.put_i32(1); // Number of partitions

    // Partition data
    request.put_i32(0); // Partition index
    request.put_i16(-1); // Current leader epoch
    request.put_i64(-2); // Timestamp (-2 = earliest)

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
        "Response should contain list offsets response"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 92, "Correlation ID should match request");

    println!("✓ ListOffsets request/response working correctly");
    Ok(())
}

/// Test create topics protocol
#[tokio::test]
async fn test_create_topics_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Create CreateTopics request using kafka-protocol
    let mut buf = BytesMut::new();

    // Create request header
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::CreateTopics as i16)
        .with_request_api_version(2) // Use version 2 which is simpler than v5
        .with_correlation_id(93)
        .with_client_id(None);

    // Encode header (version 1 for CreateTopics v2)
    header.encode(&mut buf, 1)?;

    // Create and encode request body
    let topic = CreatableTopic::default()
        .with_name(kafka_protocol::messages::TopicName::from(
            kafka_protocol::protocol::StrBytes::from_static_str("create-test"),
        ))
        .with_num_partitions(3)
        .with_replication_factor(1)
        .with_assignments(vec![])
        .with_configs(vec![]);

    let request = CreateTopicsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(30000)
        .with_validate_only(false);

    request.encode(&mut buf, 2)?;

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
        response_length > 10,
        "Response should contain create topics response"
    );

    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 93, "Correlation ID should match request");

    println!("✓ CreateTopics request/response working correctly");
    Ok(())
}

// Helper function to produce a test message
async fn produce_test_message(
    stream: &mut TcpStream,
    topic: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(0); // API key for Produce
    request.put_i16(8); // API version
    request.put_i32(999); // Correlation ID
    request.put_i16(0); // Client ID length (empty)

    // Request body
    request.put_i16(-1); // Transactional ID (null)
    request.put_i16(1); // Acks
    request.put_i32(30000); // Timeout
    request.put_i32(1); // Number of topics

    // Topic data
    request.put_i16(topic.len() as i16);
    request.put_slice(topic.as_bytes());
    request.put_i32(1); // Number of partitions

    // Partition data
    request.put_i32(0); // Partition index

    // Create a simple record batch
    let mut record_batch = BytesMut::new();

    // Record batch header
    record_batch.put_i64(0); // Base offset
    record_batch.put_i32(0); // Batch length (placeholder)
    record_batch.put_i32(0); // Partition leader epoch
    record_batch.put_i8(2); // Magic byte (v2)
    record_batch.put_i32(0); // CRC (placeholder)
    record_batch.put_i16(0); // Attributes
    record_batch.put_i32(0); // Last offset delta
    record_batch.put_i64(0); // First timestamp
    record_batch.put_i64(0); // Max timestamp
    record_batch.put_i64(-1); // Producer ID
    record_batch.put_i16(-1); // Producer epoch
    record_batch.put_i32(-1); // Base sequence
    record_batch.put_i32(1); // Number of records

    // Simple record
    record_batch.put_i8(0); // Length (varint)
    record_batch.put_i8(0); // Attributes
    record_batch.put_i8(0); // Timestamp delta (varint)
    record_batch.put_i8(0); // Offset delta (varint)
    record_batch.put_i8(key.len() as i8); // Key length (varint)
    record_batch.put_slice(key.as_bytes()); // Key
    record_batch.put_i8(value.len() as i8); // Value length (varint)
    record_batch.put_slice(value.as_bytes()); // Value
    record_batch.put_i8(0); // Headers array length (varint)

    // Update batch length
    let batch_len = record_batch.len() as i32 - 12; // Exclude offset and length fields
    record_batch[8..12].copy_from_slice(&batch_len.to_be_bytes());

    // Add record batch to request
    request.put_i32(record_batch.len() as i32);
    request.extend_from_slice(&record_batch);

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

    Ok(())
}
