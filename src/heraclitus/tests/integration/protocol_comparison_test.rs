// Protocol comparison tests - verify Heraclitus protocol compliance

use super::helpers::HeraclitusTestContext;
use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Test API versions protocol compliance
#[tokio::test]
async fn test_api_versions_protocol_compliance() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Connect to Heraclitus
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

    // Create API versions request (v0)
    let mut request = BytesMut::new();
    request.put_i16(18); // API key
    request.put_i16(0); // Version 0
    request.put_i32(42); // Correlation ID
    request.put_i16(9); // Client ID length
    request.put_slice(b"test-rust"); // Client ID

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

    // Parse response
    let mut cursor = Cursor::new(&response_buf[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 42, "Correlation ID should match");

    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "Should have no error");

    // Read API versions array
    let api_count = cursor.get_i32();
    assert!(api_count > 0, "Should support at least one API");

    let mut supported_apis = Vec::new();
    for _ in 0..api_count {
        let api_key = cursor.get_i16();
        let min_version = cursor.get_i16();
        let max_version = cursor.get_i16();
        supported_apis.push((api_key, min_version, max_version));
    }

    // Verify essential APIs are supported
    let essential_apis = vec![
        (0, "Produce"),
        (1, "Fetch"),
        (2, "ListOffsets"),
        (3, "Metadata"),
        (8, "OffsetCommit"),
        (9, "OffsetFetch"),
        (10, "FindCoordinator"),
        (11, "JoinGroup"),
        (12, "Heartbeat"),
        (13, "LeaveGroup"),
        (14, "SyncGroup"),
        (18, "ApiVersions"),
    ];

    for (api_key, api_name) in essential_apis {
        let supported = supported_apis.iter().any(|(k, _, _)| *k == api_key);
        assert!(supported, "API {api_key} ({api_name}) should be supported");
    }

    println!("✓ API versions protocol compliance verified");
    println!("✓ Supported {} APIs", supported_apis.len());

    Ok(())
}

/// Test metadata protocol with various versions
#[tokio::test]
async fn test_metadata_protocol_versions() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;

    // Test different metadata versions
    let versions_to_test = vec![0, 1, 9]; // v0 (legacy), v1, v9 (current)

    for version in versions_to_test {
        let mut stream = TcpStream::connect(&context.kafka_addr()).await?;

        let mut request = BytesMut::new();
        request.put_i16(3); // Metadata API
        request.put_i16(version);
        request.put_i32(100 + version as i32); // Unique correlation ID
        request.put_i16(0); // Client ID length

        // Version-specific request body
        match version {
            0 | 1 => {
                // v0/v1: array of topic names
                request.put_i32(0); // All topics
            }
            9 => {
                // v9: more complex structure
                request.put_i32(0); // Topics array (0 = all)
                request.put_i8(1); // allow_auto_topic_creation
                request.put_i8(0); // include_cluster_authorized_operations
                request.put_i8(0); // include_topic_authorized_operations
            }
            _ => unreachable!(),
        }

        let mut frame = BytesMut::new();
        frame.put_i32(request.len() as i32);
        frame.extend_from_slice(&request);

        stream.write_all(&frame).await?;

        let mut length_buf = [0u8; 4];
        stream.read_exact(&mut length_buf).await?;
        let response_length = i32::from_be_bytes(length_buf) as usize;

        let mut response_buf = vec![0u8; response_length];
        stream.read_exact(&mut response_buf).await?;

        let mut cursor = Cursor::new(&response_buf[..]);
        let correlation_id = cursor.get_i32();
        assert_eq!(correlation_id, 100 + version as i32);

        // Version-specific response parsing
        match version {
            0 | 1 => {
                // v0/v1 has brokers array directly
                let broker_count = cursor.get_i32();
                assert!(
                    broker_count > 0,
                    "Should have at least one broker for v{version}"
                );
            }
            9 => {
                // v9 has throttle_time_ms first
                let _throttle_time = cursor.get_i32();
                let broker_count = cursor.get_i32();
                assert!(
                    broker_count > 0,
                    "Should have at least one broker for v{version}"
                );
            }
            _ => unreachable!(),
        }

        println!("✓ Metadata v{version} protocol compliance verified");
    }

    Ok(())
}

/// Test produce/fetch protocol roundtrip
#[tokio::test]
async fn test_produce_fetch_protocol_roundtrip() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    let topic = "protocol-test";

    // First, create a metadata request to ensure topic exists
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;
    create_topic_via_metadata(&mut stream, topic).await?;

    // Produce a message
    let test_key = "protocol-key";
    let test_value = "protocol-value";
    produce_message(&mut stream, topic, test_key, test_value).await?;

    // Wait a bit for the server to flush the batch to storage
    // The batch writer has a background timer that flushes every 10ms
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Fetch the message back
    let fetched = fetch_messages(&mut stream, topic).await?;

    // Verify we got our message
    assert!(!fetched.is_empty(), "Should fetch at least one message");

    let found = fetched
        .iter()
        .any(|(k, v)| k.as_deref() == Some(test_key) && v == test_value);
    assert!(found, "Should find the produced message");

    println!("✓ Produce/Fetch protocol roundtrip verified");
    Ok(())
}

// Helper functions

async fn create_topic_via_metadata(stream: &mut TcpStream, topic: &str) -> Result<()> {
    let mut request = BytesMut::new();
    request.put_i16(3); // Metadata API
    request.put_i16(9); // Version 9
    request.put_i32(200); // Correlation ID
    request.put_i16(0); // Client ID length

    // Request specific topic
    request.put_i32(1); // One topic
    request.put_i16(topic.len() as i16);
    request.put_slice(topic.as_bytes());
    request.put_i8(1); // allow_auto_topic_creation = true
    request.put_i8(0); // include_cluster_authorized_operations
    request.put_i8(0); // include_topic_authorized_operations

    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;

    // Read and discard response
    let mut length_buf = [0u8; 4];
    stream.read_exact(&mut length_buf).await?;
    let response_length = i32::from_be_bytes(length_buf) as usize;
    let mut response_buf = vec![0u8; response_length];
    stream.read_exact(&mut response_buf).await?;

    Ok(())
}

async fn produce_message(
    stream: &mut TcpStream,
    topic: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    let mut request = BytesMut::new();
    request.put_i16(0); // Produce API
    request.put_i16(8); // Version 8
    request.put_i32(300); // Correlation ID
    request.put_i16(0); // Client ID length

    request.put_i16(-1); // Transactional ID (null)
    request.put_i16(1); // Acks
    request.put_i32(30000); // Timeout
    request.put_i32(1); // Number of topics

    request.put_i16(topic.len() as i16);
    request.put_slice(topic.as_bytes());
    request.put_i32(1); // Number of partitions

    request.put_i32(0); // Partition index

    // Create record batch
    let mut batch = BytesMut::new();
    batch.put_i64(0); // Base offset
    batch.put_i32(0); // Batch length (placeholder)
    batch.put_i32(0); // Partition leader epoch
    batch.put_i8(2); // Magic byte (v2)
    batch.put_i32(0); // CRC
    batch.put_i16(0); // Attributes
    batch.put_i32(0); // Last offset delta
    batch.put_i64(0); // First timestamp
    batch.put_i64(0); // Max timestamp
    batch.put_i64(-1); // Producer ID
    batch.put_i16(-1); // Producer epoch
    batch.put_i32(-1); // Base sequence
    batch.put_i32(1); // Number of records

    // Record
    batch.put_i8(0); // Length (varint)
    batch.put_i8(0); // Attributes
    batch.put_i8(0); // Timestamp delta
    batch.put_i8(0); // Offset delta
    batch.put_i8(key.len() as i8); // Key length
    batch.put_slice(key.as_bytes());
    batch.put_i8(value.len() as i8); // Value length
    batch.put_slice(value.as_bytes());
    batch.put_i8(0); // Headers

    let batch_len = batch.len() as i32 - 12;
    batch[8..12].copy_from_slice(&batch_len.to_be_bytes());

    request.put_i32(batch.len() as i32);
    request.extend_from_slice(&batch);

    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;

    let mut length_buf = [0u8; 4];
    stream.read_exact(&mut length_buf).await?;
    let response_length = i32::from_be_bytes(length_buf) as usize;
    let mut response_buf = vec![0u8; response_length];
    stream.read_exact(&mut response_buf).await?;

    Ok(())
}

async fn fetch_messages(
    stream: &mut TcpStream,
    topic: &str,
) -> Result<Vec<(Option<String>, String)>> {
    let mut request = BytesMut::new();
    request.put_i16(1); // Fetch API
    request.put_i16(11); // Version 11
    request.put_i32(400); // Correlation ID
    request.put_i16(0); // Client ID length

    request.put_i32(-1); // Replica ID
    request.put_i32(100); // Max wait
    request.put_i32(1); // Min bytes
    request.put_i32(1048576); // Max bytes
    request.put_i8(0); // Isolation level
    request.put_i32(0); // Session ID
    request.put_i32(-1); // Session epoch
    request.put_i32(1); // Topics

    request.put_i16(topic.len() as i16);
    request.put_slice(topic.as_bytes());
    request.put_i32(1); // Partitions

    request.put_i32(0); // Partition
    request.put_i16(-1); // Current leader epoch
    request.put_i64(0); // Fetch offset
    request.put_i64(-1); // Log start offset
    request.put_i32(1048576); // Max bytes

    request.put_i32(0); // Forgotten topics
    request.put_i16(0); // Rack ID (null)

    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;

    let mut length_buf = [0u8; 4];
    stream.read_exact(&mut length_buf).await?;
    let response_length = i32::from_be_bytes(length_buf) as usize;
    let mut response_buf = vec![0u8; response_length];
    stream.read_exact(&mut response_buf).await?;

    // Parse the Fetch response using kafka-protocol
    use kafka_protocol::messages::FetchResponse;
    use kafka_protocol::protocol::Decodable;

    let mut buf = bytes::Bytes::from(response_buf);

    // Decode response header (correlation_id)
    let _correlation_id = buf.get_i32();

    // Decode the FetchResponse with version 11
    let response = FetchResponse::decode(&mut buf, 11)
        .map_err(|e| anyhow::anyhow!("Failed to decode FetchResponse: {e}"))?;

    // Extract messages from the response
    let mut messages = Vec::new();

    for topic_resp in response.responses {
        for partition in topic_resp.partitions {
            if let Some(records_bytes) = partition.records {
                // Decode the record batch
                use kafka_protocol::records::RecordBatchDecoder;
                let mut records_buf = records_bytes.clone();

                match RecordBatchDecoder::decode(&mut records_buf) {
                    Ok(record_set) => {
                        for record in record_set.records {
                            let key = record.key.map(|k| String::from_utf8_lossy(&k).to_string());
                            let value = record
                                .value
                                .map(|v| String::from_utf8_lossy(&v).to_string())
                                .unwrap_or_default();
                            messages.push((key, value));
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to decode record batch: {e}");
                    }
                }
            }
        }
    }

    Ok(messages)
}
