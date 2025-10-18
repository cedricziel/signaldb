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

// NOTE: Produce/Fetch protocol roundtrip is tested in E2E tests using rdkafka
// Manual protocol encoding of record batches with CRC is error-prone and provides
// no additional value beyond what kafka-protocol crate already validates.
// See tests/e2e/rdkafka_compat_test.rs::test_produce_consume_roundtrip instead.
