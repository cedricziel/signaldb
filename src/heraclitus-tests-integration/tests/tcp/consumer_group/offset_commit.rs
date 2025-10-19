use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::net::TcpStream;

use super::super::helpers::{send_join_group_request, send_offset_commit_request};

#[tokio::test]
async fn test_offset_commit_v0() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-commit-v0").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-commit-group";

    // Send OffsetCommit v0 request
    let topics = vec![("test-topic".to_string(), vec![(0, 100)])];
    let response = send_offset_commit_request(&mut stream, 1, group_id, topics, 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 1);

    // No throttle_time_ms in v0
    // topics: [topic]
    let topic_count = cursor.get_i32();
    assert_eq!(topic_count, 1);

    // topic name length
    let topic_name_len = cursor.get_i16() as usize;
    let mut topic_name_bytes = vec![0u8; topic_name_len];
    cursor.copy_to_slice(&mut topic_name_bytes);
    assert_eq!(String::from_utf8(topic_name_bytes)?, "test-topic");

    // partitions: [partition]
    let partition_count = cursor.get_i32();
    assert_eq!(partition_count, 1);

    // partition_index: int32
    let partition_index = cursor.get_i32();
    assert_eq!(partition_index, 0);

    // error_code: int16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "OffsetCommit should succeed");

    tracing::info!("OffsetCommit v0 successful");

    Ok(())
}

#[tokio::test]
async fn test_offset_commit_v1_with_group() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-commit-v1").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-commit-v1-group";
    let member_id = "member-123";

    // First, join the group to create it
    send_join_group_request(&mut stream, 1, group_id, member_id, "consumer", 0).await?;

    // Now commit offsets with v1
    let topics = vec![("test-topic".to_string(), vec![(0, 200)])];
    let response = send_offset_commit_request(&mut stream, 2, group_id, topics, 1).await?;

    let mut cursor = Cursor::new(&response[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 2);

    // No throttle_time_ms in v1
    // topics: [topic]
    let topic_count = cursor.get_i32();
    assert_eq!(topic_count, 1);

    // Skip topic name
    let topic_name_len = cursor.get_i16() as usize;
    cursor.advance(topic_name_len);

    // partitions: [partition]
    let partition_count = cursor.get_i32();
    assert_eq!(partition_count, 1);

    // partition_index: int32
    let partition_index = cursor.get_i32();
    assert_eq!(partition_index, 0);

    // error_code: int16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "OffsetCommit should succeed");

    tracing::info!("OffsetCommit v1 with group successful");

    Ok(())
}

#[tokio::test]
async fn test_offset_commit_unknown_member() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-commit-unknown").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-commit-unknown-group";

    // First, create a group with one member
    send_join_group_request(&mut stream, 1, group_id, "", "consumer", 0).await?;

    // Try to commit with unknown member (note: v1 doesn't validate member in our implementation)
    let topics = vec![("test-topic".to_string(), vec![(0, 300)])];
    let response = send_offset_commit_request(&mut stream, 2, group_id, topics, 1).await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id

    // Skip to the error code
    let topic_count = cursor.get_i32();
    assert_eq!(topic_count, 1);

    // Skip topic name
    let topic_name_len = cursor.get_i16() as usize;
    cursor.advance(topic_name_len);

    // partitions: [partition]
    let partition_count = cursor.get_i32();
    assert_eq!(partition_count, 1);

    // partition_index: int32
    cursor.get_i32(); // partition_index

    // error_code: int16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 25, "Should get UNKNOWN_MEMBER_ID error");

    tracing::info!("Correctly rejected offset commit from unknown member");

    Ok(())
}

#[tokio::test]
async fn test_offset_commit_multiple_partitions() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-commit-multi").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-commit-multi-group";

    // Build OffsetCommit v0 request with multiple topics and partitions
    let topics = vec![
        ("topic-1".to_string(), vec![(0, 100), (1, 200)]),
        ("topic-2".to_string(), vec![(0, 300)]),
    ];
    let response = send_offset_commit_request(&mut stream, 1, group_id, topics, 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 1);

    // topics: [topic]
    let topic_count = cursor.get_i32();
    assert_eq!(topic_count, 2);

    // Verify both topics and all partitions got success responses
    for topic_idx in 0..2 {
        // Skip topic name
        let topic_name_len = cursor.get_i16() as usize;
        cursor.advance(topic_name_len);

        // partitions: [partition]
        let partition_count = cursor.get_i32();
        let expected_partitions = if topic_idx == 0 { 2 } else { 1 };
        assert_eq!(partition_count, expected_partitions);

        for _ in 0..partition_count {
            cursor.get_i32(); // partition_index
            let error_code = cursor.get_i16();
            assert_eq!(error_code, 0, "All partitions should succeed");
        }
    }

    tracing::info!("OffsetCommit with multiple topics/partitions successful");

    Ok(())
}
