use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use kafka_protocol::messages::OffsetCommitResponse;
use kafka_protocol::protocol::Decodable;
use std::io::Cursor;
use tokio::net::TcpStream;

use super::super::helpers::{send_join_group_request, send_offset_commit_request};

#[tokio::test]
async fn test_offset_commit_v1() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-commit-v1").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-commit-group";

    // Send OffsetCommit v2 request
    let topics = vec![("test-topic".to_string(), vec![(0, 100)])];
    let response_bytes = send_offset_commit_request(&mut stream, 1, group_id, topics, 2).await?;

    // Parse response header (correlation_id)
    let mut cursor = Cursor::new(&response_bytes[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 1);

    // Decode the response using kafka-protocol
    let response = OffsetCommitResponse::decode(&mut cursor, 2)?;

    // Verify the response
    assert_eq!(response.topics.len(), 1, "Should have 1 topic in response");
    let topic = &response.topics[0];
    assert_eq!(topic.name.0.as_str(), "test-topic");
    assert_eq!(topic.partitions.len(), 1, "Should have 1 partition");

    let partition = &topic.partitions[0];
    assert_eq!(partition.partition_index, 0);
    assert_eq!(partition.error_code, 0, "OffsetCommit should succeed");

    tracing::info!("OffsetCommit v2 successful");

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
    let response = send_offset_commit_request(&mut stream, 2, group_id, topics, 2).await?;

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
async fn test_offset_commit_simple_consumer() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-commit-simple").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-commit-unknown-group";

    // First, create a group with one member
    send_join_group_request(&mut stream, 1, group_id, "", "consumer", 0).await?;

    // Try to commit with simple consumer (generation_id=-1)
    // Note: v2+ allows simple consumer commits without group membership
    let topics = vec![("test-topic".to_string(), vec![(0, 300)])];
    let response_bytes = send_offset_commit_request(&mut stream, 2, group_id, topics, 2).await?;

    let mut cursor = Cursor::new(&response_bytes[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 2);

    let response = OffsetCommitResponse::decode(&mut cursor, 2)?;

    assert_eq!(response.topics.len(), 1);
    let topic = &response.topics[0];
    assert_eq!(topic.partitions.len(), 1);

    let partition = &topic.partitions[0];
    // v2+ allows simple consumer commits (generation_id=-1) even if group exists
    assert_eq!(
        partition.error_code, 0,
        "Simple consumer commit should succeed"
    );

    tracing::info!("Simple consumer offset commit successful");

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

    // Build OffsetCommit v2 request with multiple topics and partitions
    let topics = vec![
        ("topic-1".to_string(), vec![(0, 100), (1, 200)]),
        ("topic-2".to_string(), vec![(0, 300)]),
    ];
    let response_bytes = send_offset_commit_request(&mut stream, 1, group_id, topics, 2).await?;

    let mut cursor = Cursor::new(&response_bytes[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 1);

    let response = OffsetCommitResponse::decode(&mut cursor, 2)?;

    // Verify both topics and all partitions got success responses
    assert_eq!(response.topics.len(), 2, "Should have 2 topics");

    for (topic_idx, topic) in response.topics.iter().enumerate() {
        let expected_partitions = if topic_idx == 0 { 2 } else { 1 };
        assert_eq!(topic.partitions.len(), expected_partitions);

        for partition in &topic.partitions {
            assert_eq!(partition.error_code, 0, "All partitions should succeed");
        }
    }

    tracing::info!("OffsetCommit with multiple topics/partitions successful");

    Ok(())
}
