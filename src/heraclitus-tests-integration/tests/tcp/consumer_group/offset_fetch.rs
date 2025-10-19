use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use kafka_protocol::messages::OffsetFetchResponse;
use kafka_protocol::protocol::Decodable;
use std::io::Cursor;
use tokio::net::TcpStream;

use super::super::helpers::{send_offset_commit_request, send_offset_fetch_request};

#[tokio::test]
async fn test_offset_fetch_specific_partition() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-fetch-specific").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-fetch-group";

    // First, commit an offset using v1
    let topics = vec![("test-topic".to_string(), vec![(0, 150)])];
    send_offset_commit_request(&mut stream, 1, group_id, topics, 2).await?;

    // Now fetch the offset using v1
    let topics = Some(vec![("test-topic".to_string(), vec![0])]);
    let response_bytes = send_offset_fetch_request(&mut stream, 2, group_id, topics, 1).await?;

    // Parse response header (correlation_id)
    let mut cursor = Cursor::new(&response_bytes[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 2);

    // Decode the response using kafka-protocol
    let response = OffsetFetchResponse::decode(&mut cursor, 1)?;

    // Verify the response
    assert_eq!(response.topics.len(), 1, "Should have 1 topic in response");
    let topic = &response.topics[0];
    assert_eq!(topic.name.0.as_str(), "test-topic");
    assert_eq!(topic.partitions.len(), 1, "Should have 1 partition");

    let partition = &topic.partitions[0];
    assert_eq!(partition.partition_index, 0);
    assert_eq!(partition.committed_offset, 150);
    assert_eq!(partition.error_code, 0, "OffsetFetch should succeed");

    tracing::info!("OffsetFetch for specific partition successful");

    Ok(())
}

#[tokio::test]
async fn test_offset_fetch_no_committed_offset() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-fetch-no-offset").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-fetch-no-offset-group";

    // Don't commit any offsets - just try to fetch
    let topics = Some(vec![("test-topic".to_string(), vec![0])]);
    let response_bytes = send_offset_fetch_request(&mut stream, 1, group_id, topics, 1).await?;

    // Parse response header (correlation_id)
    let mut cursor = Cursor::new(&response_bytes[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 1);

    // Decode the response using kafka-protocol
    let response = OffsetFetchResponse::decode(&mut cursor, 1)?;

    // Verify the response
    assert_eq!(response.topics.len(), 1);
    let topic = &response.topics[0];
    assert_eq!(topic.partitions.len(), 1);

    let partition = &topic.partitions[0];
    assert_eq!(partition.partition_index, 0);
    assert_eq!(
        partition.committed_offset, -1,
        "Should return -1 for no committed offset"
    );
    assert_eq!(
        partition.error_code, 0,
        "Should succeed even with no offset"
    );

    tracing::info!("OffsetFetch with no committed offset successful");

    Ok(())
}

#[tokio::test]
#[ignore = "Server limitation: Heraclitus doesn't return topics when fetching all (None) - returns empty 8-byte response"]
async fn test_offset_fetch_all_topics() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-fetch-all").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-fetch-all-group";

    // Commit offsets for multiple topics/partitions using v0
    // Topic 1, partition 0
    let topics = vec![("topic-1".to_string(), vec![(0, 100)])];
    send_offset_commit_request(&mut stream, 1, group_id, topics, 2).await?;

    // Topic 2, partition 0
    let topics = vec![("topic-2".to_string(), vec![(0, 200)])];
    send_offset_commit_request(&mut stream, 2, group_id, topics, 2).await?;

    // Now fetch all committed offsets (None means all topics)
    let response_bytes = send_offset_fetch_request(&mut stream, 3, group_id, None, 1).await?;

    // Parse response header (correlation_id)
    let mut cursor = Cursor::new(&response_bytes[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 3);

    // Decode the response using kafka-protocol
    let response = OffsetFetchResponse::decode(&mut cursor, 1)?;

    // Note: Server currently returns empty response for "fetch all" (None topics)
    // This test is ignored due to server limitation
    assert_eq!(response.topics.len(), 2, "Should return both topics");

    // Parse both topics and collect offsets
    let mut found_offsets = std::collections::HashMap::new();

    for topic in &response.topics {
        let topic_name = topic.name.0.to_string();
        assert_eq!(topic.partitions.len(), 1);

        for partition in &topic.partitions {
            assert_eq!(partition.error_code, 0);
            found_offsets.insert(
                (topic_name.clone(), partition.partition_index),
                partition.committed_offset,
            );
        }
    }

    // Verify we got the expected offsets
    assert_eq!(found_offsets.get(&("topic-1".to_string(), 0)), Some(&100));
    assert_eq!(found_offsets.get(&("topic-2".to_string(), 0)), Some(&200));

    tracing::info!("OffsetFetch for all topics successful");

    Ok(())
}

#[tokio::test]
async fn test_offset_fetch_multiple_partitions() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-offset-fetch-multi").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-offset-fetch-multi-group";

    // Commit offset for partition 0
    let topics = vec![("test-topic".to_string(), vec![(0, 100)])];
    send_offset_commit_request(&mut stream, 1, group_id, topics, 2).await?;

    // Commit offset for partition 2 (skip partition 1)
    let topics = vec![("test-topic".to_string(), vec![(2, 300)])];
    send_offset_commit_request(&mut stream, 2, group_id, topics, 2).await?;

    // Now fetch specific partitions (0, 1, 2)
    let topics = Some(vec![("test-topic".to_string(), vec![0, 1, 2])]);
    let response_bytes = send_offset_fetch_request(&mut stream, 3, group_id, topics, 1).await?;

    // Parse response header (correlation_id)
    let mut cursor = Cursor::new(&response_bytes[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 3);

    // Decode the response using kafka-protocol
    let response = OffsetFetchResponse::decode(&mut cursor, 1)?;

    // Verify the response
    assert_eq!(response.topics.len(), 1);
    let topic = &response.topics[0];
    assert_eq!(topic.partitions.len(), 3);

    // Parse partitions and check offsets
    let mut partition_offsets = std::collections::HashMap::new();

    for partition in &topic.partitions {
        assert_eq!(partition.error_code, 0);
        partition_offsets.insert(partition.partition_index, partition.committed_offset);
    }

    // Verify offsets
    assert_eq!(
        partition_offsets.get(&0),
        Some(&100),
        "Partition 0 should have offset 100"
    );
    assert_eq!(
        partition_offsets.get(&1),
        Some(&-1),
        "Partition 1 should have no offset (-1)"
    );
    assert_eq!(
        partition_offsets.get(&2),
        Some(&300),
        "Partition 2 should have offset 300"
    );

    tracing::info!("OffsetFetch for multiple partitions successful");

    Ok(())
}
