use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
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

    // First, commit an offset using v0
    let topics = vec![("test-topic".to_string(), vec![(0, 150)])];
    send_offset_commit_request(&mut stream, 1, group_id, topics, 0).await?;

    // Now fetch the offset
    let topics = Some(vec![("test-topic".to_string(), vec![0])]);
    let response = send_offset_fetch_request(&mut stream, 2, group_id, topics, 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 2);

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

    // committed_offset: int64
    let committed_offset = cursor.get_i64();
    assert_eq!(committed_offset, 150);

    // No committed_leader_epoch in v0

    // metadata: nullable string
    let metadata_len = cursor.get_i16() as usize;
    let mut metadata_bytes = vec![0u8; metadata_len];
    cursor.copy_to_slice(&mut metadata_bytes);
    assert_eq!(String::from_utf8(metadata_bytes)?, "test-meta");

    // error_code: int16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "OffsetFetch should succeed");

    // No group-level error_code in v0

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
    let response = send_offset_fetch_request(&mut stream, 1, group_id, topics, 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id

    // Skip to the offset data
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

    // committed_offset: int64
    let committed_offset = cursor.get_i64();
    assert_eq!(
        committed_offset, -1,
        "Should return -1 for no committed offset"
    );

    // metadata: nullable string
    let metadata_len = cursor.get_i16();
    assert_eq!(metadata_len, -1, "Should have null metadata");

    // error_code: int16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "Should succeed even with no offset");

    tracing::info!("OffsetFetch with no committed offset successful");

    Ok(())
}

#[tokio::test]
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
    send_offset_commit_request(&mut stream, 1, group_id, topics, 0).await?;

    // Topic 2, partition 0
    let topics = vec![("topic-2".to_string(), vec![(0, 200)])];
    send_offset_commit_request(&mut stream, 2, group_id, topics, 0).await?;

    // Now fetch all committed offsets (None means all topics)
    let response = send_offset_fetch_request(&mut stream, 3, group_id, None, 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 3);

    // topics: [topic]
    let topic_count = cursor.get_i32();
    assert_eq!(topic_count, 2, "Should return both topics");

    // Parse both topics (they should be sorted alphabetically)
    let mut found_offsets = std::collections::HashMap::new();

    for _ in 0..topic_count {
        // topic name
        let topic_name_len = cursor.get_i16() as usize;
        let mut topic_name_bytes = vec![0u8; topic_name_len];
        cursor.copy_to_slice(&mut topic_name_bytes);
        let topic_name = String::from_utf8(topic_name_bytes)?;

        // partitions: [partition]
        let partition_count = cursor.get_i32();
        assert_eq!(partition_count, 1);

        // partition data
        let partition_index = cursor.get_i32();
        let committed_offset = cursor.get_i64();
        // Skip metadata
        let metadata_len = cursor.get_i16();
        if metadata_len > 0 {
            cursor.advance(metadata_len as usize);
        }
        let error_code = cursor.get_i16();
        assert_eq!(error_code, 0);

        found_offsets.insert((topic_name, partition_index), committed_offset);
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
    send_offset_commit_request(&mut stream, 1, group_id, topics, 0).await?;

    // Commit offset for partition 2 (skip partition 1)
    let topics = vec![("test-topic".to_string(), vec![(2, 300)])];
    send_offset_commit_request(&mut stream, 2, group_id, topics, 0).await?;

    // Now fetch specific partitions (0, 1, 2)
    let topics = Some(vec![("test-topic".to_string(), vec![0, 1, 2])]);
    let response = send_offset_fetch_request(&mut stream, 3, group_id, topics, 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id

    // topics: [topic]
    let topic_count = cursor.get_i32();
    assert_eq!(topic_count, 1);

    // Skip topic name
    let topic_name_len = cursor.get_i16() as usize;
    cursor.advance(topic_name_len);

    // partitions: [partition]
    let partition_count = cursor.get_i32();
    assert_eq!(partition_count, 3);

    // Parse partitions and check offsets
    let mut partition_offsets = std::collections::HashMap::new();

    for _ in 0..partition_count {
        let partition_index = cursor.get_i32();
        let committed_offset = cursor.get_i64();
        // Skip metadata
        let metadata_len = cursor.get_i16();
        if metadata_len > 0 {
            cursor.advance(metadata_len as usize);
        }
        let error_code = cursor.get_i16();
        assert_eq!(error_code, 0);

        partition_offsets.insert(partition_index, committed_offset);
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
