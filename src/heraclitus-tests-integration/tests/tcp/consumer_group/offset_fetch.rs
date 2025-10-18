use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Build a complete Kafka request with header
fn build_kafka_request(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &str,
    body: &[u8],
) -> Vec<u8> {
    let mut request = BytesMut::new();

    // Build header
    request.put_i16(api_key);
    request.put_i16(api_version);
    request.put_i32(correlation_id);

    // Write client_id as nullable string
    if client_id.is_empty() {
        request.put_i16(-1);
    } else {
        request.put_i16(client_id.len() as i16);
        request.put_slice(client_id.as_bytes());
    }

    // Add body
    request.extend_from_slice(body);

    // Prepend size
    let mut final_request = BytesMut::new();
    final_request.put_i32(request.len() as i32);
    final_request.extend_from_slice(&request);

    final_request.to_vec()
}

/// Helper to write string
fn write_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

/// Helper to write nullable string
fn write_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(s) => {
            buf.put_i16(s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
        None => buf.put_i16(-1),
    }
}

/// Helper to write bytes
#[allow(dead_code)]
fn write_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_i32(b.len() as i32);
    buf.put_slice(b);
}

/// Helper to commit offsets before fetching
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
async fn commit_offset(
    stream: &mut TcpStream,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    metadata: Option<&str>,
    correlation_id: i32,
) -> Result<()> {
    let mut commit_body = BytesMut::new();

    // group_id: string
    write_string(&mut commit_body, group_id);

    // generation_id: int32 (v1+)
    commit_body.put_i32(generation_id);

    // member_id: string (v1+)
    write_string(&mut commit_body, member_id);

    // topics: [topic] - 1 topic
    commit_body.put_i32(1);

    // topic name
    write_string(&mut commit_body, topic);

    // partitions: [partition] - 1 partition
    commit_body.put_i32(1);

    // partition_index: int32
    commit_body.put_i32(partition);

    // committed_offset: int64
    commit_body.put_i64(offset);

    // metadata: nullable string
    write_nullable_string(&mut commit_body, metadata);

    let commit_request = build_kafka_request(8, 1, correlation_id, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read and ignore response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    Ok(())
}

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

    // First, commit an offset using v0 (simpler)
    let mut commit_body = BytesMut::new();
    write_string(&mut commit_body, group_id);
    commit_body.put_i32(1); // topics
    write_string(&mut commit_body, "test-topic");
    commit_body.put_i32(1); // partitions
    commit_body.put_i32(0); // partition 0
    commit_body.put_i64(150); // offset
    write_nullable_string(&mut commit_body, Some("test-meta"));

    let commit_request = build_kafka_request(8, 0, 1, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read commit response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    // Now fetch the offset
    let mut fetch_body = BytesMut::new();

    // group_id: string
    write_string(&mut fetch_body, group_id);

    // topics: [topic] - 1 topic
    fetch_body.put_i32(1);

    // topic name
    write_string(&mut fetch_body, "test-topic");

    // partition_indexes: [int32] - 1 partition
    fetch_body.put_i32(1);
    fetch_body.put_i32(0); // partition 0

    let fetch_request = build_kafka_request(9, 0, 2, "test-client", &fetch_body);
    stream.write_all(&fetch_request).await?;
    stream.flush().await?;

    // Read OffsetFetch response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
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
    let mut fetch_body = BytesMut::new();

    // group_id: string
    write_string(&mut fetch_body, group_id);

    // topics: [topic] - 1 topic
    fetch_body.put_i32(1);

    // topic name
    write_string(&mut fetch_body, "test-topic");

    // partition_indexes: [int32] - 1 partition
    fetch_body.put_i32(1);
    fetch_body.put_i32(0); // partition 0

    let fetch_request = build_kafka_request(9, 0, 1, "test-client", &fetch_body);
    stream.write_all(&fetch_request).await?;
    stream.flush().await?;

    // Read OffsetFetch response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id

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
    let mut commit_body = BytesMut::new();
    write_string(&mut commit_body, group_id);
    commit_body.put_i32(1); // topics
    write_string(&mut commit_body, "topic-1");
    commit_body.put_i32(1); // partitions
    commit_body.put_i32(0); // partition 0
    commit_body.put_i64(100);
    write_nullable_string(&mut commit_body, Some("meta1"));

    let commit_request = build_kafka_request(8, 0, 1, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    // Topic 2, partition 0
    let mut commit_body = BytesMut::new();
    write_string(&mut commit_body, group_id);
    commit_body.put_i32(1); // topics
    write_string(&mut commit_body, "topic-2");
    commit_body.put_i32(1); // partitions
    commit_body.put_i32(0); // partition 0
    commit_body.put_i64(200);
    write_nullable_string(&mut commit_body, None);

    let commit_request = build_kafka_request(8, 0, 2, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    // Now fetch all committed offsets (empty topics array in v0 means all)
    let mut fetch_body = BytesMut::new();

    // group_id: string
    write_string(&mut fetch_body, group_id);

    // topics: empty array (means all topics in v0)
    fetch_body.put_i32(0);

    let fetch_request = build_kafka_request(9, 0, 3, "test-client", &fetch_body);
    stream.write_all(&fetch_request).await?;
    stream.flush().await?;

    // Read OffsetFetch response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
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
    let mut commit_body = BytesMut::new();
    write_string(&mut commit_body, group_id);
    commit_body.put_i32(1); // topics
    write_string(&mut commit_body, "test-topic");
    commit_body.put_i32(1); // partitions
    commit_body.put_i32(0); // partition 0
    commit_body.put_i64(100);
    write_nullable_string(&mut commit_body, Some("p0-meta"));

    let commit_request = build_kafka_request(8, 0, 1, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    // Commit offset for partition 2 (skip partition 1)
    let mut commit_body = BytesMut::new();
    write_string(&mut commit_body, group_id);
    commit_body.put_i32(1); // topics
    write_string(&mut commit_body, "test-topic");
    commit_body.put_i32(1); // partitions
    commit_body.put_i32(2); // partition 2
    commit_body.put_i64(300);
    write_nullable_string(&mut commit_body, None);

    let commit_request = build_kafka_request(8, 0, 2, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    // Now fetch specific partitions (0, 1, 2)
    let mut fetch_body = BytesMut::new();

    // group_id: string
    write_string(&mut fetch_body, group_id);

    // topics: [topic] - 1 topic
    fetch_body.put_i32(1);

    // topic name
    write_string(&mut fetch_body, "test-topic");

    // partition_indexes: [int32] - 3 partitions
    fetch_body.put_i32(3);
    fetch_body.put_i32(0); // partition 0
    fetch_body.put_i32(1); // partition 1
    fetch_body.put_i32(2); // partition 2

    let fetch_request = build_kafka_request(9, 0, 3, "test-client", &fetch_body);
    stream.write_all(&fetch_request).await?;
    stream.flush().await?;

    // Read OffsetFetch response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id

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
