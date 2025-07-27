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
fn write_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_i32(b.len() as i32);
    buf.put_slice(b);
}

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

    // Build OffsetCommit v0 request
    let mut commit_body = BytesMut::new();

    // group_id: string
    write_string(&mut commit_body, group_id);

    // topics: [topic] - 1 topic
    commit_body.put_i32(1);

    // topic name
    write_string(&mut commit_body, "test-topic");

    // partitions: [partition] - 1 partition
    commit_body.put_i32(1);

    // partition_index: int32
    commit_body.put_i32(0);

    // committed_offset: int64
    commit_body.put_i64(100);

    // metadata: nullable string
    write_nullable_string(&mut commit_body, Some("test-metadata"));

    let commit_request = build_kafka_request(8, 0, 1, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read OffsetCommit response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
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
    let mut join_body = BytesMut::new();
    write_string(&mut join_body, group_id);
    join_body.put_i32(30000); // session_timeout_ms
    write_string(&mut join_body, member_id);
    write_string(&mut join_body, "consumer"); // protocol_type
    join_body.put_i32(1); // protocols count
    write_string(&mut join_body, "range");
    write_bytes(&mut join_body, b"metadata");

    let join_request = build_kafka_request(11, 0, 1, "test-client", &join_body);
    stream.write_all(&join_request).await?;
    stream.flush().await?;

    // Read JoinGroup response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id
    cursor.get_i16(); // error_code
    let generation_id = cursor.get_i32();

    // Now commit offsets with v1
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
    write_string(&mut commit_body, "test-topic");

    // partitions: [partition] - 1 partition
    commit_body.put_i32(1);

    // partition_index: int32
    commit_body.put_i32(0);

    // committed_offset: int64
    commit_body.put_i64(200);

    // metadata: nullable string
    write_nullable_string(&mut commit_body, Some("v1-metadata"));

    let commit_request = build_kafka_request(8, 1, 2, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read OffsetCommit response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
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
    let mut join_body = BytesMut::new();
    write_string(&mut join_body, group_id);
    join_body.put_i32(30000); // session_timeout_ms
    write_string(&mut join_body, ""); // empty member_id for new member
    write_string(&mut join_body, "consumer"); // protocol_type
    join_body.put_i32(1); // protocols count
    write_string(&mut join_body, "range");
    write_bytes(&mut join_body, b"metadata");

    let join_request = build_kafka_request(11, 0, 1, "test-client", &join_body);
    stream.write_all(&join_request).await?;
    stream.flush().await?;

    // Read JoinGroup response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id
    cursor.get_i16(); // error_code
    let generation_id = cursor.get_i32();

    // Try to commit with unknown member
    let mut commit_body = BytesMut::new();

    // group_id: string
    write_string(&mut commit_body, group_id);

    // generation_id: int32 (v1+)
    commit_body.put_i32(generation_id);

    // member_id: string (v1+) - unknown member
    write_string(&mut commit_body, "unknown-member");

    // topics: [topic] - 1 topic
    commit_body.put_i32(1);

    // topic name
    write_string(&mut commit_body, "test-topic");

    // partitions: [partition] - 1 partition
    commit_body.put_i32(1);

    // partition_index: int32
    commit_body.put_i32(0);

    // committed_offset: int64
    commit_body.put_i64(300);

    // metadata: nullable string
    write_nullable_string(&mut commit_body, None);

    let commit_request = build_kafka_request(8, 1, 2, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read OffsetCommit response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id

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
    let mut commit_body = BytesMut::new();

    // group_id: string
    write_string(&mut commit_body, group_id);

    // topics: [topic] - 2 topics
    commit_body.put_i32(2);

    // First topic
    write_string(&mut commit_body, "topic-1");

    // partitions: [partition] - 2 partitions
    commit_body.put_i32(2);

    // partition 0
    commit_body.put_i32(0);
    commit_body.put_i64(100);
    write_nullable_string(&mut commit_body, Some("partition-0-metadata"));

    // partition 1
    commit_body.put_i32(1);
    commit_body.put_i64(200);
    write_nullable_string(&mut commit_body, Some("partition-1-metadata"));

    // Second topic
    write_string(&mut commit_body, "topic-2");

    // partitions: [partition] - 1 partition
    commit_body.put_i32(1);

    // partition 0
    commit_body.put_i32(0);
    commit_body.put_i64(300);
    write_nullable_string(&mut commit_body, None);

    let commit_request = build_kafka_request(8, 0, 1, "test-client", &commit_body);
    stream.write_all(&commit_request).await?;
    stream.flush().await?;

    // Read OffsetCommit response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
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
