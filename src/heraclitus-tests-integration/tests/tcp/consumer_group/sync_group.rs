use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::collections::HashMap;
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

/// Helper to write bytes
fn write_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_i32(b.len() as i32);
    buf.put_slice(b);
}

/// Encode consumer protocol assignment
fn encode_consumer_assignment(topics: HashMap<String, Vec<i32>>) -> Vec<u8> {
    let mut buf = BytesMut::new();

    // version: int16
    buf.put_i16(0);

    // topic_partitions: [topic, [partition]]
    buf.put_i32(topics.len() as i32);

    for (topic, partitions) in &topics {
        // topic: string
        write_string(&mut buf, topic);

        // partitions: [int32]
        buf.put_i32(partitions.len() as i32);
        for &partition in partitions {
            buf.put_i32(partition);
        }
    }

    // user_data: bytes (empty)
    buf.put_i32(0);

    buf.to_vec()
}

#[tokio::test]
async fn test_sync_group_direct() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-sync-group-direct").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-sync-group";
    let member_id = "member-123";
    let generation_id = 1;

    // First, we need to join the group
    // Send JoinGroup request
    let mut join_body = BytesMut::new();
    write_string(&mut join_body, group_id);
    join_body.put_i32(30000); // session_timeout_ms
    write_string(&mut join_body, member_id); // using existing member_id
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

    // Now send SyncGroup request
    let mut sync_body = BytesMut::new();
    write_string(&mut sync_body, group_id);
    sync_body.put_i32(generation_id); // generation_id
    write_string(&mut sync_body, member_id); // member_id

    // group_assignments (empty for follower, or with assignments for leader)
    sync_body.put_i32(0); // No assignments (follower behavior)

    let sync_request = build_kafka_request(14, 0, 2, "test-client", &sync_body);

    // Send request
    stream.write_all(&sync_request).await?;
    stream.flush().await?;

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);

    // Read response header
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 2);

    // Read response body
    // error_code: i16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "SyncGroup should succeed");

    // member_assignment: bytes
    let assignment_len = cursor.get_i32() as usize;
    let mut assignment_bytes = vec![0u8; assignment_len];
    cursor.copy_to_slice(&mut assignment_bytes);

    tracing::info!("SyncGroup response: assignment_len={}", assignment_len);

    // The assignment should be non-empty if topics exist
    // For now, we're just checking that we got a response

    Ok(())
}

#[tokio::test]
async fn test_sync_group_with_assignment() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-sync-group-assign").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // First create a topic
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    // Send metadata request to create topic
    let mut metadata_body = BytesMut::new();
    metadata_body.put_i32(1); // topics array with 1 topic
    write_string(&mut metadata_body, "test-topic-sync");
    metadata_body.put_i8(1); // allow_auto_topic_creation

    let metadata_request = build_kafka_request(3, 0, 1, "test-client", &metadata_body);
    stream.write_all(&metadata_request).await?;
    stream.flush().await?;

    // Read metadata response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    // Now test sync group with assignments
    let group_id = "test-sync-assign-group";
    let _member_id = "leader-member";

    // Join group
    let mut join_body = BytesMut::new();
    write_string(&mut join_body, group_id);
    join_body.put_i32(30000); // session_timeout_ms
    write_string(&mut join_body, ""); // empty member_id for new member
    write_string(&mut join_body, "consumer"); // protocol_type
    join_body.put_i32(1); // protocols count
    write_string(&mut join_body, "range");
    write_bytes(&mut join_body, b"metadata");

    let join_request = build_kafka_request(11, 0, 2, "test-client", &join_body);
    stream.write_all(&join_request).await?;
    stream.flush().await?;

    // Read JoinGroup response to get member_id and generation
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id
    cursor.get_i16(); // error_code
    let generation_id = cursor.get_i32();

    // Skip protocol_name
    let proto_len = cursor.get_i16();
    if proto_len > 0 {
        cursor.advance(proto_len as usize);
    }

    // Skip leader
    let leader_len = cursor.get_i16();
    cursor.advance(leader_len as usize);

    // Get member_id
    let member_id_len = cursor.get_i16() as usize;
    let mut member_id_bytes = vec![0u8; member_id_len];
    cursor.copy_to_slice(&mut member_id_bytes);
    let actual_member_id = String::from_utf8(member_id_bytes)?;

    // As leader, send SyncGroup with assignments
    let mut sync_body = BytesMut::new();
    write_string(&mut sync_body, group_id);
    sync_body.put_i32(generation_id);
    write_string(&mut sync_body, &actual_member_id);

    // group_assignments: assign partition 0 of test-topic-sync to ourselves
    sync_body.put_i32(1); // 1 assignment
    write_string(&mut sync_body, &actual_member_id); // member_id

    // Create assignment
    let mut assignment_topics = HashMap::new();
    assignment_topics.insert("test-topic-sync".to_string(), vec![0]);
    let assignment = encode_consumer_assignment(assignment_topics);
    write_bytes(&mut sync_body, &assignment);

    let sync_request = build_kafka_request(14, 0, 3, "test-client", &sync_body);
    stream.write_all(&sync_request).await?;
    stream.flush().await?;

    // Read SyncGroup response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "SyncGroup should succeed");

    // Check that we got our assignment back
    let assignment_len = cursor.get_i32() as usize;
    assert!(assignment_len > 0, "Should have received assignment");

    let mut assignment_bytes = vec![0u8; assignment_len];
    cursor.copy_to_slice(&mut assignment_bytes);

    // Decode the assignment
    let mut assign_cursor = Cursor::new(&assignment_bytes[..]);
    let version = assign_cursor.get_i16();
    assert_eq!(version, 0);

    let topic_count = assign_cursor.get_i32();
    assert_eq!(topic_count, 1, "Should have 1 topic assigned");

    Ok(())
}
