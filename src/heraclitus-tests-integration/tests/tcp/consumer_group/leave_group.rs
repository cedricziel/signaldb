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

/// Send request to Heraclitus and get response
async fn send_kafka_request(socket: &mut TcpStream, request: Vec<u8>) -> Result<Vec<u8>> {
    // Send request
    socket.write_all(&request).await?;
    socket.flush().await?;

    // Read response length
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let response_len = i32::from_be_bytes(len_buf) as usize;

    // Read response
    let mut response = vec![0u8; response_len];
    socket.read_exact(&mut response).await?;

    Ok(response)
}

/// Create a consumer group by sending a JoinGroup request
async fn create_consumer_group(
    socket: &mut TcpStream,
    group_id: &str,
    member_id: &str,
) -> Result<(String, i32)> {
    // Build JoinGroup request body
    let mut body = BytesMut::new();

    // group_id: string
    write_string(&mut body, group_id);

    // session_timeout_ms: int32
    body.put_i32(30000);

    // rebalance_timeout_ms: int32 (v1+)
    body.put_i32(30000);

    // member_id: string
    write_string(&mut body, member_id);

    // protocol_type: string
    write_string(&mut body, "consumer");

    // group_protocols: [protocol]
    body.put_i32(1); // 1 protocol

    // protocol_name: string
    write_string(&mut body, "range");

    // protocol_metadata: bytes
    body.put_i32(0); // empty metadata

    let request = build_kafka_request(11, 1, 1, "test-client", &body);
    let response = send_kafka_request(socket, request).await?;

    // Parse JoinGroup response
    let mut cursor = Cursor::new(&response[..]);
    let _correlation_id = cursor.get_i32();
    let _throttle_time_ms = cursor.get_i32();
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "JoinGroup failed with error {error_code}");

    let generation_id = cursor.get_i32();

    // Skip protocol string
    let protocol_len = cursor.get_i16();
    cursor.advance(protocol_len as usize);

    // Read leader string
    let leader_len = cursor.get_i16();
    cursor.advance(leader_len as usize);

    // Read member_id string
    let member_id_len = cursor.get_i16();
    let mut member_id_bytes = vec![0u8; member_id_len as usize];
    cursor.copy_to_slice(&mut member_id_bytes);
    let returned_member_id = String::from_utf8(member_id_bytes)?;

    Ok((returned_member_id, generation_id))
}

#[tokio::test]
#[ignore] // Requires external dependencies 
async fn test_leave_group_single_member() -> Result<()> {
    init_test_tracing();
    let minio = MinioTestContext::new("test-bucket").await?;
    let port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, port).await?;

    // Connect to Heraclitus
    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}")).await?;

    // Create a consumer group first
    let group_id = "test-group";
    let (member_id, _generation_id) = create_consumer_group(&mut socket, group_id, "").await?;

    // Build LeaveGroup request body (v0)
    let mut body = BytesMut::new();

    // group_id: string
    write_string(&mut body, group_id);

    // member_id: string
    write_string(&mut body, &member_id);

    let request = build_kafka_request(13, 0, 2, "test-client", &body);
    let response = send_kafka_request(&mut socket, request).await?;

    // Parse LeaveGroup response
    let mut cursor = Cursor::new(&response[..]);
    let _correlation_id = cursor.get_i32();
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "LeaveGroup should succeed");

    Ok(())
}

#[tokio::test]
#[ignore] // Requires external dependencies
async fn test_leave_group_nonexistent_group() -> Result<()> {
    init_test_tracing();
    let minio = MinioTestContext::new("test-bucket").await?;
    let port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, port).await?;

    // Connect to Heraclitus
    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}")).await?;

    // Build LeaveGroup request body for non-existent group
    let mut body = BytesMut::new();

    // group_id: string
    write_string(&mut body, "nonexistent-group");

    // member_id: string
    write_string(&mut body, "test-member");

    let request = build_kafka_request(13, 0, 2, "test-client", &body);
    let response = send_kafka_request(&mut socket, request).await?;

    // Parse LeaveGroup response
    let mut cursor = Cursor::new(&response[..]);
    let _correlation_id = cursor.get_i32();
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 16, "Should return COORDINATOR_NOT_AVAILABLE"); // ERROR_COORDINATOR_NOT_AVAILABLE

    Ok(())
}

#[tokio::test]
#[ignore] // Requires external dependencies
async fn test_leave_group_v3_batch() -> Result<()> {
    init_test_tracing();
    let minio = MinioTestContext::new("test-bucket").await?;
    let port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, port).await?;

    // Connect to Heraclitus
    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}")).await?;

    // Create a consumer group first
    let group_id = "test-group-batch";
    let (member_id1, _generation_id) = create_consumer_group(&mut socket, group_id, "").await?;

    // Build LeaveGroup request body (v3) with batch format
    let mut body = BytesMut::new();

    // group_id: string
    write_string(&mut body, group_id);

    // members: [member]
    body.put_i32(1); // 1 member

    // member_id: string
    write_string(&mut body, &member_id1);

    let request = build_kafka_request(13, 3, 3, "test-client", &body);
    let response = send_kafka_request(&mut socket, request).await?;

    // Parse LeaveGroup response
    let mut cursor = Cursor::new(&response[..]);
    let _correlation_id = cursor.get_i32();
    let _throttle_time_ms = cursor.get_i32(); // v1+
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "LeaveGroup should succeed");

    // v3+ should have members array, but it's empty for this test
    let members_count = cursor.get_i32();
    assert_eq!(members_count, 0, "Should return empty members array");

    Ok(())
}

#[tokio::test]
#[ignore] // Requires external dependencies
async fn test_leave_group_already_left_member() -> Result<()> {
    init_test_tracing();
    let minio = MinioTestContext::new("test-bucket").await?;
    let port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, port).await?;

    // Connect to Heraclitus
    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}")).await?;

    // Create a consumer group first
    let group_id = "test-group-already-left";
    let (member_id, _generation_id) = create_consumer_group(&mut socket, group_id, "").await?;

    // Build LeaveGroup request body
    let mut body = BytesMut::new();
    write_string(&mut body, group_id);
    write_string(&mut body, &member_id);

    // Leave the group once
    let request1 = build_kafka_request(13, 0, 4, "test-client", &body);
    let _response1 = send_kafka_request(&mut socket, request1).await?;

    // Try to leave again with the same member
    let request2 = build_kafka_request(13, 0, 5, "test-client", &body);
    let response2 = send_kafka_request(&mut socket, request2).await?;

    // Parse second LeaveGroup response
    let mut cursor = Cursor::new(&response2[..]);
    let _correlation_id = cursor.get_i32();
    let error_code = cursor.get_i16();
    // Should still succeed even if member already left
    assert_eq!(
        error_code, 0,
        "LeaveGroup should succeed even if member already left"
    );

    Ok(())
}
