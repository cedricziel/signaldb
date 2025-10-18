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

/// Helper to write bytes
fn write_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_i32(b.len() as i32);
    buf.put_slice(b);
}

#[tokio::test]
async fn test_join_group_direct() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-join-group-direct").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    // Send JoinGroup request
    let mut request_body = BytesMut::new();

    // group_id: "test-group-direct"
    write_string(&mut request_body, "test-group-direct");

    // session_timeout_ms: 30000
    request_body.put_i32(30000);

    // member_id: "" (empty for new member)
    write_string(&mut request_body, "");

    // protocol_type: "consumer"
    write_string(&mut request_body, "consumer");

    // protocols: 1 protocol
    request_body.put_i32(1);

    // protocol name: "range"
    write_string(&mut request_body, "range");

    // protocol metadata: simple metadata
    let metadata = b"test-metadata";
    write_bytes(&mut request_body, metadata);

    // Build complete request with header
    let request = build_kafka_request(11, 0, 1, "test-client", &request_body);

    // Send request
    stream.write_all(&request).await?;
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
    assert_eq!(correlation_id, 1);

    // Read response body
    // error_code: i16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "JoinGroup should succeed");

    // generation_id: i32
    let generation_id = cursor.get_i32();
    assert!(generation_id > 0, "Should have assigned a generation ID");

    // protocol_name: string (nullable)
    let protocol_name_len = cursor.get_i16();
    if protocol_name_len > 0 {
        let mut protocol_name_bytes = vec![0u8; protocol_name_len as usize];
        cursor.copy_to_slice(&mut protocol_name_bytes);
        let protocol_name = String::from_utf8(protocol_name_bytes)?;
        assert_eq!(protocol_name, "range");
    }

    // leader: string
    let leader_len = cursor.get_i16();
    let mut leader_bytes = vec![0u8; leader_len as usize];
    cursor.copy_to_slice(&mut leader_bytes);
    let leader = String::from_utf8(leader_bytes)?;

    // member_id: string
    let member_id_len = cursor.get_i16();
    let mut member_id_bytes = vec![0u8; member_id_len as usize];
    cursor.copy_to_slice(&mut member_id_bytes);
    let member_id = String::from_utf8(member_id_bytes)?;

    tracing::info!(
        "JoinGroup response: generation_id={}, leader={}, member_id={}",
        generation_id,
        leader,
        member_id
    );

    // For first member, should be the leader
    assert_eq!(leader, member_id, "First member should be the leader");
    assert!(!member_id.is_empty(), "Should have assigned a member ID");

    // members: array (only present for leader)
    let members_count = cursor.get_i32();
    assert_eq!(members_count, 1, "Leader should see 1 member");

    Ok(())
}

#[tokio::test]
async fn test_join_group_multiple_members() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-join-group-multi-direct").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let group_id = "test-group-multi-direct";

    // First member joins
    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let mut request_body = BytesMut::new();
    write_string(&mut request_body, group_id);
    request_body.put_i32(30000); // session_timeout_ms
    write_string(&mut request_body, ""); // member_id (empty for new)
    write_string(&mut request_body, "consumer"); // protocol_type
    request_body.put_i32(1); // protocols count
    write_string(&mut request_body, "range");
    write_bytes(&mut request_body, b"metadata1");

    let request = build_kafka_request(11, 0, 1, "client1", &request_body);

    stream1.write_all(&request).await?;
    stream1.flush().await?;

    // Read first response
    let mut size_buf = [0u8; 4];
    stream1.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; response_size as usize];
    stream1.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0);

    let generation1 = cursor.get_i32();

    // Get member1 ID
    let protocol_name_len = cursor.get_i16();
    if protocol_name_len > 0 {
        cursor.advance(protocol_name_len as usize);
    }
    let leader_len = cursor.get_i16();
    cursor.advance(leader_len as usize);
    let member_id_len = cursor.get_i16();
    let mut member1_id_bytes = vec![0u8; member_id_len as usize];
    cursor.copy_to_slice(&mut member1_id_bytes);
    let member1_id = String::from_utf8(member1_id_bytes)?;

    tracing::info!(
        "First member joined: id={}, generation={}",
        member1_id,
        generation1
    );

    // Second member joins - should trigger rebalance
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let mut request_body = BytesMut::new();
    write_string(&mut request_body, group_id);
    request_body.put_i32(30000); // session_timeout_ms
    write_string(&mut request_body, ""); // member_id (empty for new)
    write_string(&mut request_body, "consumer"); // protocol_type
    request_body.put_i32(1); // protocols count
    write_string(&mut request_body, "range");
    write_bytes(&mut request_body, b"metadata2");

    let request = build_kafka_request(11, 0, 2, "client2", &request_body);

    stream2.write_all(&request).await?;
    stream2.flush().await?;

    // Read second response
    let mut size_buf = [0u8; 4];
    stream2.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; response_size as usize];
    stream2.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0);

    let generation2 = cursor.get_i32();
    assert!(
        generation2 > generation1,
        "Generation should increase after new member joins"
    );

    tracing::info!("Second member joined, new generation: {}", generation2);

    Ok(())
}
