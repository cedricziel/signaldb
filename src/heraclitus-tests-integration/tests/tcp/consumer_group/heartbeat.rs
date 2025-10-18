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
async fn test_heartbeat_basic() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-heartbeat-basic").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-heartbeat-group";
    let member_id = "member-123";

    // First, join the group
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

    // Now send heartbeat
    let mut heartbeat_body = BytesMut::new();
    write_string(&mut heartbeat_body, group_id);
    heartbeat_body.put_i32(generation_id);
    write_string(&mut heartbeat_body, member_id);

    let heartbeat_request = build_kafka_request(12, 0, 2, "test-client", &heartbeat_body);
    stream.write_all(&heartbeat_request).await?;
    stream.flush().await?;

    // Read Heartbeat response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 2);

    // error_code: i16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "Heartbeat should succeed");

    tracing::info!("Heartbeat successful");

    Ok(())
}

#[tokio::test]
async fn test_heartbeat_wrong_generation() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-heartbeat-wrong-gen").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-heartbeat-gen-group";
    let member_id = "member-456";

    // First, join the group
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

    // Send heartbeat with wrong generation
    let mut heartbeat_body = BytesMut::new();
    write_string(&mut heartbeat_body, group_id);
    heartbeat_body.put_i32(generation_id + 1); // Wrong generation!
    write_string(&mut heartbeat_body, member_id);

    let heartbeat_request = build_kafka_request(12, 0, 2, "test-client", &heartbeat_body);
    stream.write_all(&heartbeat_request).await?;
    stream.flush().await?;

    // Read Heartbeat response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id

    // error_code: i16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 22, "Should get ILLEGAL_GENERATION error");

    tracing::info!("Correctly rejected heartbeat with wrong generation");

    Ok(())
}

#[tokio::test]
async fn test_heartbeat_unknown_member() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-heartbeat-unknown").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-heartbeat-unknown-group";

    // First, join the group to create it
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

    // Send heartbeat with unknown member ID
    let mut heartbeat_body = BytesMut::new();
    write_string(&mut heartbeat_body, group_id);
    heartbeat_body.put_i32(generation_id);
    write_string(&mut heartbeat_body, "unknown-member-id"); // Unknown member!

    let heartbeat_request = build_kafka_request(12, 0, 2, "test-client", &heartbeat_body);
    stream.write_all(&heartbeat_request).await?;
    stream.flush().await?;

    // Read Heartbeat response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    cursor.get_i32(); // correlation_id

    // error_code: i16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 25, "Should get UNKNOWN_MEMBER_ID error");

    tracing::info!("Correctly rejected heartbeat from unknown member");

    Ok(())
}

#[tokio::test]
async fn test_heartbeat_v1_with_throttle() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-heartbeat-v1").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect directly to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    let group_id = "test-heartbeat-v1-group";
    let member_id = "member-v1";

    // First, join the group
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

    // Send heartbeat with API version 1
    let mut heartbeat_body = BytesMut::new();
    write_string(&mut heartbeat_body, group_id);
    heartbeat_body.put_i32(generation_id);
    write_string(&mut heartbeat_body, member_id);

    let heartbeat_request = build_kafka_request(12, 1, 2, "test-client", &heartbeat_body);
    stream.write_all(&heartbeat_request).await?;
    stream.flush().await?;

    // Read Heartbeat response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;

    let mut cursor = Cursor::new(&response_buf[..]);
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 2);

    // throttle_time_ms: i32 (v1+)
    let throttle_time = cursor.get_i32();
    assert_eq!(throttle_time, 0);

    // error_code: i16
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "Heartbeat should succeed");

    tracing::info!("Heartbeat v1 successful with throttle time");

    Ok(())
}
