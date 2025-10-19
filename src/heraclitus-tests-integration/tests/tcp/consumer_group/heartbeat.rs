use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::net::TcpStream;

use super::super::helpers::{send_heartbeat_request, send_join_group_request};

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
    let response =
        send_join_group_request(&mut stream, 1, group_id, member_id, "consumer", 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id
    cursor.get_i16(); // error_code
    let generation_id = cursor.get_i32();

    // Now send heartbeat
    let response =
        send_heartbeat_request(&mut stream, 2, group_id, generation_id, member_id, 0).await?;

    let mut cursor = Cursor::new(&response[..]);
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
    let response =
        send_join_group_request(&mut stream, 1, group_id, member_id, "consumer", 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id
    cursor.get_i16(); // error_code
    let generation_id = cursor.get_i32();

    // Send heartbeat with wrong generation
    let response =
        send_heartbeat_request(&mut stream, 2, group_id, generation_id + 1, member_id, 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id

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
    let response = send_join_group_request(&mut stream, 1, group_id, "", "consumer", 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id
    cursor.get_i16(); // error_code
    let generation_id = cursor.get_i32();

    // Send heartbeat with unknown member ID
    let response = send_heartbeat_request(
        &mut stream,
        2,
        group_id,
        generation_id,
        "unknown-member-id",
        0,
    )
    .await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id

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
    let response =
        send_join_group_request(&mut stream, 1, group_id, member_id, "consumer", 0).await?;

    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id
    cursor.get_i16(); // error_code
    let generation_id = cursor.get_i32();

    // Send heartbeat with API version 1
    let response =
        send_heartbeat_request(&mut stream, 2, group_id, generation_id, member_id, 1).await?;

    let mut cursor = Cursor::new(&response[..]);
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
