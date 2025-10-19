use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::net::TcpStream;

use super::super::helpers::{send_join_group_request, send_leave_group_request};

/// Create a consumer group by sending a JoinGroup request and return (member_id, generation_id)
async fn create_consumer_group(
    socket: &mut TcpStream,
    group_id: &str,
    member_id: &str,
) -> Result<(String, i32)> {
    let response = send_join_group_request(socket, 1, group_id, member_id, "consumer", 1).await?;

    // Parse JoinGroup response
    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation_id
    cursor.advance(4); // Skip throttle_time_ms (v1+)
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "JoinGroup failed with error {error_code}");

    let generation_id = cursor.get_i32();

    // Skip protocol string
    let protocol_len = cursor.get_i16();
    cursor.advance(protocol_len as usize);

    // Skip leader string
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

    // Send LeaveGroup request (v0)
    let response = send_leave_group_request(&mut socket, 2, group_id, &member_id, 0).await?;

    // Parse LeaveGroup response
    let mut cursor = Cursor::new(&response[..]);
    let _correlation_id = cursor.get_i32();
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0, "LeaveGroup should succeed");

    Ok(())
}

#[tokio::test]
async fn test_leave_group_nonexistent_group() -> Result<()> {
    init_test_tracing();
    let minio = MinioTestContext::new("test-bucket").await?;
    let port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, port).await?;

    // Connect to Heraclitus
    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}")).await?;

    // Send LeaveGroup request for non-existent group
    let response =
        send_leave_group_request(&mut socket, 2, "nonexistent-group", "test-member", 0).await?;

    // Parse LeaveGroup response
    let mut cursor = Cursor::new(&response[..]);
    let _correlation_id = cursor.get_i32();
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 16, "Should return COORDINATOR_NOT_AVAILABLE"); // ERROR_COORDINATOR_NOT_AVAILABLE

    Ok(())
}

#[tokio::test]
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

    // Send LeaveGroup request (v3) with batch format
    let response = send_leave_group_request(&mut socket, 3, group_id, &member_id1, 3).await?;

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

    // Leave the group once
    let _response1 = send_leave_group_request(&mut socket, 4, group_id, &member_id, 0).await?;

    // Try to leave again with the same member
    let response2 = send_leave_group_request(&mut socket, 5, group_id, &member_id, 0).await?;

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
