use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::net::TcpStream;

use super::super::helpers::send_join_group_request;

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
    let response =
        send_join_group_request(&mut stream, 1, "test-group-direct", "", "consumer", 0).await?;

    // Read response
    let mut cursor = Cursor::new(&response[..]);

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
        cursor.advance(protocol_name_len as usize);
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
        "JoinGroup response: generation_id={generation_id}, leader={leader}, member_id={member_id}"
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
    let response1 = send_join_group_request(&mut stream1, 1, group_id, "", "consumer", 0).await?;

    // Read first response
    let mut cursor = Cursor::new(&response1[..]);
    cursor.advance(4); // Skip correlation_id
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

    tracing::info!("First member joined: id={member1_id}, generation={generation1}");

    // Second member joins - should trigger rebalance
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;
    let response2 = send_join_group_request(&mut stream2, 2, group_id, "", "consumer", 0).await?;

    // Read second response
    let mut cursor = Cursor::new(&response2[..]);
    cursor.advance(4); // Skip correlation_id
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0);

    let generation2 = cursor.get_i32();
    assert!(
        generation2 > generation1,
        "Generation should increase after new member joins"
    );

    tracing::info!("Second member joined, new generation: {generation2}");

    Ok(())
}
