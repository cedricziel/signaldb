use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::net::TcpStream;

use super::helpers::send_metadata_request;

#[tokio::test]
async fn test_metadata_non_existent_topic() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-metadata-topic-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    // Send metadata request for a non-existent topic
    let response = send_metadata_request(
        &mut stream,
        1,
        Some(vec!["non-existent-topic".to_string()]),
        0,
    )
    .await?;

    // Parse the response
    let mut cursor = Cursor::new(&response[..]);
    cursor.advance(4); // Skip correlation ID

    // Parse brokers
    let broker_count = cursor.get_i32();
    println!("Number of brokers: {broker_count}");

    for _ in 0..broker_count {
        let node_id = cursor.get_i32();
        let host_len = cursor.get_i16() as usize;
        let mut host_bytes = vec![0u8; host_len];
        cursor.copy_to_slice(&mut host_bytes);
        let host = String::from_utf8(host_bytes)?;
        let port = cursor.get_i32();
        println!("Broker: id={node_id}, host={host}, port={port}");
    }

    // Parse topics
    let topic_count = cursor.get_i32();
    println!("Number of topics: {topic_count}");
    assert_eq!(topic_count, 1, "Should have 1 topic in response");

    // Parse topic metadata
    let error_code = cursor.get_i16();
    let name_len = cursor.get_i16() as usize;
    let mut name_bytes = vec![0u8; name_len];
    cursor.copy_to_slice(&mut name_bytes);
    let topic_name = String::from_utf8(name_bytes)?;

    let partition_count = cursor.get_i32();
    println!(
        "Topic: name={topic_name}, error_code={error_code}, partition_count={partition_count}"
    );

    // After our change, non-existent topics should be auto-created
    // So we expect error code 0 (ERROR_NONE) and 1 partition
    assert_eq!(
        error_code, 0,
        "Auto-created topic should return error code 0 (ERROR_NONE)"
    );
    assert_eq!(
        partition_count, 1,
        "Auto-created topic should have 1 partition"
    );

    Ok(())
}
