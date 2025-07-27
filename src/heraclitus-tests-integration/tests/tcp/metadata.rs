use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
    let response =
        send_metadata_request(&mut stream, Some(vec!["non-existent-topic".to_string()])).await?;

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

async fn send_metadata_request(
    stream: &mut TcpStream,
    topics: Option<Vec<String>>,
) -> Result<Vec<u8>> {
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(3); // Metadata API
    request.put_i16(0); // Version 0
    request.put_i32(1); // Correlation ID
    request.put_i16(-1); // No client ID

    // Request body
    match topics {
        Some(topic_list) => {
            request.put_i32(topic_list.len() as i32);
            for topic in topic_list {
                request.put_i16(topic.len() as i16);
                request.extend_from_slice(topic.as_bytes());
            }
        }
        None => {
            request.put_i32(-1); // All topics
        }
    }

    // Send request with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read response
    read_response(stream).await
}

async fn read_response(stream: &mut TcpStream) -> Result<Vec<u8>> {
    // Read length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = i32::from_be_bytes(len_buf) as usize;

    // Read response body
    let mut response = vec![0u8; len];
    stream.read_exact(&mut response).await?;

    Ok(response)
}
