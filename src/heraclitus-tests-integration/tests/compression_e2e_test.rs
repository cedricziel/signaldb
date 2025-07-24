use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use heraclitus::protocol::{CompressionType, RecordBatchBuilder};
use heraclitus::storage::KafkaMessage;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::collections::HashMap;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_produce_with_compression() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-compression-e2e").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Test each compression type
    for compression_type in [
        CompressionType::None,
        CompressionType::Gzip,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Zstd,
    ] {
        println!("Testing {compression_type:?} compression");

        // Connect to Heraclitus
        let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

        // Build a compressed batch
        let mut builder = RecordBatchBuilder::new(compression_type);
        builder.add_message(KafkaMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 1000,
            key: Some(b"test-key".to_vec()),
            value: format!("Test message with {compression_type:?} compression").into_bytes(),
            headers: HashMap::new(),
            producer_id: None,
            producer_epoch: None,
            sequence: None,
        });

        let record_batch_data = builder.build()?;

        // Send produce request
        send_produce_request(&mut stream, "test-topic", 0, record_batch_data).await?;

        // Read produce response
        let response = read_response(&mut stream).await?;

        // Parse response
        let mut cursor = Cursor::new(&response[..]);
        cursor.advance(4); // Skip correlation ID

        let topic_count = cursor.get_i32();
        assert_eq!(topic_count, 1, "Should have 1 topic in response");

        // Parse topic name
        let name_len = cursor.get_i16() as usize;
        cursor.advance(name_len); // Skip topic name

        // Parse partition responses
        let partition_count = cursor.get_i32();
        assert_eq!(partition_count, 1, "Should have 1 partition");

        let partition_index = cursor.get_i32();
        let error_code = cursor.get_i16();
        let base_offset = cursor.get_i64();

        println!(
            "Produce response: partition={partition_index}, error={error_code}, offset={base_offset}"
        );
        assert_eq!(error_code, 0, "Produce should succeed");
        assert_eq!(base_offset, 0, "First message should have offset 0");
    }

    Ok(())
}

async fn send_produce_request(
    stream: &mut TcpStream,
    topic: &str,
    partition: i32,
    record_batch_data: Vec<u8>,
) -> Result<()> {
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(0); // Produce API
    request.put_i16(3); // Version 3 (supports RecordBatch format)
    request.put_i32(1); // Correlation ID
    request.put_i16(-1); // No client ID

    // Produce request body
    request.put_i16(-1); // transactional_id (null)
    request.put_i16(1); // acks
    request.put_i32(30000); // timeout_ms

    // Topic data
    request.put_i32(1); // topic count
    request.put_i16(topic.len() as i16);
    request.extend_from_slice(topic.as_bytes());

    // Partition data
    request.put_i32(1); // partition count
    request.put_i32(partition); // partition index
    request.put_i32(record_batch_data.len() as i32); // records size
    request.extend_from_slice(&record_batch_data); // records

    // Send request with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;
    stream.flush().await?;

    Ok(())
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
