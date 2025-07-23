use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use heraclitus::protocol::RecordBatch;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_produce_and_fetch_with_minio() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    // Test 1: Send metadata request to verify connection
    let metadata_response = send_metadata_request(&mut stream).await?;
    assert!(!metadata_response.is_empty());

    // Test 2: Produce a message
    let topic = "test-topic";
    let key = b"test-key";
    let value = b"test-value";
    let produce_response = produce_message(&mut stream, topic, 0, key, value).await?;

    // Verify produce succeeded
    let mut cursor = Cursor::new(&produce_response[..]);
    Buf::advance(&mut cursor, 4); // Skip correlation ID
    let topic_count = cursor.get_i32();
    assert_eq!(topic_count, 1);

    // Parse produce response to see if it was successful
    let topic_name_len = cursor.get_i16() as usize;
    let mut topic_name_bytes = vec![0u8; topic_name_len];
    cursor.copy_to_slice(&mut topic_name_bytes);
    let topic_name = String::from_utf8(topic_name_bytes)?;

    let partition_count = cursor.get_i32();
    assert_eq!(partition_count, 1);

    let partition_id = cursor.get_i32();
    let error_code = cursor.get_i16();
    let base_offset = cursor.get_i64();

    println!(
        "Produce response: topic={topic_name}, partition={partition_id}, error_code={error_code}, base_offset={base_offset}"
    );

    assert_eq!(
        error_code, 0,
        "Produce request failed with error code {error_code}"
    );

    // Give time for message to be persisted
    // The batch delay is configured to 100ms in the test config
    // Add extra time for actual write to object storage
    println!("Waiting for batch to flush...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Give a bit more time for object storage write to complete
    println!("Waiting for object storage write...");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test 3: Fetch the message back
    println!("Fetching messages from topic {topic} partition 0 offset 0");
    let fetch_response = fetch_messages(&mut stream, topic, 0, 0).await?;
    println!("Fetch response size: {} bytes", fetch_response.len());

    // Parse fetch response and verify message
    let messages = parse_fetch_response(&fetch_response)?;

    // Add some debugging
    println!("Parsed {} messages from fetch response", messages.len());
    for (i, (k, v)) in messages.iter().enumerate() {
        println!(
            "Message {}: key={:?}, value={:?}",
            i,
            String::from_utf8_lossy(k),
            String::from_utf8_lossy(v)
        );
    }

    // Now check the messages
    assert_eq!(
        messages.len(),
        1,
        "Expected 1 message but got {}",
        messages.len()
    );
    assert_eq!(messages[0].0, key);
    assert_eq!(messages[0].1, value);

    // Test 4: Verify data exists in MinIO
    let s3_client = create_s3_client(&minio).await?;
    let objects = list_objects(&s3_client, &minio.bucket).await?;

    println!("Found {} objects in MinIO:", objects.len());
    for obj in &objects {
        println!("  - {obj}");
    }

    assert!(!objects.is_empty(), "Expected Parquet files in MinIO");

    Ok(())
}

async fn send_metadata_request(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(3); // Metadata API
    request.put_i16(0); // Version
    request.put_i32(1); // Correlation ID
    request.put_i16(-1); // No client ID

    // Request body - all topics
    request.put_i32(-1);

    // Send request with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read response
    read_response(stream).await
}

async fn produce_message(
    stream: &mut TcpStream,
    topic: &str,
    partition: i32,
    key: &[u8],
    value: &[u8],
) -> Result<Vec<u8>> {
    use heraclitus::protocol::{CompressionType, RecordBatchBuilder};
    use heraclitus::storage::KafkaMessage;
    use std::collections::HashMap;

    // Create a record batch with our message
    let mut builder = RecordBatchBuilder::new(CompressionType::None);
    builder.add_message(KafkaMessage {
        topic: topic.to_string(),
        partition,
        offset: 0, // Will be assigned by server
        timestamp: chrono::Utc::now().timestamp_millis(),
        key: Some(key.to_vec()),
        value: value.to_vec(),
        headers: HashMap::new(),
        producer_id: None,
        producer_epoch: None,
        sequence: None,
    });

    let record_batch = builder.build()?;

    // Build produce request
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(0); // Produce API
    request.put_i16(3); // Version 3
    request.put_i32(2); // Correlation ID
    request.put_i16(-1); // No client ID

    // Request body
    request.put_i16(-1); // No transactional ID
    request.put_i16(1); // Required acks
    request.put_i32(30000); // Timeout
    request.put_i32(1); // Topic count

    // Topic data
    request.put_i16(topic.len() as i16);
    request.put_slice(topic.as_bytes());
    request.put_i32(1); // Partition count

    // Partition data
    request.put_i32(partition);
    request.put_i32(record_batch.len() as i32);
    request.extend_from_slice(&record_batch);

    // Send request
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;
    stream.flush().await?;

    read_response(stream).await
}

async fn fetch_messages(
    stream: &mut TcpStream,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<Vec<u8>> {
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(1); // Fetch API
    request.put_i16(0); // Version 0 for simplicity
    request.put_i32(3); // Correlation ID
    request.put_i16(-1); // No client ID

    // Request body
    request.put_i32(-1); // Replica ID (consumer)
    request.put_i32(1000); // Max wait ms
    request.put_i32(1); // Min bytes
    request.put_i32(1); // Topic count

    // Topic
    request.put_i16(topic.len() as i16);
    request.put_slice(topic.as_bytes());
    request.put_i32(1); // Partition count

    // Partition
    request.put_i32(partition);
    request.put_i64(offset);
    request.put_i32(1048576); // Max bytes

    // Send request
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    stream.write_all(&frame).await?;
    stream.flush().await?;

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

fn parse_fetch_response(response: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut cursor = Cursor::new(response);
    Buf::advance(&mut cursor, 4); // Skip correlation ID

    let topic_count = cursor.get_i32();
    let mut messages = Vec::new();

    for _ in 0..topic_count {
        // Read topic name
        let topic_len = cursor.get_i16() as usize;
        let mut topic_bytes = vec![0u8; topic_len];
        cursor.copy_to_slice(&mut topic_bytes);

        let partition_count = cursor.get_i32();

        for _ in 0..partition_count {
            let _partition = cursor.get_i32();
            let _error_code = cursor.get_i16();
            let _high_watermark = cursor.get_i64();

            let records_len = cursor.get_i32() as usize;
            if records_len > 0 {
                let mut records_data = vec![0u8; records_len];
                cursor.copy_to_slice(&mut records_data);

                // Parse RecordBatch
                let batch = RecordBatch::parse(&records_data)?;
                for record in batch.records {
                    let key = record.key.unwrap_or_default();
                    let value = record.value;
                    messages.push((key, value));
                }
            }
        }
    }

    Ok(messages)
}

async fn create_s3_client(minio: &MinioTestContext) -> Result<aws_sdk_s3::Client> {
    use aws_config::Region;
    use aws_credential_types::Credentials;

    let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(&minio.endpoint)
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .load()
        .await;

    Ok(aws_sdk_s3::Client::new(&config))
}

async fn list_objects(client: &aws_sdk_s3::Client, bucket: &str) -> Result<Vec<String>> {
    let mut objects = Vec::new();
    let mut continuation_token = None;

    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("heraclitus-test/");

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request.send().await?;

        println!(
            "List objects response: is_truncated={:?}, key_count={:?}",
            response.is_truncated, response.key_count
        );

        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(key) = object.key {
                    objects.push(key);
                }
            }
        }

        if response.is_truncated == Some(true) {
            continuation_token = response.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(objects)
}
