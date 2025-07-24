use bytes::{BufMut, BytesMut};
use common::config::Configuration;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

const KAFKA_PORT: u16 = 19098;

#[tokio::test]
async fn test_delete_topics() {
    // Create temp dir for storage
    let temp_dir = tempfile::tempdir().unwrap();

    // Create minimal config
    let config_content = format!(
        r#"
[database]
dsn = "sqlite::memory:"

[storage]
dsn = "file://{}"
"#,
        temp_dir.path().to_str().unwrap()
    );

    let config_path = temp_dir.path().join("signaldb.toml");
    std::fs::write(&config_path, config_content).unwrap();

    // Load config and create HeraclitusConfig
    let config = Configuration::load_from_path(&config_path).unwrap();
    let mut heraclitus_config = HeraclitusConfig::from_common_config(config);
    heraclitus_config.kafka_port = KAFKA_PORT;

    // Start agent
    let mut agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(format!("127.0.0.1:{KAFKA_PORT}"))
        .await
        .expect("Failed to connect to Kafka");

    // First, create some topics to delete
    let topics_to_create = vec!["topic-to-delete-1", "topic-to-delete-2", "topic-to-keep"];

    for topic in &topics_to_create {
        // Create topic request
        let mut create_body = BytesMut::new();

        // Topics array
        create_body.put_i32(1); // 1 topic

        // Topic name
        create_body.put_i16(topic.len() as i16);
        create_body.put_slice(topic.as_bytes());

        // Number of partitions (-1 for default)
        create_body.put_i32(1);

        // Replication factor (-1 for default)
        create_body.put_i16(1);

        // Replica assignments array (empty)
        create_body.put_i32(0);

        // Config entries array (empty)
        create_body.put_i32(0);

        // Timeout
        create_body.put_i32(5000);

        // Validate only (v1+)
        create_body.put_u8(0); // false

        // Build complete request
        let mut request = BytesMut::new();
        request.put_i32((create_body.len() + 10) as i32); // total size
        request.put_i16(19); // API key (CreateTopics)
        request.put_i16(1); // API version
        request.put_i32(1); // correlation ID
        request.put_i16(-1); // no client ID
        request.extend_from_slice(&create_body);

        // Send request
        stream.write_all(&request).await.unwrap();
        stream.flush().await.unwrap();

        // Read response
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf);

        let mut response_buf = vec![0u8; size as usize];
        stream.read_exact(&mut response_buf).await.unwrap();

        // Verify topic was created successfully
        let mut offset = 4; // Skip correlation ID
        let topic_count = i32::from_be_bytes([
            response_buf[offset],
            response_buf[offset + 1],
            response_buf[offset + 2],
            response_buf[offset + 3],
        ]);
        offset += 4;
        assert_eq!(topic_count, 1);

        // Skip topic name
        let name_len =
            i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
        offset += 2 + name_len;

        // Check error code
        let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
        assert_eq!(error_code, 0); // SUCCESS
    }

    // Now test DeleteTopics
    let mut delete_body = BytesMut::new();

    // Topics array
    delete_body.put_i32(3); // Delete 3 topics (2 exist, 1 doesn't)

    // Topic 1 (exists)
    let topic1 = "topic-to-delete-1";
    delete_body.put_i16(topic1.len() as i16);
    delete_body.put_slice(topic1.as_bytes());

    // Topic 2 (exists)
    let topic2 = "topic-to-delete-2";
    delete_body.put_i16(topic2.len() as i16);
    delete_body.put_slice(topic2.as_bytes());

    // Topic 3 (doesn't exist)
    let topic3 = "nonexistent-topic";
    delete_body.put_i16(topic3.len() as i16);
    delete_body.put_slice(topic3.as_bytes());

    // Timeout
    delete_body.put_i32(5000);

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((delete_body.len() + 10) as i32); // total size
    request.put_i16(20); // API key (DeleteTopics)
    request.put_i16(0); // API version
    request.put_i32(2); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&delete_body);

    // Send request
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; size as usize];
    stream.read_exact(&mut response_buf).await.unwrap();

    // Parse response header
    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 2);

    // Parse response body
    let mut offset = 4;

    // Topic errors array
    let topic_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    offset += 4;
    assert_eq!(topic_count, 3);

    // Check results for each topic
    let mut results = Vec::new();
    for _ in 0..3 {
        // Topic name
        let name_len =
            i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
        offset += 2;
        let topic_name =
            String::from_utf8(response_buf[offset..offset + name_len].to_vec()).unwrap();
        offset += name_len;

        // Error code
        let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
        offset += 2;

        results.push((topic_name, error_code));
    }

    // Verify results
    for (topic_name, error_code) in results {
        match topic_name.as_str() {
            "topic-to-delete-1" | "topic-to-delete-2" => {
                assert_eq!(error_code, 0, "Should successfully delete existing topic");
            }
            "nonexistent-topic" => {
                assert_eq!(
                    error_code, 3,
                    "Should return UNKNOWN_TOPIC_OR_PARTITION for nonexistent topic"
                );
            }
            _ => panic!("Unexpected topic in response: {topic_name}"),
        }
    }

    // Verify topics were actually deleted by trying to create them again
    // (which should succeed if they were deleted)
    let mut create_body = BytesMut::new();

    // Topics array
    create_body.put_i32(2); // Create same 2 topics again

    // Topic 1
    create_body.put_i16(topic1.len() as i16);
    create_body.put_slice(topic1.as_bytes());
    create_body.put_i32(1); // partitions
    create_body.put_i16(1); // replication factor
    create_body.put_i32(0); // replica assignments
    create_body.put_i32(0); // config entries

    // Topic 2
    create_body.put_i16(topic2.len() as i16);
    create_body.put_slice(topic2.as_bytes());
    create_body.put_i32(1); // partitions
    create_body.put_i16(1); // replication factor
    create_body.put_i32(0); // replica assignments
    create_body.put_i32(0); // config entries

    // Timeout and validate only
    create_body.put_i32(5000);
    create_body.put_u8(0); // false

    // Build create topics request
    let mut request = BytesMut::new();
    request.put_i32((create_body.len() + 10) as i32);
    request.put_i16(19); // API key (CreateTopics)
    request.put_i16(1); // API version
    request.put_i32(3); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&create_body);

    // Send request
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; size as usize];
    stream.read_exact(&mut response_buf).await.unwrap();

    // Parse create topics response to verify topics were deleted and can be recreated
    let mut offset = 4; // Skip correlation ID
    let topic_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    offset += 4;
    assert_eq!(topic_count, 2);

    // Both topics should be successfully created (error code 0)
    for _ in 0..2 {
        // Topic name
        let name_len =
            i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
        offset += 2;
        let _topic_name =
            String::from_utf8(response_buf[offset..offset + name_len].to_vec()).unwrap();
        offset += name_len;

        // Error code
        let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
        offset += 2;
        assert_eq!(error_code, 0, "Should be able to recreate deleted topic");

        // Skip error message (v1+)
        let msg_len = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
        offset += 2;
        if msg_len >= 0 {
            offset += msg_len as usize;
        }
    }
}

#[tokio::test]
async fn test_delete_topics_with_timeout() {
    // Create temp dir for storage
    let temp_dir = tempfile::tempdir().unwrap();

    // Create minimal config
    let config_content = format!(
        r#"
[database]
dsn = "sqlite::memory:"

[storage]
dsn = "file://{}"
"#,
        temp_dir.path().to_str().unwrap()
    );

    let config_path = temp_dir.path().join("signaldb.toml");
    std::fs::write(&config_path, config_content).unwrap();

    // Load config and create HeraclitusConfig
    let config = Configuration::load_from_path(&config_path).unwrap();
    let mut heraclitus_config = HeraclitusConfig::from_common_config(config);
    heraclitus_config.kafka_port = KAFKA_PORT + 1;

    // Start agent
    let mut agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", KAFKA_PORT + 1))
        .await
        .expect("Failed to connect to Kafka");

    // Test with v1 (timeout can be -1)
    let mut delete_body = BytesMut::new();

    // Topics array
    delete_body.put_i32(1);

    // Topic name
    let topic = "test-timeout";
    delete_body.put_i16(topic.len() as i16);
    delete_body.put_slice(topic.as_bytes());

    // Timeout (-1 for no timeout in v1+)
    delete_body.put_i32(-1);

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((delete_body.len() + 10) as i32);
    request.put_i16(20); // API key (DeleteTopics)
    request.put_i16(1); // API version 1
    request.put_i32(1); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&delete_body);

    // Send request
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; size as usize];
    stream.read_exact(&mut response_buf).await.unwrap();

    // Should succeed with no timeout
    let correlation_id = i32::from_be_bytes([
        response_buf[0],
        response_buf[1],
        response_buf[2],
        response_buf[3],
    ]);
    assert_eq!(correlation_id, 1);
}
