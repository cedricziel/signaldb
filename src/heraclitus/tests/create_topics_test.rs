use bytes::{BufMut, BytesMut};
use common::config::Configuration;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

const KAFKA_PORT: u16 = 19095;

#[tokio::test]
async fn test_create_topics() {
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
    let agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(format!("127.0.0.1:{KAFKA_PORT}"))
        .await
        .expect("Failed to connect to Kafka");

    // Build CreateTopics request
    let mut request_body = BytesMut::new();

    // Topics array (non-compact for v0)
    request_body.put_i32(2); // 2 topics

    // Topic 1
    request_body.put_i16(10); // name length
    request_body.put_slice(b"test-topic");
    request_body.put_i32(3); // num_partitions
    request_body.put_i16(1); // replication_factor
    request_body.put_i32(0); // no assignments
    request_body.put_i32(0); // no configs

    // Topic 2 (should fail - already exists)
    request_body.put_i16(10); // name length
    request_body.put_slice(b"test-topic");
    request_body.put_i32(1); // num_partitions
    request_body.put_i16(1); // replication_factor
    request_body.put_i32(0); // no assignments
    request_body.put_i32(0); // no configs

    // Timeout
    request_body.put_i32(5000); // 5 second timeout

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((request_body.len() + 10) as i32); // total size
    request.put_i16(19); // API key (CreateTopics)
    request.put_i16(0); // API version
    request.put_i32(1); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&request_body);

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
    assert_eq!(correlation_id, 1);

    // Parse response body
    let mut offset = 4;

    // Topics array
    let topic_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    offset += 4;
    assert_eq!(topic_count, 2);

    // Topic 1 response
    let name_len = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2;
    let topic_name = String::from_utf8(response_buf[offset..offset + name_len].to_vec()).unwrap();
    offset += name_len;
    assert_eq!(topic_name, "test-topic");

    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    offset += 2;
    assert_eq!(error_code, 0); // SUCCESS

    // Topic 2 response
    let name_len = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2;
    let topic_name = String::from_utf8(response_buf[offset..offset + name_len].to_vec()).unwrap();
    offset += name_len;
    assert_eq!(topic_name, "test-topic");

    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    assert_eq!(error_code, 36); // TOPIC_ALREADY_EXISTS
}

#[tokio::test]
async fn test_create_topics_validation_only() {
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
    let agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Connect to Kafka port
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", KAFKA_PORT + 1))
        .await
        .expect("Failed to connect to Kafka");

    // Build CreateTopics request with validate_only=true
    let mut request_body = BytesMut::new();

    // Topics array
    request_body.put_i32(1); // 1 topic

    // Topic
    request_body.put_i16(14); // name length
    request_body.put_slice(b"validate-topic");
    request_body.put_i32(5); // num_partitions
    request_body.put_i16(3); // replication_factor
    request_body.put_i32(0); // no assignments
    request_body.put_i32(0); // no configs

    // Timeout
    request_body.put_i32(5000); // 5 second timeout

    // Validate only (v1+)
    request_body.put_i8(1); // true

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((request_body.len() + 10) as i32); // total size
    request.put_i16(19); // API key (CreateTopics)
    request.put_i16(1); // API version (supports validate_only)
    request.put_i32(2); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&request_body);

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

    // Topics array
    let topic_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    offset += 4;
    assert_eq!(topic_count, 1);

    // Topic response
    let name_len = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2;
    let topic_name = String::from_utf8(response_buf[offset..offset + name_len].to_vec()).unwrap();
    offset += name_len;
    assert_eq!(topic_name, "validate-topic");

    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    offset += 2;
    assert_eq!(error_code, 0); // SUCCESS

    // For v1+, we have error_message
    let msg_len = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    assert_eq!(msg_len, -1); // null message

    // Now verify the topic was NOT actually created by trying to create it again
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", KAFKA_PORT + 1))
        .await
        .expect("Failed to connect to Kafka");

    // Send the same request but with validate_only=false
    request_body.clear();
    request_body.put_i32(1); // 1 topic
    request_body.put_i16(14); // name length
    request_body.put_slice(b"validate-topic");
    request_body.put_i32(5); // num_partitions
    request_body.put_i16(3); // replication_factor
    request_body.put_i32(0); // no assignments
    request_body.put_i32(0); // no configs
    request_body.put_i32(5000); // timeout
    request_body.put_i8(0); // validate_only = false

    request.clear();
    request.put_i32((request_body.len() + 10) as i32);
    request.put_i16(19); // CreateTopics
    request.put_i16(1); // API version
    request.put_i32(3); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&request_body);

    stream2.write_all(&request).await.unwrap();
    stream2.flush().await.unwrap();

    // Read response
    stream2.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf);
    let mut response_buf = vec![0u8; size as usize];
    stream2.read_exact(&mut response_buf).await.unwrap();

    // This should succeed since the topic wasn't actually created
    let mut offset = 4 + 4; // skip correlation_id and topic count
    offset += 2; // skip name length
    offset += 14; // skip topic name
    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    assert_eq!(error_code, 0); // SUCCESS - proving validation didn't create the topic
}
