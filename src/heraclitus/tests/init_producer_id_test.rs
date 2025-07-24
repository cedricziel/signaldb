use bytes::{BufMut, BytesMut};
use common::config::Configuration;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

const KAFKA_PORT: u16 = 19096;

#[tokio::test]
async fn test_init_producer_id() {
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

    // Build InitProducerId request
    let mut request_body = BytesMut::new();

    // Transactional ID (nullable string) - null for idempotent producer
    request_body.put_i16(-1); // null

    // Transaction timeout
    request_body.put_i32(60000); // 60 seconds

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((request_body.len() + 10) as i32); // total size
    request.put_i16(22); // API key (InitProducerId)
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

    // Error code
    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    offset += 2;
    assert_eq!(error_code, 0); // SUCCESS

    // Producer ID
    let producer_id = i64::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
        response_buf[offset + 4],
        response_buf[offset + 5],
        response_buf[offset + 6],
        response_buf[offset + 7],
    ]);
    offset += 8;
    assert!(producer_id >= 1000); // Should be assigned from our counter

    // Producer epoch
    let producer_epoch = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    assert_eq!(producer_epoch, 0); // Should start with epoch 0
}

#[tokio::test]
async fn test_init_producer_id_transactional_not_supported() {
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

    // Build InitProducerId request with transactional ID
    let mut request_body = BytesMut::new();

    // Transactional ID
    let txn_id = "test-transaction";
    request_body.put_i16(txn_id.len() as i16);
    request_body.put_slice(txn_id.as_bytes());

    // Transaction timeout
    request_body.put_i32(60000); // 60 seconds

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((request_body.len() + 10) as i32); // total size
    request.put_i16(22); // API key (InitProducerId)
    request.put_i16(0); // API version
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
    let offset = 4;

    // Error code
    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    assert_eq!(error_code, 35); // NOT_COORDINATOR - transactional producers not supported
}
