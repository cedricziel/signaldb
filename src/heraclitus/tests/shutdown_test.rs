use common::config::Configuration;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_graceful_shutdown() {
    // Create temp dir for storage
    let temp_dir = tempfile::tempdir().unwrap();

    // Create minimal config
    let config_content = format!(
        r#"
[database]
dsn = "sqlite::memory:"

[storage]
dsn = "file://{}"

[heraclitus]
shutdown_timeout_sec = 5
"#,
        temp_dir.path().to_str().unwrap()
    );

    let config_path = temp_dir.path().join("signaldb.toml");
    std::fs::write(&config_path, config_content).unwrap();

    // Load config and create HeraclitusConfig
    let config = Configuration::load_from_path(&config_path).unwrap();
    let heraclitus_config = HeraclitusConfig::from_common_config(config);

    // Start agent
    let mut agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    // Run agent in background
    let agent_handle = tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Verify it's running by checking metrics endpoint
    let response = reqwest::get("http://127.0.0.1:9093/health")
        .await
        .expect("Failed to connect to health endpoint");
    assert_eq!(response.status(), 200);

    // Send shutdown signal by aborting (simulates Ctrl+C)
    agent_handle.abort();

    // Give it time to shutdown gracefully
    sleep(Duration::from_secs(2)).await;

    // Verify it's no longer accepting connections
    assert!(reqwest::get("http://127.0.0.1:9093/health").await.is_err());
}

#[tokio::test]
async fn test_shutdown_with_pending_batches() {
    use bytes::{BufMut, BytesMut};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    // Create temp dir for storage
    let temp_dir = tempfile::tempdir().unwrap();

    // Create minimal config with batching
    let config_content = format!(
        r#"
[database]
dsn = "sqlite::memory:"

[storage]
dsn = "file://{}"

[heraclitus]
shutdown_timeout_sec = 10

[heraclitus.batching]
max_batch_size = 10
max_batch_delay_ms = 60000
"#,
        temp_dir.path().to_str().unwrap()
    );

    let config_path = temp_dir.path().join("signaldb.toml");
    std::fs::write(&config_path, config_content).unwrap();

    // Load config and create HeraclitusConfig
    let config = Configuration::load_from_path(&config_path).unwrap();
    let heraclitus_config = HeraclitusConfig::from_common_config(config);

    // Start agent
    let mut agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    // Run agent in background
    let agent_handle = tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Connect and send some messages (less than batch size)
    let mut stream = TcpStream::connect("127.0.0.1:9092")
        .await
        .expect("Failed to connect to Kafka");

    // Send a produce request with a few messages
    let mut produce_body = BytesMut::new();

    // Topic count (1)
    produce_body.put_i32(1);

    // Topic name
    produce_body.put_i16(4);
    produce_body.put_slice(b"test");

    // Partition count (1)
    produce_body.put_i32(1);

    // Partition 0
    produce_body.put_i32(0);

    // Message set size placeholder
    let size_pos = produce_body.len();
    produce_body.put_i32(0);

    // Add 3 messages (less than batch size of 10)
    let messages_start = produce_body.len();
    for i in 0..3 {
        // Offset
        produce_body.put_i64(i);

        // Message size placeholder
        let msg_size_pos = produce_body.len();
        produce_body.put_i32(0);

        let msg_start = produce_body.len();

        // CRC (0 for simplicity)
        produce_body.put_i32(0);

        // Magic byte (0)
        produce_body.put_u8(0);

        // Attributes (0)
        produce_body.put_u8(0);

        // Key (-1 for null)
        produce_body.put_i32(-1);

        // Value
        let value = format!("message-{i}");
        produce_body.put_i32(value.len() as i32);
        produce_body.put_slice(value.as_bytes());

        // Update message size
        let msg_size = (produce_body.len() - msg_start) as i32;
        let msg_size_bytes = msg_size.to_be_bytes();
        produce_body[msg_size_pos..msg_size_pos + 4].copy_from_slice(&msg_size_bytes);
    }

    // Update message set size
    let messages_size = (produce_body.len() - messages_start) as i32;
    let size_bytes = messages_size.to_be_bytes();
    produce_body[size_pos..size_pos + 4].copy_from_slice(&size_bytes);

    // Required acks
    produce_body.put_i16(1);

    // Timeout
    produce_body.put_i32(1000);

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((produce_body.len() + 10) as i32); // total size
    request.put_i16(0); // API key (Produce)
    request.put_i16(0); // API version
    request.put_i32(1); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&produce_body);

    // Send request
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let _size = i32::from_be_bytes(size_buf);

    // Give a moment for the message to be processed
    sleep(Duration::from_millis(100)).await;

    // Now shutdown - the pending messages should be flushed
    agent_handle.abort();

    // Give it time to flush and shutdown
    sleep(Duration::from_secs(3)).await;

    // Check that files were written to storage
    let storage_path = temp_dir.path();
    let mut found_parquet = false;

    for entry in walkdir::WalkDir::new(storage_path).into_iter().flatten() {
        if entry.path().to_string_lossy().ends_with(".parquet") {
            found_parquet = true;
            break;
        }
    }

    assert!(
        found_parquet,
        "Expected to find parquet files after shutdown flush"
    );
}
