use bytes::{Buf, BufMut, BytesMut};
use common::config::Configuration;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

/// Helper to build a Kafka request frame
fn build_request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &str,
    body: &[u8],
) -> Vec<u8> {
    let mut request = BytesMut::new();

    // Request header
    request.put_i16(api_key);
    request.put_i16(api_version);
    request.put_i32(correlation_id);

    // Client ID (nullable string)
    if client_id.is_empty() {
        request.put_i16(-1); // null
    } else {
        request.put_i16(client_id.len() as i16);
        request.put_slice(client_id.as_bytes());
    }

    // Request body
    request.put_slice(body);

    // Frame with length prefix
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.put_slice(&request);

    frame.to_vec()
}

/// Helper to read response frame
async fn read_response_frame(
    stream: &mut TcpStream,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Read message length
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = i32::from_be_bytes(len_buf) as usize;

    // Read message body
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;

    Ok(body)
}

#[tokio::test]
async fn test_sasl_handshake_supported() {
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
    heraclitus_config.auth.enabled = true;
    heraclitus_config.auth.mechanism = "PLAIN".to_string();
    heraclitus_config.auth.plain_username = Some("alice".to_string());
    heraclitus_config.auth.plain_password = Some("secret123".to_string());
    heraclitus_config.kafka_port = 19092; // Use a different port for tests

    // Start agent
    let agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Connect to Kafka port
    let mut stream = TcpStream::connect("127.0.0.1:19092").await.unwrap();

    // Send SaslHandshake request
    let mut handshake_body = BytesMut::new();
    handshake_body.put_i16(5); // mechanism length
    handshake_body.put_slice(b"PLAIN");

    let request_frame = build_request_frame(17, 0, 1, "test-client", &handshake_body);
    stream.write_all(&request_frame).await.unwrap();

    // Read response
    let response_body = read_response_frame(&mut stream).await.unwrap();
    let mut cursor = Cursor::new(&response_body[..]);

    // Response header
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 1);

    // Response body
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0); // ERROR_NONE

    // Enabled mechanisms
    let mechanisms_count = cursor.get_i32();
    assert_eq!(mechanisms_count, 1);

    let mechanism_len = cursor.get_i16();
    let mut mechanism_bytes = vec![0u8; mechanism_len as usize];
    cursor.copy_to_slice(&mut mechanism_bytes);
    assert_eq!(&mechanism_bytes, b"PLAIN");
}

#[tokio::test]
async fn test_sasl_handshake_unsupported_mechanism() {
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
    heraclitus_config.auth.enabled = true;
    heraclitus_config.auth.mechanism = "PLAIN".to_string();
    heraclitus_config.kafka_port = 19093; // Use a different port for tests

    // Start agent
    let agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Connect to Kafka port
    let mut stream = TcpStream::connect("127.0.0.1:19093").await.unwrap();

    // Send SaslHandshake request with unsupported mechanism
    let mut handshake_body = BytesMut::new();
    handshake_body.put_i16(13); // mechanism length
    handshake_body.put_slice(b"SCRAM-SHA-256");

    let request_frame = build_request_frame(17, 0, 1, "test-client", &handshake_body);
    stream.write_all(&request_frame).await.unwrap();

    // Read response
    let response_body = read_response_frame(&mut stream).await.unwrap();
    let mut cursor = Cursor::new(&response_body[..]);

    // Response header
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 1);

    // Response body
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 33); // ERROR_UNSUPPORTED_SASL_MECHANISM
}

#[tokio::test]
async fn test_sasl_authenticate_success() {
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
    heraclitus_config.auth.enabled = true;
    heraclitus_config.auth.mechanism = "PLAIN".to_string();
    heraclitus_config.auth.plain_username = Some("alice".to_string());
    heraclitus_config.auth.plain_password = Some("secret123".to_string());
    heraclitus_config.kafka_port = 19094; // Use a different port for tests

    // Start agent
    let agent = HeraclitusAgent::new(heraclitus_config).await.unwrap();

    tokio::spawn(async move {
        agent.run().await.unwrap();
    });

    // Wait for agent to start
    sleep(Duration::from_millis(500)).await;

    // Connect to Kafka port
    let mut stream = TcpStream::connect("127.0.0.1:19094").await.unwrap();

    // First send SaslHandshake
    let mut handshake_body = BytesMut::new();
    handshake_body.put_i16(5); // mechanism length
    handshake_body.put_slice(b"PLAIN");

    let request_frame = build_request_frame(17, 0, 1, "test-client", &handshake_body);
    stream.write_all(&request_frame).await.unwrap();
    let _ = read_response_frame(&mut stream).await.unwrap();

    // Now send SaslAuthenticate with correct credentials
    let mut auth_bytes = Vec::new();
    auth_bytes.extend_from_slice(b""); // empty authzid
    auth_bytes.push(0); // null separator
    auth_bytes.extend_from_slice(b"alice");
    auth_bytes.push(0); // null separator
    auth_bytes.extend_from_slice(b"secret123");

    let mut auth_body = BytesMut::new();
    auth_body.put_i32(auth_bytes.len() as i32);
    auth_body.put_slice(&auth_bytes);

    let request_frame = build_request_frame(36, 0, 2, "test-client", &auth_body);
    stream.write_all(&request_frame).await.unwrap();

    // Read response
    let response_body = read_response_frame(&mut stream).await.unwrap();
    let mut cursor = Cursor::new(&response_body[..]);

    // Response header
    let correlation_id = cursor.get_i32();
    assert_eq!(correlation_id, 2);

    // Response body
    let error_code = cursor.get_i16();
    assert_eq!(error_code, 0); // ERROR_NONE
}
