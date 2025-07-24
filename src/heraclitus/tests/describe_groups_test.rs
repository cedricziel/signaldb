use bytes::{BufMut, BytesMut};
use common::config::Configuration;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

const KAFKA_PORT: u16 = 19097;

#[tokio::test]
async fn test_describe_groups() {
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

    // First, create a consumer group by joining it
    let group_id = "test-describe-group";
    let member_id = ""; // Empty for new member
    let protocol_type = "consumer";

    // Send JoinGroup request
    let mut join_body = BytesMut::new();

    // Group ID
    join_body.put_i16(group_id.len() as i16);
    join_body.put_slice(group_id.as_bytes());

    // Session timeout
    join_body.put_i32(30000);

    // Rebalance timeout (v1+)
    join_body.put_i32(60000);

    // Member ID
    join_body.put_i16(member_id.len() as i16);
    join_body.put_slice(member_id.as_bytes());

    // Protocol type
    join_body.put_i16(protocol_type.len() as i16);
    join_body.put_slice(protocol_type.as_bytes());

    // Group protocols array
    join_body.put_i32(1); // 1 protocol

    // Protocol name
    let protocol_name = "range";
    join_body.put_i16(protocol_name.len() as i16);
    join_body.put_slice(protocol_name.as_bytes());

    // Protocol metadata (simplified)
    let metadata = b"subscription";
    join_body.put_i32(metadata.len() as i32);
    join_body.put_slice(metadata);

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((join_body.len() + 10) as i32); // total size
    request.put_i16(11); // API key (JoinGroup)
    request.put_i16(1); // API version
    request.put_i32(1); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&join_body);

    // Send request
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let _size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; _size as usize];
    stream.read_exact(&mut response_buf).await.unwrap();

    // Parse JoinGroup response to get member ID
    let mut offset = 4; // Skip correlation ID
    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    offset += 2;
    assert_eq!(error_code, 0, "JoinGroup should succeed");

    // Skip generation ID
    offset += 4;

    // Skip protocol
    let protocol_len =
        i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2 + protocol_len;

    // Skip leader
    let leader_len = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2 + leader_len;

    // Get member ID
    let member_id_len =
        i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2;
    let _member_id =
        String::from_utf8(response_buf[offset..offset + member_id_len].to_vec()).unwrap();

    // Now test DescribeGroups
    let mut describe_body = BytesMut::new();

    // Groups array
    describe_body.put_i32(2); // Request info for 2 groups

    // Group 1 (exists)
    describe_body.put_i16(group_id.len() as i16);
    describe_body.put_slice(group_id.as_bytes());

    // Group 2 (doesn't exist)
    let non_existent_group = "non-existent-group";
    describe_body.put_i16(non_existent_group.len() as i16);
    describe_body.put_slice(non_existent_group.as_bytes());

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((describe_body.len() + 10) as i32); // total size
    request.put_i16(15); // API key (DescribeGroups)
    request.put_i16(0); // API version
    request.put_i32(2); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&describe_body);

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

    // Groups array
    let group_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    offset += 4;
    assert_eq!(group_count, 2);

    // First group (should exist)
    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    offset += 2;
    assert_eq!(error_code, 0); // SUCCESS

    // Group ID
    let group_id_len =
        i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2;
    let returned_group_id =
        String::from_utf8(response_buf[offset..offset + group_id_len].to_vec()).unwrap();
    offset += group_id_len;
    assert_eq!(returned_group_id, group_id);

    // State
    let state_len = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2;
    let state = String::from_utf8(response_buf[offset..offset + state_len].to_vec()).unwrap();
    offset += state_len;
    assert_eq!(state, "Stable");

    // Protocol type
    let protocol_type_len =
        i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2;
    let protocol_type_result =
        String::from_utf8(response_buf[offset..offset + protocol_type_len].to_vec()).unwrap();
    offset += protocol_type_len;
    assert_eq!(protocol_type_result, protocol_type);

    // Protocol data
    let protocol_data_len =
        i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
    offset += 2;
    let protocol_data =
        String::from_utf8(response_buf[offset..offset + protocol_data_len].to_vec()).unwrap();
    offset += protocol_data_len;
    assert_eq!(protocol_data, protocol_name);

    // Members array
    let members_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    offset += 4;
    assert_eq!(members_count, 1); // Should have 1 member

    // Skip member details for now
    // Skip to second group by finding it
    while offset < response_buf.len() - 2 {
        if response_buf[offset..].starts_with(&[0, non_existent_group.len() as u8]) {
            break;
        }
        offset += 1;
    }

    // Find the error code for the non-existent group (should be right before the group ID)
    offset -= 2;
    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    assert_eq!(error_code, 69); // GROUP_ID_NOT_FOUND
}

#[tokio::test]
async fn test_describe_empty_group() {
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

    // Test DescribeGroups with empty request
    let mut describe_body = BytesMut::new();

    // Groups array (empty)
    describe_body.put_i32(0); // Request info for 0 groups

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((describe_body.len() + 10) as i32); // total size
    request.put_i16(15); // API key (DescribeGroups)
    request.put_i16(0); // API version
    request.put_i32(1); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&describe_body);

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
    let offset = 4;

    // Groups array
    let group_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    assert_eq!(group_count, 0); // Should have no groups
}
