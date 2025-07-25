use bytes::{BufMut, BytesMut};
use common::config::Configuration;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

const KAFKA_PORT: u16 = 19099;

#[tokio::test]
async fn test_list_groups() {
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

    // First test - list groups when there are none
    let list_body = BytesMut::new();
    // No states filter for v0

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((list_body.len() + 10) as i32); // total size
    request.put_i16(16); // API key (ListGroups)
    request.put_i16(0); // API version
    request.put_i32(1); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&list_body);

    // Send request
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; size as usize];
    stream.read_exact(&mut response_buf).await.unwrap();

    // Parse response
    let mut offset = 4; // Skip correlation ID

    // Error code
    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    offset += 2;
    assert_eq!(error_code, 0); // SUCCESS

    // Groups array
    let group_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    assert_eq!(group_count, 0); // No groups yet

    // Now create some consumer groups
    let groups_to_create = vec![
        ("test-group-1", "consumer"),
        ("test-group-2", "consumer"),
        ("test-group-3", "connect"),
    ];

    for (group_id, protocol_type) in &groups_to_create {
        // Send JoinGroup request
        let mut join_body = BytesMut::new();

        // Group ID
        join_body.put_i16(group_id.len() as i16);
        join_body.put_slice(group_id.as_bytes());

        // Session timeout
        join_body.put_i32(30000);

        // Rebalance timeout (v1+)
        join_body.put_i32(60000);

        // Member ID (empty for new member)
        join_body.put_i16(0);

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
        request.put_i32(2); // correlation ID
        request.put_i16(-1); // no client ID
        request.extend_from_slice(&join_body);

        // Send request
        stream.write_all(&request).await.unwrap();
        stream.flush().await.unwrap();

        // Read response
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf);

        let mut response_buf = vec![0u8; size as usize];
        stream.read_exact(&mut response_buf).await.unwrap();

        // Check that join was successful
        let offset = 4; // Skip correlation ID
        let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
        assert_eq!(error_code, 0, "JoinGroup should succeed");
    }

    // Now test ListGroups again - should return all groups
    let list_body = BytesMut::new();
    // No states filter for v0

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((list_body.len() + 10) as i32); // total size
    request.put_i16(16); // API key (ListGroups)
    request.put_i16(0); // API version
    request.put_i32(3); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&list_body);

    // Send request
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; size as usize];
    stream.read_exact(&mut response_buf).await.unwrap();

    // Parse response
    let mut offset = 4; // Skip correlation ID

    // Error code
    let error_code = i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]);
    offset += 2;
    assert_eq!(error_code, 0); // SUCCESS

    // Groups array
    let group_count = i32::from_be_bytes([
        response_buf[offset],
        response_buf[offset + 1],
        response_buf[offset + 2],
        response_buf[offset + 3],
    ]);
    offset += 4;
    assert_eq!(group_count, 3); // Should have 3 groups now

    // Collect groups
    let mut found_groups = Vec::new();
    for _ in 0..group_count {
        // Group ID
        let group_id_len =
            i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
        offset += 2;
        let group_id =
            String::from_utf8(response_buf[offset..offset + group_id_len].to_vec()).unwrap();
        offset += group_id_len;

        // Protocol type
        let protocol_type_len =
            i16::from_be_bytes([response_buf[offset], response_buf[offset + 1]]) as usize;
        offset += 2;
        let protocol_type =
            String::from_utf8(response_buf[offset..offset + protocol_type_len].to_vec()).unwrap();
        offset += protocol_type_len;

        found_groups.push((group_id, protocol_type));
    }

    // Verify all groups are present
    assert!(found_groups.contains(&("test-group-1".to_string(), "consumer".to_string())));
    assert!(found_groups.contains(&("test-group-2".to_string(), "consumer".to_string())));
    assert!(found_groups.contains(&("test-group-3".to_string(), "connect".to_string())));
}

#[tokio::test]
async fn test_list_groups_with_state_filter() {
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

    // Create a consumer group
    let mut join_body = BytesMut::new();

    // Group ID
    let group_id = "test-group-v4";
    join_body.put_i16(group_id.len() as i16);
    join_body.put_slice(group_id.as_bytes());

    // Session timeout
    join_body.put_i32(30000);

    // Rebalance timeout (v1+)
    join_body.put_i32(60000);

    // Member ID (empty for new member)
    join_body.put_i16(0);

    // Protocol type
    let protocol_type = "consumer";
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
    let size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; size as usize];
    stream.read_exact(&mut response_buf).await.unwrap();

    // Now test ListGroups v4 with state filter
    let mut list_body = BytesMut::new();

    // States filter array (v4+)
    list_body.put_i32(1); // 1 state filter
    let state_filter = "Stable";
    list_body.put_i16(state_filter.len() as i16);
    list_body.put_slice(state_filter.as_bytes());

    // Build complete request
    let mut request = BytesMut::new();
    request.put_i32((list_body.len() + 10) as i32); // total size
    request.put_i16(16); // API key (ListGroups)
    request.put_i16(4); // API version 4
    request.put_i32(2); // correlation ID
    request.put_i16(-1); // no client ID
    request.extend_from_slice(&list_body);

    // Send request
    stream.write_all(&request).await.unwrap();
    stream.flush().await.unwrap();

    // Read response
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf);

    let mut response_buf = vec![0u8; size as usize];
    stream.read_exact(&mut response_buf).await.unwrap();

    // Parse response - v4 uses compact encoding
    let mut cursor = std::io::Cursor::new(&response_buf[..]);

    // Skip correlation ID
    cursor.set_position(4);

    // v4 uses response header v1 with tagged fields
    let tagged_fields_len = read_unsigned_varint(&mut cursor).unwrap();
    // Skip tagged fields
    for _ in 0..tagged_fields_len {
        let _tag = read_unsigned_varint(&mut cursor).unwrap();
        let _size = read_unsigned_varint(&mut cursor).unwrap();
        cursor.set_position(cursor.position() + _size as u64);
    }

    // Throttle time (v1+)
    let mut throttle_bytes = [0u8; 4];
    std::io::Read::read_exact(&mut cursor, &mut throttle_bytes).unwrap();
    let _throttle_time = i32::from_be_bytes(throttle_bytes);

    // Error code
    let mut error_bytes = [0u8; 2];
    std::io::Read::read_exact(&mut cursor, &mut error_bytes).unwrap();
    let error_code = i16::from_be_bytes(error_bytes);
    assert_eq!(error_code, 0); // SUCCESS

    // Groups array (compact array in v4)
    let group_count = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(group_count, 2); // Compact arrays use length + 1, so 2 means 1 group

    // Group ID (compact string)
    let returned_group_id = read_compact_string(&mut cursor).unwrap();
    assert_eq!(returned_group_id, group_id);

    // Protocol type (compact string)
    let returned_protocol_type = read_compact_string(&mut cursor).unwrap();
    assert_eq!(returned_protocol_type, protocol_type);

    // Group state (v4+, compact string)
    let state = read_compact_string(&mut cursor).unwrap();
    assert_eq!(state, "Stable");

    // Top-level tagged fields
    let _top_tagged = read_unsigned_varint(&mut cursor).unwrap();
}

// Helper functions for reading compact encoding
fn read_unsigned_varint(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u32, std::io::Error> {
    let mut value = 0u32;
    let mut i = 0;
    loop {
        if i > 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Varint too long",
            ));
        }
        let mut byte = [0u8; 1];
        std::io::Read::read_exact(cursor, &mut byte)?;
        value |= ((byte[0] & 0x7F) as u32) << (i * 7);
        if byte[0] & 0x80 == 0 {
            break;
        }
        i += 1;
    }
    Ok(value)
}

fn read_compact_string(cursor: &mut std::io::Cursor<&[u8]>) -> Result<String, std::io::Error> {
    let len = read_unsigned_varint(cursor)?;
    if len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Null string not expected",
        ));
    }

    let actual_len = (len - 1) as usize;
    if actual_len == 0 {
        return Ok(String::new());
    }

    let mut bytes = vec![0u8; actual_len];
    std::io::Read::read_exact(cursor, &mut bytes)?;

    String::from_utf8(bytes).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid UTF-8: {e}"),
        )
    })
}
