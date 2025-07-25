use bytes::Buf;
use std::io::Cursor;

use heraclitus::protocol::{BrokerMetadata, MetadataResponse, PartitionMetadata, TopicMetadata};

/// Test metadata v10 encoding specifically for rdkafka compatibility
#[test]
fn test_metadata_v10_rdkafka_compatibility() {
    // Create a response with one broker and one topic with two partitions
    let response = MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![BrokerMetadata {
            node_id: 0,
            host: "127.0.0.1".to_string(),
            port: 9092,
        }],
        cluster_id: Some("test-cluster".to_string()),
        controller_id: 0,
        topics: vec![TopicMetadata {
            error_code: 0,
            name: "test-topic".to_string(),
            topic_id: Some([
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                0x77, 0x88,
            ]), // UUID
            is_internal: false,
            partitions: vec![
                PartitionMetadata {
                    error_code: 0,
                    partition_id: 0,
                    leader: 0,
                    replicas: vec![0],
                    isr: vec![0],
                },
                PartitionMetadata {
                    error_code: 0,
                    partition_id: 1,
                    leader: 0,
                    replicas: vec![0],
                    isr: vec![0],
                },
            ],
            topic_authorized_operations: -1,
        }],
    };

    let encoded = response.encode(10).unwrap();

    // Validate the encoding byte by byte
    let mut cursor = Cursor::new(&encoded[..]);

    // 1. Throttle time (4 bytes)
    assert_eq!(cursor.get_i32(), 0);

    // 2. Brokers array (compact array, length + 1)
    let brokers_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(brokers_len, 2); // 1 broker + 1

    // 3. Broker[0]
    assert_eq!(cursor.get_i32(), 0); // node_id

    // Host (compact string)
    let host_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(host_len, 10); // "127.0.0.1".len() + 1
    let mut host_bytes = vec![0u8; 9];
    cursor.copy_to_slice(&mut host_bytes);
    assert_eq!(String::from_utf8(host_bytes).unwrap(), "127.0.0.1");

    assert_eq!(cursor.get_i32(), 9092); // port

    // Broker tagged fields
    assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 0);

    // 4. Cluster ID (compact nullable string)
    let cluster_id_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(cluster_id_len, 13); // "test-cluster".len() + 1
    let mut cluster_bytes = vec![0u8; 12];
    cursor.copy_to_slice(&mut cluster_bytes);
    assert_eq!(String::from_utf8(cluster_bytes).unwrap(), "test-cluster");

    // 5. Controller ID
    assert_eq!(cursor.get_i32(), 0);

    // 6. Topics array (compact array, length + 1)
    let topics_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(topics_len, 2); // 1 topic + 1

    // 7. Topic[0]
    assert_eq!(cursor.get_i16(), 0); // error_code

    // Topic name (compact string)
    let name_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(name_len, 11); // "test-topic".len() + 1
    let mut name_bytes = vec![0u8; 10];
    cursor.copy_to_slice(&mut name_bytes);
    assert_eq!(String::from_utf8(name_bytes).unwrap(), "test-topic");

    // Topic ID (UUID - 16 bytes)
    let mut topic_id = [0u8; 16];
    cursor.copy_to_slice(&mut topic_id);
    assert_eq!(
        topic_id,
        [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88
        ]
    );

    // is_internal
    assert_eq!(cursor.get_u8(), 0);

    // 8. Partitions array (compact array, length + 1)
    let partitions_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(partitions_len, 3); // 2 partitions + 1

    // Partition[0]
    assert_eq!(cursor.get_i16(), 0); // error_code
    assert_eq!(cursor.get_i32(), 0); // partition_id
    assert_eq!(cursor.get_i32(), 0); // leader

    // Replicas array
    let replicas_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(replicas_len, 2); // 1 replica + 1
    assert_eq!(cursor.get_i32(), 0); // replica[0]

    // ISR array
    let isr_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(isr_len, 2); // 1 isr + 1
    assert_eq!(cursor.get_i32(), 0); // isr[0]

    // Partition tagged fields
    assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 0);

    // Partition[1]
    assert_eq!(cursor.get_i16(), 0); // error_code
    assert_eq!(cursor.get_i32(), 1); // partition_id
    assert_eq!(cursor.get_i32(), 0); // leader

    // Replicas array
    let replicas_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(replicas_len, 2); // 1 replica + 1
    assert_eq!(cursor.get_i32(), 0); // replica[0]

    // ISR array
    let isr_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(isr_len, 2); // 1 isr + 1
    assert_eq!(cursor.get_i32(), 0); // isr[0]

    // Partition tagged fields
    assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 0);

    // 9. Topic authorized operations
    assert_eq!(cursor.get_i32(), -1);

    // Topic tagged fields
    assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 0);

    // 10. Top-level tagged fields
    assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 0);

    // Should have consumed all bytes
    assert_eq!(cursor.remaining(), 0);
}

/// Test empty metadata response to ensure rdkafka doesn't see 49 topics
#[test]
fn test_metadata_v10_empty_topics() {
    let response = MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![BrokerMetadata {
            node_id: 0,
            host: "127.0.0.1".to_string(),
            port: 9092,
        }],
        cluster_id: Some("test-cluster".to_string()),
        controller_id: 0,
        topics: vec![],
    };

    let encoded = response.encode(10).unwrap();

    // Make sure the encoded size is reasonable (should be around 31 bytes)
    assert!(
        encoded.len() < 50,
        "Encoded size {} is too large, rdkafka might misinterpret",
        encoded.len()
    );

    let mut cursor = Cursor::new(&encoded[..]);

    // Skip to topics array
    cursor.get_i32(); // throttle_time_ms
    read_unsigned_varint(&mut cursor).unwrap(); // brokers array length
    cursor.get_i32(); // node_id
    let host_len = read_unsigned_varint(&mut cursor).unwrap();
    cursor.advance((host_len - 1) as usize); // skip host bytes
    cursor.get_i32(); // port
    read_unsigned_varint(&mut cursor).unwrap(); // broker tagged fields
    let cluster_id_len = read_unsigned_varint(&mut cursor).unwrap();
    cursor.advance((cluster_id_len - 1) as usize); // skip cluster_id bytes
    cursor.get_i32(); // controller_id

    // Check topics array length
    let topics_len = read_unsigned_varint(&mut cursor).unwrap();
    assert_eq!(
        topics_len, 1,
        "Topics array length should be 1 (0 + 1), not {topics_len}"
    );

    // Top-level tagged fields
    assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 0);

    // Should have consumed all bytes
    assert_eq!(cursor.remaining(), 0);
}

fn read_unsigned_varint(cursor: &mut Cursor<&[u8]>) -> Result<u32, String> {
    let mut value = 0u32;
    let mut i = 0;

    loop {
        if i > 4 {
            return Err("Varint is too long".to_string());
        }

        if cursor.remaining() < 1 {
            return Err("Not enough bytes for varint".to_string());
        }

        let b = cursor.get_u8();
        value |= ((b & 0x7F) as u32) << (i * 7);

        if (b & 0x80) == 0 {
            break;
        }

        i += 1;
    }

    Ok(value)
}
