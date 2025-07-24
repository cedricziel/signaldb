use heraclitus::protocol::list_offsets::{
    EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, ListOffsetsRequest, ListOffsetsResponse,
};
use std::io::Cursor;

#[test]
fn test_list_offsets_request_parsing() {
    // Test parsing v0 request
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    data.extend_from_slice(&(1i32).to_be_bytes()); // topic count
    data.extend_from_slice(&(10i16).to_be_bytes()); // topic name length
    data.extend_from_slice(b"test-topic"); // topic name
    data.extend_from_slice(&(2i32).to_be_bytes()); // partition count
    // Partition 0
    data.extend_from_slice(&(0i32).to_be_bytes()); // partition index
    data.extend_from_slice(&LATEST_TIMESTAMP.to_be_bytes()); // timestamp
    data.extend_from_slice(&(1i32).to_be_bytes()); // max_num_offsets
    // Partition 1
    data.extend_from_slice(&(1i32).to_be_bytes()); // partition index
    data.extend_from_slice(&EARLIEST_TIMESTAMP.to_be_bytes()); // timestamp
    data.extend_from_slice(&(1i32).to_be_bytes()); // max_num_offsets

    let mut cursor = Cursor::new(&data[..]);
    let request = ListOffsetsRequest::parse(&mut cursor, 0).unwrap();

    assert_eq!(request.replica_id, -1);
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].name, "test-topic");
    assert_eq!(request.topics[0].partitions.len(), 2);
    assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    assert_eq!(request.topics[0].partitions[0].timestamp, LATEST_TIMESTAMP);
    assert_eq!(request.topics[0].partitions[0].max_num_offsets, 1);
    assert_eq!(request.topics[0].partitions[1].partition_index, 1);
    assert_eq!(
        request.topics[0].partitions[1].timestamp,
        EARLIEST_TIMESTAMP
    );
    assert_eq!(request.topics[0].partitions[1].max_num_offsets, 1);
}

#[test]
fn test_list_offsets_request_parsing_v1() {
    // Test parsing v1 request (no max_num_offsets)
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    data.extend_from_slice(&(1i32).to_be_bytes()); // topic count
    data.extend_from_slice(&(10i16).to_be_bytes()); // topic name length
    data.extend_from_slice(b"test-topic"); // topic name
    data.extend_from_slice(&(1i32).to_be_bytes()); // partition count
    // Partition 0
    data.extend_from_slice(&(0i32).to_be_bytes()); // partition index
    data.extend_from_slice(&LATEST_TIMESTAMP.to_be_bytes()); // timestamp

    let mut cursor = Cursor::new(&data[..]);
    let request = ListOffsetsRequest::parse(&mut cursor, 1).unwrap();

    assert_eq!(request.replica_id, -1);
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].partitions[0].max_num_offsets, 1); // Default to 1
}

#[test]
fn test_list_offsets_request_parsing_v2() {
    // Test parsing v2 request (includes isolation level)
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    data.push(0); // isolation_level (read_uncommitted)
    data.extend_from_slice(&(1i32).to_be_bytes()); // topic count
    data.extend_from_slice(&(10i16).to_be_bytes()); // topic name length
    data.extend_from_slice(b"test-topic"); // topic name
    data.extend_from_slice(&(1i32).to_be_bytes()); // partition count
    // Partition 0
    data.extend_from_slice(&(0i32).to_be_bytes()); // partition index
    data.extend_from_slice(&LATEST_TIMESTAMP.to_be_bytes()); // timestamp

    let mut cursor = Cursor::new(&data[..]);
    let request = ListOffsetsRequest::parse(&mut cursor, 2).unwrap();

    assert_eq!(request.replica_id, -1);
    assert_eq!(request.isolation_level, 0);
    assert_eq!(request.topics.len(), 1);
}

#[test]
fn test_list_offsets_response_encoding_v0() {
    use heraclitus::protocol::list_offsets::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let response = ListOffsetsResponse {
        throttle_time_ms: 0,
        topics: vec![ListOffsetsTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![ListOffsetsPartitionResponse {
                partition_index: 0,
                error_code: 0,
                timestamp: LATEST_TIMESTAMP,
                offset: 100,
                old_style_offsets: vec![100],
            }],
        }],
    };

    let encoded = response.encode(0).unwrap();
    let mut cursor = Cursor::new(&encoded[..]);

    // Verify encoding
    assert_eq!(cursor.get_i32(), 1); // topic count
    assert_eq!(cursor.get_i16(), 10); // topic name length
    let mut topic_name = vec![0u8; 10];
    cursor.copy_to_slice(&mut topic_name);
    assert_eq!(&topic_name, b"test-topic");

    assert_eq!(cursor.get_i32(), 1); // partition count
    assert_eq!(cursor.get_i32(), 0); // partition index
    assert_eq!(cursor.get_i16(), 0); // error code
    assert_eq!(cursor.get_i32(), 1); // old style offsets count
    assert_eq!(cursor.get_i64(), 100); // offset
}

#[test]
fn test_list_offsets_response_encoding_v1() {
    use heraclitus::protocol::list_offsets::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let response = ListOffsetsResponse {
        throttle_time_ms: 0,
        topics: vec![ListOffsetsTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![ListOffsetsPartitionResponse {
                partition_index: 0,
                error_code: 0,
                timestamp: LATEST_TIMESTAMP,
                offset: 100,
                old_style_offsets: vec![],
            }],
        }],
    };

    let encoded = response.encode(1).unwrap();
    let mut cursor = Cursor::new(&encoded[..]);

    // Verify encoding
    assert_eq!(cursor.get_i32(), 1); // topic count
    assert_eq!(cursor.get_i16(), 10); // topic name length
    let mut topic_name = vec![0u8; 10];
    cursor.copy_to_slice(&mut topic_name);
    assert_eq!(&topic_name, b"test-topic");

    assert_eq!(cursor.get_i32(), 1); // partition count
    assert_eq!(cursor.get_i32(), 0); // partition index
    assert_eq!(cursor.get_i16(), 0); // error code
    assert_eq!(cursor.get_i64(), LATEST_TIMESTAMP); // timestamp
    assert_eq!(cursor.get_i64(), 100); // offset
}

#[test]
fn test_list_offsets_response_encoding_v2() {
    use heraclitus::protocol::list_offsets::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let response = ListOffsetsResponse {
        throttle_time_ms: 100,
        topics: vec![ListOffsetsTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![ListOffsetsPartitionResponse {
                partition_index: 0,
                error_code: 0,
                timestamp: LATEST_TIMESTAMP,
                offset: 100,
                old_style_offsets: vec![],
            }],
        }],
    };

    let encoded = response.encode(2).unwrap();
    let mut cursor = Cursor::new(&encoded[..]);

    // Verify encoding
    assert_eq!(cursor.get_i32(), 100); // throttle_time_ms
    assert_eq!(cursor.get_i32(), 1); // topic count
    // Rest is same as v1
}

use bytes::Buf;
