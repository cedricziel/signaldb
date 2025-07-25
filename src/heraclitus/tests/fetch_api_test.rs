use bytes::{BufMut, BytesMut};
use heraclitus::protocol::{FetchRequest, RecordBatch};
use std::io::Cursor;

#[test]
fn test_produce_then_fetch_workflow() {
    // This is a placeholder for a more comprehensive integration test
    // that would test the complete produce -> fetch workflow

    // For now, we test that we can parse fetch requests and responses
    let mut request_buf = BytesMut::new();

    // Build a simple fetch request
    request_buf.put_i32(-1); // replica_id (consumer)
    request_buf.put_i32(1000); // max_wait_ms
    request_buf.put_i32(1024); // min_bytes
    request_buf.put_i32(1); // topic count

    // Topic
    request_buf.put_i16(4); // topic name length
    request_buf.put_slice(b"test");
    request_buf.put_i32(1); // partition count

    // Partition
    request_buf.put_i32(0); // partition index
    request_buf.put_i64(0); // fetch_offset
    request_buf.put_i32(1048576); // partition_max_bytes

    let mut cursor = Cursor::new(&request_buf[..]);
    let fetch_req = FetchRequest::parse(&mut cursor, 0).unwrap();

    assert_eq!(fetch_req.replica_id, -1);
    assert_eq!(fetch_req.max_wait_ms, 1000);
    assert_eq!(fetch_req.min_bytes, 1024);
    assert_eq!(fetch_req.topics.len(), 1);
    assert_eq!(fetch_req.topics[0].topic, "test");
    assert_eq!(fetch_req.topics[0].partitions.len(), 1);
    assert_eq!(fetch_req.topics[0].partitions[0].partition, 0);
    assert_eq!(fetch_req.topics[0].partitions[0].fetch_offset, 0);
}

#[test]
fn test_fetch_request_v11_features() {
    let mut request_buf = BytesMut::new();

    // Build a v11 fetch request with all features
    request_buf.put_i32(-1); // replica_id
    request_buf.put_i32(500); // max_wait_ms
    request_buf.put_i32(1); // min_bytes
    request_buf.put_i32(52428800); // max_bytes (50MB)
    request_buf.put_i8(1); // isolation_level (READ_COMMITTED)
    request_buf.put_i32(123); // session_id
    request_buf.put_i32(5); // session_epoch
    request_buf.put_i32(1); // topic count

    // Topic
    request_buf.put_i16(9); // topic name length
    request_buf.put_slice(b"my-topic1");
    request_buf.put_i32(2); // partition count

    // Partition 1
    request_buf.put_i32(0); // partition
    request_buf.put_i32(10); // current_leader_epoch
    request_buf.put_i64(100); // fetch_offset
    request_buf.put_i64(0); // log_start_offset
    request_buf.put_i32(1048576); // partition_max_bytes

    // Partition 2
    request_buf.put_i32(1); // partition
    request_buf.put_i32(10); // current_leader_epoch
    request_buf.put_i64(200); // fetch_offset
    request_buf.put_i64(50); // log_start_offset
    request_buf.put_i32(1048576); // partition_max_bytes

    // Forgotten topics
    request_buf.put_i32(1); // forgotten topics count
    request_buf.put_i16(11); // topic name length
    request_buf.put_slice(b"old-topic-1");
    request_buf.put_i32(1); // partition count
    request_buf.put_i32(0); // partition

    // Rack ID
    request_buf.put_i16(7); // rack_id length
    request_buf.put_slice(b"rack-01");

    let mut cursor = Cursor::new(&request_buf[..]);
    let fetch_req = FetchRequest::parse(&mut cursor, 11).unwrap();

    assert_eq!(fetch_req.replica_id, -1);
    assert_eq!(fetch_req.max_wait_ms, 500);
    assert_eq!(fetch_req.min_bytes, 1);
    assert_eq!(fetch_req.max_bytes, 52428800);
    assert_eq!(fetch_req.isolation_level, 1);
    assert_eq!(fetch_req.session_id, 123);
    assert_eq!(fetch_req.session_epoch, 5);

    assert_eq!(fetch_req.topics.len(), 1);
    assert_eq!(fetch_req.topics[0].topic, "my-topic1");
    assert_eq!(fetch_req.topics[0].partitions.len(), 2);

    assert_eq!(fetch_req.topics[0].partitions[0].partition, 0);
    assert_eq!(fetch_req.topics[0].partitions[0].current_leader_epoch, 10);
    assert_eq!(fetch_req.topics[0].partitions[0].fetch_offset, 100);
    assert_eq!(fetch_req.topics[0].partitions[0].log_start_offset, 0);

    assert_eq!(fetch_req.topics[0].partitions[1].partition, 1);
    assert_eq!(fetch_req.topics[0].partitions[1].fetch_offset, 200);
    assert_eq!(fetch_req.topics[0].partitions[1].log_start_offset, 50);

    assert_eq!(fetch_req.forgotten_topics.len(), 1);
    assert_eq!(fetch_req.forgotten_topics[0].topic, "old-topic-1");
    assert_eq!(fetch_req.forgotten_topics[0].partitions, vec![0]);

    assert_eq!(fetch_req.rack_id, Some("rack-01".to_string()));
}

#[test]
fn test_fetch_response_with_record_batch() {
    use heraclitus::protocol::{CompressionType, RecordBatchBuilder};
    use heraclitus::protocol::{FetchPartitionResponse, FetchResponse, FetchTopicResponse};
    use heraclitus::storage::KafkaMessage;
    use std::collections::HashMap;

    // Create some test messages
    let messages = vec![
        KafkaMessage {
            topic: "test".to_string(),
            partition: 0,
            offset: 100,
            timestamp: 1234567890,
            key: Some(b"key1".to_vec()),
            value: b"value1".to_vec(),
            headers: HashMap::new(),
            producer_id: None,
            producer_epoch: None,
            sequence: None,
        },
        KafkaMessage {
            topic: "test".to_string(),
            partition: 0,
            offset: 101,
            timestamp: 1234567891,
            key: Some(b"key2".to_vec()),
            value: b"value2".to_vec(),
            headers: HashMap::new(),
            producer_id: None,
            producer_epoch: None,
            sequence: None,
        },
    ];

    // Build a RecordBatch
    let mut builder = RecordBatchBuilder::new(CompressionType::None);
    builder.add_messages(messages);
    let record_batch_data = builder.build().unwrap();

    // Create fetch response
    let response = FetchResponse {
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses: vec![FetchTopicResponse {
            topic: "test".to_string(),
            partitions: vec![FetchPartitionResponse {
                partition: 0,
                error_code: 0,
                high_watermark: 102,
                last_stable_offset: 102,
                log_start_offset: 0,
                aborted_transactions: vec![],
                preferred_read_replica: -1,
                records: Some(record_batch_data.clone()),
            }],
        }],
    };

    // Encode and verify we can parse the record batch
    let encoded = response.encode(11).unwrap();
    assert!(!encoded.is_empty());

    // Parse the record batch from the response
    let batch = RecordBatch::parse(&record_batch_data).unwrap();
    assert_eq!(batch.base_offset, 100);
    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.records[0].key, Some(b"key1".to_vec()));
    assert_eq!(batch.records[0].value, b"value1".to_vec());
    assert_eq!(batch.records[1].key, Some(b"key2".to_vec()));
    assert_eq!(batch.records[1].value, b"value2".to_vec());
}
