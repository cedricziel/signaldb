use heraclitus::protocol::{CompressionType, RecordBatch, RecordBatchBuilder};
use heraclitus::storage::KafkaMessage;
use std::collections::HashMap;

#[test]
fn test_gzip_compression() {
    test_compression_roundtrip(CompressionType::Gzip);
}

#[test]
fn test_snappy_compression() {
    test_compression_roundtrip(CompressionType::Snappy);
}

#[test]
fn test_lz4_compression() {
    test_compression_roundtrip(CompressionType::Lz4);
}

#[test]
fn test_zstd_compression() {
    test_compression_roundtrip(CompressionType::Zstd);
}

#[test]
fn test_no_compression() {
    test_compression_roundtrip(CompressionType::None);
}

fn test_compression_roundtrip(compression_type: CompressionType) {
    // Create a batch with some test data
    let mut builder = RecordBatchBuilder::new(compression_type);

    // Add multiple messages to make compression worthwhile
    for i in 0..10 {
        let key = format!("key-{i}");
        let value = format!(
            "This is a test message number {i} with some repeated content to make compression effective. Repeated content repeated content repeated content."
        );
        builder.add_message(KafkaMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: i as i64,
            timestamp: 1000 + i as i64,
            key: Some(key.as_bytes().to_vec()),
            value: value.as_bytes().to_vec(),
            headers: HashMap::new(),
            producer_id: None,
            producer_epoch: None,
            sequence: None,
        });
    }

    // Build the batch
    let encoded = builder.build().expect("Failed to build batch");

    // Parse it back
    let parsed = RecordBatch::parse(&encoded).expect("Failed to parse batch");

    // Verify we got the same data back
    assert_eq!(parsed.records.len(), 10);

    for (i, record) in parsed.records.iter().enumerate() {
        let expected_key = format!("key-{i}");
        let expected_value = format!(
            "This is a test message number {i} with some repeated content to make compression effective. Repeated content repeated content repeated content."
        );

        assert_eq!(
            record.key.as_ref().unwrap(),
            expected_key.as_bytes(),
            "Key mismatch for record {i}"
        );
        assert_eq!(
            record.value,
            expected_value.as_bytes(),
            "Value mismatch for record {i}"
        );
    }
}

#[test]
fn test_compression_size_reduction() {
    // Test that compression actually reduces size for compressible data
    let test_data = "This is highly repetitive data. ".repeat(100);

    let mut uncompressed_builder = RecordBatchBuilder::new(CompressionType::None);
    uncompressed_builder.add_message(KafkaMessage {
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 1000,
        key: None,
        value: test_data.as_bytes().to_vec(),
        headers: HashMap::new(),
        producer_id: None,
        producer_epoch: None,
        sequence: None,
    });
    let uncompressed_size = uncompressed_builder.build().unwrap().len();

    // Test each compression type
    for compression_type in [
        CompressionType::Gzip,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Zstd,
    ] {
        let mut compressed_builder = RecordBatchBuilder::new(compression_type);
        compressed_builder.add_message(KafkaMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 1000,
            key: None,
            value: test_data.as_bytes().to_vec(),
            headers: HashMap::new(),
            producer_id: None,
            producer_epoch: None,
            sequence: None,
        });
        let compressed_size = compressed_builder.build().unwrap().len();

        assert!(
            compressed_size < uncompressed_size,
            "{compression_type:?} failed to reduce size: {compressed_size} >= {uncompressed_size}"
        );

        println!(
            "{compression_type:?}: {compressed_size} bytes ({:.1}% of original)",
            (compressed_size as f64 / uncompressed_size as f64) * 100.0
        );
    }
}
