//! Unit tests proving that kafka-protocol compression works correctly
//!
//! These tests demonstrate that the kafka-protocol crate's built-in compression
//! features work as expected for all supported codecs.

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::records::{
        Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
        Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    /// Helper to create a test record
    fn create_test_record(offset: i64, key: &str, value: &str) -> Record {
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            timestamp: 1000 + offset,
            offset,
            sequence: NO_SEQUENCE,
            key: Some(Bytes::from(key.to_string())),
            value: Some(Bytes::from(value.to_string())),
            headers: Default::default(),
        }
    }

    /// Test compression roundtrip for a specific codec
    fn test_compression_roundtrip(compression: Compression) {
        // Create test data with repetitive content (compresses well)
        let test_value = "This is compressible test data! ".repeat(20);
        let records = vec![
            create_test_record(0, "key1", &test_value),
            create_test_record(1, "key2", &test_value),
            create_test_record(2, "key3", &test_value),
        ];

        // Encode with compression
        let mut encoded_buf = BytesMut::new();
        let encode_options = RecordEncodeOptions {
            version: 2,
            compression,
        };

        RecordBatchEncoder::encode(&mut encoded_buf, records.iter(), &encode_options)
            .expect("Failed to encode records");

        let encoded_size = encoded_buf.len();
        println!(
            "{compression:?}: Encoded {} records to {} bytes",
            records.len(),
            encoded_size
        );

        // Decode and verify - use decode_all to handle all batches
        let mut decode_buf = encoded_buf.freeze();
        let decoded_batches =
            RecordBatchDecoder::decode_all(&mut decode_buf).expect("Failed to decode");

        println!("Decoded {} batches", decoded_batches.len());

        // Collect all records from all batches
        let all_records: Vec<_> = decoded_batches
            .iter()
            .flat_map(|batch| &batch.records)
            .collect();

        // Verify we got all records back
        assert_eq!(all_records.len(), 3, "Should decode 3 records");

        // Verify compression type matches (check first batch)
        if let Some(first_batch) = decoded_batches.first() {
            assert_eq!(
                first_batch.compression, compression,
                "Compression type should match"
            );
        }

        // Verify record contents
        for (i, record) in all_records.iter().enumerate() {
            assert_eq!(record.offset, i as i64, "Offset should match");
            assert_eq!(
                record.key.as_ref().map(|k| k.as_ref()),
                Some(format!("key{}", i + 1).as_bytes()),
                "Key should match"
            );
            assert_eq!(
                record.value.as_ref().map(|v| v.as_ref()),
                Some(test_value.as_bytes()),
                "Value should match"
            );
        }
    }

    #[test]
    fn test_no_compression_roundtrip() {
        test_compression_roundtrip(Compression::None);
    }

    #[test]
    fn test_gzip_compression_roundtrip() {
        test_compression_roundtrip(Compression::Gzip);
    }

    #[test]
    fn test_snappy_compression_roundtrip() {
        test_compression_roundtrip(Compression::Snappy);
    }

    #[test]
    fn test_lz4_compression_roundtrip() {
        test_compression_roundtrip(Compression::Lz4);
    }

    #[test]
    fn test_zstd_compression_roundtrip() {
        test_compression_roundtrip(Compression::Zstd);
    }

    /// Verify that compressed data is actually smaller than uncompressed
    #[test]
    fn test_compression_reduces_size() {
        // Use highly compressible data
        let test_value = "AAAAAAAAAA".repeat(100); // Very repetitive = high compression
        let records = vec![
            create_test_record(0, "key", &test_value),
            create_test_record(1, "key", &test_value),
        ];

        // Encode without compression
        let mut uncompressed_buf = BytesMut::new();
        let no_compression_options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        RecordBatchEncoder::encode(
            &mut uncompressed_buf,
            records.iter(),
            &no_compression_options,
        )
        .unwrap();
        let uncompressed_size = uncompressed_buf.len();

        // Encode with gzip compression
        let mut compressed_buf = BytesMut::new();
        let gzip_options = RecordEncodeOptions {
            version: 2,
            compression: Compression::Gzip,
        };
        RecordBatchEncoder::encode(&mut compressed_buf, records.iter(), &gzip_options).unwrap();
        let compressed_size = compressed_buf.len();

        println!(
            "Uncompressed: {} bytes, Gzip compressed: {} bytes, Ratio: {:.2}x",
            uncompressed_size,
            compressed_size,
            uncompressed_size as f64 / compressed_size as f64
        );

        // Gzip should significantly reduce size for this repetitive data
        assert!(
            compressed_size < uncompressed_size,
            "Compressed size ({}) should be less than uncompressed ({})",
            compressed_size,
            uncompressed_size
        );

        // For this highly repetitive data, we should get at least 2x compression
        assert!(
            uncompressed_size > compressed_size * 2,
            "Should achieve at least 2x compression ratio"
        );
    }

    /// Test that attributes field correctly indicates compression type
    #[test]
    fn test_compression_attributes_field() {
        let records = [create_test_record(0, "key", "value")];

        for (compression, expected_bits) in [
            (Compression::None, 0i16),
            (Compression::Gzip, 1i16),
            (Compression::Snappy, 2i16),
            (Compression::Lz4, 3i16),
            (Compression::Zstd, 4i16),
        ] {
            let mut buf = BytesMut::new();
            let options = RecordEncodeOptions {
                version: 2,
                compression,
            };
            RecordBatchEncoder::encode(&mut buf, records.iter(), &options).unwrap();

            // Parse the attributes field from the encoded data
            // Record batch format: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4) + attributes(2)
            if buf.len() >= 23 {
                let attributes = i16::from_be_bytes([buf[21], buf[22]]);
                let compression_bits = attributes & 0x7; // Lower 3 bits

                assert_eq!(
                    compression_bits, expected_bits,
                    "Compression bits should be {expected_bits} for {compression:?}"
                );
            }
        }
    }
}
