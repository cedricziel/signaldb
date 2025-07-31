// Kafka protocol encoding/decoding benchmarks

use bytes::{BufMut, BytesMut};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

fn bench_record_batch_encoding(c: &mut Criterion) {
    c.bench_function("encode_record_batch_uncompressed", |b| {
        let mut records = Vec::new();
        for i in 0..100 {
            let record = Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: 1,
                producer_epoch: 0,
                sequence: i,
                timestamp: 1234567890 + i as i64,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                key: Some(format!("key-{i}").into_bytes().into()),
                value: Some(format!("value-{i}").into_bytes().into()),
                headers: IndexMap::new(),
            };
            records.push(record);
        }

        let encode_options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };

        b.iter(|| {
            let mut buffer = BytesMut::new();
            black_box(RecordBatchEncoder::encode(
                &mut buffer,
                records.iter(),
                &encode_options,
            ))
            .unwrap();
        });
    });

    c.bench_function("encode_record_batch_gzip", |b| {
        let mut records = Vec::new();
        for i in 0..100 {
            let record = Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: 1,
                producer_epoch: 0,
                sequence: i,
                timestamp: 1234567890 + i as i64,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                key: Some(format!("key-{i}").into_bytes().into()),
                value: Some(format!("value-{i}").into_bytes().into()),
                headers: IndexMap::new(),
            };
            records.push(record);
        }

        let encode_options = RecordEncodeOptions {
            version: 2,
            compression: Compression::Gzip,
        };

        b.iter(|| {
            let mut buffer = BytesMut::new();
            black_box(RecordBatchEncoder::encode(
                &mut buffer,
                records.iter(),
                &encode_options,
            ))
            .unwrap();
        });
    });
}

fn bench_kafka_frame_parsing(c: &mut Criterion) {
    c.bench_function("parse_kafka_frame", |b| {
        // Create a sample Kafka frame
        let mut frame = BytesMut::new();
        frame.put_i32(100); // Frame length
        frame.put_i16(3); // API key (Metadata)
        frame.put_i16(9); // API version
        frame.put_i32(42); // Correlation ID
        frame.put_i16(0); // Client ID length

        // Add some payload
        frame.put_i32(0); // Number of topics
        frame.put_i8(1); // Allow auto topic creation
        frame.put_i8(0); // Include cluster authorized operations
        frame.put_i8(0); // Include topic authorized operations

        let frame_bytes = frame.freeze();

        b.iter(|| {
            let mut cursor = std::io::Cursor::new(&frame_bytes[..]);
            let _length = black_box(bytes::Buf::get_i32(&mut cursor));
            let _api_key = black_box(bytes::Buf::get_i16(&mut cursor));
            let _api_version = black_box(bytes::Buf::get_i16(&mut cursor));
            let _correlation_id = black_box(bytes::Buf::get_i32(&mut cursor));
        });
    });
}

criterion_group!(
    benches,
    bench_record_batch_encoding,
    bench_kafka_frame_parsing
);
criterion_main!(benches);
