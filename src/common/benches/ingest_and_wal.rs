//! Ingest and durability write-path benchmarks.
//!
//! Both hot paths live in `common`, so they share one bench crate:
//!
//! - **ingest decode** — the acceptor's CPU work per OTLP request: protobuf
//!   decode from the wire bytes (`prost`) + OTLP → Arrow conversion
//!   (`otlp_traces_to_arrow`). No WAL or object-store write is measured.
//! - **WAL round-trip** — durability encode + read-back:
//!   `record_batch_to_bytes` then `bytes_to_record_batch`.
//!
//! Numbers are pure CPU (no network, no disk) — for relative regression
//! tracking, not absolute production latency.

use std::hint::black_box;

use common::flight::conversion::otlp_traces_to_arrow;
use common::testing::sample_trace_request;
use common::wal::{bytes_to_record_batch, record_batch_to_bytes};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;

const NUM_SPANS: usize = 1_000;

fn bench_ingest_decode(c: &mut Criterion) {
    let request = sample_trace_request(NUM_SPANS);
    let wire_bytes = request.encode_to_vec();

    let mut group = c.benchmark_group("acceptor_ingest");
    group.throughput(Throughput::Elements(NUM_SPANS as u64));

    // The acceptor's per-request CPU: decode protobuf, then convert to Arrow.
    group.bench_function("otlp_decode_and_convert", |b| {
        b.iter(|| {
            let decoded = ExportTraceServiceRequest::decode(black_box(&wire_bytes[..])).unwrap();
            let batch = otlp_traces_to_arrow(&decoded).expect("conversion should succeed");
            black_box(batch);
        });
    });

    // Conversion only (protobuf already decoded), to isolate the Arrow build.
    group.bench_function("otlp_convert_only", |b| {
        b.iter(|| {
            let batch =
                otlp_traces_to_arrow(black_box(&request)).expect("conversion should succeed");
            black_box(batch);
        });
    });

    group.finish();
}

fn bench_wal_roundtrip(c: &mut Criterion) {
    let batch =
        otlp_traces_to_arrow(&sample_trace_request(NUM_SPANS)).expect("conversion should succeed");

    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Elements(batch.num_rows() as u64));

    group.bench_function("record_batch_roundtrip", |b| {
        b.iter(|| {
            let bytes = record_batch_to_bytes(black_box(&batch)).unwrap();
            let restored = bytes_to_record_batch(&bytes).unwrap();
            black_box(restored);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_ingest_decode, bench_wal_roundtrip);
criterion_main!(benches);
