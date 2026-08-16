//! Logs & metrics ingest-decode benchmarks.
//!
//! The traces decode path already lives in `ingest_and_wal.rs`; this file
//! mirrors it for the other two signals. Each bench measures the acceptor's
//! per-request CPU work:
//!
//! - **decode + convert** — protobuf decode from the wire bytes (`prost`) plus
//!   OTLP → Arrow conversion (`otlp_logs_to_arrow` / `otlp_metrics_to_arrow`).
//! - **convert only** — the Arrow build in isolation, with the protobuf request
//!   already decoded.
//!
//! No WAL or object-store write is measured. Numbers are pure CPU (no network,
//! no disk) — for relative regression tracking, not absolute production latency.

use std::hint::black_box;

use common::flight::conversion::{otlp_logs_to_arrow, otlp_metrics_to_arrow};
use common::testing::{sample_logs_request, sample_metrics_request};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;

const NUM_LOGS: usize = 1_000;
const NUM_METRIC_POINTS: usize = 1_000;

fn bench_logs_decode(c: &mut Criterion) {
    let request = sample_logs_request(NUM_LOGS);
    let wire_bytes = request.encode_to_vec();

    let mut group = c.benchmark_group("acceptor_ingest_logs");
    group.throughput(Throughput::Elements(NUM_LOGS as u64));

    // The acceptor's per-request CPU: decode protobuf, then convert to Arrow.
    group.bench_function("otlp_decode_and_convert", |b| {
        b.iter(|| {
            let decoded = ExportLogsServiceRequest::decode(black_box(&wire_bytes[..])).unwrap();
            let batch = otlp_logs_to_arrow(&decoded).expect("conversion should succeed");
            black_box(batch);
        });
    });

    // Conversion only (protobuf already decoded), to isolate the Arrow build.
    group.bench_function("otlp_convert_only", |b| {
        b.iter(|| {
            let batch = otlp_logs_to_arrow(black_box(&request)).expect("conversion should succeed");
            black_box(batch);
        });
    });

    group.finish();
}

fn bench_metrics_decode(c: &mut Criterion) {
    let request = sample_metrics_request(NUM_METRIC_POINTS);
    let wire_bytes = request.encode_to_vec();

    let mut group = c.benchmark_group("acceptor_ingest_metrics");
    group.throughput(Throughput::Elements(NUM_METRIC_POINTS as u64));

    // The acceptor's per-request CPU: decode protobuf, then convert to Arrow.
    group.bench_function("otlp_decode_and_convert", |b| {
        b.iter(|| {
            let decoded = ExportMetricsServiceRequest::decode(black_box(&wire_bytes[..])).unwrap();
            let batch = otlp_metrics_to_arrow(&decoded).expect("conversion should succeed");
            black_box(batch);
        });
    });

    // Conversion only (protobuf already decoded), to isolate the Arrow build.
    group.bench_function("otlp_convert_only", |b| {
        b.iter(|| {
            let batch =
                otlp_metrics_to_arrow(black_box(&request)).expect("conversion should succeed");
            black_box(batch);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_logs_decode, bench_metrics_decode);
criterion_main!(benches);
