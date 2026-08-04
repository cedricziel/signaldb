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
use common::wal::{bytes_to_record_batch, record_batch_to_bytes};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;

const NUM_SPANS: usize = 1_000;

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key_strindex: 0,
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

/// Build a representative single-resource, single-scope request with
/// `NUM_SPANS` spans, each carrying a few attributes — the shape the acceptor
/// converts on every ingest call.
fn sample_request() -> ExportTraceServiceRequest {
    let spans: Vec<Span> = (0..NUM_SPANS)
        .map(|i| {
            let trace_id = format!("{:032x}", i / 10); // ~10 spans per trace
            let span_id = format!("{i:016x}");
            Span {
                trace_id: hex::decode(&trace_id).unwrap(),
                span_id: hex::decode(&span_id).unwrap(),
                parent_span_id: vec![],
                name: format!("operation-{}", i % 50),
                kind: 2, // Server
                start_time_unix_nano: 1_700_000_000_000_000_000 + (i as u64 * 1_000),
                end_time_unix_nano: 1_700_000_000_000_000_000 + (i as u64 * 1_000) + 500,
                attributes: vec![
                    string_attr("http.method", "GET"),
                    string_attr("http.route", "/api/v1/resource"),
                    string_attr("http.status_code", "200"),
                ],
                dropped_attributes_count: 0,
                events: vec![],
                dropped_events_count: 0,
                links: vec![],
                dropped_links_count: 0,
                status: Some(Status {
                    code: 1,
                    message: String::new(),
                }),
                flags: 0,
                trace_state: String::new(),
            }
        })
        .collect();

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_attr("service.name", "bench-service"),
                    string_attr("service.version", "1.0.0"),
                ],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "bench-lib".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn bench_ingest_decode(c: &mut Criterion) {
    let request = sample_request();
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
    let batch = otlp_traces_to_arrow(&sample_request()).expect("conversion should succeed");

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
