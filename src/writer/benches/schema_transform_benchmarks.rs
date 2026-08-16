//! `transform_trace_v1_to_v2` benchmark.
//!
//! `compiled-schema-materializer`'s regression gate: the plan-based
//! rewrite must show no regression against this baseline, and is expected
//! to show a measurable improvement by removing the per-field, per-batch
//! `get_column_by_name` string lookups from the hot loop.
//!
//! Pure CPU (no WAL, no object store) -- for relative regression tracking,
//! not absolute production latency.

use std::hint::black_box;

use common::flight::conversion::otlp_traces_to_arrow;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use writer::schema_transform::transform_trace_v1_to_v2;

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

/// Same representative shape as `common/benches/ingest_and_wal.rs`'s
/// `sample_request` -- a realistic v1 batch is the input to the transform
/// under benchmark here, not the OTLP decode step that bench covers.
fn sample_request() -> ExportTraceServiceRequest {
    let spans: Vec<Span> = (0..NUM_SPANS)
        .map(|i| {
            let trace_id = format!("{:032x}", i / 10);
            let span_id = format!("{i:016x}");
            Span {
                trace_id: hex::decode(&trace_id).unwrap(),
                span_id: hex::decode(&span_id).unwrap(),
                parent_span_id: vec![],
                name: format!("operation-{}", i % 50),
                kind: 2,
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

fn bench_transform_trace_v1_to_v2(c: &mut Criterion) {
    let v1_batch = otlp_traces_to_arrow(&sample_request()).expect("conversion should succeed");

    let mut group = c.benchmark_group("schema_transform");
    group.throughput(Throughput::Elements(NUM_SPANS as u64));
    group.bench_function("transform_trace_v1_to_v2", |b| {
        b.iter(|| {
            let result = transform_trace_v1_to_v2(black_box(v1_batch.clone()), &[])
                .expect("transform should succeed");
            black_box(result);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_transform_trace_v1_to_v2);
criterion_main!(benches);
