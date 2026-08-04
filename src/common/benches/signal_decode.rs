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
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric::Data,
    number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

const NUM_LOGS: usize = 1_000;
const NUM_METRIC_POINTS: usize = 1_000;

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key_strindex: 0,
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

fn resource_attrs() -> Resource {
    Resource {
        attributes: vec![
            string_attr("service.name", "bench-service"),
            string_attr("service.version", "1.0.0"),
        ],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    }
}

fn scope() -> InstrumentationScope {
    InstrumentationScope {
        name: "bench-lib".to_string(),
        version: "1.0.0".to_string(),
        attributes: vec![],
        dropped_attributes_count: 0,
    }
}

/// Build a single-resource, single-scope request with `NUM_LOGS` log records,
/// each carrying a body and a few attributes — the shape the acceptor converts
/// on every log ingest call.
fn sample_logs_request() -> ExportLogsServiceRequest {
    let log_records: Vec<LogRecord> = (0..NUM_LOGS)
        .map(|i| {
            let trace_id = format!("{:032x}", i / 10); // ~10 logs per trace
            let span_id = format!("{i:016x}");
            LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000 + (i as u64 * 1_000),
                observed_time_unix_nano: 1_700_000_000_000_000_000 + (i as u64 * 1_000) + 5,
                severity_number: 9, // INFO
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(Value::StringValue(format!("request {} completed", i % 50))),
                }),
                attributes: vec![
                    string_attr("http.method", "GET"),
                    string_attr("http.route", "/api/v1/resource"),
                    string_attr("http.status_code", "200"),
                ],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: hex::decode(&trace_id).unwrap(),
                span_id: hex::decode(&span_id).unwrap(),
                event_name: String::new(),
            }
        })
        .collect();

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource_attrs()),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope()),
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Build a single-resource, single-scope request with `NUM_METRIC_POINTS`
/// numeric data points, split evenly between gauge and sum metrics (one data
/// point per metric) — the shape the acceptor converts on every metric ingest
/// call.
fn sample_metrics_request() -> ExportMetricsServiceRequest {
    let metrics: Vec<Metric> = (0..NUM_METRIC_POINTS)
        .map(|i| {
            let point = NumberDataPoint {
                attributes: vec![
                    string_attr("host", "node-1"),
                    string_attr("region", "us-east-1"),
                ],
                start_time_unix_nano: 1_700_000_000_000_000_000,
                time_unix_nano: 1_700_000_000_000_000_000 + (i as u64 * 1_000),
                value: Some(number_data_point::Value::AsDouble(i as f64 * 1.5)),
                exemplars: vec![],
                flags: 0,
            };

            let (name, data) = if i % 2 == 0 {
                (
                    format!("gauge.metric.{}", i % 50),
                    Data::Gauge(Gauge {
                        data_points: vec![point],
                    }),
                )
            } else {
                (
                    format!("sum.metric.{}", i % 50),
                    Data::Sum(Sum {
                        data_points: vec![point],
                        aggregation_temporality: 2, // Cumulative
                        is_monotonic: true,
                    }),
                )
            };

            Metric {
                name,
                description: "bench metric".to_string(),
                unit: "1".to_string(),
                data: Some(data),
                metadata: vec![],
            }
        })
        .collect();

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource_attrs()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(scope()),
                metrics,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn bench_logs_decode(c: &mut Criterion) {
    let request = sample_logs_request();
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
    let request = sample_metrics_request();
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
