//! Representative OTLP export requests for benchmarks and tests.
//!
//! One resource, one scope, `n` records each carrying a few string
//! attributes — the shape the acceptor converts on every ingest call. Shared
//! by the ingest-decode benches (`common/benches`), the schema-transform
//! bench (`writer/benches`), and any test that needs a realistic OTLP payload
//! without hand-building protobuf structs.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric::Data,
    number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

const BASE_TIME_UNIX_NANO: u64 = 1_700_000_000_000_000_000;

/// A string-valued OTLP attribute.
pub fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key_strindex: 0,
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

fn sample_resource() -> Resource {
    Resource {
        attributes: vec![
            string_attr("service.name", "bench-service"),
            string_attr("service.version", "1.0.0"),
        ],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    }
}

fn sample_scope() -> InstrumentationScope {
    InstrumentationScope {
        name: "bench-lib".to_string(),
        version: "1.0.0".to_string(),
        attributes: vec![],
        dropped_attributes_count: 0,
    }
}

fn http_attrs() -> Vec<KeyValue> {
    vec![
        string_attr("http.method", "GET"),
        string_attr("http.route", "/api/v1/resource"),
        string_attr("http.status_code", "200"),
    ]
}

/// A single-resource, single-scope trace export with `num_spans` server
/// spans (~10 spans per trace, 50 distinct operation names), each carrying
/// three HTTP attributes.
pub fn sample_trace_request(num_spans: usize) -> ExportTraceServiceRequest {
    let spans: Vec<Span> = (0..num_spans)
        .map(|i| {
            let trace_id = format!("{:032x}", i / 10);
            let span_id = format!("{i:016x}");
            Span {
                trace_id: hex::decode(&trace_id).expect("hex trace id"),
                span_id: hex::decode(&span_id).expect("hex span id"),
                parent_span_id: vec![],
                name: format!("operation-{}", i % 50),
                kind: 2, // Server
                start_time_unix_nano: BASE_TIME_UNIX_NANO + (i as u64 * 1_000),
                end_time_unix_nano: BASE_TIME_UNIX_NANO + (i as u64 * 1_000) + 500,
                attributes: http_attrs(),
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
            resource: Some(sample_resource()),
            scope_spans: vec![ScopeSpans {
                scope: Some(sample_scope()),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// A single-resource, single-scope log export with `num_logs` INFO records
/// (~10 per trace), each with a body and three HTTP attributes.
pub fn sample_logs_request(num_logs: usize) -> ExportLogsServiceRequest {
    let log_records: Vec<LogRecord> = (0..num_logs)
        .map(|i| {
            let trace_id = format!("{:032x}", i / 10);
            let span_id = format!("{i:016x}");
            LogRecord {
                time_unix_nano: BASE_TIME_UNIX_NANO + (i as u64 * 1_000),
                observed_time_unix_nano: BASE_TIME_UNIX_NANO + (i as u64 * 1_000) + 5,
                severity_number: 9, // INFO
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(Value::StringValue(format!("request {} completed", i % 50))),
                }),
                attributes: http_attrs(),
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: hex::decode(&trace_id).expect("hex trace id"),
                span_id: hex::decode(&span_id).expect("hex span id"),
                event_name: String::new(),
            }
        })
        .collect();

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(sample_resource()),
            scope_logs: vec![ScopeLogs {
                scope: Some(sample_scope()),
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// A single-resource, single-scope metrics export with `num_points` numeric
/// data points, alternating between gauge and cumulative monotonic sum
/// metrics (one point per metric, 50 distinct names each).
pub fn sample_metrics_request(num_points: usize) -> ExportMetricsServiceRequest {
    let metrics: Vec<Metric> = (0..num_points)
        .map(|i| {
            let point = NumberDataPoint {
                attributes: vec![
                    string_attr("host", "node-1"),
                    string_attr("region", "us-east-1"),
                ],
                start_time_unix_nano: BASE_TIME_UNIX_NANO,
                time_unix_nano: BASE_TIME_UNIX_NANO + (i as u64 * 1_000),
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
            resource: Some(sample_resource()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(sample_scope()),
                metrics,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flight::conversion::{
        otlp_logs_to_arrow, otlp_metrics_to_arrow, otlp_traces_to_arrow,
    };

    #[test]
    fn sample_trace_request_converts_to_one_row_per_span() {
        let batch = otlp_traces_to_arrow(&sample_trace_request(25)).expect("convert");
        assert_eq!(batch.num_rows(), 25);
    }

    #[test]
    fn sample_logs_request_converts_to_one_row_per_record() {
        let batch = otlp_logs_to_arrow(&sample_logs_request(25)).expect("convert");
        assert_eq!(batch.num_rows(), 25);
    }

    #[test]
    fn sample_metrics_request_converts_to_one_row_per_point() {
        let batch = otlp_metrics_to_arrow(&sample_metrics_request(25)).expect("convert");
        assert_eq!(batch.num_rows(), 25);
    }
}
