use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int32Array, ListArray, RecordBatch, StringArray,
    StructArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use hex;
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::metrics::v1::ExportMetricsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue, KeyValue},
    logs::v1::{LogRecord, SeverityNumber},
    metrics::v1::{
        AggregationTemporality, ExponentialHistogram, Gauge, Histogram, Metric as OtelMetric, Sum,
        Summary,
    },
    trace::v1::{span::Event, Span as OtelSpan, Status as OtelStatus},
};
use serde_json::{json, Map, Value as JsonValue};
use std::sync::Arc;

use crate::flight::schema::FlightSchemas;

/// Convert OTLP trace data to Arrow RecordBatch using the Flight trace schema
pub fn otlp_traces_to_arrow(request: &ExportTraceServiceRequest) -> RecordBatch {
    let schemas = FlightSchemas::new();
    let schema = schemas.trace_schema.clone();

    // Extract spans from the request
    let mut trace_ids = Vec::new();
    let mut span_ids = Vec::new();
    let mut parent_span_ids = Vec::new();
    let mut names = Vec::new();
    let mut service_names = Vec::new();
    let mut start_times = Vec::new();
    let mut end_times = Vec::new();
    let mut durations = Vec::new();
    let mut span_kinds = Vec::new();
    let mut status_codes = Vec::new();
    let mut status_messages = Vec::new();
    let mut is_roots = Vec::new();
    let mut attributes_jsons = Vec::new();
    let mut resource_jsons = Vec::new();
    let mut events_data = Vec::new();
    let mut links_data = Vec::new();

    for resource_spans in &request.resource_spans {
        // Extract resource attributes as JSON
        let resource_json = if let Some(resource) = &resource_spans.resource {
            let mut resource_map = Map::new();
            for attr in &resource.attributes {
                resource_map.insert(attr.key.clone(), extract_value(&attr.value));
            }
            serde_json::to_string(&resource_map).unwrap_or_else(|_| "{}".to_string())
        } else {
            "{}".to_string()
        };

        // Extract service name from resource attributes
        let mut service_name = String::from("unknown");
        if let Some(resource) = &resource_spans.resource {
            for attr in &resource.attributes {
                if attr.key == "service.name" {
                    if let Some(val) = &attr.value {
                        if let Some(Value::StringValue(s)) = &val.value {
                            service_name = s.clone();
                        }
                    }
                }
            }
        }

        for scope_spans in &resource_spans.scope_spans {
            for span in &scope_spans.spans {
                // Convert trace and span IDs to hex strings
                let trace_id = if span.trace_id.len() == 16 {
                    TraceId::from_bytes(span.trace_id.clone().try_into().unwrap()).to_string()
                } else {
                    format!("{:032x}", 0) // Default trace ID if invalid
                };

                let span_id = if span.span_id.len() == 8 {
                    SpanId::from_bytes(span.span_id.clone().try_into().unwrap()).to_string()
                } else {
                    format!("{:016x}", 0) // Default span ID if invalid
                };

                let parent_span_id = if span.parent_span_id.is_empty() {
                    "0000000000000000".to_string()
                } else if span.parent_span_id.len() == 8 {
                    SpanId::from_bytes(span.parent_span_id.clone().try_into().unwrap()).to_string()
                } else {
                    "0000000000000000".to_string() // Default parent span ID if invalid
                };

                // Determine if this is a root span
                let is_root = parent_span_id == "0000000000000000";

                // Extract span attributes as JSON
                let mut attr_map = Map::new();
                for attr in &span.attributes {
                    attr_map.insert(attr.key.clone(), extract_value(&attr.value));
                }
                let attributes_json =
                    serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

                // Extract status
                let (status_code, status_message) = extract_status(span);

                // Extract span kind
                let span_kind = match span.kind {
                    0 => "Internal",
                    1 => "Server",
                    2 => "Client",
                    3 => "Producer",
                    4 => "Consumer",
                    _ => "Internal",
                };

                // Extract span kind

                // Extract events
                let events = extract_events(span);

                // Extract links
                let links = extract_links(span);

                // Add to arrays
                trace_ids.push(trace_id);
                span_ids.push(span_id);
                parent_span_ids.push(parent_span_id);
                names.push(span.name.clone());
                service_names.push(service_name.clone());
                start_times.push(span.start_time_unix_nano);
                end_times.push(span.end_time_unix_nano);
                durations.push(span.end_time_unix_nano - span.start_time_unix_nano);
                span_kinds.push(span_kind.to_string());
                status_codes.push(status_code);
                status_messages.push(status_message);
                is_roots.push(is_root);
                attributes_jsons.push(attributes_json);
                resource_jsons.push(resource_json.clone());
                events_data.push(events);
                links_data.push(links);
            }
        }
    }

    // Create Arrow arrays from the extracted data
    let trace_id_array: ArrayRef = Arc::new(StringArray::from(trace_ids));
    let span_id_array: ArrayRef = Arc::new(StringArray::from(span_ids));
    let parent_span_id_array: ArrayRef = Arc::new(StringArray::from(parent_span_ids));
    let name_array: ArrayRef = Arc::new(StringArray::from(names));
    let service_name_array: ArrayRef = Arc::new(StringArray::from(service_names));
    let start_time_array: ArrayRef = Arc::new(UInt64Array::from(start_times));
    let end_time_array: ArrayRef = Arc::new(UInt64Array::from(end_times));
    let duration_array: ArrayRef = Arc::new(UInt64Array::from(durations));
    let span_kind_array: ArrayRef = Arc::new(StringArray::from(span_kinds));
    let status_code_array: ArrayRef = Arc::new(StringArray::from(status_codes));
    let status_message_array: ArrayRef = Arc::new(StringArray::from(status_messages));
    let is_root_array: ArrayRef = Arc::new(BooleanArray::from(is_roots));
    let attributes_json_array: ArrayRef = Arc::new(StringArray::from(attributes_jsons));
    let resource_json_array: ArrayRef = Arc::new(StringArray::from(resource_jsons));

    // Create events list array
    let events_array = create_events_array(&events_data);

    // Create links list array
    let links_array = create_links_array(&links_data);

    // Clone schema for potential error case
    let schema_clone = schema.clone();

    // Create and return the RecordBatch
    let result = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            trace_id_array,
            span_id_array,
            parent_span_id_array,
            name_array,
            service_name_array,
            start_time_array,
            end_time_array,
            duration_array,
            span_kind_array,
            status_code_array,
            status_message_array,
            is_root_array,
            attributes_json_array,
            resource_json_array,
            events_array,
            links_array,
        ],
    );

    result.unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(schema_clone)))
}

/// Extract AnyValue to JsonValue
fn extract_value(attr_val: &Option<AnyValue>) -> JsonValue {
    match attr_val {
        Some(val) => match &val.value {
            Some(value) => match value {
                Value::BoolValue(v) => JsonValue::Bool(*v),
                Value::IntValue(v) => JsonValue::Number(serde_json::Number::from(*v)),
                Value::DoubleValue(v) => {
                    if let Some(num) = serde_json::Number::from_f64(*v) {
                        JsonValue::Number(num)
                    } else {
                        JsonValue::Null
                    }
                }
                Value::StringValue(v) => JsonValue::String(v.clone()),
                Value::ArrayValue(array_value) => {
                    let mut vals = vec![];
                    for item in array_value.values.iter() {
                        vals.push(extract_value(&Some(item.clone())));
                    }
                    JsonValue::Array(vals)
                }
                Value::KvlistValue(key_value_list) => {
                    let mut vals = Map::new();
                    for item in key_value_list.values.iter() {
                        vals.insert(
                            item.key.clone(),
                            extract_value(&item.value.as_ref().cloned()),
                        );
                    }
                    JsonValue::Object(vals)
                }
                Value::BytesValue(vec) => {
                    let s = String::from_utf8(vec.to_owned()).unwrap_or_default();
                    JsonValue::String(s)
                }
            },
            None => JsonValue::Null,
        },
        None => JsonValue::Null,
    }
}

/// Extract status code and message from span
fn extract_status(span: &OtelSpan) -> (String, String) {
    match &span.status {
        Some(status) => {
            let code = match status.code {
                0 => "Unspecified",
                1 => "Error",
                2 => "Ok",
                _ => "Unspecified",
            };
            (code.to_string(), status.message.clone())
        }
        None => ("Unspecified".to_string(), String::new()),
    }
}

/// Extract events from span
fn extract_events(span: &OtelSpan) -> Vec<(String, u64, String)> {
    let mut events = Vec::new();

    for event in &span.events {
        let mut attr_map = Map::new();
        for attr in &event.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        let attributes_json = serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

        events.push((event.name.clone(), event.time_unix_nano, attributes_json));
    }

    events
}

/// Extract links from span
fn extract_links(span: &OtelSpan) -> Vec<(String, String, String)> {
    let mut links = Vec::new();

    for link in &span.links {
        let trace_id =
            TraceId::from_bytes(link.trace_id.clone().try_into().unwrap_or([0; 16])).to_string();
        let span_id =
            SpanId::from_bytes(link.span_id.clone().try_into().unwrap_or([0; 8])).to_string();

        let mut attr_map = Map::new();
        for attr in &link.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        let attributes_json = serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

        links.push((trace_id, span_id, attributes_json));
    }

    links
}

/// Create Arrow array for events
fn create_events_array(events_data: &[Vec<(String, u64, String)>]) -> ArrayRef {
    // Define the event struct fields
    let event_struct_fields = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("timestamp_unix_nano", DataType::UInt64, false),
        Field::new("attributes_json", DataType::Utf8, true),
    ];

    let mut event_structs = Vec::new();

    for span_events in events_data {
        let mut event_names = Vec::new();
        let mut event_timestamps = Vec::new();
        let mut event_attrs = Vec::new();

        for (name, timestamp, attrs) in span_events {
            event_names.push(name.clone());
            event_timestamps.push(*timestamp);
            event_attrs.push(attrs.clone());
        }

        let name_array = StringArray::from(event_names);
        let timestamp_array = UInt64Array::from(event_timestamps);
        let attrs_array = StringArray::from(event_attrs);

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(name_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("timestamp_unix_nano", DataType::UInt64, false)),
                Arc::new(timestamp_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("attributes_json", DataType::Utf8, true)),
                Arc::new(attrs_array) as ArrayRef,
            ),
        ]);

        event_structs.push(Arc::new(struct_array) as ArrayRef);
    }

    // For now, return a placeholder empty array
    // TODO: Implement proper list array creation in a future PR
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(event_struct_fields.into()),
        true,
    ));
    Arc::new(ListArray::new_null(field, events_data.len()))
}

/// Create Arrow array for links
fn create_links_array(links_data: &[Vec<(String, String, String)>]) -> ArrayRef {
    // Define the link struct fields
    let link_struct_fields = vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("attributes_json", DataType::Utf8, true),
    ];

    let mut link_structs = Vec::new();

    for span_links in links_data {
        let mut link_trace_ids = Vec::new();
        let mut link_span_ids = Vec::new();
        let mut link_attrs = Vec::new();

        for (trace_id, span_id, attrs) in span_links {
            link_trace_ids.push(trace_id.clone());
            link_span_ids.push(span_id.clone());
            link_attrs.push(attrs.clone());
        }

        let trace_id_array = StringArray::from(link_trace_ids);
        let span_id_array = StringArray::from(link_span_ids);
        let attrs_array = StringArray::from(link_attrs);

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("trace_id", DataType::Utf8, false)),
                Arc::new(trace_id_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("span_id", DataType::Utf8, false)),
                Arc::new(span_id_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("attributes_json", DataType::Utf8, true)),
                Arc::new(attrs_array) as ArrayRef,
            ),
        ]);

        link_structs.push(Arc::new(struct_array) as ArrayRef);
    }

    // For now, return a placeholder empty array
    // TODO: Implement proper list array creation in a future PR
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(link_struct_fields.into()),
        true,
    ));
    Arc::new(ListArray::new_null(field, links_data.len()))
}

/// Convert OTLP log data to Arrow RecordBatch using the Flight log schema
pub fn otlp_logs_to_arrow(request: &ExportLogsServiceRequest) -> RecordBatch {
    let schemas = FlightSchemas::new();
    let schema = schemas.log_schema.clone();

    // Extract logs from the request
    let mut times = Vec::new();
    let mut observed_times = Vec::new();
    let mut severity_numbers = Vec::new();
    let mut severity_texts = Vec::new();
    let mut bodies = Vec::new();
    let mut trace_ids = Vec::new();
    let mut span_ids = Vec::new();
    let mut flags = Vec::new();
    let mut attributes_jsons = Vec::new();
    let mut resource_jsons = Vec::new();
    let mut scope_jsons = Vec::new();
    let mut dropped_attributes_counts = Vec::new();
    let mut service_names = Vec::new();

    for resource_logs in &request.resource_logs {
        // Extract resource attributes as JSON
        let resource_json = if let Some(resource) = &resource_logs.resource {
            let mut resource_map = Map::new();
            for attr in &resource.attributes {
                resource_map.insert(attr.key.clone(), extract_value(&attr.value));
            }
            serde_json::to_string(&resource_map).unwrap_or_else(|_| "{}".to_string())
        } else {
            "{}".to_string()
        };

        // Extract service name from resource attributes
        let mut service_name = String::from("unknown");
        if let Some(resource) = &resource_logs.resource {
            for attr in &resource.attributes {
                if attr.key == "service.name" {
                    if let Some(val) = &attr.value {
                        if let Some(Value::StringValue(s)) = &val.value {
                            service_name = s.clone();
                        }
                    }
                }
            }
        }

        for scope_logs in &resource_logs.scope_logs {
            // Extract scope attributes as JSON
            let scope_json = if let Some(scope) = &scope_logs.scope {
                let mut scope_map = Map::new();
                scope_map.insert("name".to_string(), JsonValue::String(scope.name.clone()));
                scope_map.insert(
                    "version".to_string(),
                    JsonValue::String(scope.version.clone()),
                );

                if !scope.attributes.is_empty() {
                    let mut attrs_map = Map::new();
                    for attr in &scope.attributes {
                        attrs_map.insert(attr.key.clone(), extract_value(&attr.value));
                    }
                    scope_map.insert("attributes".to_string(), JsonValue::Object(attrs_map));
                }

                serde_json::to_string(&scope_map).unwrap_or_else(|_| "{}".to_string())
            } else {
                "{}".to_string()
            };

            for log in &scope_logs.log_records {
                // Extract log attributes as JSON
                let mut attr_map = Map::new();
                for attr in &log.attributes {
                    attr_map.insert(attr.key.clone(), extract_value(&attr.value));
                }
                let attributes_json =
                    serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

                // Extract body as JSON
                let body_json = if let Some(body) = &log.body {
                    serde_json::to_string(&extract_value(&Some(body.clone())))
                        .unwrap_or_else(|_| "null".to_string())
                } else {
                    "null".to_string()
                };

                // Extract severity
                let severity_number = log.severity_number as i32;
                let severity_text = log.severity_text.clone();

                // Add to arrays
                times.push(log.time_unix_nano);
                observed_times.push(log.observed_time_unix_nano);
                severity_numbers.push(severity_number);
                severity_texts.push(severity_text);
                bodies.push(body_json);
                trace_ids.push(log.trace_id.clone());
                span_ids.push(log.span_id.clone());
                flags.push(log.flags);
                attributes_jsons.push(attributes_json);
                resource_jsons.push(resource_json.clone());
                scope_jsons.push(scope_json.clone());
                dropped_attributes_counts.push(log.dropped_attributes_count);
                service_names.push(service_name.clone());
            }
        }
    }

    // Create Arrow arrays from the extracted data
    let time_array: ArrayRef = Arc::new(UInt64Array::from(times));
    let observed_time_array: ArrayRef = Arc::new(UInt64Array::from(observed_times));
    let severity_number_array: ArrayRef = Arc::new(Int32Array::from(severity_numbers));
    let severity_text_array: ArrayRef = Arc::new(StringArray::from(severity_texts));
    let body_array: ArrayRef = Arc::new(StringArray::from(bodies));

    // Create binary arrays for trace_id and span_id
    let trace_id_array: ArrayRef =
        Arc::new(BinaryArray::from_iter(trace_ids.into_iter().map(Some)));
    let span_id_array: ArrayRef = Arc::new(BinaryArray::from_iter(span_ids.into_iter().map(Some)));

    let flags_array: ArrayRef = Arc::new(UInt32Array::from(flags));
    let attributes_json_array: ArrayRef = Arc::new(StringArray::from(attributes_jsons));
    let resource_json_array: ArrayRef = Arc::new(StringArray::from(resource_jsons));
    let scope_json_array: ArrayRef = Arc::new(StringArray::from(scope_jsons));
    let dropped_attributes_count_array: ArrayRef =
        Arc::new(UInt32Array::from(dropped_attributes_counts));
    let service_name_array: ArrayRef = Arc::new(StringArray::from(service_names));

    // Clone schema for potential error case
    let schema_clone = schema.clone();

    // Create and return the RecordBatch
    let result = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            time_array,
            observed_time_array,
            severity_number_array,
            severity_text_array,
            body_array,
            trace_id_array,
            span_id_array,
            flags_array,
            attributes_json_array,
            resource_json_array,
            scope_json_array,
            dropped_attributes_count_array,
            service_name_array,
        ],
    );

    result.unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(schema_clone)))
}

/// Convert OTLP metric data to Arrow RecordBatch using the Flight metric schema
pub fn otlp_metrics_to_arrow(request: &ExportMetricsServiceRequest) -> RecordBatch {
    let schemas = FlightSchemas::new();
    let schema = schemas.metric_schema.clone();

    // Extract metrics from the request
    let mut names = Vec::new();
    let mut descriptions = Vec::new();
    let mut units = Vec::new();
    let mut start_times = Vec::new();
    let mut times = Vec::new();
    let mut attributes_jsons = Vec::new();
    let mut resource_jsons = Vec::new();
    let mut scope_jsons = Vec::new();
    let mut metric_types = Vec::new();
    let mut data_jsons = Vec::new();
    let mut aggregation_temporalities = Vec::new();
    let mut is_monotonics = Vec::new();

    for resource_metrics in &request.resource_metrics {
        // Extract resource attributes as JSON
        let resource_json = if let Some(resource) = &resource_metrics.resource {
            let mut resource_map = Map::new();
            for attr in &resource.attributes {
                resource_map.insert(attr.key.clone(), extract_value(&attr.value));
            }
            serde_json::to_string(&resource_map).unwrap_or_else(|_| "{}".to_string())
        } else {
            "{}".to_string()
        };

        for scope_metrics in &resource_metrics.scope_metrics {
            // Extract scope attributes as JSON
            let scope_json = if let Some(scope) = &scope_metrics.scope {
                let mut scope_map = Map::new();
                scope_map.insert("name".to_string(), JsonValue::String(scope.name.clone()));
                scope_map.insert(
                    "version".to_string(),
                    JsonValue::String(scope.version.clone()),
                );

                if !scope.attributes.is_empty() {
                    let mut attrs_map = Map::new();
                    for attr in &scope.attributes {
                        attrs_map.insert(attr.key.clone(), extract_value(&attr.value));
                    }
                    scope_map.insert("attributes".to_string(), JsonValue::Object(attrs_map));
                }

                serde_json::to_string(&scope_map).unwrap_or_else(|_| "{}".to_string())
            } else {
                "{}".to_string()
            };

            for metric in &scope_metrics.metrics {
                // Extract common metric fields
                let name = metric.name.clone();
                let description = metric.description.clone();
                let unit = metric.unit.clone();

                // Extract metric-specific fields based on data type
                let (metric_type, data_json, start_time, time, agg_temporality, is_monotonic) = match &metric.data {
                    Some(data) => match data {
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                            let data_points = extract_number_data_points(&gauge.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !gauge.data_points.is_empty() {
                                (
                                    gauge.data_points[0].start_time_unix_nano,
                                    gauge.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            ("gauge".to_string(), data_json, start_time, time, 0, false)
                        },
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
                            let data_points = extract_number_data_points(&sum.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !sum.data_points.is_empty() {
                                (
                                    sum.data_points[0].start_time_unix_nano,
                                    sum.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            (
                                "sum".to_string(),
                                data_json,
                                start_time,
                                time,
                                sum.aggregation_temporality as i32,
                                sum.is_monotonic,
                            )
                        },
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(histogram) => {
                            let data_points = extract_histogram_data_points(&histogram.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !histogram.data_points.is_empty() {
                                (
                                    histogram.data_points[0].start_time_unix_nano,
                                    histogram.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            (
                                "histogram".to_string(),
                                data_json,
                                start_time,
                                time,
                                histogram.aggregation_temporality as i32,
                                false,
                            )
                        },
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::ExponentialHistogram(exp_histogram) => {
                            let data_points = extract_exponential_histogram_data_points(&exp_histogram.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !exp_histogram.data_points.is_empty() {
                                (
                                    exp_histogram.data_points[0].start_time_unix_nano,
                                    exp_histogram.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            (
                                "exponential_histogram".to_string(),
                                data_json,
                                start_time,
                                time,
                                exp_histogram.aggregation_temporality as i32,
                                false,
                            )
                        },
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Summary(summary) => {
                            let data_points = extract_summary_data_points(&summary.data_points);
                            let data_json = serde_json::to_string(&data_points).unwrap_or_else(|_| "[]".to_string());

                            // Get time from the first data point if available
                            let (start_time, time) = if !summary.data_points.is_empty() {
                                (
                                    summary.data_points[0].start_time_unix_nano,
                                    summary.data_points[0].time_unix_nano,
                                )
                            } else {
                                (0, 0)
                            };

                            ("summary".to_string(), data_json, start_time, time, 0, false)
                        },
                    },
                    None => ("unknown".to_string(), "{}".to_string(), 0, 0, 0, false),
                };

                // Extract metadata attributes as JSON
                let mut attr_map = Map::new();
                for attr in &metric.metadata {
                    attr_map.insert(attr.key.clone(), extract_value(&attr.value));
                }
                let attributes_json =
                    serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

                // Add to arrays
                names.push(name);
                descriptions.push(description);
                units.push(unit);
                start_times.push(start_time);
                times.push(time);
                attributes_jsons.push(attributes_json);
                resource_jsons.push(resource_json.clone());
                scope_jsons.push(scope_json.clone());
                metric_types.push(metric_type);
                data_jsons.push(data_json);
                aggregation_temporalities.push(agg_temporality);
                is_monotonics.push(is_monotonic);
            }
        }
    }

    // Create Arrow arrays from the extracted data
    let name_array: ArrayRef = Arc::new(StringArray::from(names));
    let description_array: ArrayRef = Arc::new(StringArray::from(descriptions));
    let unit_array: ArrayRef = Arc::new(StringArray::from(units));
    let start_time_array: ArrayRef = Arc::new(UInt64Array::from(start_times));
    let time_array: ArrayRef = Arc::new(UInt64Array::from(times));
    let attributes_json_array: ArrayRef = Arc::new(StringArray::from(attributes_jsons));
    let resource_json_array: ArrayRef = Arc::new(StringArray::from(resource_jsons));
    let scope_json_array: ArrayRef = Arc::new(StringArray::from(scope_jsons));
    let metric_type_array: ArrayRef = Arc::new(StringArray::from(metric_types));
    let data_json_array: ArrayRef = Arc::new(StringArray::from(data_jsons));
    let aggregation_temporality_array: ArrayRef =
        Arc::new(Int32Array::from(aggregation_temporalities));
    let is_monotonic_array: ArrayRef = Arc::new(BooleanArray::from(is_monotonics));

    // Clone schema for potential error case
    let schema_clone = schema.clone();

    // Create and return the RecordBatch
    let result = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            name_array,
            description_array,
            unit_array,
            start_time_array,
            time_array,
            attributes_json_array,
            resource_json_array,
            scope_json_array,
            metric_type_array,
            data_json_array,
            aggregation_temporality_array,
            is_monotonic_array,
        ],
    );

    result.unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(schema_clone)))
}

/// Extract number data points from OTLP NumberDataPoint
fn extract_number_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for point in data_points {
        let mut point_map = Map::new();

        // Add timestamps
        point_map.insert(
            "start_time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.start_time_unix_nano)),
        );
        point_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.time_unix_nano)),
        );

        // Add value
        match &point.value {
            Some(value) => match value {
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v) => {
                    if let Some(num) = serde_json::Number::from_f64(*v) {
                        point_map.insert("value".to_string(), JsonValue::Number(num));
                    } else {
                        point_map.insert("value".to_string(), JsonValue::Null);
                    }
                }
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(v) => {
                    point_map.insert(
                        "value".to_string(),
                        JsonValue::Number(serde_json::Number::from(*v)),
                    );
                }
            },
            None => {
                point_map.insert("value".to_string(), JsonValue::Null);
            }
        }

        // Add attributes
        let mut attr_map = Map::new();
        for attr in &point.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        point_map.insert("attributes".to_string(), JsonValue::Object(attr_map));

        // Add flags
        point_map.insert(
            "flags".to_string(),
            JsonValue::Number(serde_json::Number::from(point.flags)),
        );

        // Add exemplars if present
        if !point.exemplars.is_empty() {
            let exemplars = extract_exemplars(&point.exemplars);
            point_map.insert("exemplars".to_string(), JsonValue::Array(exemplars));
        }

        result.push(JsonValue::Object(point_map));
    }

    result
}

/// Extract histogram data points from OTLP HistogramDataPoint
fn extract_histogram_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for point in data_points {
        let mut point_map = Map::new();

        // Add timestamps
        point_map.insert(
            "start_time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.start_time_unix_nano)),
        );
        point_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.time_unix_nano)),
        );

        // Add count and sum
        point_map.insert(
            "count".to_string(),
            JsonValue::Number(serde_json::Number::from(point.count)),
        );
        if let Some(sum) = point.sum {
            if let Some(num) = serde_json::Number::from_f64(sum) {
                point_map.insert("sum".to_string(), JsonValue::Number(num));
            }
        }

        // Add min/max if present
        if let Some(min) = point.min {
            if let Some(num) = serde_json::Number::from_f64(min) {
                point_map.insert("min".to_string(), JsonValue::Number(num));
            }
        }

        if let Some(max) = point.max {
            if let Some(num) = serde_json::Number::from_f64(max) {
                point_map.insert("max".to_string(), JsonValue::Number(num));
            }
        }

        // Add bucket boundaries and counts
        let mut bounds = Vec::new();
        for bound in &point.explicit_bounds {
            if let Some(num) = serde_json::Number::from_f64(*bound) {
                bounds.push(JsonValue::Number(num));
            }
        }
        point_map.insert("explicit_bounds".to_string(), JsonValue::Array(bounds));

        let mut counts = Vec::new();
        for count in &point.bucket_counts {
            counts.push(JsonValue::Number(serde_json::Number::from(*count)));
        }
        point_map.insert("bucket_counts".to_string(), JsonValue::Array(counts));

        // Add attributes
        let mut attr_map = Map::new();
        for attr in &point.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        point_map.insert("attributes".to_string(), JsonValue::Object(attr_map));

        // Add flags
        point_map.insert(
            "flags".to_string(),
            JsonValue::Number(serde_json::Number::from(point.flags)),
        );

        // Add exemplars if present
        if !point.exemplars.is_empty() {
            let exemplars = extract_exemplars(&point.exemplars);
            point_map.insert("exemplars".to_string(), JsonValue::Array(exemplars));
        }

        result.push(JsonValue::Object(point_map));
    }

    result
}

/// Extract exponential histogram data points from OTLP ExponentialHistogramDataPoint
fn extract_exponential_histogram_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for point in data_points {
        let mut point_map = Map::new();

        // Add timestamps
        point_map.insert(
            "start_time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.start_time_unix_nano)),
        );
        point_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.time_unix_nano)),
        );

        // Add count, sum, and scale
        point_map.insert(
            "count".to_string(),
            JsonValue::Number(serde_json::Number::from(point.count)),
        );
        if let Some(sum) = point.sum {
            if let Some(num) = serde_json::Number::from_f64(sum) {
                point_map.insert("sum".to_string(), JsonValue::Number(num));
            }
        }
        point_map.insert(
            "scale".to_string(),
            JsonValue::Number(serde_json::Number::from(point.scale)),
        );
        point_map.insert(
            "zero_count".to_string(),
            JsonValue::Number(serde_json::Number::from(point.zero_count)),
        );

        // Add min/max if present
        if let Some(min) = point.min {
            if let Some(num) = serde_json::Number::from_f64(min) {
                point_map.insert("min".to_string(), JsonValue::Number(num));
            }
        }

        if let Some(max) = point.max {
            if let Some(num) = serde_json::Number::from_f64(max) {
                point_map.insert("max".to_string(), JsonValue::Number(num));
            }
        }

        // Add positive and negative buckets
        if let Some(positive) = &point.positive {
            let mut pos_map = Map::new();
            pos_map.insert(
                "offset".to_string(),
                JsonValue::Number(serde_json::Number::from(positive.offset)),
            );

            let mut counts = Vec::new();
            for count in &positive.bucket_counts {
                counts.push(JsonValue::Number(serde_json::Number::from(*count)));
            }
            pos_map.insert("bucket_counts".to_string(), JsonValue::Array(counts));

            point_map.insert("positive".to_string(), JsonValue::Object(pos_map));
        }

        if let Some(negative) = &point.negative {
            let mut neg_map = Map::new();
            neg_map.insert(
                "offset".to_string(),
                JsonValue::Number(serde_json::Number::from(negative.offset)),
            );

            let mut counts = Vec::new();
            for count in &negative.bucket_counts {
                counts.push(JsonValue::Number(serde_json::Number::from(*count)));
            }
            neg_map.insert("bucket_counts".to_string(), JsonValue::Array(counts));

            point_map.insert("negative".to_string(), JsonValue::Object(neg_map));
        }

        // Add zero threshold
        point_map.insert(
            "zero_threshold".to_string(),
            JsonValue::Number(
                serde_json::Number::from_f64(point.zero_threshold)
                    .unwrap_or(serde_json::Number::from(0)),
            ),
        );

        // Add attributes
        let mut attr_map = Map::new();
        for attr in &point.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        point_map.insert("attributes".to_string(), JsonValue::Object(attr_map));

        // Add flags
        point_map.insert(
            "flags".to_string(),
            JsonValue::Number(serde_json::Number::from(point.flags)),
        );

        // Add exemplars if present
        if !point.exemplars.is_empty() {
            let exemplars = extract_exemplars(&point.exemplars);
            point_map.insert("exemplars".to_string(), JsonValue::Array(exemplars));
        }

        result.push(JsonValue::Object(point_map));
    }

    result
}

/// Extract summary data points from OTLP SummaryDataPoint
fn extract_summary_data_points(
    data_points: &[opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for point in data_points {
        let mut point_map = Map::new();

        // Add timestamps
        point_map.insert(
            "start_time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.start_time_unix_nano)),
        );
        point_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(point.time_unix_nano)),
        );

        // Add count and sum
        point_map.insert(
            "count".to_string(),
            JsonValue::Number(serde_json::Number::from(point.count)),
        );
        if let Some(num) = serde_json::Number::from_f64(point.sum) {
            point_map.insert("sum".to_string(), JsonValue::Number(num));
        }

        // Add quantile values
        let mut quantiles = Vec::new();
        for q in &point.quantile_values {
            let mut q_map = Map::new();
            q_map.insert(
                "quantile".to_string(),
                JsonValue::Number(
                    serde_json::Number::from_f64(q.quantile).unwrap_or(serde_json::Number::from(0)),
                ),
            );
            q_map.insert(
                "value".to_string(),
                JsonValue::Number(
                    serde_json::Number::from_f64(q.value).unwrap_or(serde_json::Number::from(0)),
                ),
            );
            quantiles.push(JsonValue::Object(q_map));
        }
        point_map.insert("quantile_values".to_string(), JsonValue::Array(quantiles));

        // Add attributes
        let mut attr_map = Map::new();
        for attr in &point.attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        point_map.insert("attributes".to_string(), JsonValue::Object(attr_map));

        // Add flags
        point_map.insert(
            "flags".to_string(),
            JsonValue::Number(serde_json::Number::from(point.flags)),
        );

        result.push(JsonValue::Object(point_map));
    }

    result
}

/// Extract exemplars from OTLP Exemplar
fn extract_exemplars(
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
) -> Vec<JsonValue> {
    let mut result = Vec::new();

    for exemplar in exemplars {
        let mut exemplar_map = Map::new();

        // Add timestamp
        exemplar_map.insert(
            "time_unix_nano".to_string(),
            JsonValue::Number(serde_json::Number::from(exemplar.time_unix_nano)),
        );

        // Add value
        match &exemplar.value {
            Some(value) => match value {
                opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(v) => {
                    if let Some(num) = serde_json::Number::from_f64(*v) {
                        exemplar_map.insert("value".to_string(), JsonValue::Number(num));
                    } else {
                        exemplar_map.insert("value".to_string(), JsonValue::Null);
                    }
                }
                opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsInt(v) => {
                    exemplar_map.insert(
                        "value".to_string(),
                        JsonValue::Number(serde_json::Number::from(*v)),
                    );
                }
            },
            None => {
                exemplar_map.insert("value".to_string(), JsonValue::Null);
            }
        }

        // Add filtered attributes
        let mut attr_map = Map::new();
        for attr in &exemplar.filtered_attributes {
            attr_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        exemplar_map.insert(
            "filtered_attributes".to_string(),
            JsonValue::Object(attr_map),
        );

        // Add trace and span IDs if present
        if !exemplar.trace_id.is_empty() {
            exemplar_map.insert(
                "trace_id".to_string(),
                JsonValue::String(hex::encode(&exemplar.trace_id)),
            );
        }

        if !exemplar.span_id.is_empty() {
            exemplar_map.insert(
                "span_id".to_string(),
                JsonValue::String(hex::encode(&exemplar.span_id)),
            );
        }

        result.push(JsonValue::Object(exemplar_map));
    }

    result
}

/// Convert Arrow RecordBatch back to OTLP format for metrics
pub fn arrow_to_otlp_metrics(batch: &RecordBatch) -> ExportMetricsServiceRequest {
    // This is a placeholder for the reverse conversion
    // Implementation will be added in a future PR
    ExportMetricsServiceRequest {
        resource_metrics: vec![],
    }
}

/// Convert Arrow RecordBatch back to OTLP format for logs
pub fn arrow_to_otlp_logs(batch: &RecordBatch) -> ExportLogsServiceRequest {
    // This is a placeholder for the reverse conversion
    // Implementation will be added in a future PR
    ExportLogsServiceRequest {
        resource_logs: vec![],
    }
}

/// Convert Arrow RecordBatch back to OTLP format
pub fn arrow_to_otlp_traces(batch: &RecordBatch) -> ExportTraceServiceRequest {
    // This is a placeholder for the reverse conversion
    // Implementation will be added in a future PR
    ExportTraceServiceRequest {
        resource_spans: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };

    #[test]
    fn test_otlp_traces_to_arrow_empty() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![],
        };

        let batch = otlp_traces_to_arrow(&request);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 16); // All the fields in the schema
    }

    #[test]
    fn test_otlp_traces_to_arrow_single_span() {
        // Create a simple OTLP request with one span
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        trace_state: String::new(),
                        parent_span_id: vec![],
                        name: "test-span".to_string(),
                        kind: 1, // SERVER
                        start_time_unix_nano: 1000,
                        end_time_unix_nano: 2000,
                        attributes: vec![KeyValue {
                            key: "test.attribute".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("test-value".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            code: 2, // OK
                            message: String::new(),
                        }),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = otlp_traces_to_arrow(&request);

        // Verify the batch has one row
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 16);

        // Verify some of the values
        let trace_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let span_id = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let service_name = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let span_kind = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let status_code = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(name.value(0), "test-span");
        assert_eq!(service_name.value(0), "test-service");
        assert_eq!(span_kind.value(0), "Server");
        assert_eq!(status_code.value(0), "Ok");
    }

    #[test]
    fn test_otlp_logs_to_arrow_empty() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![],
        };

        let batch = otlp_logs_to_arrow(&request);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 13); // All the fields in the log schema
    }

    #[test]
    fn test_otlp_logs_to_arrow_single_log() {
        // Create a simple OTLP request with one log record
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1000,
                        observed_time_unix_nano: 2000,
                        severity_number: 9, // INFO (9) according to the SeverityNumber enum
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("Test log message".to_string())),
                        }),
                        attributes: vec![KeyValue {
                            key: "test.attribute".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("test-value".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = otlp_logs_to_arrow(&request);

        // Verify the batch has one row
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 13);

        // Verify some of the values
        let time = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let observed_time = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let severity_number = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let severity_text = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let service_name = batch
            .column(12)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(time.value(0), 1000);
        assert_eq!(observed_time.value(0), 2000);
        assert_eq!(severity_number.value(0), 9); // INFO (9) according to the SeverityNumber enum
        assert_eq!(severity_text.value(0), "INFO");
        assert_eq!(service_name.value(0), "test-service");
    }

    #[test]
    fn test_extract_value() {
        // Test boolean value
        let bool_value = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(extract_value(&Some(bool_value)), JsonValue::Bool(true));

        // Test integer value
        let int_value = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(
            extract_value(&Some(int_value)),
            JsonValue::Number(serde_json::Number::from(42))
        );

        // Test string value
        let string_value = AnyValue {
            value: Some(Value::StringValue("test".to_string())),
        };
        assert_eq!(
            extract_value(&Some(string_value)),
            JsonValue::String("test".to_string())
        );

        // Test null value
        let null_value = AnyValue { value: None };
        assert_eq!(extract_value(&Some(null_value)), JsonValue::Null);

        // Test None
        assert_eq!(extract_value(&None), JsonValue::Null);
    }
}
