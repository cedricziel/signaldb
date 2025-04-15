use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int32Array, ListArray, RecordBatch, StringArray, StructArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue, KeyValue},
    logs::v1::{LogRecord, SeverityNumber},
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
                let attributes_json = serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

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

        events.push((
            event.name.clone(),
            event.time_unix_nano,
            attributes_json,
        ));
    }

    events
}

/// Extract links from span
fn extract_links(span: &OtelSpan) -> Vec<(String, String, String)> {
    let mut links = Vec::new();

    for link in &span.links {
        let trace_id = TraceId::from_bytes(link.trace_id.clone().try_into().unwrap_or([0; 16])).to_string();
        let span_id = SpanId::from_bytes(link.span_id.clone().try_into().unwrap_or([0; 8])).to_string();

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
                scope_map.insert("version".to_string(), JsonValue::String(scope.version.clone()));

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
                let attributes_json = serde_json::to_string(&attr_map).unwrap_or_else(|_| "{}".to_string());

                // Extract body as JSON
                let body_json = if let Some(body) = &log.body {
                    serde_json::to_string(&extract_value(&Some(body.clone()))).unwrap_or_else(|_| "null".to_string())
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
    let trace_id_array: ArrayRef = Arc::new(BinaryArray::from_iter(trace_ids.into_iter().map(Some)));
    let span_id_array: ArrayRef = Arc::new(BinaryArray::from_iter(span_ids.into_iter().map(Some)));

    let flags_array: ArrayRef = Arc::new(UInt32Array::from(flags));
    let attributes_json_array: ArrayRef = Arc::new(StringArray::from(attributes_jsons));
    let resource_json_array: ArrayRef = Arc::new(StringArray::from(resource_jsons));
    let scope_json_array: ArrayRef = Arc::new(StringArray::from(scope_jsons));
    let dropped_attributes_count_array: ArrayRef = Arc::new(UInt32Array::from(dropped_attributes_counts));
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
        let trace_id = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let span_id = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let name = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let service_name = batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
        let span_kind = batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
        let status_code = batch.column(9).as_any().downcast_ref::<StringArray>().unwrap();

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
        let time = batch.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let observed_time = batch.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
        let severity_number = batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        let severity_text = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let service_name = batch.column(12).as_any().downcast_ref::<StringArray>().unwrap();

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
