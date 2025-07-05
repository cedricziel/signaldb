use datafusion::arrow::{
    array::{Array, ArrayRef, BooleanArray, ListArray, StringArray, StructArray, UInt64Array},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use hex;
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::KeyValue,
    trace::v1::{ResourceSpans, ScopeSpans, Span as OtelSpan},
};
use serde_json::Map;
use std::sync::Arc;

use crate::flight::conversion::conversion_common::{
    extract_resource_json, extract_service_name, extract_value, json_value_to_any_value,
};
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
        let resource_json = extract_resource_json(&resource_spans.resource);

        // Extract service name from resource attributes
        let service_name = extract_service_name(&resource_spans.resource);

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

    // Collect all events from all spans into flat arrays
    let mut all_event_names = Vec::new();
    let mut all_event_timestamps = Vec::new();
    let mut all_event_attrs = Vec::new();

    // Create offsets array - tracks where each span's events start/end
    let mut offsets = Vec::with_capacity(events_data.len() + 1);
    offsets.push(0i32);

    for span_events in events_data {
        for (name, timestamp, attrs) in span_events {
            all_event_names.push(name.clone());
            all_event_timestamps.push(*timestamp);
            all_event_attrs.push(attrs.clone());
        }
        offsets.push(all_event_names.len() as i32);
    }

    // Create the struct array containing all events
    let values = if all_event_names.is_empty() {
        // Create empty struct array with correct schema
        StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(StringArray::new_null(0)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("timestamp_unix_nano", DataType::UInt64, false)),
                Arc::new(UInt64Array::new_null(0)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("attributes_json", DataType::Utf8, true)),
                Arc::new(StringArray::new_null(0)) as ArrayRef,
            ),
        ])
    } else {
        StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(StringArray::from(all_event_names)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("timestamp_unix_nano", DataType::UInt64, false)),
                Arc::new(UInt64Array::from(all_event_timestamps)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("attributes_json", DataType::Utf8, true)),
                Arc::new(StringArray::from(all_event_attrs)) as ArrayRef,
            ),
        ])
    };

    // Create the list array field
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(event_struct_fields.into()),
        true,
    ));

    // Create the list array
    let offsets_buffer = OffsetBuffer::new(offsets.into());
    Arc::new(ListArray::try_new(field, offsets_buffer, Arc::new(values), None).unwrap())
}

/// Create Arrow array for links
fn create_links_array(links_data: &[Vec<(String, String, String)>]) -> ArrayRef {
    // Define the link struct fields
    let link_struct_fields = vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("attributes_json", DataType::Utf8, true),
    ];

    // Collect all links from all spans into flat arrays
    let mut all_link_trace_ids = Vec::new();
    let mut all_link_span_ids = Vec::new();
    let mut all_link_attrs = Vec::new();

    // Create offsets array - tracks where each span's links start/end
    let mut offsets = Vec::with_capacity(links_data.len() + 1);
    offsets.push(0i32);

    for span_links in links_data {
        for (trace_id, span_id, attrs) in span_links {
            all_link_trace_ids.push(trace_id.clone());
            all_link_span_ids.push(span_id.clone());
            all_link_attrs.push(attrs.clone());
        }
        offsets.push(all_link_trace_ids.len() as i32);
    }

    // Create the struct array containing all links
    let values = if all_link_trace_ids.is_empty() {
        // Create empty struct array with correct schema
        StructArray::from(vec![
            (
                Arc::new(Field::new("trace_id", DataType::Utf8, false)),
                Arc::new(StringArray::new_null(0)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("span_id", DataType::Utf8, false)),
                Arc::new(StringArray::new_null(0)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("attributes_json", DataType::Utf8, true)),
                Arc::new(StringArray::new_null(0)) as ArrayRef,
            ),
        ])
    } else {
        StructArray::from(vec![
            (
                Arc::new(Field::new("trace_id", DataType::Utf8, false)),
                Arc::new(StringArray::from(all_link_trace_ids)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("span_id", DataType::Utf8, false)),
                Arc::new(StringArray::from(all_link_span_ids)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("attributes_json", DataType::Utf8, true)),
                Arc::new(StringArray::from(all_link_attrs)) as ArrayRef,
            ),
        ])
    };

    // Create the list array field
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(link_struct_fields.into()),
        true,
    ));

    // Create the list array
    let offsets_buffer = OffsetBuffer::new(offsets.into());
    Arc::new(ListArray::try_new(field, offsets_buffer, Arc::new(values), None).unwrap())
}

/// Parse events from ListArray for a specific row
fn parse_events_from_list_array(
    events_array: &ListArray,
    row: usize,
) -> Vec<opentelemetry_proto::tonic::trace::v1::span::Event> {
    use opentelemetry_proto::tonic::trace::v1::span::Event;

    let mut events = Vec::new();

    // Get the list for this row
    if !events_array.is_null(row) {
        let list_values = events_array.value(row);
        if let Some(struct_array) = list_values.as_any().downcast_ref::<StructArray>() {
            let name_array = struct_array
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let timestamp_array = struct_array
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let attrs_array = struct_array
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..struct_array.len() {
                let name = name_array.value(i).to_string();
                let timestamp = timestamp_array.value(i);
                let attrs_json = attrs_array.value(i);

                // Parse attributes from JSON
                let mut attributes = Vec::new();
                if let Ok(attrs_value) = serde_json::from_str::<serde_json::Value>(attrs_json) {
                    if let Some(attrs_obj) = attrs_value.as_object() {
                        for (key, value) in attrs_obj {
                            attributes.push(KeyValue {
                                key: key.clone(),
                                value: Some(json_value_to_any_value(value)),
                            });
                        }
                    }
                }

                events.push(Event {
                    time_unix_nano: timestamp,
                    name,
                    attributes,
                    dropped_attributes_count: 0,
                });
            }
        }
    }

    events
}

/// Parse links from ListArray for a specific row  
fn parse_links_from_list_array(
    links_array: &ListArray,
    row: usize,
) -> Vec<opentelemetry_proto::tonic::trace::v1::span::Link> {
    use opentelemetry_proto::tonic::trace::v1::span::Link;

    let mut links = Vec::new();

    // Get the list for this row
    if !links_array.is_null(row) {
        let list_values = links_array.value(row);
        if let Some(struct_array) = list_values.as_any().downcast_ref::<StructArray>() {
            let trace_id_array = struct_array
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let span_id_array = struct_array
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let attrs_array = struct_array
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..struct_array.len() {
                let trace_id_str = trace_id_array.value(i);
                let span_id_str = span_id_array.value(i);
                let attrs_json = attrs_array.value(i);

                // Convert hex strings back to bytes
                let trace_id_bytes = hex::decode(trace_id_str).unwrap_or(vec![0; 16]);
                let span_id_bytes = hex::decode(span_id_str).unwrap_or(vec![0; 8]);

                // Parse attributes from JSON
                let mut attributes = Vec::new();
                if let Ok(attrs_value) = serde_json::from_str::<serde_json::Value>(attrs_json) {
                    if let Some(attrs_obj) = attrs_value.as_object() {
                        for (key, value) in attrs_obj {
                            attributes.push(KeyValue {
                                key: key.clone(),
                                value: Some(json_value_to_any_value(value)),
                            });
                        }
                    }
                }

                links.push(Link {
                    trace_id: trace_id_bytes,
                    span_id: span_id_bytes,
                    trace_state: "".to_string(),
                    attributes,
                    dropped_attributes_count: 0,
                    flags: 0,
                });
            }
        }
    }

    links
}

/// Convert Arrow RecordBatch to OTLP ExportTraceServiceRequest
pub fn arrow_to_otlp_traces(batch: &RecordBatch) -> ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::trace::v1::Status;
    use std::convert::TryInto;

    let columns = batch.columns();

    // Extract columns by index based on the schema order in otlp_traces_to_arrow
    let trace_id_array = columns[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("trace_id column should be StringArray");
    let span_id_array = columns[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("span_id column should be StringArray");
    let parent_span_id_array = columns[2]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("parent_span_id column should be StringArray");
    let name_array = columns[3]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be StringArray");
    let service_name_array = columns[4]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("service_name column should be StringArray");
    let start_time_array = columns[5]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("start_time_unix_nano column should be UInt64Array");
    let end_time_array = columns[6]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("end_time_unix_nano column should be UInt64Array");
    let _duration_array = columns[7]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("duration column should be UInt64Array");
    let span_kind_array = columns[8]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("span_kind column should be StringArray");
    let status_code_array = columns[9]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("status_code column should be StringArray");
    let status_message_array = columns[10]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("status_message column should be StringArray");
    let _is_root_array = columns[11]
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("is_root column should be BooleanArray");
    let attributes_json_array = columns[12]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("attributes_json column should be StringArray");
    let resource_json_array = columns[13]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("resource_json column should be StringArray");
    let events_array = columns[14]
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("events column should be ListArray");
    let links_array = columns[15]
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("links column should be ListArray");

    let mut resource_spans_map = std::collections::HashMap::<String, ResourceSpans>::new();

    for row in 0..batch.num_rows() {
        // Parse trace_id and span_id from hex string to bytes
        let trace_id_str = trace_id_array.value(row);
        let trace_id_bytes = hex::decode(trace_id_str).unwrap_or(vec![0; 16]);
        let trace_id: [u8; 16] = trace_id_bytes.as_slice().try_into().unwrap_or([0; 16]);

        let span_id_str = span_id_array.value(row);
        let span_id_bytes = hex::decode(span_id_str).unwrap_or(vec![0; 8]);
        let span_id: [u8; 8] = span_id_bytes.as_slice().try_into().unwrap_or([0; 8]);

        let parent_span_id_str = parent_span_id_array.value(row);
        let parent_span_id_bytes = hex::decode(parent_span_id_str).unwrap_or(vec![0; 8]);
        let parent_span_id: [u8; 8] = parent_span_id_bytes.as_slice().try_into().unwrap_or([0; 8]);

        let name = name_array.value(row).to_string();
        let service_name = service_name_array.value(row).to_string();
        let start_time_unix_nano = start_time_array.value(row);
        let end_time_unix_nano = end_time_array.value(row);
        let span_kind_str = span_kind_array.value(row);
        let status_code_str = status_code_array.value(row);
        let status_message_str = status_message_array.value(row);
        let attributes_json_str = attributes_json_array.value(row);
        let resource_json_str = resource_json_array.value(row);

        // Convert span kind string to enum
        let span_kind = match span_kind_str {
            "Internal" => 0,
            "Server" => 1,
            "Client" => 2,
            "Producer" => 3,
            "Consumer" => 4,
            _ => 0,
        };

        // Convert status code string to enum
        let status_code = match status_code_str {
            "Unspecified" => 0,
            "Error" => 1,
            "Ok" => 2,
            _ => 0,
        };

        // Parse attributes JSON string to KeyValue vector
        let attributes: Vec<KeyValue> = if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(attributes_json_str)
        {
            map.into_iter()
                .map(|(k, v)| KeyValue {
                    key: k,
                    value: Some(
                        crate::flight::conversion::conversion_common::json_value_to_any_value(&v),
                    ),
                })
                .collect()
        } else {
            vec![]
        };

        // Parse resource JSON string to KeyValue vector
        let resource_attributes: Vec<KeyValue> = if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(resource_json_str)
        {
            map.into_iter()
                .map(|(k, v)| KeyValue {
                    key: k,
                    value: Some(
                        crate::flight::conversion::conversion_common::json_value_to_any_value(&v),
                    ),
                })
                .collect()
        } else {
            vec![]
        };

        // Construct the Span
        let span = OtelSpan {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            parent_span_id: parent_span_id.to_vec(),
            name: name.to_string(),
            kind: span_kind,
            start_time_unix_nano,
            end_time_unix_nano,
            attributes,
            dropped_attributes_count: 0,
            events: parse_events_from_list_array(events_array, row),
            dropped_events_count: 0,
            links: parse_links_from_list_array(links_array, row),
            dropped_links_count: 0,
            status: Some(Status {
                code: status_code,
                message: status_message_str.to_string(),
            }),
            flags: 0,
            trace_state: "".to_string(),
        };

        // Group spans by service name in resource_spans_map
        let resource_spans = resource_spans_map
            .entry(service_name.clone())
            .or_insert_with(|| ResourceSpans {
                resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                    attributes: resource_attributes.clone(),
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![],
                schema_url: "".to_string(),
            });

        // Find or create ScopeSpans for this service
        let scope_spans = resource_spans.scope_spans.iter_mut().find(|_ss| {
            // For now, no scope differentiation, so just use the first one
            true
        });

        if let Some(scope_spans) = scope_spans {
            scope_spans.spans.push(span);
        } else {
            resource_spans.scope_spans.push(ScopeSpans {
                scope: None,
                spans: vec![span],
                schema_url: "".to_string(),
            });
        }
    }

    ExportTraceServiceRequest {
        resource_spans: resource_spans_map.into_values().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::{
        array::{BooleanArray, StringArray, UInt64Array},
        datatypes::Schema,
    };
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue},
        trace::v1::{Span, Status},
    };
    use std::sync::Arc;

    #[test]
    fn test_otlp_traces_to_arrow() {
        // Create a simple OTLP trace
        let trace_id_bytes = hex::decode("0123456789abcdef0123456789abcdef").unwrap();
        let span_id_bytes = hex::decode("0123456789abcdef").unwrap();

        // Create a span with attributes
        let attributes = vec![KeyValue {
            key: "attr1".to_string(),
            value: Some(AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "value1".to_string(),
                    ),
                ),
            }),
        }];

        // Create events for the span
        let events = vec![opentelemetry_proto::tonic::trace::v1::span::Event {
            time_unix_nano: 1500000000,
            name: "test-event".to_string(),
            attributes: vec![KeyValue {
                key: "event_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "event_value".to_string(),
                        ),
                    ),
                }),
            }],
            dropped_attributes_count: 0,
        }];

        // Create links for the span
        let mut links = Vec::new();
        let link_trace_id = hex::decode("fedcba9876543210fedcba9876543210").unwrap();
        let link_span_id = hex::decode("fedcba9876543210").unwrap();
        links.push(opentelemetry_proto::tonic::trace::v1::span::Link {
            trace_id: link_trace_id,
            span_id: link_span_id,
            trace_state: "".to_string(),
            attributes: vec![KeyValue {
                key: "link_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "link_value".to_string(),
                        ),
                    ),
                }),
            }],
            dropped_attributes_count: 0,
            flags: 0,
        });

        // Create a span
        let span = Span {
            trace_id: trace_id_bytes,
            span_id: span_id_bytes,
            parent_span_id: vec![], // Root span
            name: "test-span".to_string(),
            kind: 1, // Server
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes,
            dropped_attributes_count: 0,
            events,
            dropped_events_count: 0,
            links,
            dropped_links_count: 0,
            status: Some(Status {
                code: 2, // Ok
                message: "Success".to_string(),
            }),
            flags: 0,
            trace_state: "".to_string(),
        };

        // Create resource attributes
        let resource_attributes = vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "test_service".to_string(),
                    ),
                ),
            }),
        }];

        // Create resource
        let resource = opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: resource_attributes,
            dropped_attributes_count: 0,
        };

        // Create scope spans
        let scope_spans = ScopeSpans {
            scope: None,
            spans: vec![span],
            schema_url: "".to_string(),
        };

        // Create resource spans
        let resource_spans = ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![scope_spans],
            schema_url: "".to_string(),
        };

        // Create the OTLP request
        let request = ExportTraceServiceRequest {
            resource_spans: vec![resource_spans],
        };

        // Convert OTLP to Arrow
        let result = otlp_traces_to_arrow(&request);

        // Verify the result
        assert_eq!(result.num_rows(), 1);

        // Get columns
        let columns = result.columns();
        let trace_id_array = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
        let span_id_array = columns[1].as_any().downcast_ref::<StringArray>().unwrap();
        let parent_span_id_array = columns[2].as_any().downcast_ref::<StringArray>().unwrap();
        let name_array = columns[3].as_any().downcast_ref::<StringArray>().unwrap();
        let service_name_array = columns[4].as_any().downcast_ref::<StringArray>().unwrap();
        let start_time_array = columns[5].as_any().downcast_ref::<UInt64Array>().unwrap();
        let end_time_array = columns[6].as_any().downcast_ref::<UInt64Array>().unwrap();
        let span_kind_array = columns[8].as_any().downcast_ref::<StringArray>().unwrap();
        let status_code_array = columns[9].as_any().downcast_ref::<StringArray>().unwrap();
        let status_message_array = columns[10].as_any().downcast_ref::<StringArray>().unwrap();
        let is_root_array = columns[11].as_any().downcast_ref::<BooleanArray>().unwrap();
        let attributes_json_array = columns[12].as_any().downcast_ref::<StringArray>().unwrap();
        let resource_json_array = columns[13].as_any().downcast_ref::<StringArray>().unwrap();

        // Verify values
        assert_eq!(trace_id_array.value(0), "0123456789abcdef0123456789abcdef");
        assert_eq!(span_id_array.value(0), "0123456789abcdef");
        assert_eq!(parent_span_id_array.value(0), "0000000000000000"); // Root span has empty parent
        assert_eq!(name_array.value(0), "test-span");
        assert_eq!(service_name_array.value(0), "test_service");
        assert_eq!(start_time_array.value(0), 1000000000);
        assert_eq!(end_time_array.value(0), 2000000000);
        assert_eq!(span_kind_array.value(0), "Server");
        assert_eq!(status_code_array.value(0), "Ok");
        assert_eq!(status_message_array.value(0), "Success");
        assert!(is_root_array.value(0)); // Should be a root span

        // Verify JSON strings
        let attributes_json: serde_json::Value =
            serde_json::from_str(attributes_json_array.value(0)).unwrap();
        assert_eq!(attributes_json["attr1"], "value1");

        let resource_json: serde_json::Value =
            serde_json::from_str(resource_json_array.value(0)).unwrap();
        assert_eq!(resource_json["service.name"], "test_service");
    }

    #[test]
    fn test_arrow_to_otlp_traces() {
        // Create a simple trace in Arrow format
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::UInt64, false),
            Field::new("end_time_unix_nano", DataType::UInt64, false),
            Field::new("duration_nano", DataType::UInt64, false),
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("status_code", DataType::Utf8, false),
            Field::new("status_message", DataType::Utf8, true),
            Field::new("is_root", DataType::Boolean, false),
            Field::new("attributes_json", DataType::Utf8, true),
            Field::new("resource_json", DataType::Utf8, true),
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("name", DataType::Utf8, false),
                            Field::new("timestamp_unix_nano", DataType::UInt64, false),
                            Field::new("attributes_json", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    true,
                ))),
                true,
            ),
            Field::new(
                "links",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("trace_id", DataType::Utf8, false),
                            Field::new("span_id", DataType::Utf8, false),
                            Field::new("attributes_json", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    true,
                ))),
                true,
            ),
        ]));

        // Sample data for a trace
        let trace_id = "0123456789abcdef0123456789abcdef";
        let span_id = "0123456789abcdef";
        let parent_span_id = "0000000000000000"; // Root span

        let trace_id_array = StringArray::from(vec![trace_id]);
        let span_id_array = StringArray::from(vec![span_id]);
        let parent_span_id_array = StringArray::from(vec![parent_span_id]);
        let name_array = StringArray::from(vec!["test-span"]);
        let service_name_array = StringArray::from(vec!["test-service"]);
        let start_time_array = UInt64Array::from(vec![1000000000]);
        let end_time_array = UInt64Array::from(vec![2000000000]);
        let duration_array = UInt64Array::from(vec![1000000000]); // 1 second
        let span_kind_array = StringArray::from(vec!["Server"]);
        let status_code_array = StringArray::from(vec!["Ok"]);
        let status_message_array = StringArray::from(vec!["Success"]);
        let is_root_array = BooleanArray::from(vec![true]);
        let attributes_json_array = StringArray::from(vec!["{\"attr1\":\"value1\"}"]);
        let resource_json_array = StringArray::from(vec!["{\"service.name\":\"test_service\"}"]);

        // Create empty events and links arrays
        let field_events = Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("timestamp_unix_nano", DataType::UInt64, false),
                    Field::new("attributes_json", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ));
        let events_array = Arc::new(ListArray::new_null(field_events, 1));

        let field_links = Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("trace_id", DataType::Utf8, false),
                    Field::new("span_id", DataType::Utf8, false),
                    Field::new("attributes_json", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ));
        let links_array = Arc::new(ListArray::new_null(field_links, 1));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id_array),
                Arc::new(span_id_array),
                Arc::new(parent_span_id_array),
                Arc::new(name_array),
                Arc::new(service_name_array),
                Arc::new(start_time_array),
                Arc::new(end_time_array),
                Arc::new(duration_array),
                Arc::new(span_kind_array),
                Arc::new(status_code_array),
                Arc::new(status_message_array),
                Arc::new(is_root_array),
                Arc::new(attributes_json_array),
                Arc::new(resource_json_array),
                events_array,
                links_array,
            ],
        )
        .unwrap();

        // Convert Arrow to OTLP
        let result = arrow_to_otlp_traces(&batch);

        // Verify the result
        assert_eq!(result.resource_spans.len(), 1);
        let resource_spans = &result.resource_spans[0];

        // Verify resource
        assert!(resource_spans.resource.is_some());
        let resource = resource_spans.resource.as_ref().unwrap();
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");

        // Verify scope spans
        assert_eq!(resource_spans.scope_spans.len(), 1);
        let scope_spans = &resource_spans.scope_spans[0];

        // Verify spans
        assert_eq!(scope_spans.spans.len(), 1);
        let span = &scope_spans.spans[0];

        // Verify span properties
        assert_eq!(hex::encode(&span.trace_id), trace_id);
        assert_eq!(hex::encode(&span.span_id), span_id);
        assert_eq!(span.name, "test-span");
        assert_eq!(span.kind, 1); // Server
        assert_eq!(span.start_time_unix_nano, 1000000000);
        assert_eq!(span.end_time_unix_nano, 2000000000);

        // Verify status
        assert!(span.status.is_some());
        let status = span.status.as_ref().unwrap();
        assert_eq!(status.code, 2); // Ok
        assert_eq!(status.message, "Success");

        // Verify attributes
        assert_eq!(span.attributes.len(), 1);
        assert_eq!(span.attributes[0].key, "attr1");
    }

    #[test]
    fn test_bidirectional_conversion() {
        // Create a simple OTLP trace
        let trace_id_bytes = hex::decode("0123456789abcdef0123456789abcdef").unwrap();
        let span_id_bytes = hex::decode("0123456789abcdef").unwrap();

        // Create a span with attributes
        let attributes = vec![
            KeyValue {
                key: "attr1".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "value1".to_string(),
                        ),
                    ),
                }),
            },
            KeyValue {
                key: "attr2".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(42),
                    ),
                }),
            },
        ];

        // Create events for the span
        let events = vec![
            opentelemetry_proto::tonic::trace::v1::span::Event {
                time_unix_nano: 1500000000,
                name: "span-event".to_string(),
                attributes: vec![KeyValue {
                    key: "event_key".to_string(),
                    value: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "event_val".to_string(),
                            ),
                        ),
                    }),
                }],
                dropped_attributes_count: 0,
            },
            opentelemetry_proto::tonic::trace::v1::span::Event {
                time_unix_nano: 1600000000,
                name: "second-event".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            },
        ];

        // Create links for the span
        let mut links = Vec::new();
        let link_trace_id = hex::decode("abcdef0123456789abcdef0123456789").unwrap();
        let link_span_id = hex::decode("abcdef0123456789").unwrap();
        links.push(opentelemetry_proto::tonic::trace::v1::span::Link {
            trace_id: link_trace_id,
            span_id: link_span_id,
            trace_state: "".to_string(),
            attributes: vec![KeyValue {
                key: "link_key".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(123),
                    ),
                }),
            }],
            dropped_attributes_count: 0,
            flags: 0,
        });

        // Create a span
        let span = Span {
            trace_id: trace_id_bytes,
            span_id: span_id_bytes,
            parent_span_id: vec![], // Root span
            name: "test-span".to_string(),
            kind: 1, // Server
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes,
            dropped_attributes_count: 0,
            events,
            dropped_events_count: 0,
            links,
            dropped_links_count: 0,
            status: Some(Status {
                code: 2, // Ok
                message: "Success".to_string(),
            }),
            flags: 0,
            trace_state: "".to_string(),
        };

        // Create resource attributes
        let resource_attributes = vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "test_service".to_string(),
                    ),
                ),
            }),
        }];

        // Create resource
        let resource = opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: resource_attributes,
            dropped_attributes_count: 0,
        };

        // Create scope spans
        let scope_spans = ScopeSpans {
            scope: None,
            spans: vec![span],
            schema_url: "".to_string(),
        };

        // Create resource spans
        let resource_spans = ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![scope_spans],
            schema_url: "".to_string(),
        };

        // Create the OTLP request
        let original_request = ExportTraceServiceRequest {
            resource_spans: vec![resource_spans],
        };

        // Convert OTLP to Arrow
        let arrow_batch = otlp_traces_to_arrow(&original_request);

        // Convert Arrow back to OTLP
        let converted_request = arrow_to_otlp_traces(&arrow_batch);

        // Verify the result
        assert_eq!(converted_request.resource_spans.len(), 1);
        let resource_spans = &converted_request.resource_spans[0];

        // Verify resource
        assert!(resource_spans.resource.is_some());
        let resource = resource_spans.resource.as_ref().unwrap();
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");

        // Verify scope spans
        assert_eq!(resource_spans.scope_spans.len(), 1);
        let scope_spans = &resource_spans.scope_spans[0];

        // Verify spans
        assert_eq!(scope_spans.spans.len(), 1);
        let span = &scope_spans.spans[0];

        // Verify span properties
        assert_eq!(
            hex::encode(&span.trace_id),
            "0123456789abcdef0123456789abcdef"
        );
        assert_eq!(hex::encode(&span.span_id), "0123456789abcdef");
        assert_eq!(span.name, "test-span");
        assert_eq!(span.kind, 1); // Server
        assert_eq!(span.start_time_unix_nano, 1000000000);
        assert_eq!(span.end_time_unix_nano, 2000000000);

        // Verify status
        assert!(span.status.is_some());
        let status = span.status.as_ref().unwrap();
        assert_eq!(status.code, 2); // Ok
        assert_eq!(status.message, "Success");

        // Verify attributes (should have both attributes)
        assert_eq!(span.attributes.len(), 2);

        // Find attributes by key
        let attr1 = span
            .attributes
            .iter()
            .find(|attr| attr.key == "attr1")
            .unwrap();
        let attr2 = span
            .attributes
            .iter()
            .find(|attr| attr.key == "attr2")
            .unwrap();

        // Verify attribute values
        if let Some(AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(val)),
        }) = &attr1.value
        {
            assert_eq!(val, "value1");
        } else {
            panic!("Expected string value for attr1");
        }

        if let Some(AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(val)),
        }) = &attr2.value
        {
            assert_eq!(*val, 42);
        } else {
            panic!("Expected int value for attr2");
        }

        // Verify events
        assert_eq!(span.events.len(), 2);

        let event1 = &span.events[0];
        assert_eq!(event1.name, "span-event");
        assert_eq!(event1.time_unix_nano, 1500000000);
        assert_eq!(event1.attributes.len(), 1);
        assert_eq!(event1.attributes[0].key, "event_key");
        if let Some(AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(val)),
        }) = &event1.attributes[0].value
        {
            assert_eq!(val, "event_val");
        } else {
            panic!("Expected string value for event attribute");
        }

        let event2 = &span.events[1];
        assert_eq!(event2.name, "second-event");
        assert_eq!(event2.time_unix_nano, 1600000000);
        assert_eq!(event2.attributes.len(), 0);

        // Verify links
        assert_eq!(span.links.len(), 1);

        let link = &span.links[0];
        assert_eq!(
            hex::encode(&link.trace_id),
            "abcdef0123456789abcdef0123456789"
        );
        assert_eq!(hex::encode(&link.span_id), "abcdef0123456789");
        assert_eq!(link.attributes.len(), 1);
        assert_eq!(link.attributes[0].key, "link_key");
        if let Some(AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(val)),
        }) = &link.attributes[0].value
        {
            assert_eq!(*val, 123);
        } else {
            panic!("Expected int value for link attribute");
        }
    }
}
