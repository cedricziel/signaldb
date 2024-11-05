use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Fields};
use common::{
    dataset::{self, DataSet, DataSetType, DataStore},
    model::span::{Span, SpanBatch, SpanKind, SpanStatus},
};
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue},
};

use serde_json::{Map, Value as JsonValue};

use crate::get_parquet_writer;

#[tracing::instrument]
pub async fn handle_grpc_otlp_traces(request: ExportTraceServiceRequest) {
    log::info!("Got a request: {:?}", request);

    let ds = DataSet::new(DataSetType::Traces, DataStore::Disk);

    let resource_spans = request.resource_spans;

    let mut spans = vec![];

    let mut span_batch = SpanBatch::new();

    for resource_span in resource_spans {
        if let Some(resource) = resource_span.resource {
            log::info!("Resource: {:?}", resource);

            let mut resource_attributes = HashMap::new();
            let mut service_name = String::from("unknown");

            for attr in resource.attributes {
                let key = attr.key;
                let val = extract_value(&attr.value.as_ref());

                log::info!("Resource attribute: {} = {:?}", key, val);

                if key == "service.name" {
                    service_name = val.clone().to_string();
                }

                // Field::new(key, extract_type(val), true)
                resource_attributes.insert(key, val);
            }

            for span in resource_span.scope_spans {
                for span in span.spans {
                    log::info!("Span: {:?}", span);

                    let mut span_attributes = HashMap::new();

                    for attr in span.attributes {
                        let key = attr.key;
                        let val = extract_value(&attr.value.as_ref());

                        log::info!("Span attribute: {} = {:?}", key, val);

                        // Field::new(key, extract_type(val), true)
                        span_attributes.insert(key, val);
                    }

                    for event in span.events {
                        log::info!("Event: {:?}", event);

                        for attr in event.attributes {
                            let key = attr.key;
                            let val = extract_value(&attr.value.as_ref());

                            log::info!("Event attribute: {} = {:?}", key, val);
                        }
                    }

                    let trace_id =
                        TraceId::from_bytes(span.trace_id.try_into().unwrap()).to_string();
                    let span_id = SpanId::from_bytes(span.span_id.try_into().unwrap()).to_string();
                    let parent_span_id =
                        SpanId::from_bytes(span.parent_span_id.try_into().unwrap_or([0; 8]))
                            .to_string();

                    let span_status = match span.status {
                        Some(status) => match status.code {
                            0 => SpanStatus::Unspecified,
                            1 => SpanStatus::Error,
                            2 => SpanStatus::Ok,
                            _ => SpanStatus::Unspecified,
                        },
                        None => SpanStatus::Unspecified,
                    };

                    let span = Span {
                        trace_id: trace_id.clone(),
                        span_id: span_id.clone(),
                        parent_span_id: parent_span_id.clone(),
                        status: span_status,
                        is_root: parent_span_id == "0000000000000000".to_string(),
                        name: span.name,
                        service_name: service_name.clone(),
                        span_kind: SpanKind::Internal,
                        attributes: span_attributes,
                        resource: resource_attributes.clone(),
                    };

                    spans.push(span.clone());
                    span_batch.add_span(span.clone());
                }
            }
        }
    }

    let record_batch = span_batch.to_record_batch();
    let mut writer = get_parquet_writer(ds, (*record_batch.schema()).clone()).await;
    writer
        .write(&record_batch)
        .await
        .expect("Cant write to parquet");
    writer.close().await.expect("Cant close parquet writer");
}

/// Rewrites OTLPs `AnyValue` into a `JsonValue`
fn extract_value(attr_val: &Option<&AnyValue>) -> JsonValue {
    match attr_val {
        Some(local_val) => match &local_val.value {
            Some(val) => match val {
                Value::BoolValue(v) => JsonValue::Bool(*v),
                Value::IntValue(v) => JsonValue::Number(serde_json::Number::from(*v)),
                Value::DoubleValue(v) => {
                    JsonValue::Number(serde_json::Number::from_f64(*v).unwrap())
                }
                Value::StringValue(v) => JsonValue::String(v.clone()),
                Value::ArrayValue(array_value) => {
                    let mut vals = vec![];
                    for item in array_value.values.iter() {
                        vals.push(extract_value(&Some(item)));
                    }
                    JsonValue::Array(vals)
                }
                Value::KvlistValue(key_value_list) => {
                    let mut vals = Map::new();
                    for item in key_value_list.values.iter() {
                        vals.insert(item.key.clone(), extract_value(&item.value.as_ref()));
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

fn extract_type(value: JsonValue) -> DataType {
    match value {
        JsonValue::Null => DataType::Null,
        JsonValue::Bool(_) => DataType::Boolean,
        JsonValue::Number(_) => DataType::Int64,
        JsonValue::String(_) => DataType::Utf8,
        JsonValue::Array(_) => DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        JsonValue::Object(_) => DataType::Struct(Fields::from(Vec::<Field>::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

    #[test]
    fn test_extract_value() {
        let bool_val = Some(&AnyValue {
            value: Some(Value::BoolValue(true)),
        });
        let int_val = Some(&AnyValue {
            value: Some(Value::IntValue(42)),
        });
        let double_val = Some(&AnyValue {
            value: Some(Value::DoubleValue(42.0)),
        });

        let binding = AnyValue {
            value: Some(Value::StringValue("hello".to_string())),
        };
        let string_val = Some(&binding);
        let binding = AnyValue {
            value: Some(Value::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::IntValue(42)),
                    }],
                },
            )),
        };
        let array_val = Some(&binding);
        let binding = AnyValue {
            value: Some(Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList {
                    values: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "key".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::IntValue(42)),
                        }),
                    }],
                },
            )),
        };
        let kvlist_val = Some(&binding);
        let binding = AnyValue {
            value: Some(Value::BytesValue(vec![104, 101, 108, 108, 111])),
        };
        let bytes_val = Some(&binding);

        assert_eq!(extract_value(&bool_val), JsonValue::Bool(true));
        assert_eq!(extract_value(&int_val), JsonValue::Number(42.into()));
        assert_eq!(extract_value(&double_val), serde_json::json!(42.0));
        assert_eq!(
            extract_value(&string_val),
            JsonValue::String("hello".to_string())
        );
        assert_eq!(
            extract_value(&array_val),
            JsonValue::Array(vec![JsonValue::Number(42.into())])
        );
        assert_eq!(
            extract_value(&kvlist_val),
            JsonValue::Object(
                [("key".to_string(), JsonValue::Number(42.into()))]
                    .iter()
                    .cloned()
                    .collect()
            )
        );
        assert_eq!(
            extract_value(&bytes_val),
            JsonValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_extract_type() {
        assert_eq!(extract_type(JsonValue::Null), DataType::Null);
        assert_eq!(extract_type(JsonValue::Bool(true)), DataType::Boolean);
        assert_eq!(extract_type(JsonValue::Number(42.into())), DataType::Int64);
        assert_eq!(
            extract_type(JsonValue::String("hello".to_string())),
            DataType::Utf8
        );
        assert_eq!(
            extract_type(JsonValue::Array(vec![JsonValue::String(
                "hello".to_string()
            )])),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
        );
        assert_eq!(
            extract_type(JsonValue::Object(
                [("key".to_string(), JsonValue::String("hello".to_string()))]
                    .iter()
                    .cloned()
                    .collect()
            )),
            DataType::Struct(Fields::from(Vec::<Field>::new()))
        );
    }

    #[tokio::test]
    async fn test_handle_grpc_otlp_traces() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                    attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![opentelemetry_proto::tonic::trace::v1::ScopeSpans {
                    spans: vec![opentelemetry_proto::tonic::trace::v1::Span {
                        trace_id: vec![0; 16],
                        span_id: vec![0; 8],
                        parent_span_id: vec![0; 8],
                        name: "test".to_string(),
                        kind: SpanKind::Internal as i32,
                        start_time_unix_nano: 0,
                        end_time_unix_nano: 0,
                        attributes: vec![],
                        events: vec![],
                        links: vec![],
                        status: None,
                        trace_state: "".to_string(),
                        flags: 0,
                        dropped_attributes_count: 0,
                        dropped_events_count: 0,
                        dropped_links_count: 0,
                    }],
                    scope: Some(InstrumentationScope {
                        name: "test".to_string(),
                        version: "0.1.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "my.schema.url".to_string(),
                }],
                schema_url: String::new(),
            }],
        };

        handle_grpc_otlp_traces(request).await;
    }
}
