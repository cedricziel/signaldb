use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Fields};
use common::{
    model::span::{Span, SpanBatch, SpanKind, SpanStatus},
    queue::{memory::InMemoryQueue, Message, MessageType, Queue},
};
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue},
};
use serde_json::{Map, Value as JsonValue};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct TraceHandler {
    queue: Arc<Mutex<InMemoryQueue>>,
}

#[cfg(test)]
pub struct MockTraceHandler {
    pub handle_grpc_otlp_traces_calls: std::sync::Mutex<Vec<ExportTraceServiceRequest>>,
}

#[cfg(test)]
impl MockTraceHandler {
    pub fn new() -> Self {
        Self {
            handle_grpc_otlp_traces_calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        self.handle_grpc_otlp_traces_calls
            .lock()
            .unwrap()
            .push(request);
    }

    pub fn expect_handle_grpc_otlp_traces(&mut self) -> &mut Self {
        self
    }
}

impl TraceHandler {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(InMemoryQueue::default())),
        }
    }

    fn extract_value(&self, attr_val: &Option<&AnyValue>) -> JsonValue {
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
                            vals.push(self.extract_value(&Some(item)));
                        }
                        JsonValue::Array(vals)
                    }
                    Value::KvlistValue(key_value_list) => {
                        let mut vals = Map::new();
                        for item in key_value_list.values.iter() {
                            vals.insert(item.key.clone(), self.extract_value(&item.value.as_ref()));
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

    #[allow(dead_code)]
    fn extract_type(&self, value: JsonValue) -> DataType {
        match value {
            JsonValue::Null => DataType::Null,
            JsonValue::Bool(_) => DataType::Boolean,
            JsonValue::Number(_) => DataType::Int64,
            JsonValue::String(_) => DataType::Utf8,
            JsonValue::Array(_) => DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            JsonValue::Object(_) => DataType::Struct(Fields::from(Vec::<Field>::new())),
        }
    }

    pub async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        log::info!("Got a request: {:?}", request);

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
                    let val = self.extract_value(&attr.value.as_ref());

                    log::info!("Resource attribute: {} = {:?}", key, val);

                    if key == "service.name" {
                        service_name = val.clone().to_string();
                    }

                    resource_attributes.insert(key, val);
                }

                for span in resource_span.scope_spans {
                    for span in span.spans {
                        log::info!("Span: {:?}", span);

                        let mut span_attributes = HashMap::new();

                        for attr in span.attributes {
                            let key = attr.key;
                            let val = self.extract_value(&attr.value.as_ref());

                            log::info!("Span attribute: {} = {:?}", key, val);

                            span_attributes.insert(key, val);
                        }

                        for event in span.events {
                            log::info!("Event: {:?}", event);

                            for attr in event.attributes {
                                let key = attr.key;
                                let val = self.extract_value(&attr.value.as_ref());

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
                            name: span.name.clone(),
                            service_name: service_name.clone(),
                            span_kind: SpanKind::Internal,
                            start_time_unix_nano: span.start_time_unix_nano,
                            duration_nano: span.end_time_unix_nano - span.start_time_unix_nano,
                            attributes: span_attributes,
                            resource: resource_attributes.clone(),
                            children: vec![],
                        };

                        spans.push(span.clone());
                        span_batch.add_span(span.clone());
                    }
                }
            }
        }

        let _record_batch = span_batch.to_record_batch();

        // Convert record batch to JSON for queue
        let json_batch = serde_json::to_value(&spans).unwrap();

        // Send batch to queue
        let message = Message {
            message_type: MessageType::Signal,
            subtype: "spans".to_string(),
            payload: json_batch,
            metadata: Default::default(),
            timestamp: std::time::SystemTime::now(),
        };

        self.queue
            .lock()
            .await
            .publish(message)
            .await
            .expect("Failed to publish message to queue");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::{any_value::Value, AnyValue, KeyValue},
        resource::v1::Resource,
        trace::v1::{span::SpanKind, ResourceSpans, ScopeSpans, Span as OtlpSpan, Status as OtlpSpanStatus},
    };

    #[test]
    fn test_extract_value() {
        let handler = TraceHandler::new();
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

        assert_eq!(handler.extract_value(&bool_val), JsonValue::Bool(true));
        assert_eq!(
            handler.extract_value(&int_val),
            JsonValue::Number(serde_json::Number::from(42))
        );
        assert_eq!(
            handler.extract_value(&double_val),
            JsonValue::Number(serde_json::Number::from_f64(42.0).unwrap())
        );
        assert_eq!(
            handler.extract_value(&string_val),
            JsonValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_extract_type() {
        let handler = TraceHandler::new();
        assert_eq!(handler.extract_type(JsonValue::Null), DataType::Null);
        assert_eq!(handler.extract_type(JsonValue::Bool(true)), DataType::Boolean);
        assert_eq!(
            handler.extract_type(JsonValue::Number(serde_json::Number::from(42))),
            DataType::Int64
        );
        assert_eq!(
            handler.extract_type(JsonValue::String("hello".to_string())),
            DataType::Utf8
        );
    }

    #[tokio::test]
    async fn test_handle_grpc_otlp_traces() {
        let handler = MockTraceHandler::new();

        // Create a test request
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
                    spans: vec![OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        trace_state: String::new(),
                        parent_span_id: vec![0; 8],
                        name: "test-span".to_string(),
                        kind: SpanKind::Internal as i32,
                        start_time_unix_nano: 1703163191000000000,
                        end_time_unix_nano: 1703163192000000000,
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
                        status: Some(OtlpSpanStatus {
                            code: opentelemetry_proto::tonic::trace::v1::status::StatusCode::Ok as i32,
                            message: String::new(),
                        }),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        handler.handle_grpc_otlp_traces(request).await;
        assert_eq!(handler.handle_grpc_otlp_traces_calls.lock().unwrap().len(), 1);
    }
}
