use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Fields};
use common::flight::conversion::otlp_traces_to_arrow;
use messaging::{
    messages::{
        batch::BatchWrapper,
        span::{Span, SpanBatch, SpanKind, SpanStatus},
    },
    Message, MessagingBackend,
};
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue},
};
use serde_json::{Map, Value as JsonValue};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct TraceHandler<Q: MessagingBackend> {
    queue: Arc<Mutex<Q>>,
}

#[cfg(test)]
pub struct MockTraceHandler {
    pub handle_grpc_otlp_traces_calls: tokio::sync::Mutex<Vec<ExportTraceServiceRequest>>,
}

#[cfg(test)]
impl MockTraceHandler {
    pub fn new() -> Self {
        Self {
            handle_grpc_otlp_traces_calls: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        self.handle_grpc_otlp_traces_calls
            .lock()
            .await
            .push(request);
    }

    pub fn expect_handle_grpc_otlp_traces(&mut self) -> &mut Self {
        self
    }
}

impl<Q: MessagingBackend> TraceHandler<Q> {
    pub fn new(queue: Arc<Mutex<Q>>) -> Self {
        Self { queue }
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
            JsonValue::Array(_) => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
            }
            JsonValue::Object(_) => DataType::Struct(Fields::from(Vec::<Field>::new())),
        }
    }

    pub async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        log::info!("Got a request: {:?}", request);

        // Convert OTLP to Arrow RecordBatch using the new conversion function
        let record_batch = otlp_traces_to_arrow(&request);

        // Create a batch wrapper from the record batch
        let batch_wrapper = BatchWrapper::from(record_batch);
        let message = Message::Batch(batch_wrapper);

        // Also create a SpanBatch for backward compatibility
        let mut spans = Vec::new();

        for resource_spans in &request.resource_spans {
            let mut resource_attributes = HashMap::new();
            let mut service_name = String::from("unknown");

            if let Some(resource) = &resource_spans.resource {
                for attr in &resource.attributes {
                    let key = attr.key.clone();
                    let val = self.extract_value(&attr.value.as_ref());

                    if key == "service.name" {
                        if let JsonValue::String(s) = val.clone() {
                            service_name = s;
                        }
                    }

                    resource_attributes.insert(key, val);
                }
            }

            for scope_spans in &resource_spans.scope_spans {
                for span in &scope_spans.spans {
                    let mut span_attributes = HashMap::new();

                    for attr in &span.attributes {
                        span_attributes
                            .insert(attr.key.clone(), self.extract_value(&attr.value.as_ref()));
                    }

                    let trace_id =
                        TraceId::from_bytes(span.trace_id.clone().try_into().unwrap()).to_string();
                    let span_id =
                        SpanId::from_bytes(span.span_id.clone().try_into().unwrap()).to_string();
                    let parent_span_id = if span.parent_span_id.is_empty() {
                        "0000000000000000".to_string()
                    } else {
                        SpanId::from_bytes(span.parent_span_id.clone().try_into().unwrap())
                            .to_string()
                    };

                    let span_status = match &span.status {
                        Some(status) => match status.code {
                            0 => SpanStatus::Unspecified,
                            1 => SpanStatus::Error,
                            2 => SpanStatus::Ok,
                            _ => SpanStatus::Unspecified,
                        },
                        None => SpanStatus::Unspecified,
                    };

                    spans.push(Span {
                        trace_id,
                        span_id,
                        parent_span_id: parent_span_id.clone(),
                        status: span_status,
                        is_root: parent_span_id == "0000000000000000".to_string(),
                        name: span.name.clone(),
                        service_name: service_name.clone(),
                        span_kind: match span.kind {
                            0 => SpanKind::Internal,
                            1 => SpanKind::Server,
                            2 => SpanKind::Client,
                            3 => SpanKind::Producer,
                            4 => SpanKind::Consumer,
                            _ => SpanKind::Internal,
                        },
                        start_time_unix_nano: span.start_time_unix_nano,
                        duration_nano: span.end_time_unix_nano - span.start_time_unix_nano,
                        attributes: span_attributes,
                        resource: resource_attributes.clone(),
                        children: vec![],
                    });
                }
            }
        }

        let span_batch = SpanBatch::new_with_spans(spans);
        let span_message = Message::SpanBatch(span_batch);

        let queue = self.queue.lock().await;

        // Send both messages
        let _ = queue
            .send_message("arrow-traces", message)
            .await
            .map_err(|e| {
                log::error!("Failed to publish arrow trace message: {:?}", e);
                e
            });

        let _ = queue
            .send_message("traces", span_message)
            .await
            .map_err(|e| {
                log::error!("Failed to publish trace message: {:?}", e);
                e
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use messaging::backend::memory::InMemoryStreamingBackend;

    #[test]
    fn test_extract_value() {
        let queue = Arc::new(Mutex::new(InMemoryStreamingBackend::new(10)));
        let handler = TraceHandler::new(queue);

        // Test boolean value
        let bool_value = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(
            handler.extract_value(&Some(&bool_value)),
            JsonValue::Bool(true)
        );

        // Test integer value
        let int_value = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(
            handler.extract_value(&Some(&int_value)),
            JsonValue::Number(serde_json::Number::from(42))
        );

        // Test string value
        let string_value = AnyValue {
            value: Some(Value::StringValue("test".to_string())),
        };
        assert_eq!(
            handler.extract_value(&Some(&string_value)),
            JsonValue::String("test".to_string())
        );
    }

    #[test]
    fn test_extract_type() {
        let queue = Arc::new(Mutex::new(InMemoryStreamingBackend::new(10)));
        let handler = TraceHandler::new(queue);

        assert_eq!(handler.extract_type(JsonValue::Null), DataType::Null);
        assert_eq!(
            handler.extract_type(JsonValue::Bool(true)),
            DataType::Boolean
        );
        assert_eq!(
            handler.extract_type(JsonValue::Number(serde_json::Number::from(42))),
            DataType::Int64
        );
        assert_eq!(
            handler.extract_type(JsonValue::String("test".to_string())),
            DataType::Utf8
        );
    }

    #[tokio::test]
    async fn test_handle_grpc_otlp_traces() {
        // Create a mock queue that we can inspect
        let queue = Arc::new(Mutex::new(InMemoryStreamingBackend::new(10)));
        let handler = TraceHandler::new(queue.clone());

        // Create a test request
        let request = ExportTraceServiceRequest {
            resource_spans: vec![opentelemetry_proto::tonic::trace::v1::ResourceSpans {
                resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![opentelemetry_proto::tonic::trace::v1::ScopeSpans {
                    scope: None,
                    spans: vec![opentelemetry_proto::tonic::trace::v1::Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        trace_state: String::new(),
                        parent_span_id: vec![],
                        name: "test".to_string(),
                        kind: 1,
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: None,
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        // Process the request
        handler.handle_grpc_otlp_traces(request).await;

        // Verify that both messages were sent
        // Note: In a real test, we would use a mock queue that allows us to inspect the messages
        // For now, we just verify that the function doesn't panic
    }
}
