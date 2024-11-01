use common::dataset::{DataSet, DataSetType, DataStore};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue},
};

use serde_json::{Map, Value as JsonValue};

#[tracing::instrument]
pub fn handle_grpc_otlp_traces(request: ExportTraceServiceRequest) {
    println!("Got a request: {:?}", request);

    let _ds = DataSet::new(DataSetType::Traces, DataStore::InMemory);

    let spans = request.resource_spans;

    for resource_spans in spans {
        if let Some(resource) = resource_spans.resource {
            log::info!("Resource: {:?}", resource);

            for attr in resource.attributes {
                let key = attr.key;
                let val = extract_value(&attr.value.as_ref());

                log::info!("Resource attribute: {} = {:?}", key, val);
            }

            for span in resource_spans.scope_spans {
                for span in span.spans {
                    log::info!("Span: {:?}", span);

                    for attr in span.attributes {
                        let key = attr.key;
                        let val = extract_value(&attr.value.as_ref());

                        log::info!("Span attribute: {} = {:?}", key, val);
                    }

                    for event in span.events {
                        log::info!("Event: {:?}", event);

                        for attr in event.attributes {
                            let key = attr.key;
                            let val = extract_value(&attr.value.as_ref());

                            log::info!("Event attribute: {} = {:?}", key, val);
                        }
                    }
                }
            }
        }
    }
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

mod test {
    use super::*;

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
}
