
use opentelemetry_proto::tonic::{
    resource::v1::Resource,
    common::v1::{KeyValue, any_value::Value, AnyValue, ArrayValue, KeyValueList},
};
use serde_json::{Map, Value as JsonValue};

/// Extract AnyValue to JsonValue
pub fn extract_value(attr_val: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>) -> JsonValue {
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

/// Extract resource attributes as JSON string
pub fn extract_resource_json(resource: &Option<Resource>) -> String {
    if let Some(resource) = resource {
        let mut resource_map = Map::new();
        for attr in &resource.attributes {
            resource_map.insert(attr.key.clone(), extract_value(&attr.value));
        }
        serde_json::to_string(&resource_map).unwrap_or_else(|_| "{}".to_string())
    } else {
        "{}".to_string()
    }
}

/// Extract service name from resource attributes
pub fn extract_service_name(resource: &Option<Resource>) -> String {
    if let Some(resource) = resource {
        for attr in &resource.attributes {
            if attr.key == "service.name" {
                if let Some(val) = &attr.value {
                    if let Some(Value::StringValue(s)) = &val.value {
                        return s.clone();
                    }
                }
            }
        }
    }
    "unknown".to_string()
}

/// Extract scope attributes as JSON string
pub fn extract_scope_json(scope: &Option<opentelemetry_proto::tonic::common::v1::InstrumentationScope>) -> String {
    if let Some(scope) = scope {
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
    }
}

/// Convert JsonValue to AnyValue
/// This is the inverse of extract_value
pub fn json_value_to_any_value(json_val: &JsonValue) -> AnyValue {
    match json_val {
        JsonValue::Null => AnyValue {
            value: None,
        },
        JsonValue::Bool(b) => AnyValue {
            value: Some(Value::BoolValue(*b)),
        },
        JsonValue::Number(n) => {
            if n.is_i64() {
                AnyValue {
                    value: Some(Value::IntValue(n.as_i64().unwrap())),
                }
            } else if n.is_u64() {
                // Convert u64 to i64 if possible, otherwise use double
                let u_val = n.as_u64().unwrap();
                if u_val <= i64::MAX as u64 {
                    AnyValue {
                        value: Some(Value::IntValue(u_val as i64)),
                    }
                } else {
                    AnyValue {
                        value: Some(Value::DoubleValue(u_val as f64)),
                    }
                }
            } else {
                AnyValue {
                    value: Some(Value::DoubleValue(n.as_f64().unwrap())),
                }
            }
        },
        JsonValue::String(s) => AnyValue {
            value: Some(Value::StringValue(s.clone())),
        },
        JsonValue::Array(arr) => {
            let values = arr.iter()
                .map(|v| json_value_to_any_value(v))
                .collect();

            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue { values })),
            }
        },
        JsonValue::Object(obj) => {
            let values = obj.iter()
                .map(|(k, v)| KeyValue {
                    key: k.clone(),
                    value: Some(json_value_to_any_value(v)),
                })
                .collect();

            AnyValue {
                value: Some(Value::KvlistValue(KeyValueList { values })),
            }
        },
    }
}
