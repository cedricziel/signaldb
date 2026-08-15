use base64::Engine as _;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value},
    resource::v1::Resource,
};
use serde_json::{Map, Value as JsonValue};

/// Extract AnyValue to its JSON wire carrier.
pub fn extract_value(
    attr_val: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
) -> JsonValue {
    extract_value_with_string_table(attr_val, None)
}

/// Extract an AnyValue using a Profiles dictionary when indexed strings are valid.
pub fn extract_value_with_string_table(
    attr_val: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
    string_table: Option<&[String]>,
) -> JsonValue {
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
                        vals.push(extract_value_with_string_table(
                            &Some(item.clone()),
                            string_table,
                        ));
                    }
                    JsonValue::Array(vals)
                }
                Value::KvlistValue(key_value_list) => {
                    let mut vals = Map::new();
                    for item in key_value_list.values.iter() {
                        vals.insert(
                            item.key.clone(),
                            extract_value_with_string_table(
                                &item.value.as_ref().cloned(),
                                string_table,
                            ),
                        );
                    }
                    JsonValue::Object(vals)
                }
                Value::BytesValue(bytes) => serde_json::json!({
                    "$otlp_type": "bytes",
                    "base64": base64::engine::general_purpose::STANDARD.encode(bytes),
                }),
                Value::StringValueStrindex(index) => string_table
                    .and_then(|table| usize::try_from(*index).ok().and_then(|i| table.get(i)))
                    .cloned()
                    .map(JsonValue::String)
                    .unwrap_or(JsonValue::Null),
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
/// `service_name` used when a resource carries no `service.name`. OTLP allows
/// such resources (an OTel Collector hostmetrics pipeline without a resource
/// processor is the classic producer) even though semconv requires the
/// attribute; every signal stores the same fallback so `service_name` stays
/// non-nullable and such data groups together instead of being rejected.
pub const UNKNOWN_SERVICE_NAME: &str = "unknown";

pub fn extract_service_name(resource: &Option<Resource>) -> String {
    if let Some(resource) = resource {
        for attr in &resource.attributes {
            if attr.key == "service.name"
                && let Some(val) = &attr.value
                && let Some(Value::StringValue(s)) = &val.value
            {
                return s.clone();
            }
        }
    }
    UNKNOWN_SERVICE_NAME.to_string()
}

/// Extract scope attributes as JSON string
pub fn extract_scope_json(
    scope: &Option<opentelemetry_proto::tonic::common::v1::InstrumentationScope>,
) -> String {
    if let Some(scope) = scope {
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
    }
}

/// Convert JsonValue to AnyValue
/// This is the inverse of extract_value
pub fn json_value_to_any_value(json_val: &JsonValue) -> AnyValue {
    match json_val {
        JsonValue::Null => AnyValue { value: None },
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
        }
        JsonValue::String(s) => AnyValue {
            value: Some(Value::StringValue(s.clone())),
        },
        JsonValue::Array(arr) => {
            let values = arr.iter().map(json_value_to_any_value).collect();

            AnyValue {
                value: Some(Value::ArrayValue(ArrayValue { values })),
            }
        }
        JsonValue::Object(obj)
            if obj.get("$otlp_type") == Some(&JsonValue::String("bytes".to_string())) =>
        {
            obj.get("base64")
                .and_then(JsonValue::as_str)
                .and_then(|encoded| {
                    base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .ok()
                })
                .map(|bytes| AnyValue {
                    value: Some(Value::BytesValue(bytes)),
                })
                .unwrap_or(AnyValue { value: None })
        }
        JsonValue::Object(obj) => {
            let values = obj
                .iter()
                .map(|(k, v)| KeyValue {
                    key_strindex: 0,
                    key: k.clone(),
                    value: Some(json_value_to_any_value(v)),
                })
                .collect();

            AnyValue {
                value: Some(Value::KvlistValue(KeyValueList { values })),
            }
        }
    }
}

/// Encode an `f64` for the JSON-carried v1 wire format. JSON has no NaN or
/// infinities, so those are spelled the way Prometheus's own exposition
/// formats do (`"NaN"`, `"+Inf"`, `"-Inf"`); finite values stay numbers.
/// Never `null`: NaN (Prometheus staleness marker, `0/0` rates) and ±Inf are
/// legitimate metric values, and collapsing them to `null` used to make the
/// writer reject the whole batch — permanently — against its non-nullable
/// value columns (#1061). Reversed by [`json_to_f64`].
pub fn f64_to_json(v: f64) -> JsonValue {
    match serde_json::Number::from_f64(v) {
        Some(num) => JsonValue::Number(num),
        None if v.is_nan() => JsonValue::String("NaN".to_string()),
        None if v.is_sign_positive() => JsonValue::String("+Inf".to_string()),
        None => JsonValue::String("-Inf".to_string()),
    }
}

/// Decode a value written by [`f64_to_json`] (or any JSON number). Accepts the
/// Prometheus/Go spellings of the non-finite values case-insensitively;
/// anything else (including `null`) is `None`.
pub fn json_to_f64(v: &JsonValue) -> Option<f64> {
    match v {
        JsonValue::Number(n) => n
            .as_f64()
            .or_else(|| n.as_i64().map(|i| i as f64))
            .or_else(|| n.as_u64().map(|u| u as f64)),
        JsonValue::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "nan" => Some(f64::NAN),
            "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
            "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_bytes_in_a_distinguishable_json_carrier() {
        let value = Some(AnyValue {
            value: Some(Value::BytesValue(vec![0, 255, b'x'])),
        });

        let extracted = extract_value(&value);

        assert_eq!(extracted["$otlp_type"], "bytes");
        assert_eq!(
            json_value_to_any_value(&extracted).value,
            Some(Value::BytesValue(vec![0, 255, b'x']))
        );
    }

    #[test]
    fn f64_json_codec_round_trips_non_finite_values() {
        for v in [0.0, -1.5, 1e300, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(json_to_f64(&f64_to_json(v)), Some(v));
        }
        assert!(json_to_f64(&f64_to_json(f64::NAN)).unwrap().is_nan());
        assert_eq!(f64_to_json(f64::NAN), JsonValue::String("NaN".into()));
        assert_eq!(f64_to_json(f64::INFINITY), JsonValue::String("+Inf".into()));
        assert_eq!(
            f64_to_json(f64::NEG_INFINITY),
            JsonValue::String("-Inf".into())
        );
        assert_eq!(
            json_to_f64(&JsonValue::String("inf".into())),
            Some(f64::INFINITY)
        );
        assert_eq!(json_to_f64(&JsonValue::Null), None);
        assert_eq!(json_to_f64(&JsonValue::String("nope".into())), None);
    }
}
