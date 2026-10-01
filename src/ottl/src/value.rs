//! The OTTL value model, mapped onto `opentelemetry_proto::tonic::common::v1::AnyValue`.

use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};

/// A runtime OTTL value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Nil,
}

impl Value {
    /// The value's type name, used in error messages instead of its content —
    /// error text must never interpolate the value itself, since OTTL is often
    /// used to redact sensitive telemetry data.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "String",
            Value::Int(_) => "Int",
            Value::Double(_) => "Double",
            Value::Bool(_) => "Bool",
            Value::Bytes(_) => "Bytes",
            Value::List(_) => "List",
            Value::Nil => "Nil",
        }
    }

    pub fn from_any_value(value: &AnyValue) -> Value {
        match &value.value {
            None => Value::Nil,
            Some(any_value::Value::StringValue(s)) => Value::String(s.clone()),
            Some(any_value::Value::BoolValue(b)) => Value::Bool(*b),
            Some(any_value::Value::IntValue(i)) => Value::Int(*i),
            Some(any_value::Value::DoubleValue(d)) => Value::Double(*d),
            Some(any_value::Value::BytesValue(b)) => Value::Bytes(b.clone()),
            Some(any_value::Value::ArrayValue(arr)) => {
                Value::List(arr.values.iter().map(Value::from_any_value).collect())
            }
            // Key-value lists and string-table references are outside the supported
            // subset (design.md non-goals: nested map/slice paths).
            Some(any_value::Value::KvlistValue(_))
            | Some(any_value::Value::StringValueStrindex(_)) => Value::Nil,
        }
    }

    pub fn into_any_value(self) -> AnyValue {
        let inner = match self {
            Value::String(s) => Some(any_value::Value::StringValue(s)),
            Value::Int(i) => Some(any_value::Value::IntValue(i)),
            Value::Double(d) => Some(any_value::Value::DoubleValue(d)),
            Value::Bool(b) => Some(any_value::Value::BoolValue(b)),
            Value::Bytes(b) => Some(any_value::Value::BytesValue(b)),
            Value::List(items) => Some(any_value::Value::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue {
                    values: items.into_iter().map(Value::into_any_value).collect(),
                },
            )),
            Value::Nil => None,
        };
        AnyValue { value: inner }
    }

    pub fn is_nil(&self) -> bool {
        matches!(self, Value::Nil)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// A rough textual form, used by converters like `String()` and `Concat()`.
    pub fn to_display_string(&self) -> String {
        match self {
            Value::String(s) => s.clone(),
            Value::Int(i) => i.to_string(),
            Value::Double(d) => d.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Bytes(b) => format!("{b:?}"),
            Value::List(items) => {
                let parts: Vec<String> = items.iter().map(Value::to_display_string).collect();
                format!("[{}]", parts.join(", "))
            }
            Value::Nil => String::new(),
        }
    }
}
