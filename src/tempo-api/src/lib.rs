use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Result of GET /api/search
/// See https://grafana.com/docs/tempo/latest/api_docs/#example-of-traceql-search
pub struct SearchResult {
    pub traces: Vec<Trace>,
    pub metrics: HashMap<String, u16>,
}

/// A trace is a collection of spans that represent a single request
///
/// Example:
/// {
///   "traceID": "2f3e0cee77ae5dc9c17ade3689eb2e54",
///   "rootServiceName": "shop-backend",
///   "rootTraceName": "update-billing",
///   "startTimeUnixNano": "1684778327699392724",
///   "durationMs": 557,
///   "spanSets": [
///     {
///       "spans": [
///         {
///           "spanID": "563d623c76514f8e",
///           "startTimeUnixNano": "1684778327735077898",
///           "durationNanos": "446979497",
///           "attributes": [
///             {
///               "key": "status",
///               "value": {
///                 "stringValue": "error"
///               }
///             }
///           ]
///         }
///       ],
///       "matched": 1
///     }
///   ]
#[derive(Serialize, Deserialize)]
pub struct Trace {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(rename = "rootServiceName")]
    pub root_service_name: String,
    #[serde(rename = "rootTraceName")]
    pub root_trace_name: String,
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    #[serde(rename = "durationMs")]
    pub duration_ms: u64,
    #[serde(rename = "spanSets")]
    pub span_sets: Vec<SpanSet>,
}

#[derive(Serialize, Deserialize)]
pub struct SpanSet {
    pub spans: Vec<Span>,
    pub matched: u16,
}

#[derive(Serialize, Deserialize)]
pub struct Span {
    #[serde(rename = "spanID")]
    pub span_id: String,
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    #[serde(rename = "durationNanos")]
    pub duration_nanos: String,
    pub attributes: HashMap<String, Attribute>,
}

#[derive(Serialize, Deserialize)]
pub struct Attribute {
    pub key: String,
    pub value: Value,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Value {
    #[serde(rename = "stringValue")]
    StringValue(String),
    #[serde(rename = "intValue")]
    IntValue(i64),
    #[serde(rename = "boolValue")]
    BoolValue(bool),
    #[serde(rename = "doubleValue")]
    DoubleValue(f64),
}

mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_search_result() {
        let traces = vec![Trace {
            trace_id: "2f3e0cee77ae5dc9c17ade3689eb2e54".to_string(),
            root_service_name: "shop-backend".to_string(),
            root_trace_name: "update-billing".to_string(),
            start_time_unix_nano: "1684778327699392724".to_string(),
            duration_ms: 557,
            span_sets: vec![SpanSet {
                spans: vec![Span {
                    span_id: "563d623c76514f8e".to_string(),
                    start_time_unix_nano: "1684778327735077898".to_string(),
                    duration_nanos: "446979497".to_string(),
                    attributes: vec![Attribute {
                        key: "status".to_string(),
                        value: Value::StringValue("error".to_string()),
                    }]
                    .into_iter()
                    .map(|attr| (attr.key.clone(), attr))
                    .collect(),
                }],
                matched: 1,
            }],
        }];

        let metrics = vec![("error".to_string(), 1)].into_iter().collect();

        let search_result = SearchResult { traces, metrics };

        assert_eq!(search_result.traces.len(), 1);
        assert_eq!(search_result.metrics.len(), 1);
    }

    #[test]
    fn test_trace() {
        let trace = Trace {
            trace_id: "2f3e0cee77ae5dc9c17ade3689eb2e54".to_string(),
            root_service_name: "shop-backend".to_string(),
            root_trace_name: "update-billing".to_string(),
            start_time_unix_nano: "1684778327699392724".to_string(),
            duration_ms: 557,
            span_sets: vec![SpanSet {
                spans: vec![Span {
                    span_id: "563d623c76514f8e".to_string(),
                    start_time_unix_nano: "1684778327735077898".to_string(),
                    duration_nanos: "446979497".to_string(),
                    attributes: vec![Attribute {
                        key: "status".to_string(),
                        value: Value::StringValue("error".to_string()),
                    }]
                    .into_iter()
                    .map(|attr| (attr.key.clone(), attr))
                    .collect(),
                }],
                matched: 1,
            }],
        };

        assert_eq!(trace.trace_id, "2f3e0cee77ae5dc9c17ade3689eb2e54");
        assert_eq!(trace.root_service_name, "shop-backend");
        assert_eq!(trace.root_trace_name, "update-billing");
        assert_eq!(trace.start_time_unix_nano, "1684778327699392724");
        assert_eq!(trace.duration_ms, 557);
        assert_eq!(trace.span_sets.len(), 1);
    }

    #[test]
    fn test_span_set() {
        let span_set = SpanSet {
            spans: vec![Span {
                span_id: "563d623c76514f8e".to_string(),
                start_time_unix_nano: "1684778327735077898".to_string(),
                duration_nanos: "446979497".to_string(),
                attributes: vec![Attribute {
                    key: "status".to_string(),
                    value: Value::StringValue("error".to_string()),
                }]
                .into_iter()
                .map(|attr| (attr.key.clone(), attr))
                .collect(),
            }],
            matched: 1,
        };

        assert_eq!(span_set.spans.len(), 1);
        assert_eq!(span_set.matched, 1);
    }

    #[test]
    fn test_span() {
        let span = Span {
            span_id: "563d623c76514f8e".to_string(),
            start_time_unix_nano: "1684778327735077898".to_string(),
            duration_nanos: "446979497".to_string(),
            attributes: vec![Attribute {
                key: "status".to_string(),
                value: Value::StringValue("error".to_string()),
            }]
            .into_iter()
            .map(|attr| (attr.key.clone(), attr))
            .collect(),
        };

        assert_eq!(span.span_id, "563d623c76514f8e");
        assert_eq!(span.start_time_unix_nano, "1684778327735077898");
        assert_eq!(span.duration_nanos, "446979497");
        assert_eq!(span.attributes.len(), 1);
    }

    #[test]
    fn test_attribute() {
        let attribute = Attribute {
            key: "status".to_string(),
            value: Value::StringValue("error".to_string()),
        };

        assert_eq!(attribute.key, "status");
        assert_eq!(attribute.value, Value::StringValue("error".to_string()));
    }

    #[test]
    fn test_value() {
        let value = Value::StringValue("error".to_string());

        assert_eq!(value, Value::StringValue("error".to_string()));
    }
}