use std::collections::HashMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[allow(dead_code)]
pub mod tempopb {
    include!("generated/tempopb.rs");

    pub mod common {
        pub mod v1 {
            include!("generated/tempopb.common.v1.rs");
        }
    }

    pub mod resource {
        pub mod v1 {
            include!("generated/tempopb.resource.v1.rs");
        }
    }
    pub mod trace {
        pub mod v1 {
            include!("generated/tempopb.trace.v1.rs");
        }
    }
}

pub mod v2;

#[derive(Deserialize, Debug)]
pub struct TraceQueryParams {
    #[allow(dead_code)]
    start: Option<String>,
    #[allow(dead_code)]
    end: Option<String>,
}

/// Parameters for TraceQL metrics queries
#[derive(Deserialize, Debug)]
pub struct MetricsQueryParams {
    /// TraceQL query with metrics function (e.g., "{service.name='api'}|count()")
    pub q: String,
    /// Start time (unix seconds)
    pub start: Option<i64>,
    /// End time (unix seconds)
    pub end: Option<i64>,
    /// Duration to look back from now (e.g., "1h")
    pub since: Option<String>,
}

/// Parameters for range metrics queries (includes step for time series)
#[derive(Deserialize, Debug)]
pub struct MetricsRangeQueryParams {
    /// TraceQL query with metrics function
    pub q: String,
    /// Start time (unix seconds)
    pub start: Option<i64>,
    /// End time (unix seconds)  
    pub end: Option<i64>,
    /// Duration to look back from now (e.g., "1h")
    pub since: Option<String>,
    /// Time series granularity in seconds (e.g., 60 for 1-minute buckets)
    pub step: Option<i64>,
    /// Maximum number of exemplar traces per series
    pub exemplars: Option<i32>,
}

/// Prometheus-compatible metrics response
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct MetricsResponse {
    pub status: String,
    pub data: MetricsData,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct MetricsData {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: Vec<MetricSeries>,
}

/// A single metric time series
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct MetricSeries {
    /// Labels for this series (e.g., {"service.name": "api"})
    pub metric: HashMap<String, String>,
    /// For instant queries: single [timestamp, value] pair
    /// For range queries: array of [timestamp, value] pairs
    pub values: Vec<(i64, String)>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SearchQueryParams {
    pub q: Option<String>,
    pub tags: Option<String>,
    pub min_duration: Option<i32>,
    pub max_duration: Option<i32>,
    pub limit: Option<i32>,
    pub start: Option<i32>,
    pub end: Option<i32>,
    pub spss: Option<i32>,
}

/// Result of GET /api/search
/// See <https://grafana.com/docs/tempo/latest/api_docs/#example-of-traceql-search>
#[derive(Serialize, Deserialize, Debug, PartialEq)]
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
#[derive(Serialize, Deserialize, Debug, PartialEq)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct SpanSet {
    pub spans: Vec<Span>,
    pub matched: u16,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Span {
    #[serde(rename = "spanID")]
    pub span_id: String,
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    #[serde(rename = "durationNanos")]
    pub duration_nanos: String,
    pub attributes: HashMap<String, Attribute>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
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

/// GET /api/search/tags?scope=<resource|span|intrinsic>
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
}

impl TagScope {
    pub fn as_str(&self) -> &str {
        match self {
            TagScope::Resource => "resource",
            TagScope::Span => "span",
            TagScope::Intrinsic => "intrinsic",
        }
    }
}

impl FromStr for TagScope {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "resource" => Ok(TagScope::Resource),
            "span" => Ok(TagScope::Span),
            "intrinsic" => Ok(TagScope::Intrinsic),
            _ => Err(()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct TagSearchResponse {
    #[serde(rename = "tagNames")]
    pub tag_names: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct TagValuesResponse {
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<String>,
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
