use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[allow(dead_code)] // prost generates both directions of every message; only one side is used per binary
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

/// Query parameters for single-trace lookup.
///
/// `start`/`end` are optional unix-second hints bracketing the expected
/// trace, used to prune the scanned time range.
#[derive(Deserialize, Debug, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TraceQueryParams {
    pub start: Option<i64>,
    pub end: Option<i64>,
    /// When true, attach summaries of profiles linked to this trace.
    #[serde(default)]
    pub include_profiles: Option<bool>,
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

#[derive(Serialize, Deserialize, Debug, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
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
#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct SearchResult {
    pub traces: Vec<Trace>,
    pub metrics: HashMap<String, u16>,
}

/// A trace is a collection of spans that represent a single request
///
/// Example:
/// ```json
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
///           "attributes": {
///             "status": {
///               "key": "status",
///               "value": {
///                 "stringValue": "error"
///               }
///             }
///           }
///         }
///       ],
///       "matched": 1
///     }
///   ]
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
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
    /// Summaries of profiles linked to this trace; present only when the
    /// client asked for them via `include_profiles`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profiles: Option<Vec<ProfileSummary>>,
}

/// Summary of a stored profile linked to a trace, without the bulky
/// stack/sample payloads.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, utoipa::ToSchema)]
pub struct ProfileSummary {
    #[serde(rename = "profileID")]
    pub profile_id: String,
    #[serde(rename = "timeUnixNano")]
    pub time_unix_nano: String,
    #[serde(rename = "durationNano")]
    pub duration_nano: String,
    #[serde(rename = "sampleType")]
    pub sample_type: String,
    #[serde(rename = "sampleUnit")]
    pub sample_unit: String,
    #[serde(rename = "serviceName")]
    pub service_name: String,
    #[serde(rename = "spanID", default, skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct SpanSet {
    pub spans: Vec<Span>,
    pub matched: u16,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct Span {
    #[serde(rename = "spanID")]
    pub span_id: String,
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    #[serde(rename = "durationNanos")]
    pub duration_nanos: String,
    /// Span name intrinsic (Tempo exposes it as `name` on spanset spans).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Parent span id; empty/absent for root spans. Needed by clients that
    /// reconstruct the span hierarchy (e.g. waterfall views).
    #[serde(
        rename = "parentSpanID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_span_id: Option<String>,
    #[serde(
        rename = "serviceName",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub service_name: Option<String>,
    /// Span status (`ok`, `error`, `unset`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub attributes: HashMap<String, Attribute>,
    /// Span events (annotations, exceptions). Omitted when empty. Exceptions are
    /// the event named `exception`, carrying `exception.message`/`.type`/
    /// `.stacktrace` in their attributes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<SpanEvent>,
}

/// A span event in the Tempo API span shape.
#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct SpanEvent {
    pub name: String,
    #[serde(rename = "timeUnixNano")]
    pub time_unix_nano: String,
    #[serde(default)]
    pub attributes: HashMap<String, Attribute>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct Attribute {
    pub key: String,
    pub value: Value,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
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
///
/// `rename_all = "lowercase"` matters here: the Tempo API (and Grafana's
/// Tempo datasource, which is what actually sends this) uses lowercase
/// scope values. Without it, serde only accepts the Rust variant names
/// (`Resource`/`Span`/`Intrinsic`) and every real client 400s (#1073).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct TagSearchResponse {
    #[serde(rename = "tagNames")]
    pub tag_names: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct TagValuesResponse {
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<String>,
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use serde_json::json;

    #[test]
    fn trace_serializes_to_tempo_wire_format() {
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
                    name: None,
                    parent_span_id: None,
                    service_name: None,
                    status: None,
                    attributes: HashMap::from([(
                        "status".to_string(),
                        Attribute {
                            key: "status".to_string(),
                            value: Value::StringValue("error".to_string()),
                        },
                    )]),
                    events: Vec::new(),
                }],
                matched: 1,
            }],
            profiles: None,
        };

        // Pins the Tempo-compatible field names (traceID, rootServiceName,
        // spanID, durationNanos, ...) and that unset optional fields
        // (name, parentSpanID, serviceName, status, events, profiles) are
        // omitted rather than serialized as null, per the Grafana Tempo
        // datasource contract.
        assert_eq!(
            serde_json::to_value(&trace).unwrap(),
            json!({
                "traceID": "2f3e0cee77ae5dc9c17ade3689eb2e54",
                "rootServiceName": "shop-backend",
                "rootTraceName": "update-billing",
                "startTimeUnixNano": "1684778327699392724",
                "durationMs": 557,
                "spanSets": [{
                    "spans": [{
                        "spanID": "563d623c76514f8e",
                        "startTimeUnixNano": "1684778327735077898",
                        "durationNanos": "446979497",
                        "attributes": {
                            "status": {"key": "status", "value": {"stringValue": "error"}}
                        }
                    }],
                    "matched": 1
                }]
            })
        );
    }

    #[test]
    fn search_result_serializes_to_tempo_wire_format() {
        let search_result = SearchResult {
            traces: vec![],
            metrics: HashMap::from([("inspectedTraces".to_string(), 42u16)]),
        };

        assert_eq!(
            serde_json::to_value(&search_result).unwrap(),
            json!({"traces": [], "metrics": {"inspectedTraces": 42}})
        );
    }

    #[test]
    fn value_variants_serialize_to_tempo_wire_format() {
        // Each Value variant must externally tag as its Tempo attribute
        // kind (stringValue/intValue/boolValue/doubleValue), matching the
        // shape Grafana's Tempo datasource parses.
        assert_eq!(
            serde_json::to_value(Value::StringValue("error".to_string())).unwrap(),
            json!({"stringValue": "error"})
        );
        assert_eq!(
            serde_json::to_value(Value::IntValue(42)).unwrap(),
            json!({"intValue": 42})
        );
        assert_eq!(
            serde_json::to_value(Value::BoolValue(true)).unwrap(),
            json!({"boolValue": true})
        );
        assert_eq!(
            serde_json::to_value(Value::DoubleValue(1.5)).unwrap(),
            json!({"doubleValue": 1.5})
        );
    }

    /// #1073 (part 3): Tempo's `/api/v2/search/tags?scope=…` — and Grafana's
    /// Tempo datasource, which is what actually sends the request — use
    /// lowercase scope values. `rename_all = "lowercase"` is what a
    /// query-string deserializer (e.g. axum's `Query` extractor) consults
    /// for enum variant matching, the same as `serde_json` here.
    #[test]
    fn tag_scope_deserializes_tempo_wire_lowercase_values() {
        assert_eq!(
            serde_json::from_value::<TagScope>(json!("resource")).unwrap(),
            TagScope::Resource
        );
        assert_eq!(
            serde_json::from_value::<TagScope>(json!("span")).unwrap(),
            TagScope::Span
        );
        assert_eq!(
            serde_json::from_value::<TagScope>(json!("intrinsic")).unwrap(),
            TagScope::Intrinsic
        );
    }

    #[test]
    fn tag_scope_rejects_capitalized_rust_variant_names() {
        assert!(serde_json::from_value::<TagScope>(json!("Span")).is_err());
    }
}
