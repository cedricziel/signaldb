//! # Prometheus API Types
//!
//! Response types for the Prometheus-compatible HTTP API, mirroring the
//! wire format Grafana's Prometheus datasource consumes.
//!
//! Query responses wrap a [`QueryData`] whose `resultType` discriminates
//! between `matrix` (range vectors — a series of samples per label set),
//! `vector` (instant vectors — one sample per label set), `scalar`, and
//! `string`. Metadata endpoints (`/labels`, `/label/{name}/values`,
//! `/series`) share the flat `{status, data}` envelope. Errors use the
//! `{status: "error", errorType, error}` envelope.

use std::collections::HashMap;

use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Envelope for `/api/v1/query` and `/api/v1/query_range`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    /// `"success"` or `"error"`.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<QueryData>,
    /// Prometheus error category, e.g. `"bad_data"` (error responses).
    #[serde(rename = "errorType", default, skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    /// Human-readable error message (error responses).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl QueryResponse {
    /// Successful response carrying a typed result.
    pub fn success(result: QueryResult) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(QueryData { result }),
            error_type: None,
            error: None,
        }
    }

    /// Error response with a Prometheus error type and message.
    pub fn error(error_type: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error_type: Some(error_type.into()),
            error: Some(error.into()),
        }
    }
}

/// The `data` object of a query response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryData {
    #[serde(flatten)]
    pub result: QueryResult,
}

/// Query result payload, discriminated by the `resultType` field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "resultType", content = "result", rename_all = "lowercase")]
pub enum QueryResult {
    /// Range vectors: a series of samples per label set.
    Matrix(Vec<RangeVector>),
    /// Instant vectors: one sample per label set.
    Vector(Vec<InstantVector>),
    /// A single scalar value at an instant.
    Scalar(Sample),
    /// A string value at an instant.
    String(Sample),
}

/// One series of a `matrix` result.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RangeVector {
    /// Series labels, including `__name__`.
    pub metric: HashMap<String, String>,
    /// Samples ordered by timestamp.
    pub values: Vec<Sample>,
}

/// One entry of a `vector` result.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InstantVector {
    /// Series labels, including `__name__`.
    pub metric: HashMap<String, String>,
    /// The sample at the query's evaluation timestamp.
    pub value: Sample,
}

/// A Prometheus sample: `[<unix epoch seconds>, "<value>"]`.
///
/// The timestamp is a JSON number (integral seconds serialize without a
/// fractional part), the value a decimal string.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Sample {
    /// Unix timestamp in seconds; may carry sub-second precision.
    pub timestamp: f64,
    /// Sample value rendered as a string, e.g. `"137"` or `"NaN"`.
    pub value: String,
}

impl Sample {
    pub fn new(timestamp: f64, value: impl Into<String>) -> Self {
        Self {
            timestamp,
            value: value.into(),
        }
    }
}

impl Serialize for Sample {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(2))?;
        // Prometheus emits whole-second timestamps as integers.
        if self.timestamp.fract() == 0.0
            && self.timestamp >= i64::MIN as f64
            && self.timestamp <= i64::MAX as f64
        {
            seq.serialize_element(&(self.timestamp as i64))?;
        } else {
            seq.serialize_element(&self.timestamp)?;
        }
        seq.serialize_element(&self.value)?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Sample {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SampleVisitor;

        impl<'de> Visitor<'de> for SampleVisitor {
            type Value = Sample;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a [timestamp, value] pair")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Sample, A::Error> {
                let timestamp: f64 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let value: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(Sample { timestamp, value })
            }
        }

        deserializer.deserialize_seq(SampleVisitor)
    }
}

/// Response of `/api/v1/labels` and `/api/v1/label/{name}/values`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelsResponse {
    pub status: String,
    #[serde(default)]
    pub data: Vec<String>,
}

impl LabelsResponse {
    pub fn success(data: Vec<String>) -> Self {
        Self {
            status: "success".to_string(),
            data,
        }
    }
}

/// Response of `/api/v1/series`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SeriesResponse {
    pub status: String,
    #[serde(default)]
    pub data: Vec<HashMap<String, String>>,
}

impl SeriesResponse {
    pub fn success(data: Vec<HashMap<String, String>>) -> Self {
        Self {
            status: "success".to_string(),
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metric(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn matrix_response_matches_prometheus_wire_format() {
        let response = QueryResponse::success(QueryResult::Matrix(vec![RangeVector {
            metric: metric(&[("__name__", "up"), ("job", "api")]),
            values: vec![
                Sample::new(1_700_000_000.0, "1"),
                Sample::new(1_700_000_015.0, "1"),
            ],
        }]));

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [{
                        "metric": {"__name__": "up", "job": "api"},
                        "values": [[1_700_000_000i64, "1"], [1_700_000_015i64, "1"]]
                    }]
                }
            })
        );
    }

    #[test]
    fn vector_response_matches_prometheus_wire_format() {
        let response = QueryResponse::success(QueryResult::Vector(vec![InstantVector {
            metric: metric(&[("__name__", "up")]),
            value: Sample::new(1_700_000_000.0, "1"),
        }]));
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"metric": {"__name__": "up"}, "value": [1_700_000_000i64, "1"]}]
                }
            })
        );
    }

    #[test]
    fn scalar_response_shape() {
        let response =
            QueryResponse::success(QueryResult::Scalar(Sample::new(1_700_000_000.0, "42")));
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "status": "success",
                "data": {"resultType": "scalar", "result": [1_700_000_000i64, "42"]}
            })
        );
    }

    #[test]
    fn error_response_shape() {
        let response = QueryResponse::error("bad_data", "invalid parameter 'query'");
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "status": "error",
                "errorType": "bad_data",
                "error": "invalid parameter 'query'"
            })
        );
    }

    #[test]
    fn sample_preserves_subsecond_precision() {
        assert_eq!(
            serde_json::to_value(Sample::new(1_700_000_000.5, "3")).unwrap(),
            json!([1_700_000_000.5, "3"])
        );
    }

    #[test]
    fn query_response_round_trips() {
        let response = QueryResponse::success(QueryResult::Matrix(vec![RangeVector {
            metric: metric(&[("__name__", "requests")]),
            values: vec![Sample::new(1_700_000_000.0, "42")],
        }]));
        let parsed: QueryResponse =
            serde_json::from_str(&serde_json::to_string(&response).unwrap()).unwrap();
        assert_eq!(parsed, response);
    }

    #[test]
    fn labels_and_series_shapes() {
        assert_eq!(
            serde_json::to_value(LabelsResponse::success(vec![
                "__name__".to_string(),
                "job".to_string()
            ]))
            .unwrap(),
            json!({"status": "success", "data": ["__name__", "job"]})
        );
        assert_eq!(
            serde_json::to_value(SeriesResponse::success(vec![metric(&[("__name__", "up")])]))
                .unwrap(),
            json!({"status": "success", "data": [{"__name__": "up"}]})
        );
    }

    #[test]
    fn parses_prometheus_matrix_from_json() {
        let raw = r#"{"status":"success","data":{"resultType":"matrix","result":[
            {"metric":{"__name__":"up","job":"api"},"values":[[1700000000,"1"]]}]}}"#;
        let parsed: QueryResponse = serde_json::from_str(raw).unwrap();
        let QueryResult::Matrix(m) = parsed.data.unwrap().result else {
            panic!("expected matrix");
        };
        assert_eq!(m[0].metric.get("job"), Some(&"api".to_string()));
        assert_eq!(m[0].values[0], Sample::new(1_700_000_000.0, "1"));
    }
}
