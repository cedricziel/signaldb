//! # Loki API Types
//!
//! Response types for the Loki-compatible HTTP API, mirroring the wire
//! format Grafana's Loki datasource consumes.
//!
//! Query responses wrap a [`QueryData`] whose `resultType` discriminates
//! between log `streams` (raw log lines keyed by nanosecond timestamps)
//! and Prometheus-style `matrix`/`vector` results for LogQL metric
//! queries. Metadata endpoints (`/labels`, `/label/{name}/values`,
//! `/series`) share the flat `{status, data}` envelope.

use std::collections::HashMap;

use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Envelope for `/loki/api/v1/query` and `/loki/api/v1/query_range`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    /// `"success"` or `"error"`.
    pub status: String,
    pub data: QueryData,
}

impl QueryResponse {
    /// Successful response carrying the given result payload.
    pub fn success(result: QueryResult) -> Self {
        Self {
            status: "success".to_string(),
            data: QueryData {
                result,
                stats: None,
            },
        }
    }
}

/// The `data` object of a query response: a typed result plus optional
/// execution statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryData {
    #[serde(flatten)]
    pub result: QueryResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<QueryStatistics>,
}

/// Query result payload, discriminated by the `resultType` field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "resultType", content = "result", rename_all = "lowercase")]
pub enum QueryResult {
    /// Log lines grouped by label set (log queries).
    Streams(Vec<Stream>),
    /// Range vectors (metric range queries).
    Matrix(Vec<MetricSeries>),
    /// Instant vectors (metric instant queries).
    Vector(Vec<InstantValue>),
}

/// One log stream: a unique label set and its entries.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Stream {
    /// Stream labels, e.g. `{"service_name": "api"}`.
    pub stream: HashMap<String, String>,
    /// Entries as `[timestamp, line]` pairs, timestamp in nanoseconds
    /// rendered as a decimal string (Loki wire format).
    pub values: Vec<LogEntry>,
}

/// A single log entry: `["<unix epoch ns>", "<log line>"]`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LogEntry(pub String, pub String);

impl LogEntry {
    /// Build an entry from a nanosecond timestamp and a log line.
    pub fn new(timestamp_ns: i64, line: impl Into<String>) -> Self {
        Self(timestamp_ns.to_string(), line.into())
    }
}

/// One series of a `matrix` result.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricSeries {
    /// Series labels.
    pub metric: HashMap<String, String>,
    /// Samples ordered by timestamp.
    pub values: Vec<Sample>,
}

/// One entry of a `vector` result.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InstantValue {
    /// Series labels.
    pub metric: HashMap<String, String>,
    /// The sample at the query's evaluation timestamp.
    pub value: Sample,
}

/// A Prometheus-style sample: `[<unix epoch seconds>, "<value>"]`.
///
/// The timestamp is a JSON number (integral seconds serialize without a
/// fractional part, matching Loki), the value a decimal string.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Sample {
    /// Unix timestamp in seconds; may carry sub-second precision.
    pub timestamp: f64,
    /// Sample value rendered as a string, e.g. `"137"`.
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
        // Loki emits whole-second timestamps as integers; only emit a
        // float when there is actual sub-second precision.
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

/// Response of `/loki/api/v1/labels` and `/loki/api/v1/label/{name}/values`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelsResponse {
    /// `"success"` or `"error"`.
    pub status: String,
    /// Label names (or values), sorted.
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

/// Response of `/loki/api/v1/series`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SeriesResponse {
    /// `"success"` or `"error"`.
    pub status: String,
    /// One label set per matching series.
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

/// Query execution statistics (`data.stats`).
///
/// SignalDB populates the summary block only; Loki's full statistics
/// tree (querier, ingester, cache breakdowns) is not modeled.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryStatistics {
    pub summary: SummaryStats,
}

/// The `stats.summary` block.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SummaryStats {
    pub bytes_processed_per_second: i64,
    pub lines_processed_per_second: i64,
    pub total_bytes_processed: i64,
    pub total_lines_processed: i64,
    /// Wall-clock execution time in seconds.
    pub exec_time: f64,
    /// Time spent queued before execution, in seconds.
    pub queue_time: f64,
    pub total_entries_returned: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn streams_response_matches_loki_wire_format() {
        let response = QueryResponse::success(QueryResult::Streams(vec![Stream {
            stream: HashMap::from([("service_name".to_string(), "api".to_string())]),
            values: vec![
                LogEntry::new(1_569_266_497_240_578_000, "foo"),
                LogEntry::new(1_569_266_492_548_155_000, "bar"),
            ],
        }]));

        let expected = json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"service_name": "api"},
                        "values": [
                            ["1569266497240578000", "foo"],
                            ["1569266492548155000", "bar"]
                        ]
                    }
                ]
            }
        });
        assert_eq!(serde_json::to_value(&response).unwrap(), expected);
    }

    #[test]
    fn matrix_response_matches_loki_wire_format() {
        let response = QueryResponse::success(QueryResult::Matrix(vec![MetricSeries {
            metric: HashMap::from([("level".to_string(), "info".to_string())]),
            values: vec![
                Sample::new(1_588_889_221.0, "137"),
                Sample::new(1_588_889_236.0, "467"),
            ],
        }]));

        let expected = json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {"level": "info"},
                        "values": [[1_588_889_221i64, "137"], [1_588_889_236i64, "467"]]
                    }
                ]
            }
        });
        assert_eq!(serde_json::to_value(&response).unwrap(), expected);
    }

    #[test]
    fn vector_response_matches_loki_wire_format() {
        let response = QueryResponse::success(QueryResult::Vector(vec![InstantValue {
            metric: HashMap::new(),
            value: Sample::new(1_588_889_221.0, "1"),
        }]));

        let expected = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {}, "value": [1_588_889_221i64, "1"]}]
            }
        });
        assert_eq!(serde_json::to_value(&response).unwrap(), expected);
    }

    #[test]
    fn sample_preserves_subsecond_precision() {
        let sample = Sample::new(1_588_889_221.5, "3");
        assert_eq!(
            serde_json::to_value(&sample).unwrap(),
            json!([1_588_889_221.5, "3"])
        );
    }

    #[test]
    fn query_response_round_trips() {
        let response = QueryResponse::success(QueryResult::Matrix(vec![MetricSeries {
            metric: HashMap::from([("job".to_string(), "worker".to_string())]),
            values: vec![Sample::new(1_588_889_221.0, "42")],
        }]));

        let json = serde_json::to_string(&response).unwrap();
        let parsed: QueryResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, response);
    }

    #[test]
    fn streams_response_round_trips_from_loki_json() {
        let raw = r#"{
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"level": "error"},
                        "values": [["1569266497240578000", "boom"]]
                    }
                ],
                "stats": {
                    "summary": {
                        "bytesProcessedPerSecond": 2048,
                        "execTime": 0.25,
                        "totalEntriesReturned": 1
                    }
                }
            }
        }"#;

        let parsed: QueryResponse = serde_json::from_str(raw).unwrap();
        let QueryResult::Streams(streams) = &parsed.data.result else {
            panic!("expected streams result");
        };
        assert_eq!(
            streams[0].values[0],
            LogEntry::new(1_569_266_497_240_578_000, "boom")
        );
        let stats = parsed.data.stats.as_ref().unwrap();
        assert_eq!(stats.summary.bytes_processed_per_second, 2048);
        assert_eq!(stats.summary.exec_time, 0.25);
        assert_eq!(stats.summary.total_entries_returned, 1);
    }

    #[test]
    fn labels_response_matches_loki_wire_format() {
        let response =
            LabelsResponse::success(vec!["level".to_string(), "service_name".to_string()]);
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({"status": "success", "data": ["level", "service_name"]})
        );
    }

    #[test]
    fn series_response_matches_loki_wire_format() {
        let response = SeriesResponse::success(vec![HashMap::from([(
            "service_name".to_string(),
            "api".to_string(),
        )])]);
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({"status": "success", "data": [{"service_name": "api"}]})
        );
    }
}
