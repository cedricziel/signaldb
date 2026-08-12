pub mod error;
pub mod histogram;
pub mod ir_planner;
pub mod logql;
pub mod logql_metric;
pub mod logs;
pub mod metrics;
pub mod profile;
pub mod promql;
pub mod search_filter;
pub mod table_lookup;
pub mod table_ref;
pub mod trace;

/// Parameters carried in the `query_logs` Flight ticket (JSON-encoded).
///
/// Mirrors Loki's range/instant query surface: a LogQL string plus a
/// nanosecond time window, a row limit, and the scan direction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogQueryParams {
    /// The LogQL query string.
    pub query: String,
    /// Inclusive range start, unix epoch nanoseconds.
    pub start: i64,
    /// Inclusive range end, unix epoch nanoseconds.
    pub end: i64,
    /// Maximum rows to return.
    pub limit: u32,
    /// `"forward"` or `"backward"` (default).
    #[serde(default)]
    pub direction: Option<String>,
}

/// Parameters carried in the `query_ir` Flight ticket (JSON-encoded).
///
/// The native Query IR surface: a versioned IR document plus the server-stamped
/// clock the router captured at the ticket boundary. Relative time anchors
/// (`now-1h`) are resolved once against `now_ns`, so every stage of the plan
/// sees identical absolute bounds.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IrQueryParams {
    /// The IR query document (see `common::query_ir::Document`).
    pub document: serde_json::Value,
    /// Server-received clock, unix epoch nanoseconds, for resolving relative
    /// time anchors deterministically.
    pub now_ns: i64,
}

/// Parameters carried in the `query_metric` Flight ticket (JSON-encoded).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricQueryParams {
    /// The LogQL metric query string.
    pub query: String,
    /// Inclusive range start, unix epoch nanoseconds.
    pub start: i64,
    /// Inclusive range end, unix epoch nanoseconds.
    pub end: i64,
    /// Bucket width (query resolution) in nanoseconds.
    pub step: i64,
}

/// Parameters carried in the `query_promql` Flight ticket (JSON-encoded).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PromQlQueryParams {
    /// The PromQL query string.
    pub query: String,
    /// Inclusive range start, unix epoch nanoseconds.
    pub start: i64,
    /// Inclusive range end, unix epoch nanoseconds.
    pub end: i64,
    /// Bucket width (query resolution) in nanoseconds.
    pub step: i64,
}

/// Parameters carried in the `query_metric_series` Flight ticket.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricSeriesParams {
    /// The PromQL selector, e.g. `http_requests_total{job="api"}`.
    pub selector: String,
    /// Inclusive range start, unix epoch nanoseconds.
    pub start: i64,
    /// Inclusive range end, unix epoch nanoseconds.
    pub end: i64,
}

/// Parameters carried in the `query_logs_series` Flight ticket.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogSeriesParams {
    /// The stream selector, e.g. `{service_name="api"}`.
    pub selector: String,
    /// Inclusive range start, unix epoch nanoseconds.
    pub start: i64,
    /// Inclusive range end, unix epoch nanoseconds.
    pub end: i64,
}

/// Parameters carried in the `query_logs_detected_fields` Flight ticket.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DetectedFieldsParams {
    /// Optional stream selector restricting the sample, e.g.
    /// `{service_name="api"}`. Empty/absent samples everything in range.
    #[serde(default)]
    pub query: Option<String>,
    /// Inclusive range start, unix epoch nanoseconds.
    pub start: i64,
    /// Inclusive range end, unix epoch nanoseconds.
    pub end: i64,
    /// Maximum number of fields to return.
    pub limit: u32,
}

/// One discovered attribute field: its name, inferred type, and an
/// approximate distinct-value count over the sampled window.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DetectedField {
    pub label: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub cardinality: u64,
    /// Parsers Loki would need to extract the field; our attributes are
    /// the structured-metadata analog, so this stays empty.
    pub parsers: Vec<String>,
}

/// Parameters for single-trace lookup.
#[derive(Debug)]
pub struct FindTraceByIdParams {
    pub trace_id: String,
    /// Optional unix-second hint: only consider spans starting at or after this time.
    pub start: Option<i64>,
    /// Optional unix-second hint: only consider spans starting at or before this time.
    pub end: Option<i64>,
}

/// Search parameters carried in the `search_traces` Flight ticket.
///
/// Mirrors the Tempo search API. `spss` (spans per span set) is applied by
/// the router when shaping the HTTP response and is intentionally absent
/// here; unknown JSON fields in the ticket are ignored on deserialization.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SearchQueryParams {
    pub q: Option<String>,
    pub tags: Option<String>,
    /// Minimum span duration in nanoseconds
    pub min_duration: Option<i64>,
    /// Maximum span duration in nanoseconds
    pub max_duration: Option<i64>,
    pub limit: Option<i32>,
    /// Search window start (unix seconds)
    pub start: Option<i64>,
    /// Search window end (unix seconds)
    pub end: Option<i64>,
}
