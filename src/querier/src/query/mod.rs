pub mod error;
pub mod logql;
pub mod logql_metric;
pub mod logs;
pub mod profile;
pub mod search_filter;
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
