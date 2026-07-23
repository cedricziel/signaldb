pub mod error;
pub mod profile;
pub mod search_filter;
pub mod table_ref;
pub mod trace;

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
