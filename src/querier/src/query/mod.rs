pub mod error;
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
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
