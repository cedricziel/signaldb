pub mod error;
pub mod search_filter;
pub mod table_ref;
pub mod trace;

#[derive(Debug)]
#[allow(dead_code)]
pub struct FindTraceByIdParams {
    pub trace_id: String,
    pub start: Option<String>,
    pub end: Option<String>,
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
