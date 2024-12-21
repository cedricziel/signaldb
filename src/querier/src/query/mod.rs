use common::model;

use async_trait::async_trait;
use error::QuerierError;

pub mod error;
pub mod trace;

#[derive(Debug)]
#[allow(dead_code)]
pub struct FindTraceByIdParams {
    pub trace_id: String,
    pub start: Option<String>,
    pub end: Option<String>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct SearchQueryParams {
    q: Option<String>,
    tags: Option<String>,
    min_duration: Option<i32>,
    max_duration: Option<i32>,
    limit: Option<i32>,
    start: Option<i32>,
    end: Option<i32>,
    spss: Option<i32>,
}

#[async_trait]
#[allow(dead_code)]
pub trait TraceQuerier: Send + Sync + Clone {
    async fn find_shallow_by_id(
        &self,
        params: FindTraceByIdParams,
    ) -> Result<Option<model::trace::Trace>, QuerierError>;
    async fn find_by_id(
        &self,
        params: FindTraceByIdParams,
    ) -> Result<Option<model::trace::Trace>, QuerierError>;
    async fn find_traces(
        &self,
        query: SearchQueryParams,
    ) -> Result<Vec<model::trace::Trace>, QuerierError>;
}
