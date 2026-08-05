use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum QuerierError {
    #[error("Trace not found")]
    TraceNotFound,
    #[error("Query failed: {0}")]
    QueryFailed(#[from] DataFusionError),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Unsupported query feature: {0}")]
    Unsupported(String),
    #[error(
        "PromQL query produced too many groups for row-wise evaluation: {count} groups exceeds the limit of {limit}; narrow the label selectors or time range"
    )]
    TooManyGroups { count: usize, limit: usize },
}
