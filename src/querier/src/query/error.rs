use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum QuerierError {
    #[error("Trace not found")]
    TraceNotFound,
    #[error("Query failed: {0}")]
    QueryFailed(#[from] DataFusionError),
    #[error("Failed to register parquet: {0}")]
    FailedToRegisterParquet(DataFusionError),
}
