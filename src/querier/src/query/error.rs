use datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QuerierError {
    #[error("Trace not found")]
    TraceNotFound,
    #[error("Query failed: {0}")]
    QueryFailed(#[from] DataFusionError),
    #[error("Failed to register parquet: {0}")]
    FailedToRegisterParquet(DataFusionError),
}
