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

/// The TraceQL parser's two rejection classes map 1:1 onto ours, and the
/// mapping is what decides the HTTP status a client sees: unparseable input is
/// the client's mistake (400), a construct we do not lower is ours (501).
impl From<traceql::ParseError> for QuerierError {
    fn from(err: traceql::ParseError) -> Self {
        match err {
            traceql::ParseError::Syntax(msg) => QuerierError::InvalidInput(msg),
            traceql::ParseError::Unsupported(msg) => QuerierError::Unsupported(msg),
            // `ParseError` is `#[non_exhaustive]`: the parser releases
            // independently, so it may grow a class this build predates.
            // Unknown rejections are ours, not the caller's.
            other => QuerierError::Unsupported(other.to_string()),
        }
    }
}
