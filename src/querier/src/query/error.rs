use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum QuerierError {
    #[error("Trace not found")]
    TraceNotFound,
    #[error("Query failed: {0}")]
    QueryFailed(#[source] DataFusionError),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Unsupported query feature: {0}")]
    Unsupported(String),
}

/// Finds a caller error an operator raised inside execution
/// (`DataFusionError::External(QuerierError::InvalidInput)`), looking through
/// the wrappers DataFusion and Arrow add around it.
fn invalid_input_in(err: &DataFusionError) -> Option<String> {
    match err {
        DataFusionError::External(inner) => external(inner.as_ref()),
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            ArrowError::ExternalError(inner) => external(inner.as_ref()),
            _ => None,
        },
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            invalid_input_in(inner)
        }
        DataFusionError::Shared(inner) => invalid_input_in(inner),
        DataFusionError::Collection(errs) => errs.iter().find_map(invalid_input_in),
        _ => None,
    }
}

fn external(err: &(dyn std::error::Error + Send + Sync + 'static)) -> Option<String> {
    if let Some(QuerierError::InvalidInput(msg)) = err.downcast_ref::<QuerierError>() {
        return Some(msg.clone());
    }
    err.downcast_ref::<DataFusionError>()
        .and_then(invalid_input_in)
}

impl From<DataFusionError> for QuerierError {
    fn from(err: DataFusionError) -> Self {
        match invalid_input_in(&err) {
            Some(msg) => QuerierError::InvalidInput(msg),
            None => QuerierError::QueryFailed(err),
        }
    }
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

/// A `ql_ir` lowering failure maps onto the same accept/reject classes the
/// TraceQL path above uses: a parse failure keeps its existing 400-vs-501
/// split (via the `From<traceql::ParseError>` impl above), and a construct
/// `ql_ir` recognises but cannot express becomes `Unsupported` (501) — the
/// same class the old TraceQL path's own catch-all selector/value arms
/// return for an unhandled construct (`ir-single-lowering`, tasks 3.2/3.3).
impl From<ql_ir::LowerError> for QuerierError {
    fn from(err: ql_ir::LowerError) -> Self {
        match err {
            ql_ir::LowerError::Parse(parse_err) => QuerierError::from(parse_err),
            ql_ir::LowerError::Inexpressible(msg) => QuerierError::Unsupported(msg),
            other => QuerierError::Unsupported(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn raised() -> DataFusionError {
        DataFusionError::External(Box::new(QuerierError::InvalidInput("bad".into())))
    }

    #[test]
    fn execution_invalid_input_stays_invalid_input() {
        let wrapped = DataFusionError::Shared(Arc::new(DataFusionError::Context(
            "ctx".into(),
            Box::new(raised()),
        )));
        assert!(matches!(
            QuerierError::from(wrapped),
            QuerierError::InvalidInput(m) if m == "bad"
        ));
    }

    #[test]
    fn invalid_input_is_found_through_arrow_and_collection_wrappers() {
        let via_arrow = DataFusionError::ArrowError(
            Box::new(ArrowError::ExternalError(Box::new(raised()))),
            None,
        );
        let collected =
            DataFusionError::Collection(vec![DataFusionError::Plan("x".into()), via_arrow]);
        assert!(matches!(
            QuerierError::from(collected),
            QuerierError::InvalidInput(m) if m == "bad"
        ));
    }

    #[test]
    fn query_failed_keeps_its_source() {
        let err = QuerierError::from(DataFusionError::Plan("x".into()));
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn other_datafusion_errors_stay_query_failed() {
        let err = DataFusionError::Plan("x".into());
        assert!(matches!(
            QuerierError::from(err),
            QuerierError::QueryFailed(_)
        ));
    }
}
