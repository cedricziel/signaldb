//! # IR versioning and the operator/function registry
//!
//! IR documents are persisted (stored in dashboards), so the format is a
//! compatibility surface from commit #1. The document carries an integer
//! `irVersion`; the server accepts a bounded range and reports it. The
//! operator/function set is a **versioned registry**, not an open enum — adding
//! a new agg fn or the deferred `regex` extract parser is a registry + version
//! bump (additive), which enables capability negotiation with clients.

use super::predicate::ComparisonOp;
use super::stage::{AggFn, Parser};

/// The lowest IR document version this server understands.
pub const MIN_IR_VERSION: i64 = 1;
/// The highest IR document version this server understands.
pub const MAX_IR_VERSION: i64 = 3;

/// Whether `version` is within the supported range.
pub fn is_supported(version: i64) -> bool {
    (MIN_IR_VERSION..=MAX_IR_VERSION).contains(&version)
}

/// The set of operators, aggregate functions, and parsers available at a given
/// IR version. Closed enums already reject unknown members at parse time; this
/// registry documents the versioned capability set and is the home for future
/// additive members (e.g. the deferred `regex` extract parser).
#[derive(Debug, Clone)]
pub struct OperatorRegistry {
    pub version: i64,
    ops: Vec<ComparisonOp>,
    aggs: Vec<AggFn>,
    parsers: Vec<Parser>,
}

impl OperatorRegistry {
    /// The registry for IR version 1.
    pub fn v1() -> Self {
        OperatorRegistry {
            version: 1,
            ops: vec![
                ComparisonOp::Eq,
                ComparisonOp::Ne,
                ComparisonOp::Gt,
                ComparisonOp::Gte,
                ComparisonOp::Lt,
                ComparisonOp::Lte,
                ComparisonOp::In,
                ComparisonOp::Between,
                ComparisonOp::Contains,
                ComparisonOp::Regex,
                ComparisonOp::Exists,
            ],
            aggs: vec![
                AggFn::Count,
                AggFn::Sum,
                AggFn::Avg,
                AggFn::Min,
                AggFn::Max,
                AggFn::Quantile,
            ],
            // `regex` extract parser is deferred to a later registry entry.
            parsers: vec![Parser::Json, Parser::Logfmt],
        }
    }

    /// The registry for a supported version, or `None` if out of range.
    pub fn for_version(version: i64) -> Option<Self> {
        is_supported(version).then(OperatorRegistry::v1)
    }

    pub fn supports_op(&self, op: ComparisonOp) -> bool {
        self.ops.contains(&op)
    }

    pub fn supports_agg(&self, agg: AggFn) -> bool {
        self.aggs.contains(&agg)
    }

    pub fn supports_parser(&self, parser: Parser) -> bool {
        self.parsers.contains(&parser)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_range_is_bounded() {
        assert!(is_supported(1));
        assert!(!is_supported(0));
        assert!(is_supported(2));
        assert!(is_supported(3));
        assert!(!is_supported(4));
    }

    #[test]
    fn v1_registry_covers_the_core_set() {
        let r = OperatorRegistry::v1();
        assert!(r.supports_op(ComparisonOp::Regex));
        assert!(r.supports_agg(AggFn::Quantile));
        assert!(r.supports_parser(Parser::Logfmt));
    }
}
