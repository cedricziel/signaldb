//! # IR versioning and the operator/function registry
//!
//! IR documents are persisted (stored in dashboards), so the format is a
//! compatibility surface from commit #1. The document carries an integer
//! `irVersion`; the server accepts a bounded range and reports it. The
//! operator/function set is a **versioned registry**, not an open enum — adding
//! a new agg fn or the deferred `regex` extract parser is a registry + version
//! bump (additive), which enables capability negotiation with clients.
//!
//! [`OperatorRegistry`] is the single source of truth for every version gate:
//! comparison operators, aggregate functions, extract parsers, and
//! stage-level features (`heatmap`, `histogram_quantile`, `describe`, an
//! aggregate's `divisor`/`across`/`window`). [`validate`](super::validate)
//! asks the registry rather than hand-writing `if doc.ir_version < N` checks,
//! so a new gate can't bypass it.

use super::predicate::ComparisonOp;
use super::stage::{AggFn, Parser};

/// The lowest IR document version this server understands.
pub const MIN_IR_VERSION: i64 = 1;
/// The highest IR document version this server understands.
pub const MAX_IR_VERSION: i64 = 7;

/// Whether `version` is within the supported range.
pub fn is_supported(version: i64) -> bool {
    (MIN_IR_VERSION..=MAX_IR_VERSION).contains(&version)
}

/// A stage-level capability gated by `irVersion` that isn't itself an
/// operator, aggregate function, or parser — a whole stage (`heatmap`,
/// `histogram_quantile`, `describe`) or an operand of one (an aggregate's
/// `divisor`, `across`, `window`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    /// The `heatmap` stage and result envelope.
    Heatmap,
    /// The `histogram_quantile` stage.
    HistogramQuantile,
    /// The `describe` stage and `metadata` result envelope.
    Describe,
    /// An aggregate's `divisor` operand.
    AggregateDivisor,
    /// A range-function aggregate's `across` reducer.
    AggregateAcross,
    /// A range-function aggregate's `window` operand.
    AggregateWindow,
}

/// Comparison operators, keyed by the `irVersion` that introduced them.
const OPS: &[(ComparisonOp, i64)] = &[
    (ComparisonOp::Eq, 1),
    (ComparisonOp::Ne, 1),
    (ComparisonOp::Gt, 1),
    (ComparisonOp::Gte, 1),
    (ComparisonOp::Lt, 1),
    (ComparisonOp::Lte, 1),
    (ComparisonOp::In, 1),
    (ComparisonOp::Between, 1),
    (ComparisonOp::Contains, 1),
    (ComparisonOp::Regex, 1),
    (ComparisonOp::Exists, 1),
];

/// Aggregate functions, keyed by the `irVersion` that introduced them.
const AGGS: &[(AggFn, i64)] = &[
    (AggFn::Count, 1),
    (AggFn::Sum, 1),
    (AggFn::Avg, 1),
    (AggFn::Min, 1),
    (AggFn::Max, 1),
    (AggFn::Quantile, 1),
    (AggFn::Stddev, 5),
    (AggFn::Stdvar, 5),
    (AggFn::First, 5),
    (AggFn::Last, 5),
    (AggFn::Rate, 6),
    (AggFn::Increase, 6),
    (AggFn::Irate, 7),
    (AggFn::AvgOverTime, 7),
    (AggFn::MinOverTime, 7),
    (AggFn::MaxOverTime, 7),
    (AggFn::SumOverTime, 7),
    (AggFn::CountOverTime, 7),
];

/// Extract parsers, keyed by the `irVersion` that introduced them.
// `regex` extract parser is deferred to a later registry entry.
const PARSERS: &[(Parser, i64)] = &[(Parser::Json, 1), (Parser::Logfmt, 1)];

/// Stage-level features, keyed by the `irVersion` that introduced them.
const FEATURES: &[(Feature, i64)] = &[
    (Feature::Heatmap, 2),
    (Feature::HistogramQuantile, 3),
    (Feature::Describe, 4),
    (Feature::AggregateDivisor, 5),
    (Feature::AggregateAcross, 7),
    (Feature::AggregateWindow, 7),
];

fn min_version<T: PartialEq + Copy>(table: &[(T, i64)], member: T) -> Option<i64> {
    table
        .iter()
        .find_map(|(candidate, version)| (*candidate == member).then_some(*version))
}

/// The set of operators, aggregate functions, parsers, and stage-level
/// features available at a given IR version. Closed enums already reject
/// unknown members at parse time; this registry is the versioned capability
/// set for members that are known but not yet unlocked, and the home for
/// future additive members (e.g. the deferred `regex` extract parser).
#[derive(Debug, Clone, Copy)]
pub struct OperatorRegistry {
    pub version: i64,
}

impl OperatorRegistry {
    /// The registry for a supported version, or `None` if out of range.
    pub fn for_version(version: i64) -> Option<Self> {
        is_supported(version).then_some(OperatorRegistry { version })
    }

    pub fn supports_op(&self, op: ComparisonOp) -> bool {
        min_version(OPS, op).is_some_and(|v| v <= self.version)
    }

    pub fn supports_agg(&self, agg: AggFn) -> bool {
        min_version(AGGS, agg).is_some_and(|v| v <= self.version)
    }

    pub fn supports_parser(&self, parser: Parser) -> bool {
        min_version(PARSERS, parser).is_some_and(|v| v <= self.version)
    }

    pub fn supports_feature(&self, feature: Feature) -> bool {
        min_version(FEATURES, feature).is_some_and(|v| v <= self.version)
    }

    /// The `irVersion` that introduced `agg`. Panics-free: every `AggFn`
    /// member is registered, enforced by
    /// [`tests::every_agg_fn_is_registered`].
    pub fn agg_min_version(agg: AggFn) -> i64 {
        min_version(AGGS, agg).unwrap_or(i64::MAX)
    }

    /// The `irVersion` that introduced `feature`. Panics-free: every
    /// [`Feature`] member is registered, enforced by
    /// [`tests::every_feature_is_registered`].
    pub fn feature_min_version(feature: Feature) -> i64 {
        min_version(FEATURES, feature).unwrap_or(i64::MAX)
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
        assert!(is_supported(4));
        assert!(is_supported(5));
        assert!(is_supported(6));
        assert!(is_supported(7));
        assert!(!is_supported(8));
    }

    #[test]
    fn v1_registry_covers_the_core_set() {
        let r = OperatorRegistry::for_version(1).unwrap();
        assert!(r.supports_op(ComparisonOp::Regex));
        assert!(r.supports_agg(AggFn::Quantile));
        assert!(r.supports_parser(Parser::Logfmt));
        assert!(!r.supports_agg(AggFn::Stddev));
        assert!(!r.supports_feature(Feature::Heatmap));
    }

    #[test]
    fn registry_unlocks_versioned_members_at_their_minimum_version() {
        assert!(
            !OperatorRegistry::for_version(4)
                .unwrap()
                .supports_agg(AggFn::Stddev)
        );
        assert!(
            OperatorRegistry::for_version(5)
                .unwrap()
                .supports_agg(AggFn::Stddev)
        );

        assert!(
            !OperatorRegistry::for_version(6)
                .unwrap()
                .supports_agg(AggFn::Irate)
        );
        assert!(
            OperatorRegistry::for_version(7)
                .unwrap()
                .supports_agg(AggFn::Irate)
        );

        assert!(
            !OperatorRegistry::for_version(1)
                .unwrap()
                .supports_feature(Feature::Describe)
        );
        assert!(
            OperatorRegistry::for_version(4)
                .unwrap()
                .supports_feature(Feature::Describe)
        );
    }

    /// Every `AggFn` member must be registered so no future function can
    /// gate its version outside [`OperatorRegistry`].
    #[test]
    fn every_agg_fn_is_registered() {
        let all = [
            AggFn::Count,
            AggFn::Sum,
            AggFn::Avg,
            AggFn::Min,
            AggFn::Max,
            AggFn::Quantile,
            AggFn::Stddev,
            AggFn::Stdvar,
            AggFn::First,
            AggFn::Last,
            AggFn::Rate,
            AggFn::Increase,
            AggFn::Irate,
            AggFn::AvgOverTime,
            AggFn::MinOverTime,
            AggFn::MaxOverTime,
            AggFn::SumOverTime,
            AggFn::CountOverTime,
        ];
        for agg in all {
            assert!(
                min_version(AGGS, agg).is_some(),
                "{agg:?} has no registry entry"
            );
        }
    }

    /// Every gated stage-level feature must be registered so a future gate
    /// can't bypass the registry with a hand-written version comparison.
    #[test]
    fn every_feature_is_registered() {
        let all = [
            Feature::Heatmap,
            Feature::HistogramQuantile,
            Feature::Describe,
            Feature::AggregateDivisor,
            Feature::AggregateAcross,
            Feature::AggregateWindow,
        ];
        for feature in all {
            assert!(
                min_version(FEATURES, feature).is_some(),
                "{feature:?} has no registry entry"
            );
        }
    }
}
