//! # LogQL Metric Query AST
//!
//! Metric queries turn log streams into numeric time series. They wrap a
//! [`LogQuery`](crate::ast::LogQuery) in a range aggregation (`rate`,
//! `count_over_time`, ...), optionally nest it in a vector aggregation
//! (`sum by (...)`, `topk`, ...), and combine results with binary
//! operators.

use crate::ast::{Grouping, LogQuery, Unwrap};
use std::time::Duration;

/// A parsed LogQL metric query.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricQuery {
    /// A range aggregation over a log selector, e.g. `rate({a="b"}[5m])`.
    Range(Box<RangeAggregation>),
    /// A vector aggregation over an inner metric query, e.g.
    /// `sum by (level) (rate(...))`.
    Vector(Box<VectorAggregation>),
    /// A binary operation between two metric queries or scalars.
    Binary(Box<BinaryExpr>),
    /// A scalar literal (e.g. the `2` in `rate(...) * 2`).
    Literal(f64),
    /// `vector(<scalar>)` — a scalar promoted to an instant vector, used
    /// for fallbacks like `... or vector(0)`.
    VectorLiteral(f64),
    /// `label_replace(v, dst, replacement, src, regex)` — rewrite a label
    /// from a regex capture of another label.
    LabelReplace(Box<LabelReplace>),
}

/// A `label_replace(v, dst, replacement, src, regex)` call.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelReplace {
    /// The inner metric query whose labels are rewritten.
    pub inner: MetricQuery,
    /// Destination label name.
    pub dst_label: String,
    /// Replacement template referencing regex capture groups (`$1`).
    pub replacement: String,
    /// Source label the regex is matched against.
    pub src_label: String,
    /// Regex applied to the source label's value.
    pub regex: String,
}

/// A range aggregation: a range function applied to a ranged log query.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeAggregation {
    pub function: RangeFunction,
    /// The log selector and pipeline being aggregated.
    pub log_query: LogQuery,
    /// The range window, e.g. `5m` in `[5m]`.
    pub range: Duration,
    /// Optional `offset <duration>` time shift.
    pub offset: Option<Duration>,
    /// Optional `| unwrap <label>` for value extraction (required by the
    /// `*_over_time` functions that operate on extracted samples).
    pub unwrap: Option<Unwrap>,
    /// Scalar parameter for `quantile_over_time(<q>, ...)`.
    pub param: Option<f64>,
    /// Optional `by`/`without` grouping on unwrapped range aggregations.
    pub grouping: Option<Grouping>,
}

/// Range aggregation functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFunction {
    Rate,
    RateCounter,
    CountOverTime,
    BytesRate,
    BytesOverTime,
    AvgOverTime,
    SumOverTime,
    MinOverTime,
    MaxOverTime,
    StdvarOverTime,
    StddevOverTime,
    QuantileOverTime,
    FirstOverTime,
    LastOverTime,
    AbsentOverTime,
}

impl RangeFunction {
    /// Resolve a function name to its variant.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "rate" => RangeFunction::Rate,
            "rate_counter" => RangeFunction::RateCounter,
            "count_over_time" => RangeFunction::CountOverTime,
            "bytes_rate" => RangeFunction::BytesRate,
            "bytes_over_time" => RangeFunction::BytesOverTime,
            "avg_over_time" => RangeFunction::AvgOverTime,
            "sum_over_time" => RangeFunction::SumOverTime,
            "min_over_time" => RangeFunction::MinOverTime,
            "max_over_time" => RangeFunction::MaxOverTime,
            "stdvar_over_time" => RangeFunction::StdvarOverTime,
            "stddev_over_time" => RangeFunction::StddevOverTime,
            "quantile_over_time" => RangeFunction::QuantileOverTime,
            "first_over_time" => RangeFunction::FirstOverTime,
            "last_over_time" => RangeFunction::LastOverTime,
            "absent_over_time" => RangeFunction::AbsentOverTime,
            _ => return None,
        })
    }

    /// Whether this function requires a leading scalar parameter, as in
    /// `quantile_over_time(0.99, ...)`.
    pub fn takes_param(self) -> bool {
        matches!(self, RangeFunction::QuantileOverTime)
    }
}

/// A vector aggregation over an inner metric query.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorAggregation {
    pub function: AggregationFunction,
    /// Scalar parameter for `topk`/`bottomk` (the `k`).
    pub param: Option<f64>,
    /// Optional `by`/`without` grouping.
    pub grouping: Option<Grouping>,
    /// The metric query being aggregated.
    pub inner: MetricQuery,
}

/// Vector aggregation functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationFunction {
    Sum,
    Avg,
    Min,
    Max,
    Stddev,
    Stdvar,
    Count,
    Topk,
    Bottomk,
    Sort,
    SortDesc,
}

impl AggregationFunction {
    /// Resolve an aggregation name to its variant.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "sum" => AggregationFunction::Sum,
            "avg" => AggregationFunction::Avg,
            "min" => AggregationFunction::Min,
            "max" => AggregationFunction::Max,
            "stddev" => AggregationFunction::Stddev,
            "stdvar" => AggregationFunction::Stdvar,
            "count" => AggregationFunction::Count,
            "topk" => AggregationFunction::Topk,
            "bottomk" => AggregationFunction::Bottomk,
            "sort" => AggregationFunction::Sort,
            "sort_desc" => AggregationFunction::SortDesc,
            _ => return None,
        })
    }

    /// Whether this aggregation requires a leading scalar parameter, as
    /// in `topk(5, ...)`.
    pub fn takes_param(self) -> bool {
        matches!(
            self,
            AggregationFunction::Topk | AggregationFunction::Bottomk
        )
    }
}

/// A binary operation between two metric queries.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub op: BinOp,
    pub left: MetricQuery,
    pub right: MetricQuery,
    /// `true` when a comparison carries the `bool` modifier
    /// (`> bool 5`), turning the filter into a 0/1 result.
    pub bool_modifier: bool,
}

/// Binary operators, grouped by precedence tier (see the parser).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    // Comparison
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    // Logical / set
    And,
    Or,
    Unless,
}
