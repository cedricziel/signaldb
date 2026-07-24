//! # PromQL query lowering
//!
//! Parses a PromQL query with `promql-parser` and lowers the supported
//! subset into a [`MetricPlan`] — a backend-neutral description of the
//! aggregation the querier runs over the metrics tables. The
//! [`super::metrics`] service turns this into a DataFusion aggregate.
//!
//! ## Scope
//!
//! This covers the common instant/range shapes:
//!
//! - A bare selector `metric{label="v"}` — the series' samples, bucketed
//!   by step (last value per bucket).
//! - A vector aggregation `sum|avg|min|max|count [by (labels)] (metric)`.
//!
//! - Range-vector functions `rate(metric[range])` and
//!   `increase(metric[range])`, optionally under an outer aggregation.
//! - The `<agg>_over_time(metric[range])` family (avg/sum/min/max/count/
//!   last/stddev/stdvar) — a per-bucket reducer over the raw samples.
//! - `histogram_quantile(phi, metric)` over a stored OTLP histogram — the
//!   quantile is interpolated per series from the metric's buckets.
//! - Unary math functions (`abs`, `ceil`, `floor`, `round`, `sqrt`, `exp`,
//!   `ln`, `log2`, `log10`, `sgn`, `clamp*`) and scalar arithmetic
//!   (`metric * 8`, `1024 / metric`) as value transforms on the result.
//!
//! `irate`, comparison/logical/vector-to-vector operators, subqueries, and
//! `histogram_quantile` over an inner `rate()`/aggregation are not lowered yet
//! and return [`QuerierError::Unsupported`] (#335).
//!
//! Like the log path, range aggregations use fixed step-aligned buckets
//! (`date_bin`), not Prometheus's sliding window — exact when the step
//! equals the range.

use promql_parser::parser::{self, Expr, LabelModifier};

use super::error::QuerierError;

/// The per-bucket, per-series aggregate to compute over `value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricAgg {
    /// Last value in the bucket (bare selector sampling).
    Last,
    Sum,
    Avg,
    Min,
    Max,
    Count,
    /// Population standard deviation (`stddev_over_time`).
    Stddev,
    /// Population variance (`stdvar_over_time`).
    StdVar,
}

/// How series are grouped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grouping {
    /// Group by the natural series identity (bare selector).
    Natural,
    /// Collapse all series into one (`sum(metric)` with no `by`).
    Collapse,
    /// Group by the given labels (`sum by (a, b) (metric)`).
    By(Vec<String>),
}

/// A label matcher on a selector (excluding `__name__`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatch {
    pub name: String,
    pub op: MatchKind,
    pub value: String,
}

/// Label matcher operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchKind {
    Eq,
    Neq,
    Re,
    Nre,
}

/// A range-vector function applied over the `[range]` window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFn {
    /// `rate(metric[range])` — per-second average increase of a counter.
    Rate,
    /// `increase(metric[range])` — total increase over the window.
    Increase,
}

/// A range-vector spec: the function and its window in seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RangeSpec {
    pub function: RangeFn,
    pub seconds: f64,
}

impl Eq for RangeSpec {}

/// A scalar arithmetic operator in a `vector OP scalar` expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

/// A transform applied to the result `value` column, in order. Covers the
/// unary math functions and scalar arithmetic (`metric * 8`, `metric / 1024`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueOp {
    Abs,
    Ceil,
    Floor,
    Round,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    /// `sgn` — sign of the value (-1, 0, 1).
    Sgn,
    ClampMin(f64),
    ClampMax(f64),
    Clamp(f64, f64),
    /// Scalar arithmetic; the bool is true when the scalar is on the left
    /// (`scalar OP vector`), false for `vector OP scalar`.
    Arith(ArithOp, f64, bool),
}

impl Eq for ValueOp {}

/// A lowered PromQL query.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricPlan {
    /// The metric name being queried.
    pub metric_name: String,
    /// Label matchers to filter series (excluding `__name__`).
    pub matchers: Vec<LabelMatch>,
    /// The aggregate computed per bucket per series. Ignored for the
    /// per-series stage of a range function; applied as the outer
    /// aggregation over a range result.
    pub aggregate: MetricAgg,
    pub grouping: Grouping,
    /// Set for `rate`/`increase` — a per-series counter delta over the
    /// window is computed before any outer aggregation.
    pub range: Option<RangeSpec>,
    /// Set for `histogram_quantile(phi, <selector>)` — the query targets
    /// the `metrics_histogram` table and interpolates the phi-quantile from
    /// each series' stored buckets instead of aggregating a scalar `value`.
    pub quantile: Option<f64>,
    /// Math/scalar-arithmetic transforms applied to the result `value`, in
    /// order (e.g. `abs`, `metric * 8`). Empty for a plain query.
    pub transforms: Vec<ValueOp>,
}

impl Eq for MetricPlan {}

/// Parse and lower a PromQL query.
pub fn plan_promql(query: &str) -> Result<MetricPlan, QuerierError> {
    let expr = parser::parse(query)
        .map_err(|e| QuerierError::InvalidInput(format!("invalid PromQL: {e}")))?;
    lower(&expr)
}

fn lower(expr: &Expr) -> Result<MetricPlan, QuerierError> {
    match expr {
        Expr::Paren(p) => lower(&p.expr),
        // `histogram_quantile(phi, <selector>)` targets the histogram table.
        Expr::Call(call) if call.func.name == "histogram_quantile" => {
            lower_histogram_quantile(call)
        }
        // A math function wrapping a vector: `abs(...)`, `clamp_min(..., 0)`.
        Expr::Call(call) if is_math_fn(call.func.name) => lower_math_call(call),
        // Bare selector or bare range function.
        Expr::VectorSelector(_) | Expr::Call(_) => {
            build_plan(expr, MetricAgg::Last, Grouping::Natural)
        }
        Expr::Aggregate(agg) => {
            let aggregate = aggregate_op(&format!("{}", agg.op))?;
            if agg.param.is_some() {
                return Err(QuerierError::Unsupported(
                    "parameterized aggregation (topk/quantile/count_values)".to_string(),
                ));
            }
            let grouping = grouping_from(agg.modifier.as_ref())?;
            build_plan(unwrap_paren(&agg.expr), aggregate, grouping)
        }
        // Scalar arithmetic: `metric * 8`, `1024 / metric`. Applied to the
        // result value. Vector-to-vector operations remain unsupported.
        Expr::Binary(bin) => lower_binary(bin),
        Expr::MatrixSelector(_) | Expr::Subquery(_) => Err(QuerierError::Unsupported(
            "range-vector selector without a function".to_string(),
        )),
        other => Err(QuerierError::Unsupported(format!(
            "PromQL expression: {}",
            expr_kind(other)
        ))),
    }
}

/// Build a plan from the innermost expression (a selector or a range
/// function over a selector) plus the outer aggregate/grouping.
fn build_plan(
    inner: &Expr,
    aggregate: MetricAgg,
    grouping: Grouping,
) -> Result<MetricPlan, QuerierError> {
    match inner {
        Expr::Paren(p) => build_plan(&p.expr, aggregate, grouping),
        Expr::VectorSelector(vs) => lower_selector(vs, aggregate, grouping, None),
        Expr::Call(call) => {
            let arg = call.args.first().ok_or_else(|| {
                QuerierError::InvalidInput(format!("{} expects one argument", call.func.name))
            })?;
            let Expr::MatrixSelector(ms) = unwrap_paren(&arg) else {
                return Err(QuerierError::Unsupported(format!(
                    "{} requires a range-vector selector like metric[5m]",
                    call.func.name
                )));
            };

            // `<agg>_over_time(metric[range])` reduces the raw samples in each
            // bucket. Only the bare form is supported; an outer aggregation
            // would need a second reduce stage (#335).
            if let Some(reducer) = over_time_agg(call.func.name) {
                if grouping != Grouping::Natural || aggregate != MetricAgg::Last {
                    return Err(QuerierError::Unsupported(format!(
                        "{} under an outer aggregation is not supported yet (#335)",
                        call.func.name
                    )));
                }
                return lower_selector(&ms.vs, reducer, Grouping::Natural, None);
            }

            let function = match call.func.name {
                "rate" => RangeFn::Rate,
                "increase" => RangeFn::Increase,
                other => {
                    return Err(QuerierError::Unsupported(format!(
                        "function '{other}' (supported: rate, increase, *_over_time)"
                    )));
                }
            };
            let seconds = ms.range.as_secs_f64();
            lower_selector(
                &ms.vs,
                aggregate,
                grouping,
                Some(RangeSpec { function, seconds }),
            )
        }
        other => Err(QuerierError::Unsupported(format!(
            "aggregation over {}",
            expr_kind(other)
        ))),
    }
}

fn lower_selector(
    vs: &parser::VectorSelector,
    aggregate: MetricAgg,
    grouping: Grouping,
    range: Option<RangeSpec>,
) -> Result<MetricPlan, QuerierError> {
    // The metric name may be given directly or via a `__name__` matcher.
    let mut metric_name = vs.name.clone();
    let mut matchers = Vec::new();
    for m in &vs.matchers.matchers {
        if m.name == "__name__" {
            metric_name.get_or_insert_with(|| m.value.clone());
            continue;
        }
        matchers.push(LabelMatch {
            name: m.name.clone(),
            op: match_kind(&m.op),
            value: m.value.clone(),
        });
    }
    let metric_name = metric_name
        .ok_or_else(|| QuerierError::InvalidInput("selector has no metric name".to_string()))?;
    Ok(MetricPlan {
        metric_name,
        matchers,
        aggregate,
        grouping,
        range,
        quantile: None,
        transforms: Vec::new(),
    })
}

/// Lower `histogram_quantile(phi, <selector>)`. The phi must be a numeric
/// literal and the inner argument a plain vector selector — SignalDB stores
/// whole OTLP histograms per series (bucket arrays), so the quantile is
/// interpolated per series rather than from `le`-labelled bucket series.
fn lower_histogram_quantile(call: &parser::Call) -> Result<MetricPlan, QuerierError> {
    let phi = match unwrap_paren(call.args.args.first().ok_or_else(|| {
        QuerierError::InvalidInput("histogram_quantile expects (phi, selector)".to_string())
    })?) {
        Expr::NumberLiteral(n) => n.val,
        _ => {
            return Err(QuerierError::Unsupported(
                "histogram_quantile requires a numeric literal quantile".to_string(),
            ));
        }
    };
    let inner = call.args.args.get(1).ok_or_else(|| {
        QuerierError::InvalidInput("histogram_quantile expects (phi, selector)".to_string())
    })?;
    let Expr::VectorSelector(vs) = unwrap_paren(inner) else {
        return Err(QuerierError::Unsupported(
            "histogram_quantile over rate()/aggregations is not supported yet (#335); \
             use histogram_quantile(phi, metric)"
                .to_string(),
        ));
    };
    let mut plan = lower_selector(vs, MetricAgg::Last, Grouping::Natural, None)?;
    plan.quantile = Some(phi);
    Ok(plan)
}

/// Whether a function name is a supported unary math / clamp function.
fn is_math_fn(name: &str) -> bool {
    matches!(
        name,
        "abs"
            | "ceil"
            | "floor"
            | "round"
            | "sqrt"
            | "exp"
            | "ln"
            | "log2"
            | "log10"
            | "sgn"
            | "clamp_min"
            | "clamp_max"
            | "clamp"
    )
}

/// Lower a math function `f(vector, [scalars…])` by lowering the vector
/// argument and appending the corresponding value transform.
fn lower_math_call(call: &parser::Call) -> Result<MetricPlan, QuerierError> {
    let inner = call.args.args.first().ok_or_else(|| {
        QuerierError::InvalidInput(format!("{} expects an argument", call.func.name))
    })?;
    let op = match call.func.name {
        "abs" => ValueOp::Abs,
        "ceil" => ValueOp::Ceil,
        "floor" => ValueOp::Floor,
        "round" => ValueOp::Round,
        "sqrt" => ValueOp::Sqrt,
        "exp" => ValueOp::Exp,
        "ln" => ValueOp::Ln,
        "log2" => ValueOp::Log2,
        "log10" => ValueOp::Log10,
        "sgn" => ValueOp::Sgn,
        "clamp_min" => ValueOp::ClampMin(scalar_arg(call, 1)?),
        "clamp_max" => ValueOp::ClampMax(scalar_arg(call, 1)?),
        "clamp" => ValueOp::Clamp(scalar_arg(call, 1)?, scalar_arg(call, 2)?),
        other => {
            return Err(QuerierError::Unsupported(format!("function '{other}'")));
        }
    };
    let mut plan = lower(unwrap_paren(inner))?;
    plan.transforms.push(op);
    Ok(plan)
}

/// Lower a binary expression. Only `vector OP scalar` / `scalar OP vector`
/// arithmetic is supported; comparison, logical, and vector-to-vector
/// operations are not (#335).
fn lower_binary(bin: &parser::BinaryExpr) -> Result<MetricPlan, QuerierError> {
    if bin.op.is_comparison_operator() {
        return Err(QuerierError::Unsupported(
            "comparison operators".to_string(),
        ));
    }
    let arith = match format!("{}", bin.op).as_str() {
        "+" => ArithOp::Add,
        "-" => ArithOp::Sub,
        "*" => ArithOp::Mul,
        "/" => ArithOp::Div,
        "%" => ArithOp::Mod,
        "^" => ArithOp::Pow,
        other => {
            return Err(QuerierError::Unsupported(format!(
                "binary operator '{other}'"
            )));
        }
    };
    match (as_scalar(&bin.lhs), as_scalar(&bin.rhs)) {
        (Some(_), Some(_)) => Err(QuerierError::Unsupported(
            "constant-only arithmetic".to_string(),
        )),
        (None, None) => Err(QuerierError::Unsupported(
            "vector-to-vector binary operations".to_string(),
        )),
        // scalar OP vector
        (Some(s), None) => {
            let mut plan = lower(unwrap_paren(&bin.rhs))?;
            plan.transforms.push(ValueOp::Arith(arith, s, true));
            Ok(plan)
        }
        // vector OP scalar
        (None, Some(s)) => {
            let mut plan = lower(unwrap_paren(&bin.lhs))?;
            plan.transforms.push(ValueOp::Arith(arith, s, false));
            Ok(plan)
        }
    }
}

/// The `i`-th call argument as a numeric literal.
fn scalar_arg(call: &parser::Call, i: usize) -> Result<f64, QuerierError> {
    match call.args.args.get(i).map(|a| unwrap_paren(a)) {
        Some(Expr::NumberLiteral(n)) => Ok(n.val),
        _ => Err(QuerierError::Unsupported(format!(
            "{} requires a numeric literal argument",
            call.func.name
        ))),
    }
}

/// A parenthesized number literal, if the expression is one.
fn as_scalar(expr: &Expr) -> Option<f64> {
    match unwrap_paren(expr) {
        Expr::NumberLiteral(n) => Some(n.val),
        _ => None,
    }
}

fn unwrap_paren(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(p) => unwrap_paren(&p.expr),
        other => other,
    }
}

fn aggregate_op(op: &str) -> Result<MetricAgg, QuerierError> {
    Ok(match op {
        "sum" => MetricAgg::Sum,
        "avg" => MetricAgg::Avg,
        "min" => MetricAgg::Min,
        "max" => MetricAgg::Max,
        "count" => MetricAgg::Count,
        other => {
            return Err(QuerierError::Unsupported(format!(
                "aggregation operator '{other}'"
            )));
        }
    })
}

/// Map an `<agg>_over_time` function name to its per-bucket reducer.
fn over_time_agg(name: &str) -> Option<MetricAgg> {
    Some(match name {
        "avg_over_time" => MetricAgg::Avg,
        "sum_over_time" => MetricAgg::Sum,
        "min_over_time" => MetricAgg::Min,
        "max_over_time" => MetricAgg::Max,
        "count_over_time" => MetricAgg::Count,
        "last_over_time" => MetricAgg::Last,
        "stddev_over_time" => MetricAgg::Stddev,
        "stdvar_over_time" => MetricAgg::StdVar,
        _ => return None,
    })
}

fn grouping_from(modifier: Option<&LabelModifier>) -> Result<Grouping, QuerierError> {
    match modifier {
        None => Ok(Grouping::Collapse),
        Some(LabelModifier::Include(labels)) => Ok(Grouping::By(labels.labels.clone())),
        Some(LabelModifier::Exclude(labels)) if labels.is_empty() => Ok(Grouping::Collapse),
        Some(LabelModifier::Exclude(_)) => Err(QuerierError::Unsupported(
            "'without (labels)' grouping".to_string(),
        )),
    }
}

fn match_kind(op: &promql_parser::label::MatchOp) -> MatchKind {
    use promql_parser::label::MatchOp;
    match op {
        MatchOp::Equal => MatchKind::Eq,
        MatchOp::NotEqual => MatchKind::Neq,
        MatchOp::Re(_) => MatchKind::Re,
        MatchOp::NotRe(_) => MatchKind::Nre,
    }
}

fn expr_kind(expr: &Expr) -> &'static str {
    match expr {
        Expr::Aggregate(_) => "an aggregation",
        Expr::Unary(_) => "a unary expression",
        Expr::Binary(_) => "a binary expression",
        Expr::Paren(_) => "a parenthesized expression",
        Expr::Subquery(_) => "a subquery",
        Expr::NumberLiteral(_) => "a number literal",
        Expr::StringLiteral(_) => "a string literal",
        Expr::VectorSelector(_) => "a vector selector",
        Expr::MatrixSelector(_) => "a range-vector selector",
        Expr::Call(_) => "a function call",
        Expr::Extension(_) => "an extension expression",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(q: &str) -> MetricPlan {
        plan_promql(q).expect("plan")
    }

    fn err(q: &str) -> QuerierError {
        plan_promql(q).unwrap_err()
    }

    #[test]
    fn bare_selector() {
        let p = plan(r#"http_requests_total"#);
        assert_eq!(p.metric_name, "http_requests_total");
        assert!(p.matchers.is_empty());
        assert_eq!(p.aggregate, MetricAgg::Last);
        assert_eq!(p.grouping, Grouping::Natural);
    }

    #[test]
    fn selector_with_matchers() {
        let p = plan(r#"http_requests_total{job="api", code!="500", path=~"/v1/.*"}"#);
        assert_eq!(p.metric_name, "http_requests_total");
        assert_eq!(
            p.matchers,
            vec![
                LabelMatch {
                    name: "job".into(),
                    op: MatchKind::Eq,
                    value: "api".into()
                },
                LabelMatch {
                    name: "code".into(),
                    op: MatchKind::Neq,
                    value: "500".into()
                },
                LabelMatch {
                    name: "path".into(),
                    op: MatchKind::Re,
                    value: "/v1/.*".into()
                },
            ]
        );
    }

    #[test]
    fn name_via_name_matcher() {
        let p = plan(r#"{__name__="up", job="api"}"#);
        assert_eq!(p.metric_name, "up");
        assert_eq!(p.matchers.len(), 1);
        assert_eq!(p.matchers[0].name, "job");
    }

    #[test]
    fn sum_collapses_all_series() {
        let p = plan(r#"sum(http_requests_total)"#);
        assert_eq!(p.aggregate, MetricAgg::Sum);
        assert_eq!(p.grouping, Grouping::Collapse);
    }

    #[test]
    fn sum_by_labels() {
        let p = plan(r#"sum by (job, code) (http_requests_total{env="prod"})"#);
        assert_eq!(p.aggregate, MetricAgg::Sum);
        assert_eq!(p.grouping, Grouping::By(vec!["job".into(), "code".into()]));
        assert_eq!(p.matchers.len(), 1);
    }

    #[test]
    fn every_supported_aggregate() {
        for (q, a) in [
            ("avg(x)", MetricAgg::Avg),
            ("min(x)", MetricAgg::Min),
            ("max(x)", MetricAgg::Max),
            ("count(x)", MetricAgg::Count),
        ] {
            assert_eq!(plan(q).aggregate, a, "{q}");
        }
    }

    #[test]
    fn without_empty_collapses() {
        // `sum without () (x)` collapses like a bare `sum`.
        assert_eq!(plan(r#"sum without () (x)"#).grouping, Grouping::Collapse);
    }

    #[test]
    fn rate_and_increase() {
        let r = plan(r#"rate(http_requests_total{job="api"}[5m])"#);
        assert_eq!(r.metric_name, "http_requests_total");
        assert_eq!(r.matchers.len(), 1);
        assert_eq!(
            r.range,
            Some(RangeSpec {
                function: RangeFn::Rate,
                seconds: 300.0
            })
        );
        assert_eq!(r.grouping, Grouping::Natural);

        let inc = plan(r#"increase(errors[1m])"#);
        assert_eq!(
            inc.range,
            Some(RangeSpec {
                function: RangeFn::Increase,
                seconds: 60.0
            })
        );
    }

    #[test]
    fn sum_over_rate() {
        let p = plan(r#"sum(rate(http_requests_total[5m]))"#);
        assert_eq!(p.aggregate, MetricAgg::Sum);
        assert_eq!(p.grouping, Grouping::Collapse);
        assert_eq!(p.range.unwrap().function, RangeFn::Rate);
    }

    #[test]
    fn over_time_maps_to_reducer() {
        for (q, a) in [
            ("avg_over_time(x[5m])", MetricAgg::Avg),
            ("sum_over_time(x[5m])", MetricAgg::Sum),
            ("min_over_time(x[5m])", MetricAgg::Min),
            ("max_over_time(x[5m])", MetricAgg::Max),
            ("count_over_time(x[5m])", MetricAgg::Count),
            ("last_over_time(x[5m])", MetricAgg::Last),
            ("stddev_over_time(x[5m])", MetricAgg::Stddev),
            ("stdvar_over_time(x[5m])", MetricAgg::StdVar),
        ] {
            let p = plan(q);
            assert_eq!(p.aggregate, a, "{q}");
            assert_eq!(p.grouping, Grouping::Natural, "{q}");
            assert!(p.range.is_none(), "{q}");
        }
    }

    #[test]
    fn over_time_under_aggregation_unsupported() {
        assert!(matches!(
            err(r#"sum(avg_over_time(x[5m]))"#),
            QuerierError::Unsupported(_)
        ));
    }

    #[test]
    fn math_functions_append_transforms() {
        assert_eq!(plan("abs(x)").transforms, vec![ValueOp::Abs]);
        assert_eq!(plan("ceil(x)").transforms, vec![ValueOp::Ceil]);
        assert_eq!(plan("sqrt(x)").transforms, vec![ValueOp::Sqrt]);
        assert_eq!(plan("sgn(x)").transforms, vec![ValueOp::Sgn]);
        assert_eq!(
            plan("clamp_min(x, 0)").transforms,
            vec![ValueOp::ClampMin(0.0)]
        );
        assert_eq!(
            plan("clamp_max(x, 5)").transforms,
            vec![ValueOp::ClampMax(5.0)]
        );
        assert_eq!(
            plan("clamp(x, 1, 9)").transforms,
            vec![ValueOp::Clamp(1.0, 9.0)]
        );
    }

    #[test]
    fn scalar_arithmetic_appends_transform() {
        assert_eq!(
            plan("x * 8").transforms,
            vec![ValueOp::Arith(ArithOp::Mul, 8.0, false)]
        );
        assert_eq!(
            plan("1024 / x").transforms,
            vec![ValueOp::Arith(ArithOp::Div, 1024.0, true)]
        );
        assert_eq!(
            plan("x + 1").transforms,
            vec![ValueOp::Arith(ArithOp::Add, 1.0, false)]
        );
    }

    #[test]
    fn math_over_rate_keeps_range_and_transform() {
        let p = plan("abs(rate(x[5m]))");
        assert_eq!(p.range.unwrap().function, RangeFn::Rate);
        assert_eq!(p.transforms, vec![ValueOp::Abs]);
    }

    #[test]
    fn nested_transforms_order_inner_to_outer() {
        // abs(x) * 2 → abs first, then *2.
        let p = plan("abs(x) * 2");
        assert_eq!(
            p.transforms,
            vec![ValueOp::Abs, ValueOp::Arith(ArithOp::Mul, 2.0, false)]
        );
    }

    #[test]
    fn vector_binary_and_comparison_unsupported() {
        assert!(matches!(err("x + y"), QuerierError::Unsupported(_)));
        assert!(matches!(err("x > 5"), QuerierError::Unsupported(_)));
    }

    #[test]
    fn unsupported_shapes() {
        assert!(matches!(
            err(r#"irate(x[5m])"#),
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(err(r#"topk(5, x)"#), QuerierError::Unsupported(_)));
        assert!(matches!(err(r#"x + y"#), QuerierError::Unsupported(_)));
        assert!(matches!(
            err(r#"sum without (job) (x)"#),
            QuerierError::Unsupported(_)
        ));
        // histogram_quantile over an inner rate() is not lowered yet.
        assert!(matches!(
            err(r#"histogram_quantile(0.9, rate(x[5m]))"#),
            QuerierError::Unsupported(_)
        ));
    }

    #[test]
    fn histogram_quantile_over_selector() {
        let p = plan(r#"histogram_quantile(0.95, http_request_duration_seconds{job="api"})"#);
        assert_eq!(p.metric_name, "http_request_duration_seconds");
        assert_eq!(p.quantile, Some(0.95));
        assert_eq!(p.matchers.len(), 1);
        assert_eq!(p.grouping, Grouping::Natural);
        assert!(p.range.is_none());
    }

    #[test]
    fn non_histogram_plan_has_no_quantile() {
        assert_eq!(plan(r#"http_requests_total"#).quantile, None);
        assert_eq!(plan(r#"sum(rate(x[5m]))"#).quantile, None);
    }

    #[test]
    fn invalid_promql_is_rejected() {
        assert!(matches!(err(r#"sum(("#), QuerierError::InvalidInput(_)));
    }
}
