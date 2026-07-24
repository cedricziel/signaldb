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
//!
//! `irate`, binary operators, subqueries, and `histogram_quantile` over an
//! inner `rate()`/aggregation are not lowered yet and return
//! [`QuerierError::Unsupported`] (#335).
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
        Expr::Binary(_) => Err(QuerierError::Unsupported(
            "binary operations between metric queries".to_string(),
        )),
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
