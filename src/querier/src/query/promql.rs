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
//! Range-vector functions (`rate`, `increase`, `irate`), binary
//! operators, subqueries, and `histogram_quantile` are not lowered here
//! yet and return [`QuerierError::Unsupported`] (tracked in #334/#335).
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

/// A lowered PromQL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricPlan {
    /// The metric name being queried.
    pub metric_name: String,
    /// Label matchers to filter series (excluding `__name__`).
    pub matchers: Vec<LabelMatch>,
    /// The aggregate computed per bucket per series.
    pub aggregate: MetricAgg,
    pub grouping: Grouping,
}

/// Parse and lower a PromQL query.
pub fn plan_promql(query: &str) -> Result<MetricPlan, QuerierError> {
    let expr = parser::parse(query)
        .map_err(|e| QuerierError::InvalidInput(format!("invalid PromQL: {e}")))?;
    lower(&expr)
}

fn lower(expr: &Expr) -> Result<MetricPlan, QuerierError> {
    match expr {
        Expr::VectorSelector(vs) => lower_selector(vs, MetricAgg::Last, Grouping::Natural),
        Expr::Paren(p) => lower(&p.expr),
        Expr::Aggregate(agg) => {
            let aggregate = aggregate_op(&format!("{}", agg.op))?;
            if agg.param.is_some() {
                return Err(QuerierError::Unsupported(
                    "parameterized aggregation (topk/quantile/count_values)".to_string(),
                ));
            }
            let grouping = grouping_from(agg.modifier.as_ref())?;
            match unwrap_paren(&agg.expr) {
                Expr::VectorSelector(vs) => lower_selector(vs, aggregate, grouping),
                other => Err(QuerierError::Unsupported(format!(
                    "aggregation over {}",
                    expr_kind(other)
                ))),
            }
        }
        Expr::Call(call) => Err(QuerierError::Unsupported(format!(
            "function '{}' (range functions land in #334)",
            call.func.name
        ))),
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

fn lower_selector(
    vs: &parser::VectorSelector,
    aggregate: MetricAgg,
    grouping: Grouping,
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
    })
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
    fn unsupported_shapes() {
        assert!(matches!(
            err(r#"rate(x[5m])"#),
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(err(r#"topk(5, x)"#), QuerierError::Unsupported(_)));
        assert!(matches!(err(r#"x + y"#), QuerierError::Unsupported(_)));
        assert!(matches!(
            err(r#"sum without (job) (x)"#),
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(
            err(r#"histogram_quantile(0.9, x)"#),
            QuerierError::Unsupported(_)
        ));
    }

    #[test]
    fn invalid_promql_is_rejected() {
        assert!(matches!(err(r#"sum(("#), QuerierError::InvalidInput(_)));
    }
}
