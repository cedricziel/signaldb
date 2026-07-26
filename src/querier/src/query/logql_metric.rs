//! # LogQL metric-query lowering
//!
//! Lowers a parsed [`logql::MetricQuery`] into a [`MetricPlan`] — a
//! backend-neutral description of the aggregation the querier executes:
//! which log rows to scan, how to bucket them in time, what to compute
//! per bucket, and how to group series. The [`super::logs`] service turns
//! this into a DataFusion aggregate over the logs table.
//!
//! ## Scope and approximation
//!
//! Range aggregations are evaluated over **fixed, step-aligned buckets**
//! (`date_bin(step, timestamp)`), not Loki's sliding `[range]` window —
//! exact when the caller's step equals the range (Grafana's default for
//! `count_over_time` panels), an approximation otherwise. The common
//! shapes are supported:
//!
//! - Range aggregations: `count_over_time`, `rate`, `bytes_over_time`,
//!   `bytes_rate`, and the unwrap-based `sum/avg/min/max_over_time`,
//!   `stddev/stdvar_over_time`, `first/last_over_time`, and
//!   `quantile_over_time`.
//! - A single outer vector aggregation wrapping one of the above:
//!   `sum` (sum-of-counts folds into a grouped count/rate), and
//!   `avg` / `min` / `max` / `count` / `stddev` / `stdvar`, which reduce
//!   the range aggregation across series within each group (see
//!   [`OuterAgg`]).
//! - `topk` / `bottomk`, which keep the `k` highest/lowest series per
//!   bucket after aggregation (see [`TopKSpec`]).
//! - Vector-scalar arithmetic (`expr * 2`, `expr / 60`), folded into the
//!   plan as a [`ScalarOp`].
//!
//! Vector-to-vector binary operators, comparisons, logical/set operators,
//! `sort`/`sort_desc`, `vector()`, and `label_replace` return
//! [`QuerierError::Unsupported`].

use logql::{AggregationFunction, BinOp, Grouping, LogQuery, MetricQuery, RangeFunction};

use super::error::QuerierError;

/// The per-bucket, per-series aggregate to compute.
#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    /// `count(*)` — row count in the bucket.
    Count,
    /// `sum(character_length(body))` — total log bytes.
    BytesSum,
    /// `sum(<unwrapped label>)`.
    UnwrapSum(String),
    /// `avg(<unwrapped label>)`.
    UnwrapAvg(String),
    /// `min(<unwrapped label>)`.
    UnwrapMin(String),
    /// `max(<unwrapped label>)`.
    UnwrapMax(String),
    /// `approx_percentile_cont(<unwrapped label>, <quantile>)` — the
    /// φ-quantile of the unwrapped sample values in the bucket.
    UnwrapQuantile { quantile: f64, label: String },
    /// `stddev_pop(<unwrapped label>)` — population standard deviation of
    /// the unwrapped sample values in the bucket.
    UnwrapStddev(String),
    /// `var_pop(<unwrapped label>)` — population variance of the unwrapped
    /// sample values in the bucket.
    UnwrapStdvar(String),
    /// `first_value(<unwrapped label> order by timestamp)` — the unwrapped
    /// value of the earliest sample in the bucket.
    UnwrapFirst(String),
    /// `last_value(<unwrapped label> order by timestamp)` — the unwrapped
    /// value of the latest sample in the bucket.
    UnwrapLast(String),
}

/// A non-`sum` outer vector aggregation that reduces the per-series range
/// aggregation across the series in each group.
///
/// `sum` is absent because it folds directly into the grouped range
/// aggregate (sum-of-counts == count-of-group) and needs no second pass;
/// the others genuinely reduce across per-series values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OuterAgg {
    /// `avg(...)` — mean of the per-series values.
    Avg,
    /// `min(...)` — smallest per-series value.
    Min,
    /// `max(...)` — largest per-series value.
    Max,
    /// `count(...)` — number of series with data in the bucket.
    Count,
    /// `stddev(...)` — population standard deviation across series.
    Stddev,
    /// `stdvar(...)` — population variance across series.
    Stdvar,
}

/// A binary arithmetic operator (`+`, `-`, `*`, `/`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// A scalar arithmetic operation applied to every value of a metric
/// query, e.g. `rate(...) / 60` or `100 * rate(...)`. `scalar_is_lhs`
/// records the operand order so non-commutative `-` / `/` stay correct.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScalarOp {
    pub op: ArithOp,
    pub scalar: f64,
    pub scalar_is_lhs: bool,
}

/// `topk(k, ...)` / `bottomk(k, ...)`: keep the `k` highest (or lowest)
/// series by value in each time bucket, applied after the range and any
/// outer aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopKSpec {
    /// How many series to keep per bucket.
    pub k: usize,
    /// `true` for `bottomk` (keep the lowest), `false` for `topk`.
    pub bottom: bool,
}

/// A lowered metric query.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricPlan {
    /// The log selector and pipeline whose matching rows are aggregated.
    pub log_query: LogQuery,
    /// Labels to group series by (logical names, mapped to columns by the
    /// executor). Empty means group by the natural series columns.
    pub group_labels: Vec<String>,
    /// The aggregate computed per bucket per series.
    pub aggregate: Aggregate,
    /// When set, divide the aggregate by this many seconds (`rate`).
    pub rate_divisor_seconds: Option<f64>,
    /// A non-`sum` outer aggregation to apply across series after the range
    /// aggregation. When set, `group_labels` names the surviving group
    /// (empty = collapse across all series); when `None`, the range
    /// aggregation is grouped directly by `group_labels`.
    pub outer_agg: Option<OuterAgg>,
    /// A scalar arithmetic op applied to every value after aggregation and
    /// before any `topk`/`bottomk` ranking.
    pub scalar_op: Option<ScalarOp>,
    /// A `topk`/`bottomk` per-bucket ranking to apply after aggregation.
    pub topk: Option<TopKSpec>,
    /// The `[range]` window in nanoseconds (informational; bucketing uses
    /// the caller's step).
    pub range_ns: u128,
}

/// Lower a metric query to a [`MetricPlan`].
pub fn plan_metric_query(query: &MetricQuery) -> Result<MetricPlan, QuerierError> {
    match query {
        MetricQuery::Range(range) => plan_range(range, Vec::new()),
        MetricQuery::Vector(vector) => {
            // `topk`/`bottomk` rank the inner query's series per bucket:
            // lower the inner, then attach a `TopKSpec` post-pass.
            if let AggregationFunction::Topk | AggregationFunction::Bottomk = vector.function {
                let bottom = vector.function == AggregationFunction::Bottomk;
                let k = vector
                    .param
                    .filter(|k| k.is_finite() && *k >= 0.0)
                    .ok_or_else(|| {
                        QuerierError::Unsupported(
                            "topk/bottomk requires a non-negative k parameter".to_string(),
                        )
                    })?;
                if vector.grouping.is_some() {
                    return Err(QuerierError::Unsupported(
                        "grouped topk/bottomk".to_string(),
                    ));
                }
                let mut plan = plan_metric_query(&vector.inner)?;
                if plan.topk.is_some() {
                    return Err(QuerierError::Unsupported("nested topk/bottomk".to_string()));
                }
                plan.topk = Some(TopKSpec {
                    k: k as usize,
                    bottom,
                });
                return Ok(plan);
            }

            // `sum` folds into the grouped range aggregate (`outer_agg`
            // stays `None`); `avg`/`min`/`max`/`count` reduce across series
            // in a second pass. Other parameterized or exotic aggregations
            // still need per-series machinery we lack.
            let outer_agg = match vector.function {
                AggregationFunction::Sum => None,
                AggregationFunction::Avg => Some(OuterAgg::Avg),
                AggregationFunction::Min => Some(OuterAgg::Min),
                AggregationFunction::Max => Some(OuterAgg::Max),
                AggregationFunction::Count => Some(OuterAgg::Count),
                AggregationFunction::Stddev => Some(OuterAgg::Stddev),
                AggregationFunction::Stdvar => Some(OuterAgg::Stdvar),
                other => {
                    return Err(QuerierError::Unsupported(format!(
                        "outer aggregation '{other:?}' over a range aggregation"
                    )));
                }
            };
            if vector.param.is_some() {
                return Err(QuerierError::Unsupported(
                    "parameterized outer aggregation".to_string(),
                ));
            }
            let group_labels = grouping_labels(vector.grouping.as_ref())?;
            match &vector.inner {
                MetricQuery::Range(range) => {
                    let mut plan = plan_range(range, group_labels)?;
                    plan.outer_agg = outer_agg;
                    Ok(plan)
                }
                other => Err(QuerierError::Unsupported(format!(
                    "nested metric expression: {other:?}"
                ))),
            }
        }
        MetricQuery::Binary(bin) => plan_binary(bin),
        MetricQuery::Literal(_) | MetricQuery::VectorLiteral(_) => Err(QuerierError::Unsupported(
            "scalar/vector literal metric query".to_string(),
        )),
        MetricQuery::LabelReplace(_) => Err(QuerierError::Unsupported("label_replace".to_string())),
    }
}

fn plan_range(
    range: &logql::RangeAggregation,
    group_labels: Vec<String>,
) -> Result<MetricPlan, QuerierError> {
    let range_seconds = range.range.as_secs_f64();
    let unwrap_label = || {
        range
            .unwrap
            .as_ref()
            .map(|u| u.label.clone())
            .ok_or_else(|| {
                QuerierError::Unsupported(format!(
                    "{:?} requires an unwrap expression",
                    range.function
                ))
            })
    };

    let (aggregate, rate_divisor_seconds) = match range.function {
        RangeFunction::CountOverTime => (Aggregate::Count, None),
        RangeFunction::Rate => (Aggregate::Count, Some(range_seconds)),
        RangeFunction::BytesOverTime => (Aggregate::BytesSum, None),
        RangeFunction::BytesRate => (Aggregate::BytesSum, Some(range_seconds)),
        RangeFunction::SumOverTime => (Aggregate::UnwrapSum(unwrap_label()?), None),
        RangeFunction::AvgOverTime => (Aggregate::UnwrapAvg(unwrap_label()?), None),
        RangeFunction::MinOverTime => (Aggregate::UnwrapMin(unwrap_label()?), None),
        RangeFunction::MaxOverTime => (Aggregate::UnwrapMax(unwrap_label()?), None),
        RangeFunction::StddevOverTime => (Aggregate::UnwrapStddev(unwrap_label()?), None),
        RangeFunction::StdvarOverTime => (Aggregate::UnwrapStdvar(unwrap_label()?), None),
        RangeFunction::FirstOverTime => (Aggregate::UnwrapFirst(unwrap_label()?), None),
        RangeFunction::LastOverTime => (Aggregate::UnwrapLast(unwrap_label()?), None),
        RangeFunction::QuantileOverTime => {
            let quantile = range.param.ok_or_else(|| {
                QuerierError::Unsupported(
                    "quantile_over_time requires a quantile parameter".to_string(),
                )
            })?;
            (
                Aggregate::UnwrapQuantile {
                    quantile,
                    label: unwrap_label()?,
                },
                None,
            )
        }
        other => {
            return Err(QuerierError::Unsupported(format!(
                "range function {other:?}"
            )));
        }
    };

    // A grouping directly on the range aggregation (unwrapped forms) is
    // honored too, unless the caller already supplied an outer grouping.
    let group_labels = if group_labels.is_empty() {
        grouping_labels(range.grouping.as_ref())?
    } else {
        group_labels
    };

    Ok(MetricPlan {
        log_query: range.log_query.clone(),
        group_labels,
        aggregate,
        rate_divisor_seconds,
        outer_agg: None,
        scalar_op: None,
        topk: None,
        range_ns: range.range.as_nanos(),
    })
}

/// Lower a binary expression. Only **vector-scalar arithmetic** is
/// supported: one operand is a scalar literal and the other a metric
/// query, combined with `+`/`-`/`*`/`/`. The scalar folds into the
/// metric query's plan as a [`ScalarOp`]. Vector-to-vector operations,
/// comparisons, and logical/set operators are not supported yet.
fn plan_binary(bin: &logql::BinaryExpr) -> Result<MetricPlan, QuerierError> {
    let op = match bin.op {
        BinOp::Add => ArithOp::Add,
        BinOp::Sub => ArithOp::Sub,
        BinOp::Mul => ArithOp::Mul,
        BinOp::Div => ArithOp::Div,
        other => {
            return Err(QuerierError::Unsupported(format!(
                "binary operator {other:?} between metric queries"
            )));
        }
    };

    let (scalar, vector, scalar_is_lhs) = match (&bin.left, &bin.right) {
        (MetricQuery::Literal(s), rhs) if !is_literal(rhs) => (*s, rhs, true),
        (lhs, MetricQuery::Literal(s)) if !is_literal(lhs) => (*s, lhs, false),
        (MetricQuery::Literal(_), MetricQuery::Literal(_)) => {
            return Err(QuerierError::Unsupported(
                "arithmetic between two scalars".to_string(),
            ));
        }
        _ => {
            return Err(QuerierError::Unsupported(
                "binary operation between two metric queries".to_string(),
            ));
        }
    };

    let mut plan = plan_metric_query(vector)?;
    if plan.scalar_op.is_some() {
        return Err(QuerierError::Unsupported(
            "chained scalar arithmetic".to_string(),
        ));
    }
    plan.scalar_op = Some(ScalarOp {
        op,
        scalar,
        scalar_is_lhs,
    });
    Ok(plan)
}

/// Whether a metric query is a bare scalar literal.
fn is_literal(query: &MetricQuery) -> bool {
    matches!(
        query,
        MetricQuery::Literal(_) | MetricQuery::VectorLiteral(_)
    )
}

/// Extract the `by (...)` labels. `without` is only supported when empty
/// (i.e. aggregate across all series), since resolving its complement
/// needs the full label set.
fn grouping_labels(grouping: Option<&Grouping>) -> Result<Vec<String>, QuerierError> {
    match grouping {
        None => Ok(Vec::new()),
        Some(g) if !g.without => Ok(g.labels.clone()),
        Some(g) if g.labels.is_empty() => Ok(Vec::new()),
        Some(_) => Err(QuerierError::Unsupported(
            "'without (labels)' grouping".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logql::parse;

    fn plan(query: &str) -> MetricPlan {
        let logql::Expr::Metric(mq) = parse(query).expect("parse") else {
            panic!("expected a metric query");
        };
        plan_metric_query(&mq).expect("plan")
    }

    fn err(query: &str) -> QuerierError {
        let logql::Expr::Metric(mq) = parse(query).expect("parse") else {
            panic!("expected a metric query");
        };
        plan_metric_query(&mq).unwrap_err()
    }

    #[test]
    fn count_over_time_is_a_plain_count() {
        let p = plan(r#"count_over_time({service_name="api"}[5m])"#);
        assert_eq!(p.aggregate, Aggregate::Count);
        assert_eq!(p.rate_divisor_seconds, None);
        assert!(p.group_labels.is_empty());
        assert_eq!(p.log_query.selector.matchers.len(), 1);
    }

    #[test]
    fn rate_divides_by_the_range_seconds() {
        let p = plan(r#"rate({service_name="api"}[5m])"#);
        assert_eq!(p.aggregate, Aggregate::Count);
        assert_eq!(p.rate_divisor_seconds, Some(300.0));
    }

    #[test]
    fn bytes_functions() {
        assert_eq!(
            plan(r#"bytes_over_time({a="b"}[1m])"#).aggregate,
            Aggregate::BytesSum
        );
        let br = plan(r#"bytes_rate({a="b"}[1m])"#);
        assert_eq!(br.aggregate, Aggregate::BytesSum);
        assert_eq!(br.rate_divisor_seconds, Some(60.0));
    }

    #[test]
    fn unwrap_aggregations() {
        assert_eq!(
            plan(r#"sum_over_time({a="b"} | unwrap latency [5m])"#).aggregate,
            Aggregate::UnwrapSum("latency".to_string())
        );
        assert_eq!(
            plan(r#"avg_over_time({a="b"} | unwrap latency [5m])"#).aggregate,
            Aggregate::UnwrapAvg("latency".to_string())
        );
        assert_eq!(
            plan(r#"max_over_time({a="b"} | unwrap latency [5m])"#).aggregate,
            Aggregate::UnwrapMax("latency".to_string())
        );
    }

    #[test]
    fn quantile_over_time_carries_the_quantile_and_label() {
        assert_eq!(
            plan(r#"quantile_over_time(0.9, {a="b"} | unwrap latency [5m])"#).aggregate,
            Aggregate::UnwrapQuantile {
                quantile: 0.9,
                label: "latency".to_string()
            }
        );
        // Missing the `| unwrap` is rejected (no samples to rank).
        assert!(matches!(
            err(r#"quantile_over_time(0.9, {a="b"}[5m])"#),
            QuerierError::Unsupported(_)
        ));
    }

    #[test]
    fn stddev_and_stdvar_over_time_are_unwrap_aggregations() {
        assert_eq!(
            plan(r#"stddev_over_time({a="b"} | unwrap latency [5m])"#).aggregate,
            Aggregate::UnwrapStddev("latency".to_string())
        );
        assert_eq!(
            plan(r#"stdvar_over_time({a="b"} | unwrap latency [5m])"#).aggregate,
            Aggregate::UnwrapStdvar("latency".to_string())
        );
        // Both require an unwrap expression.
        assert!(matches!(
            err(r#"stddev_over_time({a="b"}[5m])"#),
            QuerierError::Unsupported(_)
        ));
    }

    #[test]
    fn first_and_last_over_time_are_unwrap_aggregations() {
        assert_eq!(
            plan(r#"first_over_time({a="b"} | unwrap latency [5m])"#).aggregate,
            Aggregate::UnwrapFirst("latency".to_string())
        );
        assert_eq!(
            plan(r#"last_over_time({a="b"} | unwrap latency [5m])"#).aggregate,
            Aggregate::UnwrapLast("latency".to_string())
        );
        assert!(matches!(
            err(r#"first_over_time({a="b"}[5m])"#),
            QuerierError::Unsupported(_)
        ));
    }

    #[test]
    fn sum_by_pushes_grouping_down() {
        let p = plan(r#"sum by (level) (count_over_time({a="b"}[5m]))"#);
        assert_eq!(p.aggregate, Aggregate::Count);
        assert_eq!(p.group_labels, vec!["level".to_string()]);
    }

    #[test]
    fn sum_by_over_rate_keeps_the_divisor() {
        let p = plan(r#"sum by (level) (rate({a="b"}[5m]))"#);
        assert_eq!(p.rate_divisor_seconds, Some(300.0));
        assert_eq!(p.group_labels, vec!["level".to_string()]);
    }

    #[test]
    fn range_level_grouping_is_honored() {
        let p = plan(r#"avg_over_time({a="b"} | unwrap x [5m]) by (level)"#);
        assert_eq!(p.group_labels, vec!["level".to_string()]);
    }

    #[test]
    fn non_sum_outer_aggregations_reduce_across_series() {
        // `sum` folds into the grouped range aggregate (no second pass).
        let s = plan(r#"sum by (level) (rate({a="b"}[5m]))"#);
        assert_eq!(s.outer_agg, None);
        assert_eq!(s.group_labels, vec!["level".to_string()]);

        // The others carry an `OuterAgg` for the cross-series reduction.
        for (query, expected) in [
            (r#"avg(rate({a="b"}[5m]))"#, OuterAgg::Avg),
            (r#"min(rate({a="b"}[5m]))"#, OuterAgg::Min),
            (r#"max by (level) (rate({a="b"}[5m]))"#, OuterAgg::Max),
            (
                r#"count by (level) (count_over_time({a="b"}[5m]))"#,
                OuterAgg::Count,
            ),
            (r#"stddev(rate({a="b"}[5m]))"#, OuterAgg::Stddev),
            (r#"stdvar by (level) (rate({a="b"}[5m]))"#, OuterAgg::Stdvar),
        ] {
            let p = plan(query);
            assert_eq!(p.outer_agg, Some(expected), "{query}");
        }

        // `by` labels survive onto the plan for the second pass.
        assert_eq!(
            plan(r#"max by (level) (rate({a="b"}[5m]))"#).group_labels,
            vec!["level".to_string()]
        );
        // No `by`: collapse across all series (empty group).
        assert!(plan(r#"avg(rate({a="b"}[5m]))"#).group_labels.is_empty());
    }

    #[test]
    fn topk_and_bottomk_rank_the_inner_series() {
        let t = plan(r#"topk(5, rate({a="b"}[5m]))"#);
        assert_eq!(
            t.topk,
            Some(TopKSpec {
                k: 5,
                bottom: false
            })
        );
        // The inner range aggregation is preserved underneath.
        assert_eq!(t.aggregate, Aggregate::Count);
        assert_eq!(t.rate_divisor_seconds, Some(300.0));

        let b = plan(r#"bottomk(3, count_over_time({a="b"}[5m]))"#);
        assert_eq!(b.topk, Some(TopKSpec { k: 3, bottom: true }));

        // topk over a grouped inner keeps the grouping.
        let g = plan(r#"topk(2, sum by (level) (rate({a="b"}[5m])))"#);
        assert_eq!(
            g.topk,
            Some(TopKSpec {
                k: 2,
                bottom: false
            })
        );
        assert_eq!(g.group_labels, vec!["level".to_string()]);
    }

    #[test]
    fn vector_scalar_arithmetic_folds_into_the_plan() {
        // Scalar on the right.
        let p = plan(r#"rate({a="b"}[5m]) / 60"#);
        assert_eq!(
            p.scalar_op,
            Some(ScalarOp {
                op: ArithOp::Div,
                scalar: 60.0,
                scalar_is_lhs: false
            })
        );
        assert_eq!(p.aggregate, Aggregate::Count);

        // Scalar on the left records the operand order.
        let l = plan(r#"100 * count_over_time({a="b"}[5m])"#);
        assert_eq!(
            l.scalar_op,
            Some(ScalarOp {
                op: ArithOp::Mul,
                scalar: 100.0,
                scalar_is_lhs: true
            })
        );

        // Vector-to-vector arithmetic and comparisons are still rejected.
        assert!(matches!(
            err(r#"rate({a="b"}[5m]) / rate({a="c"}[5m])"#),
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(
            err(r#"rate({a="b"}[5m]) > 5"#),
            QuerierError::Unsupported(_)
        ));
    }

    #[test]
    fn unsupported_shapes_are_rejected() {
        assert!(matches!(
            err(r#"sort(rate({a="b"}[5m]))"#),
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(
            err(r#"sum(rate({a="b"}[1m])) / sum(rate({a="c"}[1m]))"#),
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(
            err(r#"sum_over_time({a="b"}[5m])"#), // no unwrap
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(
            err(r#"sum without (level) (rate({a="b"}[5m]))"#),
            QuerierError::Unsupported(_)
        ));
    }
}
