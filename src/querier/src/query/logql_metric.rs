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
//!   `bytes_rate`, and the unwrap-based `sum/avg/min/max_over_time`.
//! - A single outer `sum by (...)` / `sum without ()` vector aggregation
//!   wrapping one of the above (sum-of-counts folds into a grouped
//!   count/rate).
//!
//! Binary operators, `topk`/`bottomk`, `quantile*`, `vector()`,
//! `label_replace`, and non-`sum` outer aggregations return
//! [`QuerierError::Unsupported`].

use logql::{AggregationFunction, Grouping, LogQuery, MetricQuery, RangeFunction};

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
    /// The `[range]` window in nanoseconds (informational; bucketing uses
    /// the caller's step).
    pub range_ns: u128,
}

/// Lower a metric query to a [`MetricPlan`].
pub fn plan_metric_query(query: &MetricQuery) -> Result<MetricPlan, QuerierError> {
    match query {
        MetricQuery::Range(range) => plan_range(range, Vec::new()),
        MetricQuery::Vector(vector) => {
            // Only a `sum` outer aggregation folds cleanly into grouped
            // counts/rates; others need per-series values we don't
            // materialize.
            if vector.function != AggregationFunction::Sum {
                return Err(QuerierError::Unsupported(format!(
                    "outer aggregation '{:?}' over a range aggregation",
                    vector.function
                )));
            }
            if vector.param.is_some() {
                return Err(QuerierError::Unsupported(
                    "parameterized outer aggregation".to_string(),
                ));
            }
            let group_labels = grouping_labels(vector.grouping.as_ref())?;
            match &vector.inner {
                MetricQuery::Range(range) => plan_range(range, group_labels),
                other => Err(QuerierError::Unsupported(format!(
                    "nested metric expression: {other:?}"
                ))),
            }
        }
        MetricQuery::Binary(_) => Err(QuerierError::Unsupported(
            "binary operations between metric queries".to_string(),
        )),
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
        range_ns: range.range.as_nanos(),
    })
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
    fn unsupported_shapes_are_rejected() {
        assert!(matches!(
            err(r#"topk(5, rate({a="b"}[5m]))"#),
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(
            err(r#"avg(rate({a="b"}[5m]))"#),
            QuerierError::Unsupported(_)
        ));
        assert!(matches!(
            err(r#"quantile_over_time(0.9, {a="b"} | unwrap x [5m])"#),
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
