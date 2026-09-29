//! Plans a histogram statistic over the metric point stream: each point joins
//! every evaluation instant whose window covers it, then [`histogram_udaf`]
//! reduces each (group, instant).

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions_nested::expr_fn::cardinality;
use datafusion::logical_expr::{Expr, cast, col, lit};
use datafusion::prelude::{DataFrame, ident};
use datafusion::scalar::ScalarValue;

use super::hist::{HistStat, histogram_udaf};
use super::hist_math::Mode;
use super::hist_state::column_types;
use super::instants::covering_instants;
use crate::query::error::QuerierError;
use crate::query::histogram::NON_SCALAR_METRIC_TYPES;
use crate::query::table_lookup::metric_type_filter;

/// The stored columns [`histogram_udaf`] reads, in argument order.
const POINT_COLUMNS: [&str; 18] = [
    "series_id",
    "timestamp",
    "start_timestamp",
    "aggregation_temporality",
    "metric_type",
    "explicit_bounds",
    "bucket_counts",
    "count",
    "sum",
    "scale",
    "zero_count",
    "zero_threshold",
    "positive_offset",
    "positive_bucket_counts",
    "negative_offset",
    "negative_bucket_counts",
    "min",
    "max",
];

const INSTANT: &str = "__instant";

/// One histogram statistic evaluated at `first + k·step <= last`, each instant
/// `t` reading the window `(t - window, t]` and labelled `t + offset`.
pub(crate) struct HistEval {
    pub stat: HistStat,
    pub mode: Mode,
    pub first_ns: i64,
    pub last_ns: i64,
    pub step_ns: i64,
    pub window_ns: i64,
    pub offset_ns: i64,
}

/// Reduces metric points to `bucket` (the evaluation instant), the `groups`
/// aliases (Utf8) and a non-null Float64 `value` column. `df` must already
/// hold the points of `(first - window, last]`. A column the table lacks
/// reads as null.
pub(crate) fn histogram_series(
    df: DataFrame,
    groups: &[(Expr, String)],
    eval: &HistEval,
    value: &str,
) -> Result<DataFrame, QuerierError> {
    let present: HashSet<String> = df
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut proj: Vec<Expr> = groups
        .iter()
        .map(|(e, alias)| cast(e.clone(), DataType::Utf8).alias(alias))
        .collect();
    let mut args = Vec::with_capacity(POINT_COLUMNS.len() + 1);
    for (i, (name, t)) in POINT_COLUMNS.iter().zip(column_types()).enumerate() {
        let e = if present.contains(*name) {
            col(*name)
        } else {
            lit(ScalarValue::try_from(&t).map_err(QuerierError::QueryFailed)?)
        };
        let arg = format!("__h{i}");
        proj.push(e.alias(&arg));
        args.push(ident(arg));
    }
    args.push(ident(INSTANT));
    proj.push(
        covering_instants(eval.first_ns, eval.last_ns, eval.step_ns, eval.window_ns).alias(INSTANT),
    );
    let mut keys: Vec<Expr> = groups.iter().map(|(_, a)| ident(a)).collect();
    keys.push(ident(INSTANT));
    let udaf = histogram_udaf(eval.stat, eval.mode, eval.window_ns);
    let mut out = vec![
        cast(
            ident(INSTANT) + lit(eval.offset_ns),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .alias("bucket"),
    ];
    out.extend(groups.iter().map(|(_, a)| ident(a)));
    out.push(ident(value));
    df.filter(metric_type_filter(NON_SCALAR_METRIC_TYPES).and(fits_bounds()))
        .and_then(|df| df.select(proj))
        .and_then(|df| df.unnest_columns(&[INSTANT]))
        .and_then(|df| df.filter(ident(INSTANT).is_not_null()))
        .and_then(|df| df.aggregate(keys, vec![udaf.call(args).alias(value)]))
        .and_then(|df| df.select(out))
        .and_then(|df| df.filter(ident(value).is_not_null()))
        .map_err(QuerierError::QueryFailed)
}

/// An exponential point, or an explicit one whose `bucket_counts` hold one
/// more entry than its non-empty `explicit_bounds`; other explicit rows are
/// skipped, as the row-wise quantile always did.
fn fits_bounds() -> Expr {
    let bounds = || cardinality(col("explicit_bounds"));
    col("metric_type").not_eq(lit("histogram")).or(bounds()
        .gt(lit(0u64))
        .and(cardinality(col("bucket_counts")).eq(bounds() + lit(1u64))))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{Float64Type, TimestampNanosecondType};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::query::metric_ops::fixtures::histogram_points;

    /// One service's two cumulative series `s1`, `s2`.
    const TWO_SERIES: &[(&str, i64, &[i64])] = &[
        ("s1", 10, &[1, 1, 0, 0]),
        ("s2", 15, &[0, 1, 0, 0]),
        ("s1", 20, &[2, 2, 1, 0]),
        ("s2", 25, &[0, 2, 1, 0]),
        ("s1", 30, &[3, 4, 2, 0]),
        ("s2", 35, &[1, 3, 1, 0]),
    ];

    /// `(instant, p50)` at instants 20 and 40, each reading the 30ns before it.
    async fn p50(mode: Mode) -> Vec<(i64, f64)> {
        p50_of(mode, TWO_SERIES).await
    }

    async fn p50_of(mode: Mode, rows: &[(&str, i64, &[i64])]) -> Vec<(i64, f64)> {
        let df = SessionContext::new()
            .read_batch(histogram_points("histogram", rows))
            .unwrap();
        let eval = HistEval {
            stat: HistStat::Quantile(0.5),
            mode,
            first_ns: 20,
            last_ns: 40,
            step_ns: 20,
            window_ns: 30,
            offset_ns: 0,
        };
        let groups = [(col("metric_name"), "metric_name".to_string())];
        let out = histogram_series(df, &groups, &eval, "p50")
            .unwrap()
            .sort(vec![col("bucket").sort(true, false)])
            .unwrap()
            .collect()
            .await
            .unwrap();
        out.iter()
            .flat_map(|b| {
                let t = b.column(0).as_primitive::<TimestampNanosecondType>();
                let v = b.column(2).as_primitive::<Float64Type>();
                (0..b.num_rows())
                    .map(|i| (t.value(i), v.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn rate_differences_each_series_against_itself() {
        // t=20 reads (-10, 20]: s1 adds [1,1,1,0]; s2's lone point is only a baseline.
        // t=40 reads (10, 40]: [1,2,1,0] + [1,2,1,0]. Both medians are 1.5.
        assert_eq!(p50(Mode::Rate).await, vec![(20, 1.5), (40, 1.5)]);
    }

    /// Rows whose `bucket_counts` do not fit the bounds are skipped, not fatal.
    #[tokio::test]
    async fn malformed_explicit_rows_are_skipped() {
        let rows: &[(&str, i64, &[i64])] = &[("s", 10, &[1, 1, 0, 0]), ("s", 20, &[5, 5])];
        assert_eq!(p50_of(Mode::Instant, rows).await, vec![(20, 1.0)]);
    }

    #[tokio::test]
    async fn instant_merges_each_series_latest_point() {
        // t=20: [2,2,1,0] + [0,1,0,0]; t=40: [3,4,2,0] + [1,3,1,0].
        assert_eq!(
            p50(Mode::Instant).await,
            vec![(20, 1.0 + 1.0 / 3.0), (40, 1.0 + 3.0 / 7.0)]
        );
    }
}
