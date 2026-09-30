//! Plans a histogram statistic over the metric point stream: each point joins
//! every evaluation instant whose window covers it, then [`histogram_udaf`]
//! reduces each (group, instant).

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions_nested::expr_fn::{cardinality, make_array};
use datafusion::logical_expr::{Expr, cast, col, lit, when};
use datafusion::prelude::{DataFrame, ident};
use datafusion::scalar::ScalarValue;

use super::hist::{HistStat, histogram_udaf};
use super::hist_math::Mode;
use super::hist_state::column_types;
use super::instants::covering_instants;
use super::series_key::series_key;
use crate::query::error::QuerierError;
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

/// The metric types a histogram statistic lets through: the histograms it
/// reads, and `summary`, which [`histogram_udaf`] rejects as invalid input
/// rather than skipping. Every other row is ignored.
const HISTOGRAM_TYPES: &[&str] = &["histogram", "exponential_histogram", "summary"];

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
/// reads as null, and a missing `series_id` is derived.
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
        let e = if *name == "series_id" {
            series_key(df.schema())
        } else if present.contains(*name) {
            col(*name)
        } else {
            lit(ScalarValue::try_from(&t).map_err(QuerierError::QueryFailed)?)
        };
        let arg = format!("__h{i}");
        proj.push(e.alias(&arg));
        args.push(ident(arg));
    }
    args.push(ident(INSTANT));
    let covering = covering_instants(eval.first_ns, eval.last_ns, eval.step_ns, eval.window_ns);
    // A summary joins a fixed instant, so no window geometry can drop it
    // before the accumulator rejects it.
    let instants = if present.contains("metric_type") {
        when(
            col("metric_type").eq(lit("summary")),
            make_array(vec![lit(eval.first_ns)]),
        )
        .otherwise(covering)
        .map_err(QuerierError::QueryFailed)?
    } else {
        covering
    };
    proj.push(instants.alias(INSTANT));
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
    let rows = metric_type_filter(HISTOGRAM_TYPES);
    let rows = if present.contains("explicit_bounds") && present.contains("bucket_counts") {
        rows.and(fits_bounds())
    } else {
        rows
    };
    df.filter(rows)
        .and_then(|df| df.select(proj))
        .and_then(|df| df.unnest_columns(&[INSTANT]))
        .and_then(|df| df.filter(ident(INSTANT).is_not_null()))
        .and_then(|df| df.aggregate(keys, vec![udaf.call(args).alias(value)]))
        .and_then(|df| df.select(out))
        .and_then(|df| df.filter(ident(value).is_not_null()))
        .map_err(QuerierError::QueryFailed)
}

/// An exponential point, or an explicit one whose `bucket_counts` hold one
/// more entry than its `explicit_bounds`. Other rows the accumulator cannot
/// read are skipped there.
fn fits_bounds() -> Expr {
    col("metric_type")
        .not_eq(lit("histogram"))
        .or(cardinality(col("bucket_counts")).eq(cardinality(col("explicit_bounds")) + lit(1u64)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{AsArray, ListArray, RecordBatch};
    use datafusion::arrow::datatypes::{Float64Type, TimestampNanosecondType};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::query::metric_ops::fixtures::{
        HIVE_MERGED_P50, HIVE_SERIES, histogram_points, without_series_id,
    };

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
        p50_over(mode, histogram_points("histogram", rows)).await
    }

    /// `batch` with every row's `explicit_bounds` replaced by `bounds`.
    fn with_bounds(batch: RecordBatch, bounds: &[f64]) -> RecordBatch {
        let i = batch.schema().index_of("explicit_bounds").unwrap();
        let mut cols = batch.columns().to_vec();
        cols[i] = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
            (0..batch.num_rows())
                .map(|_| Some(bounds.iter().map(|b| Some(*b)).collect::<Vec<_>>())),
        ));
        RecordBatch::try_new(batch.schema(), cols).unwrap()
    }

    fn p50_eval(mode: Mode, first_ns: i64, last_ns: i64, step_ns: i64, window_ns: i64) -> HistEval {
        HistEval {
            stat: HistStat::Quantile(0.5),
            mode,
            first_ns,
            last_ns,
            step_ns,
            window_ns,
            offset_ns: 0,
        }
    }

    async fn p50_over(mode: Mode, batch: RecordBatch) -> Vec<(i64, f64)> {
        p50_at(p50_eval(mode, 20, 40, 20, 30), batch).await
    }

    async fn p50_at(eval: HistEval, batch: RecordBatch) -> Vec<(i64, f64)> {
        let df = SessionContext::new().read_batch(batch).unwrap();
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

    /// The hive shape, at one instant whose window holds every point.
    #[tokio::test]
    async fn rate_merges_each_series_increase() {
        let batch = histogram_points("histogram", HIVE_SERIES);
        let out = p50_at(p50_eval(Mode::Rate, 40, 40, 10, 40), batch).await;
        assert_eq!(out, vec![(40, HIVE_MERGED_P50)]);
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
        let rows: &[(&str, i64, &[i64])] = &[("s", 10, &[1, 1, 0, 0]), ("s", 20, &[1, -1, 0, 0])];
        assert_eq!(p50_of(Mode::Instant, rows).await, vec![(20, 1.0)]);
        let unsorted = with_bounds(histogram_points("histogram", rows), &[2.0, 1.0, 4.0]);
        assert!(p50_over(Mode::Instant, unsorted).await.is_empty());
    }

    /// A single `+Inf` bucket is a histogram too.
    #[tokio::test]
    async fn single_bucket_histograms_are_read() {
        let rows: &[(&str, i64, &[i64])] = &[("s", 10, &[3])];
        let single = with_bounds(histogram_points("histogram", rows), &[]);
        let out = p50_over(Mode::Instant, single).await;
        assert!(out.len() == 1 && out[0].1.is_nan(), "{out:?}");
    }

    /// The error of `p50` over `batch`, which must hold a summary.
    async fn p50_error(eval: HistEval, batch: RecordBatch) -> QuerierError {
        let df = SessionContext::new().read_batch(batch).unwrap();
        let groups = [(col("metric_name"), "metric_name".to_string())];
        let err = histogram_series(df, &groups, &eval, "p50")
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        QuerierError::from(err)
    }

    /// A summary is rejected in either mode, also when the window is
    /// narrower than the step so no instant covers its points.
    #[tokio::test]
    async fn summaries_are_rejected_even_when_no_instant_covers_them() {
        // Instants 20 and 40 with a 5ns window cover (15, 20] and (35, 40]:
        // neither point below is read.
        let uncovered: &[(&str, i64, &[i64])] =
            &[("s", 10, &[1, 1, 0, 0]), ("s", 25, &[1, 1, 0, 0])];
        for (mode, window) in [(Mode::Instant, 30), (Mode::Rate, 30), (Mode::Instant, 5)] {
            let batch =
                histogram_points("summary", if window == 5 { uncovered } else { TWO_SERIES });
            let err = p50_error(p50_eval(mode, 20, 40, 20, window), batch).await;
            assert!(
                matches!(&err, QuerierError::InvalidInput(m)
                    if m.contains("histogram_quantile is not supported on summary metrics")),
                "{err:?}"
            );
        }
    }

    /// Without a `series_id`, series are told apart by their identity
    /// columns: the rate answer is the per-series one either way.
    #[tokio::test]
    async fn a_missing_series_id_falls_back_to_the_identity_columns() {
        for drop in [true, false] {
            let batch = without_series_id(histogram_points("histogram", TWO_SERIES), drop);
            assert_eq!(
                p50_over(Mode::Rate, batch).await,
                vec![(20, 1.5), (40, 1.5)],
                "drop={drop}"
            );
        }
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
