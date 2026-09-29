//! Plans a range function over the metric point stream: each point joins every
//! evaluation instant whose window covers it, then [`range_udaf`] reduces each
//! (series, instant).

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_expr::{Expr, cast, col, lit};
use datafusion::prelude::{DataFrame, ident};
use datafusion::scalar::ScalarValue;

use super::instants::covering_instants;
use super::range::range_udaf;
use super::range_math::RangeFn;
use super::series_key::series_key;
use crate::query::error::QuerierError;

const INSTANT: &str = "__instant";

/// `f` evaluated at `first + k·step <= last`, each instant `t` reading `(t - window, t]`.
pub(crate) struct RangeEval {
    pub f: RangeFn,
    pub first_ns: i64,
    pub last_ns: i64,
    pub step_ns: i64,
    pub window_ns: i64,
}

/// One row per (series, instant) with a value: `bucket` (the instant), the
/// `groups` aliases (constant within a series) and Float64 `out`. `df` must
/// already hold the points of `(first - window, last]`; a stored column the
/// table lacks reads as null, and a missing `series_id` is derived.
pub(crate) fn range_series(
    df: DataFrame,
    value: Expr,
    groups: &[(Expr, String)],
    eval: &RangeEval,
    out: &str,
) -> Result<DataFrame, QuerierError> {
    let has = |name: &str| df.schema().fields().iter().any(|f| f.name() == name);
    let stored = |name: &str, t: DataType| -> Result<Expr, QuerierError> {
        Ok(if has(name) {
            col(name)
        } else {
            lit(ScalarValue::try_from(&t).map_err(QuerierError::QueryFailed)?)
        })
    };
    let mut proj: Vec<Expr> = groups.iter().map(|(e, a)| e.clone().alias(a)).collect();
    let args = [
        col("timestamp"),
        cast(value, DataType::Float64),
        stored("start_timestamp", DataType::Int64)?,
        stored("aggregation_temporality", DataType::Int32)?,
        stored("is_monotonic", DataType::Boolean)?,
        stored("metric_type", DataType::Utf8)?,
    ];
    let names: Vec<String> = (0..args.len()).map(|i| format!("__r{i}")).collect();
    proj.extend(args.into_iter().zip(&names).map(|(e, n)| e.alias(n)));
    proj.push(series_key(df.schema()).alias("__series"));
    proj.push(
        covering_instants(eval.first_ns, eval.last_ns, eval.step_ns, eval.window_ns).alias(INSTANT),
    );
    let labels = || groups.iter().map(|(_, a)| ident(a));
    let keys: Vec<Expr> = [ident("__series"), ident(INSTANT)]
        .into_iter()
        .chain(labels())
        .collect();
    let mut call: Vec<Expr> = names.iter().map(ident).collect();
    call.push(ident(INSTANT));
    let udaf = range_udaf(eval.f, eval.window_ns);
    let bucket = cast(
        ident(INSTANT),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    );
    let select: Vec<Expr> = [bucket.alias("bucket")]
        .into_iter()
        .chain(labels())
        .chain([ident(out)])
        .collect();
    df.select(proj)
        .and_then(|df| df.unnest_columns(&[INSTANT]))
        .and_then(|df| df.filter(ident(INSTANT).is_not_null()))
        .and_then(|df| df.aggregate(keys, vec![udaf.call(call).alias(out)]))
        .and_then(|df| df.filter(ident(out).is_not_null()))
        .and_then(|df| df.select(select))
        .map_err(QuerierError::QueryFailed)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{Float64Type, TimestampNanosecondType};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::query::metric_ops::fixtures::{counter_points, without_series_id};

    /// Without a `series_id`, series are told apart by their identity columns.
    #[tokio::test]
    async fn a_missing_series_id_falls_back_to_the_identity_columns() {
        let rows: &[(&str, i64, f64)] = &[
            ("s1", 10, 10.0),
            ("s2", 15, 100.0),
            ("s1", 20, 20.0),
            ("s2", 25, 110.0),
        ];
        for drop in [true, false] {
            let batch = without_series_id(counter_points("sum", rows), drop);
            let df = SessionContext::new().read_batch(batch).unwrap();
            let eval = RangeEval {
                f: RangeFn::Increase,
                first_ns: 30,
                last_ns: 30,
                step_ns: 10,
                window_ns: 30,
            };
            let out = range_series(df, col("value"), &[], &eval, "v")
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut got: Vec<f64> = out
                .iter()
                .flat_map(|b| b.column(1).as_primitive::<Float64Type>().values().to_vec())
                .collect();
            got.sort_by(f64::total_cmp);
            assert_eq!(got, vec![10.0, 10.0], "drop={drop}");
        }
    }

    #[tokio::test]
    async fn increase_is_per_series_at_each_instant() {
        let rows: &[(&str, i64, f64)] = &[
            ("s1", 10, 10.0),
            ("s2", 15, 100.0),
            ("s1", 20, 20.0),
            ("s2", 25, 110.0),
            ("s1", 30, 30.0),
            ("s2", 35, 125.0),
        ];
        let df = SessionContext::new()
            .read_batch(counter_points("sum", rows))
            .unwrap();
        let eval = RangeEval {
            f: RangeFn::Increase,
            first_ns: 20,
            last_ns: 40,
            step_ns: 20,
            window_ns: 30,
        };
        let groups = [(col("series_id"), "sid".to_string())];
        let out = range_series(df, col("value"), &groups, &eval, "v")
            .unwrap()
            .sort(vec![
                col("bucket").sort(true, false),
                col("sid").sort(true, false),
            ])
            .unwrap()
            .collect()
            .await
            .unwrap();
        let got: Vec<(i64, String, f64)> = out
            .iter()
            .flat_map(|b| {
                let t = b.column(0).as_primitive::<TimestampNanosecondType>();
                let s = b.column(1).as_string::<i32>();
                let v = b.column(2).as_primitive::<Float64Type>();
                (0..b.num_rows())
                    .map(|i| (t.value(i), s.value(i).to_owned(), v.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect();
        // t=20 reads (-10, 20]: s1 10→20; s2 holds one point, a baseline only.
        // t=40 reads (10, 40]: s1 20→30, s2 100→110→125.
        let want = [(20, "s1", 10.0), (40, "s1", 10.0), (40, "s2", 25.0)];
        let want: Vec<_> = want
            .iter()
            .map(|(t, s, v)| (*t, s.to_string(), *v))
            .collect();
        assert_eq!(got, want);
    }
}
