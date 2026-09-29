//! Aggregate UDF that reduces histogram and exponential-histogram points to one statistic per group.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

use super::hist_math::{HistPt, Mode, fraction, merge_across, quantile, series_value};
use super::hist_state::{ARGS, Row, column_types, decode, encode, list_of, parse_rows};
use super::instants::{as_ns, invalid};
use crate::query::error::QuerierError;

/// What to compute from the merged histogram of a group.
#[derive(Debug, Clone, Copy)]
pub enum HistStat {
    Quantile(f64),
    Fraction(f64, f64),
    Count,
    Sum,
}

impl HistStat {
    fn key(&self) -> (u8, u64, u64) {
        match *self {
            HistStat::Quantile(q) => (0, q.to_bits(), 0),
            HistStat::Fraction(a, b) => (1, a.to_bits(), b.to_bits()),
            HistStat::Count => (2, 0, 0),
            HistStat::Sum => (3, 0, 0),
        }
    }

    /// Name fragment that includes the parameters, so two UDAFs in one plan never collide.
    fn label(&self) -> String {
        match *self {
            HistStat::Quantile(q) => format!("quantile_q{q}"),
            HistStat::Fraction(lo, hi) => format!("fraction_{lo}_{hi}"),
            HistStat::Count => "count".into(),
            HistStat::Sum => "sum".into(),
        }
    }
}

impl PartialEq for HistStat {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for HistStat {}
impl Hash for HistStat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

/// Aggregate UDF: one Float64 per group; the planner groups by `(output labels…, instant)`.
/// Arguments: `series_id, timestamp, start_timestamp, aggregation_temporality, metric_type,
/// explicit_bounds, bucket_counts, count, sum, scale, zero_count, zero_threshold,
/// positive_offset, positive_bucket_counts, negative_offset, negative_bucket_counts, min, max,
/// instant`. Points are reduced per series, merged across series, then `stat` is applied.
pub fn histogram_udaf(stat: HistStat, mode: Mode, window_ns: i64) -> AggregateUDF {
    AggregateUDF::new_from_impl(HistUdaf {
        stat,
        mode,
        window_ns,
        name: format!(
            "hist_{}_{}_w{window_ns}",
            stat.label(),
            format!("{mode:?}").to_lowercase()
        ),
        signature: Signature::any(ARGS, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct HistUdaf {
    stat: HistStat,
    mode: Mode,
    window_ns: i64,
    name: String,
    signature: Signature,
}

impl AggregateUDFImpl for HistUdaf {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let mut fields: Vec<FieldRef> = column_types()
            .into_iter()
            .enumerate()
            .map(|(i, t)| Arc::new(Field::new(format!("{}[c{i}]", args.name), list_of(t), true)))
            .collect();
        fields.push(Arc::new(Field::new(
            format!("{}[instant]", args.name),
            DataType::Int64,
            true,
        )));
        Ok(fields)
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if self.window_ns <= 0 {
            return Err(invalid("histogram window must be positive"));
        }
        Ok(Box::new(HistAcc {
            stat: self.stat,
            mode: self.mode,
            window_ns: self.window_ns,
            rows: Vec::new(),
            instant: None,
        }))
    }
}

#[derive(Debug)]
struct HistAcc {
    stat: HistStat,
    mode: Mode,
    window_ns: i64,
    rows: Vec<Row>,
    instant: Option<i64>,
}

impl Accumulator for HistAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != ARGS {
            return Err(DataFusionError::Internal(format!(
                "histogram accumulator expects {ARGS} arguments"
            )));
        }
        let instant = as_ns(&values[ARGS - 1])?;
        if self.instant.is_none() {
            self.instant = (0..instant.len())
                .find(|&i| instant.is_valid(i))
                .map(|i| instant.value(i));
        }
        self.rows.extend(parse_rows(&values[..ARGS - 1])?);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != ARGS {
            return Err(DataFusionError::Internal(format!(
                "histogram accumulator expects {ARGS} state columns"
            )));
        }
        let (rows, instant) = decode(states)?;
        self.rows.extend(rows);
        self.instant = self.instant.or(instant);
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(encode(&self.rows, self.instant))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(instant) = self.instant else {
            return Ok(ScalarValue::Float64(None));
        };
        let mut order: Vec<&Row> = self.rows.iter().collect();
        order.sort_by_cached_key(|r| r.sort_key());
        let mut by_series: BTreeMap<&str, Vec<HistPt>> = BTreeMap::new();
        for row in order {
            by_series.entry(&row.series).or_default().push(row.point()?);
        }
        let ext = |e: QuerierError| DataFusionError::External(Box::new(e));
        let values = by_series
            .values()
            .map(|pts| series_value(pts, self.mode, instant, self.window_ns))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ext)?
            .into_iter()
            .flatten()
            .collect();
        let Some(h) = merge_across(values).map_err(ext)? else {
            return Ok(ScalarValue::Float64(None));
        };
        Ok(ScalarValue::Float64(match self.stat {
            HistStat::Quantile(q) => Some(quantile(&h, q)),
            HistStat::Fraction(lo, hi) => Some(fraction(&h, lo, hi)),
            HistStat::Count => Some(h.count() as f64),
            HistStat::Sum => h.sum(),
        }))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + self.rows.capacity() * size_of::<Row>()
            + self.rows.iter().map(Row::heap).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        AsArray, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
        TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Float64Type, Int64Type};
    use datafusion::arrow::datatypes::{Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionContext, col};

    use super::*;

    const S: i64 = 1_000_000_000;

    #[derive(Clone)]
    struct P {
        series: &'static str,
        ts: i64,
        kind: &'static str,
        temp: i32,
        start: i64,
        bounds: Vec<f64>,
        counts: Vec<i64>,
        sum: f64,
        instant: i64,
    }

    /// Explicit histogram, bounds [1, 2, 4], cumulative, started long before the window.
    fn eh(series: &'static str, ts: i64, counts: &[i64], sum: f64, instant: i64) -> P {
        P {
            series,
            ts,
            kind: "histogram",
            temp: 2,
            start: 5,
            bounds: vec![1.0, 2.0, 4.0],
            counts: counts.to_vec(),
            sum,
            instant,
        }
    }

    /// Exponential histogram at scale 0, positive offset 0, cumulative.
    fn xh(series: &'static str, ts: i64, counts: &[i64], instant: i64) -> P {
        P {
            kind: "exponential_histogram",
            ..eh(series, ts, counts, 0.0, instant)
        }
    }

    fn ll<T: datafusion::arrow::datatypes::ArrowPrimitiveType>(
        rows: &[P],
        f: impl Fn(&P) -> Option<Vec<T::Native>>,
    ) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<T, _, _>(rows.iter().map(
            |p| f(p).map(|v| v.into_iter().map(Some).collect::<Vec<_>>()),
        )))
    }

    async fn run(
        rows: &[P],
        stat: HistStat,
        mode: Mode,
        window_s: i64,
    ) -> Result<Vec<Option<f64>>, QuerierError> {
        let list = |t| list_of(t);
        let mut fields = vec![
            Field::new("series_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "start_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("aggregation_temporality", DataType::Int32, true),
            Field::new("metric_type", DataType::Utf8, false),
            Field::new("explicit_bounds", list(DataType::Float64), true),
            Field::new("bucket_counts", list(DataType::Int64), true),
            Field::new("count", DataType::Int64, true),
            Field::new("sum", DataType::Float64, true),
            Field::new("scale", DataType::Int32, true),
            Field::new("zero_count", DataType::Int64, true),
            Field::new("zero_threshold", DataType::Float64, true),
            Field::new("positive_offset", DataType::Int32, true),
            Field::new("positive_bucket_counts", list(DataType::Int64), true),
            Field::new("negative_offset", DataType::Int32, true),
            Field::new("negative_bucket_counts", list(DataType::Int64), true),
            Field::new("min", DataType::Float64, true),
            Field::new("max", DataType::Float64, true),
            Field::new("instant", DataType::Int64, false),
        ];
        let names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        let is_exp = |p: &P| p.kind == "exponential_histogram";
        let n = rows.len();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|p| p.series))),
            Arc::new(TimestampNanosecondArray::from_iter_values(
                rows.iter().map(|p| p.ts * S),
            )),
            Arc::new(TimestampNanosecondArray::from_iter_values(
                rows.iter().map(|p| p.start * S),
            )),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|p| p.temp))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|p| p.kind))),
            ll::<Float64Type>(rows, |p| (!is_exp(p)).then(|| p.bounds.clone())),
            ll::<Int64Type>(rows, |p| (!is_exp(p)).then(|| p.counts.clone())),
            Arc::new(Int64Array::from(vec![None; n])),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|p| p.sum))),
            Arc::new(Int32Array::from(
                rows.iter()
                    .map(|p| is_exp(p).then_some(0))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|p| is_exp(p).then_some(0))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|p| is_exp(p).then_some(0.0))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter()
                    .map(|p| is_exp(p).then_some(0))
                    .collect::<Vec<_>>(),
            )),
            ll::<Int64Type>(rows, |p| is_exp(p).then(|| p.counts.clone())),
            Arc::new(Int32Array::from(vec![None; n])),
            ll::<Int64Type>(rows, |_| None),
            Arc::new(Float64Array::from(vec![None; n])),
            Arc::new(Float64Array::from(vec![None; n])),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|p| p.instant * S),
            )),
        ];
        // Keep the schema honest about the types actually built.
        for (f, c) in fields.iter_mut().zip(&cols) {
            *f = Field::new(f.name(), c.data_type().clone(), true);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), cols)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let ctx = SessionContext::new();
        let half = n / 2;
        let parts = vec![
            vec![batch.slice(0, half)],
            vec![batch.slice(half, n - half)],
        ];
        ctx.register_table("m", Arc::new(MemTable::try_new(schema, parts)?))?;
        let out = ctx
            .table("m")
            .await?
            .aggregate(
                vec![col("instant")],
                vec![
                    histogram_udaf(stat, mode, window_s * S)
                        .call(names.iter().map(|n| col(n.as_str())).collect())
                        .alias("out"),
                ],
            )?
            .collect()
            .await?;
        Ok(out
            .iter()
            .flat_map(|b| {
                let v = b.column(1).as_primitive::<Float64Type>();
                (0..v.len())
                    .map(|i| v.is_valid(i).then(|| v.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    /// Two cumulative series of one metric (the hive shape); window (40, 100].
    fn two_series() -> Vec<P> {
        vec![
            eh("a", 50, &[1, 1, 0, 0], 5.0, 100),
            eh("a", 70, &[2, 2, 1, 0], 12.0, 100),
            eh("a", 90, &[3, 4, 2, 0], 20.0, 100),
            eh("b", 50, &[0, 1, 0, 0], 1.0, 100),
            eh("b", 70, &[0, 2, 1, 0], 4.0, 100),
            eh("b", 90, &[1, 3, 1, 0], 9.0, 100),
        ]
    }

    #[tokio::test]
    async fn rate_quantile_over_two_series_is_finite() {
        // Deltas: a [2,3,2,0], b [1,2,1,0]; summed [3,5,3,0], median rank 5.5 in (1, 2].
        let out = run(&two_series(), HistStat::Quantile(0.5), Mode::Rate, 60)
            .await
            .unwrap();
        assert_eq!(out, vec![Some(1.5)]);
        let count = run(&two_series(), HistStat::Count, Mode::Rate, 60)
            .await
            .unwrap();
        assert_eq!(count, vec![Some(11.0)]);
        let sum = run(&two_series(), HistStat::Sum, Mode::Rate, 60)
            .await
            .unwrap();
        assert_eq!(sum, vec![Some(23.0)]);
        let frac = run(&two_series(), HistStat::Fraction(1.0, 2.0), Mode::Rate, 60)
            .await
            .unwrap();
        assert_eq!(frac, vec![Some(5.0 / 11.0)]);
    }

    #[tokio::test]
    async fn instant_takes_each_series_latest_point() {
        // Latest: a [3,4,2,0] + b [1,3,1,0] = [4,7,3,0]; rank 7 falls in (1, 2].
        let out = run(&two_series(), HistStat::Quantile(0.5), Mode::Instant, 60)
            .await
            .unwrap();
        let q = out[0].unwrap();
        assert!((q - (1.0 + 3.0 / 7.0)).abs() < 1e-12, "{q}");
    }

    #[tokio::test]
    async fn exponential_quantile_and_ignored_kinds() {
        let mut rows = vec![xh("x", 50, &[1, 1], 100), xh("x", 70, &[1, 3], 100)];
        rows.push(P {
            kind: "gauge",
            ..eh("g", 70, &[], 0.0, 100)
        });
        // Delta [0, 2]: both observations in (2, 4]; the median is 2 * 2^0.5.
        let out = run(&rows, HistStat::Quantile(0.5), Mode::Rate, 60)
            .await
            .unwrap();
        let q = out[0].unwrap();
        assert!((q - 2f64.powf(1.5)).abs() < 1e-9, "{q}");
    }

    #[tokio::test]
    async fn summary_and_bad_window_are_invalid_input() {
        let rows = [P {
            kind: "summary",
            ..eh("s", 70, &[], 0.0, 100)
        }];
        let err = run(&rows, HistStat::Count, Mode::Instant, 60)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, QuerierError::InvalidInput(m) if m.contains("summary")),
            "{err:?}"
        );
        let err = run(&two_series(), HistStat::Count, Mode::Rate, 0)
            .await
            .unwrap_err();
        assert!(matches!(err, QuerierError::InvalidInput(_)), "{err:?}");
    }
}
