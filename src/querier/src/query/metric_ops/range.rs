//! Aggregate UDF that evaluates a windowed range function per (series, evaluation instant).

use std::hash::Hash;
use std::mem::size_of;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanBuilder, ListArray, ListBuilder,
    PrimitiveArray, PrimitiveBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int32Type, Int64Type};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

use super::instants::{as_ns, invalid};
use super::range_math::{Pt, RangeFn, eval_points};

/// Aggregate UDF: one value per group; the planner groups by
/// `(series, evaluation instant)`. Arguments: `timestamp`, `value`,
/// `start_timestamp`, `aggregation_temporality`, `is_monotonic`, `metric_type`,
/// `instant`. Returns Float64, null when the series has no point in the window.
pub fn range_udaf(f: RangeFn, window_ns: i64) -> AggregateUDF {
    AggregateUDF::new_from_impl(RangeUdaf {
        f,
        window_ns,
        name: match f {
            RangeFn::QuantileOverTime(q) => format!("range_{}_q{q}_w{window_ns}", f.name()),
            _ => format!("range_{}_w{window_ns}", f.name()),
        },
        signature: Signature::any(7, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RangeUdaf {
    f: RangeFn,
    window_ns: i64,
    name: String,
    signature: Signature,
}

impl AggregateUDFImpl for RangeUdaf {
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
        let list = |t| DataType::List(Arc::new(Field::new_list_field(t, true)));
        let field = |n: &str, t| Arc::new(Field::new(format!("{}[{n}]", args.name), t, true));
        Ok(vec![
            field("ts", list(DataType::Int64)),
            field("value", list(DataType::Float64)),
            field("start", list(DataType::Int64)),
            field("temporality", list(DataType::Int32)),
            field("monotonic", list(DataType::Boolean)),
            field("kind", DataType::Utf8),
            field("instant", DataType::Int64),
        ])
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if self.window_ns <= 0 {
            return Err(invalid("range window must be positive"));
        }
        Ok(Box::new(RangeAcc {
            f: self.f,
            window_ns: self.window_ns,
            pts: Vec::new(),
            newest_kind: None,
            instant: None,
        }))
    }
}

#[derive(Debug)]
struct RangeAcc {
    f: RangeFn,
    window_ns: i64,
    pts: Vec<Pt>,
    /// The metric type of the newest point and that point's timestamp. A
    /// Series merges stored series whose label sets coincide, which may
    /// differ in type; the newest point decides, the greater type on a tie.
    newest_kind: Option<(i64, String)>,
    instant: Option<i64>,
}

fn opt<A: Array>(a: &A, i: usize) -> bool {
    a.is_valid(i)
}

fn bad_state(what: &str) -> DataFusionError {
    DataFusionError::Internal(format!(
        "range accumulator state `{what}` has the wrong type"
    ))
}

fn state_list<'a>(a: &'a ArrayRef, what: &str) -> Result<&'a ListArray> {
    a.as_list_opt::<i32>().ok_or_else(|| bad_state(what))
}

fn state_values<T: ArrowPrimitiveType>(a: &ArrayRef, what: &str) -> Result<PrimitiveArray<T>> {
    a.as_primitive_opt::<T>()
        .cloned()
        .ok_or_else(|| bad_state(what))
}

/// One list scalar holding `vals`.
fn list_scalar<T: ArrowPrimitiveType>(
    vals: impl Iterator<Item = Option<T::Native>>,
) -> ScalarValue {
    let mut b = ListBuilder::new(PrimitiveBuilder::<T>::new());
    b.values().extend(vals);
    b.append(true);
    ScalarValue::List(Arc::new(b.finish()))
}

impl RangeAcc {
    fn absorb_kind(&mut self, ts: i64, kind: Option<&str>) {
        let Some(kind) = kind else { return };
        let newer = self
            .newest_kind
            .as_ref()
            .is_none_or(|(t, k)| (ts, kind) > (*t, k.as_str()));
        if newer {
            self.newest_kind = Some((ts, kind.to_owned()));
        }
    }
}

impl Accumulator for RangeAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [ts, v, start, temporality, monotonic, kind, instant] = values else {
            return Err(DataFusionError::Internal(
                "range accumulator expects 7 arguments".into(),
            ));
        };
        let (ts, start, instant) = (as_ns(ts)?, as_ns(start)?, as_ns(instant)?);
        let v = cast(v, &DataType::Float64)?;
        let v = v.as_primitive::<Float64Type>();
        let temporality = cast(temporality, &DataType::Int32)?;
        let temporality = temporality.as_primitive::<Int32Type>();
        let monotonic = cast(monotonic, &DataType::Boolean)?;
        let monotonic = monotonic.as_boolean();
        let kind = cast(kind, &DataType::Utf8)?;
        let kind = kind.as_string::<i32>();
        for i in 0..ts.len() {
            if opt(&ts, i) && opt(v, i) {
                self.pts.push(Pt {
                    ts: ts.value(i),
                    v: v.value(i),
                    start: if opt(&start, i) { start.value(i) } else { 0 },
                    temporality: opt(temporality, i).then(|| temporality.value(i)),
                    monotonic: opt(monotonic, i).then(|| monotonic.value(i)),
                });
                self.absorb_kind(ts.value(i), opt(kind, i).then(|| kind.value(i)));
            }
            self.instant = self.instant.or(opt(&instant, i).then(|| instant.value(i)));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [ts, v, start, temporality, monotonic, kind, instant] = states else {
            return Err(DataFusionError::Internal(
                "range accumulator expects 7 state columns".into(),
            ));
        };
        let (ts, v, start) = (
            state_list(ts, "ts")?,
            state_list(v, "value")?,
            state_list(start, "start")?,
        );
        let (temporality, monotonic) = (
            state_list(temporality, "temporality")?,
            state_list(monotonic, "monotonic")?,
        );
        let kind = kind
            .as_string_opt::<i32>()
            .ok_or_else(|| bad_state("kind"))?;
        let instant = state_values::<Int64Type>(instant, "instant")?;
        for row in 0..ts.len() {
            if ts.is_valid(row) {
                let t = state_values::<Int64Type>(&ts.value(row), "ts")?;
                let val = state_values::<Float64Type>(&v.value(row), "value")?;
                let s = state_values::<Int64Type>(&start.value(row), "start")?;
                let tp = state_values::<Int32Type>(&temporality.value(row), "temporality")?;
                let mo = monotonic.value(row);
                let mo = mo.as_boolean_opt().ok_or_else(|| bad_state("monotonic"))?;
                if let Some(newest) = t.values().iter().max() {
                    self.absorb_kind(*newest, opt(kind, row).then(|| kind.value(row)));
                }
                for i in 0..t.len() {
                    self.pts.push(Pt {
                        ts: t.value(i),
                        v: val.value(i),
                        start: s.value(i),
                        temporality: opt(&tp, i).then(|| tp.value(i)),
                        monotonic: opt(mo, i).then(|| mo.value(i)),
                    });
                }
            }
            self.instant = self
                .instant
                .or(opt(&instant, row).then(|| instant.value(row)));
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let pts = &self.pts;
        let mut monotonic = ListBuilder::new(BooleanBuilder::new());
        monotonic.values().extend(pts.iter().map(|p| p.monotonic));
        monotonic.append(true);
        Ok(vec![
            list_scalar::<Int64Type>(pts.iter().map(|p| Some(p.ts))),
            list_scalar::<Float64Type>(pts.iter().map(|p| Some(p.v))),
            list_scalar::<Int64Type>(pts.iter().map(|p| Some(p.start))),
            list_scalar::<Int32Type>(pts.iter().map(|p| p.temporality)),
            ScalarValue::List(Arc::new(monotonic.finish())),
            ScalarValue::Utf8(self.newest_kind.as_ref().map(|(_, k)| k.clone())),
            ScalarValue::Int64(self.instant),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(instant) = self.instant else {
            return Ok(ScalarValue::Float64(None));
        };
        let out = eval_points(
            self.f,
            &self.pts,
            self.newest_kind.as_ref().map(|(_, k)| k.as_str()),
            instant,
            self.window_ns,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ScalarValue::Float64(out))
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.pts.capacity() * size_of::<Pt>()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionContext, col};

    use super::*;
    use crate::query::error::QuerierError;

    const S: i64 = 1_000_000_000;

    #[test]
    fn udaf_names_carry_the_window_and_quantile() {
        let name = |f, w| range_udaf(f, w).name().to_string();
        assert_ne!(name(RangeFn::Rate, 1), name(RangeFn::Rate, 2));
        assert_ne!(
            name(RangeFn::QuantileOverTime(0.5), 1),
            name(RangeFn::QuantileOverTime(0.9), 1)
        );
    }

    #[derive(Clone)]
    struct Row {
        series: &'static str,
        ts: i64,
        v: f64,
        start: Option<i64>,
        temp: Option<i32>,
        mono: Option<bool>,
        kind: &'static str,
        instant: i64,
    }

    fn cum(series: &'static str, ts: i64, v: f64, start: Option<i64>, instant: i64) -> Row {
        Row {
            series,
            ts,
            v,
            start,
            temp: Some(2),
            mono: Some(true),
            kind: "sum",
            instant,
        }
    }

    /// Runs the UDAF through a real two-partition plan; rows are in seconds.
    async fn run(
        rows: &[Row],
        f: RangeFn,
        window_s: i64,
    ) -> Result<Vec<(String, Option<f64>)>, QuerierError> {
        let ns = |x: i64| x * S;
        let schema = Arc::new(Schema::new(vec![
            Field::new("series_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new(
                "start_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("aggregation_temporality", DataType::Int32, true),
            Field::new("is_monotonic", DataType::Boolean, true),
            Field::new("metric_type", DataType::Utf8, false),
            Field::new("instant", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.series))),
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    rows.iter().map(|r| ns(r.ts)),
                )),
                Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.v))),
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter().map(|r| r.start.map(ns)).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.temp).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| r.mono).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.kind))),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|r| ns(r.instant)),
                )),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let ctx = SessionContext::new();
        let half = batch.num_rows() / 2;
        let parts = vec![
            vec![batch.slice(0, half)],
            vec![batch.slice(half, batch.num_rows() - half)],
        ];
        ctx.register_table("m", Arc::new(MemTable::try_new(schema, parts)?))?;
        let args = [
            "timestamp",
            "value",
            "start_timestamp",
            "aggregation_temporality",
            "is_monotonic",
            "metric_type",
            "instant",
        ]
        .map(col)
        .to_vec();
        let out = ctx
            .table("m")
            .await?
            .aggregate(
                vec![col("series_id"), col("instant")],
                vec![range_udaf(f, window_s * S).call(args).alias("out")],
            )?
            .sort(vec![col("series_id").sort(true, true)])?
            .collect()
            .await?;
        let mut res = Vec::new();
        for b in &out {
            let ids = b.column(0).as_string::<i32>();
            let vals = b.column(2).as_primitive::<Float64Type>();
            for i in 0..b.num_rows() {
                res.push((ids.value(i).to_owned(), opt(vals, i).then(|| vals.value(i))));
            }
        }
        Ok(res)
    }

    async fn one(rows: &[Row], f: RangeFn, window_s: i64) -> Option<f64> {
        let out = run(rows, f, window_s).await.unwrap();
        assert_eq!(out.len(), 1);
        out[0].1
    }

    #[tokio::test]
    async fn cumulative_delta_and_reset_through_a_plan() {
        let ramp: Vec<Row> = [(121, 0.0), (150, 60.0), (180, 120.0)]
            .map(|(ts, v)| cum("a", ts, v, Some(5), 180))
            .to_vec();
        assert_eq!(one(&ramp, RangeFn::Increase, 60).await, Some(120.0));
        assert_eq!(one(&ramp, RangeFn::Rate, 60).await, Some(2.0));

        let delta: Vec<Row> = [(10, 3.0), (20, 4.0), (30, 5.0)]
            .map(|(ts, v)| Row {
                temp: Some(1),
                ..cum("a", ts, v, None, 30)
            })
            .to_vec();
        assert_eq!(one(&delta, RangeFn::Increase, 60).await, Some(12.0));

        let reset = [(110, 10.0, 5), (120, 20.0, 5), (130, 25.0, 125)]
            .map(|(ts, v, s)| cum("a", ts, v, Some(s), 140));
        assert_eq!(one(&reset, RangeFn::Increase, 60).await, Some(35.0));
    }

    #[tokio::test]
    async fn series_stay_separate_and_ties_are_deterministic() {
        let mut rows: Vec<Row> = [(110, 100.0), (120, 110.0)]
            .map(|(ts, v)| cum("a", ts, v, Some(5), 130))
            .to_vec();
        rows.extend([(110, 5.0), (120, 9.0)].map(|(ts, v)| cum("b", ts, v, Some(5), 130)));
        let out = run(&rows, RangeFn::Increase, 60).await.unwrap();
        assert_eq!(out, vec![("a".into(), Some(10.0)), ("b".into(), Some(4.0))]);

        let tied =
            [(110, 5.0), (120, 9.0), (120, 7.0)].map(|(ts, v)| cum("a", ts, v, Some(5), 120));
        let mut swapped = tied.clone();
        swapped.reverse();
        assert_eq!(one(&tied, RangeFn::Increase, 60).await, Some(4.0));
        assert_eq!(one(&swapped, RangeFn::Increase, 60).await, Some(4.0));
    }

    #[tokio::test]
    async fn non_positive_window_is_invalid_input() {
        let rows = [cum("a", 10, 1.0, Some(5), 30)];
        for w in [0, -5] {
            let err = run(&rows, RangeFn::Rate, w).await.unwrap_err();
            assert!(matches!(err, QuerierError::InvalidInput(_)), "{err:?}");
        }
    }

    #[tokio::test]
    async fn gauge_rate_maps_to_invalid_input_and_delta_works() {
        let gauge = |ts, v| Row {
            temp: None,
            mono: None,
            kind: "gauge",
            ..cum("a", ts, v, None, 30)
        };
        let rows = [gauge(10, 1.0), gauge(20, 5.0)];
        for f in [RangeFn::Rate, RangeFn::Increase, RangeFn::Irate] {
            let err = run(&rows, f, 60).await.unwrap_err();
            assert!(
                matches!(&err, QuerierError::InvalidInput(m) if m.contains("gauge") && m.contains("delta or deriv")),
                "{err:?}"
            );
        }
        assert_eq!(one(&rows, RangeFn::Delta, 60).await, Some(4.0));
        assert_eq!(one(&rows, RangeFn::Deriv, 60).await, Some(0.4));
    }

    #[tokio::test]
    async fn the_newest_point_decides_the_metric_type() {
        let gauge = |ts, v| Row {
            kind: "gauge",
            ..cum("a", ts, v, None, 30)
        };
        let rows = [
            gauge(10, 1.0),
            cum("a", 20, 5.0, None, 30),
            cum("a", 30, 9.0, None, 30),
        ];
        let mut swapped = rows.clone();
        swapped.reverse();
        assert_eq!(one(&rows, RangeFn::Increase, 60).await, Some(8.0));
        assert_eq!(one(&swapped, RangeFn::Increase, 60).await, Some(8.0));
        let rows = [cum("a", 10, 1.0, None, 30), gauge(20, 5.0)];
        let err = run(&rows, RangeFn::Rate, 60).await.unwrap_err();
        assert!(matches!(err, QuerierError::InvalidInput(_)), "{err:?}");
    }
}
