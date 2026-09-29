//! Typed parsing of histogram argument columns into rows, and the accumulator's list-state encoding.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, ListArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Int64Type};
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;

use super::exp_histogram::{Buckets, ExpHistogram};
use super::hist_math::{HistPoint, HistPt};
use super::instants::{as_ns, invalid};

/// Number of UDAF arguments: 18 per-point columns plus the evaluation `instant`.
pub(super) const ARGS: usize = 19;

pub(super) fn list_of(t: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(t, true)))
}

/// Element types of the 18 per-point columns (everything before `instant`).
pub(super) fn column_types() -> [DataType; ARGS - 1] {
    use DataType::*;
    [
        Utf8,
        Int64,
        Int64,
        Int32,
        Utf8,
        list_of(Float64),
        list_of(Int64),
        Int64,
        Float64,
        Int32,
        Int64,
        Float64,
        Int32,
        list_of(Int64),
        Int32,
        list_of(Int64),
        Float64,
        Float64,
    ]
}

/// One accepted histogram / exponential-histogram row, as stored.
#[derive(Debug, Clone)]
pub(super) struct Row {
    pub(super) series: String,
    ts: i64,
    start: i64,
    temporality: Option<i32>,
    exp: bool,
    bounds: Vec<f64>,
    bucket_counts: Vec<i64>,
    count: Option<i64>,
    sum: Option<f64>,
    scale: Option<i32>,
    zero_count: Option<i64>,
    zero_threshold: Option<f64>,
    pos_offset: Option<i32>,
    pos_counts: Vec<i64>,
    neg_offset: Option<i32>,
    neg_counts: Vec<i64>,
    min: Option<f64>,
    max: Option<f64>,
}

impl Row {
    /// Validate and convert; malformed stored data is `InvalidInput` naming the column.
    pub(super) fn point(&self) -> Result<HistPt> {
        let count = |name: &str, v: i64| {
            u64::try_from(v).map_err(|_| invalid(format!("{name} must not be negative")))
        };
        let counts = |name: &str, v: &[i64]| {
            v.iter()
                .map(|&c| count(name, c))
                .collect::<Result<Vec<_>>>()
        };
        let h = if self.exp {
            let scale = self
                .scale
                .ok_or_else(|| invalid("scale is required on exponential histograms"))?;
            let buckets = |name: &str, offset: Option<i32>, v: &[i64]| {
                if offset.is_none() && !v.is_empty() {
                    return Err(invalid(format!(
                        "{name}_offset is required with {name}_bucket_counts"
                    )));
                }
                counts(&format!("{name}_bucket_counts"), v)?;
                Buckets::from_stored(offset.unwrap_or(0), v)
                    .ok_or_else(|| invalid(format!("{name}_bucket_counts span is too large")))
            };
            let exp = ExpHistogram::from_stored(
                scale,
                self.zero_count.unwrap_or(0),
                self.zero_threshold.unwrap_or(0.0),
                buckets("positive", self.pos_offset, &self.pos_counts)?,
                buckets("negative", self.neg_offset, &self.neg_counts)?,
                self.min,
                self.max,
            )
            .ok_or_else(|| {
                invalid("scale, zero_count or zero_threshold is out of range on an exponential histogram")
            })?;
            HistPoint::Exp(exp, self.sum)
        } else {
            if !self.bounds.iter().all(|b| b.is_finite()) || !self.bounds.is_sorted_by(|a, b| a < b)
            {
                return Err(invalid(
                    "explicit_bounds must be finite and strictly increasing",
                ));
            }
            let total = self.count.map(|c| count("count", c)).transpose()?;
            // No bounds and no buckets: one bucket holding every observation.
            let counts = if self.bounds.is_empty() && self.bucket_counts.is_empty() {
                vec![total.unwrap_or(0)]
            } else if self.bucket_counts.len() == self.bounds.len() + 1 {
                counts("bucket_counts", &self.bucket_counts)?
            } else {
                return Err(invalid(
                    "bucket_counts must have one more entry than explicit_bounds",
                ));
            };
            let sum = counts.iter().fold(0u64, |a, &c| a.saturating_add(c));
            HistPoint::Explicit {
                bounds: self.bounds.clone(),
                counts,
                sum: self.sum,
                count: total.unwrap_or(sum),
            }
        };
        Ok(HistPt {
            ts: self.ts,
            start: self.start,
            temporality: self.temporality,
            h,
        })
    }

    /// Total order for deterministic evaluation regardless of arrival order.
    pub(super) fn sort_key(&self) -> impl Ord + use<> {
        let total = [&self.bucket_counts, &self.pos_counts, &self.neg_counts]
            .into_iter()
            .flatten()
            .fold(0i64, |a, &c| a.saturating_add(c));
        (
            self.series.clone(),
            self.ts,
            self.start,
            self.count.unwrap_or(total),
            self.sum.map_or(0, f64::to_bits),
            self.bucket_counts.clone(),
            self.pos_counts.clone(),
            self.neg_counts.clone(),
        )
    }

    pub(super) fn heap(&self) -> usize {
        self.series.capacity()
            + 8 * (self.bounds.capacity()
                + self.bucket_counts.capacity()
                + self.pos_counts.capacity()
                + self.neg_counts.capacity())
    }
}

fn as_list(a: &ArrayRef, item: DataType) -> Result<ListArray> {
    Ok(cast(a, &list_of(item))?.as_list::<i32>().clone())
}

fn list_values<T: ArrowPrimitiveType>(
    l: &ListArray,
    i: usize,
    name: &str,
) -> Result<Vec<T::Native>> {
    if l.is_null(i) {
        return Ok(Vec::new());
    }
    let v = l.value(i);
    if v.null_count() > 0 {
        return Err(invalid(format!("{name} must not contain nulls")));
    }
    Ok(v.as_primitive::<T>().values().to_vec())
}

/// Parse 18 per-point columns (the UDAF arguments, or flattened state lists) into rows.
pub(super) fn parse_rows(cols: &[ArrayRef]) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let [
        series,
        ts,
        start,
        temp,
        kind,
        bounds,
        bcounts,
        count,
        sum,
        scale,
        zero_count,
        zt,
        po,
        pc,
        no,
        nc,
        min,
        max,
    ] = cols
    else {
        return Err(DataFusionError::Internal(
            "histogram accumulator expects 18 point columns".into(),
        ));
    };
    let series = cast(series, &DataType::Utf8)?;
    let series = series.as_string::<i32>();
    let (ts, start) = (as_ns(ts)?, as_ns(start)?);
    let temp = cast(temp, &DataType::Int32)?;
    let temp = temp.as_primitive::<Int32Type>();
    let kind = cast(kind, &DataType::Utf8)?;
    let kind = kind.as_string::<i32>();
    let (bounds, bcounts) = (
        as_list(bounds, DataType::Float64)?,
        as_list(bcounts, DataType::Int64)?,
    );
    let (pc, nc) = (as_list(pc, DataType::Int64)?, as_list(nc, DataType::Int64)?);
    let i64s = |a: &ArrayRef| cast(a, &DataType::Int64);
    let f64s = |a: &ArrayRef| cast(a, &DataType::Float64);
    let i32s = |a: &ArrayRef| cast(a, &DataType::Int32);
    let (count, zero_count) = (i64s(count)?, i64s(zero_count)?);
    let (sum, zt, min, max) = (f64s(sum)?, f64s(zt)?, f64s(min)?, f64s(max)?);
    let (scale, po, no) = (i32s(scale)?, i32s(po)?, i32s(no)?);
    let (count, zero_count) = (
        count.as_primitive::<Int64Type>(),
        zero_count.as_primitive::<Int64Type>(),
    );
    let (sum, zt) = (
        sum.as_primitive::<Float64Type>(),
        zt.as_primitive::<Float64Type>(),
    );
    let (min, max) = (
        min.as_primitive::<Float64Type>(),
        max.as_primitive::<Float64Type>(),
    );
    let (scale, po, no) = (
        scale.as_primitive::<Int32Type>(),
        po.as_primitive::<Int32Type>(),
        no.as_primitive::<Int32Type>(),
    );
    for i in 0..series.len() {
        let exp = match kind.is_valid(i).then(|| kind.value(i)) {
            Some("histogram") => false,
            Some("exponential_histogram") => true,
            Some("summary") => {
                return Err(invalid(
                    "histogram functions are not supported on summary metrics",
                ));
            }
            Some("gauge" | "sum") | None => continue,
            Some(other) => return Err(invalid(format!("unknown metric_type {other:?}"))),
        };
        if !series.is_valid(i) || !ts.is_valid(i) {
            continue;
        }
        let row = Row {
            series: series.value(i).to_owned(),
            ts: ts.value(i),
            start: if start.is_valid(i) { start.value(i) } else { 0 },
            temporality: temp.is_valid(i).then(|| temp.value(i)),
            exp,
            bounds: list_values::<Float64Type>(&bounds, i, "explicit_bounds")?,
            bucket_counts: list_values::<Int64Type>(&bcounts, i, "bucket_counts")?,
            count: count.is_valid(i).then(|| count.value(i)),
            sum: sum.is_valid(i).then(|| sum.value(i)),
            scale: scale.is_valid(i).then(|| scale.value(i)),
            zero_count: zero_count.is_valid(i).then(|| zero_count.value(i)),
            zero_threshold: zt.is_valid(i).then(|| zt.value(i)),
            pos_offset: po.is_valid(i).then(|| po.value(i)),
            pos_counts: list_values::<Int64Type>(&pc, i, "positive_bucket_counts")?,
            neg_offset: no.is_valid(i).then(|| no.value(i)),
            neg_counts: list_values::<Int64Type>(&nc, i, "negative_bucket_counts")?,
            min: min.is_valid(i).then(|| min.value(i)),
            max: max.is_valid(i).then(|| max.value(i)),
        };
        row.point()?;
        rows.push(row);
    }
    Ok(rows)
}
fn list_scalar(vals: Vec<ScalarValue>, t: DataType) -> ScalarValue {
    ScalarValue::List(ScalarValue::new_list_nullable(&vals, &t))
}

fn nested<T>(vals: &[T], f: fn(T) -> ScalarValue, t: DataType) -> ScalarValue
where
    T: Copy,
{
    list_scalar(vals.iter().map(|&v| f(v)).collect(), t)
}

pub(super) fn encode(r: &[Row], instant: Option<i64>) -> Vec<ScalarValue> {
    let col =
        |get: fn(&Row) -> ScalarValue, t: DataType| list_scalar(r.iter().map(get).collect(), t);
    let f64s = |get: fn(&Row) -> &Vec<f64>| {
        list_scalar(
            r.iter()
                .map(|x| nested(get(x), |v| ScalarValue::Float64(Some(v)), DataType::Float64))
                .collect(),
            list_of(DataType::Float64),
        )
    };
    let i64s = |get: fn(&Row) -> &Vec<i64>| {
        list_scalar(
            r.iter()
                .map(|x| nested(get(x), |v| ScalarValue::Int64(Some(v)), DataType::Int64))
                .collect(),
            list_of(DataType::Int64),
        )
    };
    vec![
        col(
            |x| ScalarValue::Utf8(Some(x.series.clone())),
            DataType::Utf8,
        ),
        col(|x| ScalarValue::Int64(Some(x.ts)), DataType::Int64),
        col(|x| ScalarValue::Int64(Some(x.start)), DataType::Int64),
        col(|x| ScalarValue::Int32(x.temporality), DataType::Int32),
        col(
            |x| {
                ScalarValue::Utf8(Some(
                    if x.exp {
                        "exponential_histogram"
                    } else {
                        "histogram"
                    }
                    .into(),
                ))
            },
            DataType::Utf8,
        ),
        f64s(|x| &x.bounds),
        i64s(|x| &x.bucket_counts),
        col(|x| ScalarValue::Int64(x.count), DataType::Int64),
        col(|x| ScalarValue::Float64(x.sum), DataType::Float64),
        col(|x| ScalarValue::Int32(x.scale), DataType::Int32),
        col(|x| ScalarValue::Int64(x.zero_count), DataType::Int64),
        col(
            |x| ScalarValue::Float64(x.zero_threshold),
            DataType::Float64,
        ),
        col(|x| ScalarValue::Int32(x.pos_offset), DataType::Int32),
        i64s(|x| &x.pos_counts),
        col(|x| ScalarValue::Int32(x.neg_offset), DataType::Int32),
        i64s(|x| &x.neg_counts),
        col(|x| ScalarValue::Float64(x.min), DataType::Float64),
        col(|x| ScalarValue::Float64(x.max), DataType::Float64),
        ScalarValue::Int64(instant),
    ]
}

/// Decode partial states (18 list columns plus `instant`) into rows and the instant.
pub(super) fn decode(states: &[ArrayRef]) -> Result<(Vec<Row>, Option<i64>)> {
    if states.len() != ARGS {
        return Err(DataFusionError::Internal(format!(
            "histogram accumulator expects {ARGS} state columns"
        )));
    }
    let lists: Vec<&ListArray> = states[..ARGS - 1]
        .iter()
        .map(|s| s.as_list::<i32>())
        .collect();
    let instant = states[ARGS - 1].as_primitive::<Int64Type>();
    let mut rows = Vec::new();
    let mut first = None;
    for row in 0..instant.len() {
        if instant.is_valid(row) && first.is_none() {
            first = Some(instant.value(row));
        }
        if lists[0].is_valid(row) {
            let cols: Vec<ArrayRef> = lists.iter().map(|l| l.value(row)).collect();
            rows.extend(parse_rows(&cols)?);
        }
    }
    Ok((rows, first))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::Float64Type as F64;
    use datafusion::arrow::datatypes::Int64Type as I64;

    use super::*;

    /// 18 columns for `n` identical rows of the given kind.
    fn cols(kind: &str, n: usize, bounds: Option<Vec<f64>>, counts: Vec<i64>) -> Vec<ArrayRef> {
        let exp = kind == "exponential_histogram";
        let lf = |v: Option<Vec<f64>>| {
            Arc::new(ListArray::from_iter_primitive::<F64, _, _>((0..n).map(
                |_| {
                    v.clone()
                        .map(|v| v.into_iter().map(Some).collect::<Vec<_>>())
                },
            ))) as ArrayRef
        };
        let li = |v: Option<Vec<i64>>| {
            Arc::new(ListArray::from_iter_primitive::<I64, _, _>((0..n).map(
                |_| {
                    v.clone()
                        .map(|v| v.into_iter().map(Some).collect::<Vec<_>>())
                },
            ))) as ArrayRef
        };
        let i32s = |v: Option<i32>| Arc::new(Int32Array::from(vec![v; n])) as ArrayRef;
        let i64s = |v: Option<i64>| Arc::new(Int64Array::from(vec![v; n])) as ArrayRef;
        let f64s = |v: Option<f64>| Arc::new(Float64Array::from(vec![v; n])) as ArrayRef;
        vec![
            Arc::new(StringArray::from(vec!["s"; n])),
            i64s(Some(20)),
            i64s(Some(5)),
            i32s(Some(2)),
            Arc::new(StringArray::from(vec![kind; n])),
            lf(bounds),
            li((!exp).then(|| counts.clone())),
            i64s(None),
            f64s(Some(1.5)),
            i32s(exp.then_some(0)),
            i64s(exp.then_some(0)),
            f64s(exp.then_some(0.0)),
            i32s(exp.then_some(0)),
            li(exp.then(|| counts.clone())),
            i32s(None),
            li(None),
            f64s(None),
            f64s(None),
        ]
    }

    fn points(rows: &[Row]) -> Vec<HistPoint> {
        rows.iter().map(|r| r.point().unwrap().h).collect()
    }

    fn state_arrays(rows: &[Row], instant: Option<i64>) -> Vec<ArrayRef> {
        encode(rows, instant)
            .iter()
            .map(|s| s.to_array().unwrap())
            .collect()
    }

    #[test]
    fn state_round_trips_through_arrays() {
        let rows = parse_rows(&cols("histogram", 2, Some(vec![1.0, 2.0]), vec![1, 2, 3])).unwrap();
        let states = state_arrays(&rows, Some(7));
        assert_eq!(states.len(), ARGS);
        let (back, instant) = decode(&states).unwrap();
        assert_eq!(instant, Some(7));
        assert_eq!(back.len(), 2);
        assert_eq!(points(&back), points(&rows));
    }

    #[test]
    fn sort_key_is_total_and_state_matches_column_types() {
        let mut rows = parse_rows(&cols("histogram", 2, Some(vec![1.0]), vec![1, 2])).unwrap();
        rows[0].bucket_counts = vec![2, 2];
        rows.reverse();
        rows.sort_by_cached_key(|r| r.sort_key());
        assert_eq!(rows[0].bucket_counts, vec![1, 2]);
        assert!(rows[0].heap() > 0);
        let types = column_types();
        let scalars = encode(&rows, None);
        for (s, t) in scalars.iter().zip(types) {
            assert_eq!(s.data_type(), list_of(t));
        }
    }
}
