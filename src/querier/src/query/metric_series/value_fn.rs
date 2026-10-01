//! Per-value functions of a Series or Scalar frame (`map`, `filter`, and a
//! `binop` with a number), evaluated in Rust so NaN, ±Inf and −0 follow IEEE
//! as in Prometheus (Arrow's float comparisons use a total order instead).

use std::sync::Arc;

use chrono::{DateTime, Datelike, Months, NaiveDate, Timelike};
use common::query_ir::{BinopOp, MapFn};
use datafusion::arrow::array::{Array, AsArray, Float64Builder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, col,
};

const NS_PER_SEC: f64 = 1e9;

/// What one value becomes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum ValueOp {
    /// A `map` function and its `args` (as bits).
    Map(MapFn, Vec<u64>),
    /// `value op number` (`number op value` when `reverse`); a filtering
    /// comparison keeps the frame's value.
    Number {
        op: BinopOp,
        number: u64,
        reverse: bool,
        bool: bool,
    },
}

impl ValueOp {
    pub(super) fn map(func: MapFn, args: &[f64]) -> Self {
        ValueOp::Map(func, args.iter().map(|a| a.to_bits()).collect())
    }

    pub(super) fn number(op: BinopOp, number: f64, reverse: bool, bool: bool) -> Self {
        ValueOp::Number {
            op,
            number: number.to_bits(),
            reverse,
            bool,
        }
    }

    /// The new value of `v` at instant `t_ns`, `None` to drop the row.
    fn apply(&self, v: f64, t_ns: i64) -> Option<f64> {
        match self {
            ValueOp::Map(func, args) => {
                let arg = |i: usize| args.get(i).map(|a| f64::from_bits(*a));
                map(*func, v, t_ns, arg)
            }
            ValueOp::Number {
                op,
                number,
                reverse,
                bool,
            } => {
                let n = f64::from_bits(*number);
                let (l, r) = if *reverse { (n, v) } else { (v, n) };
                match compare(*op, l, r) {
                    Some(holds) if *bool => Some(if holds { 1.0 } else { 0.0 }),
                    Some(holds) => holds.then_some(v),
                    None => arithmetic(*op, l, r),
                }
            }
        }
    }
}

/// `l op r` for an arithmetic operator.
pub(crate) fn arithmetic(op: BinopOp, l: f64, r: f64) -> Option<f64> {
    Some(match op {
        BinopOp::Add => l + r,
        BinopOp::Sub => l - r,
        BinopOp::Mul => l * r,
        BinopOp::Div => l / r,
        BinopOp::Mod => l % r,
        BinopOp::Pow => l.powf(r),
        BinopOp::Atan2 => l.atan2(r),
        _ => return None,
    })
}

/// Whether `l op r` holds, for a comparison operator (IEEE: NaN compares
/// unequal to everything).
pub(crate) fn compare(op: BinopOp, l: f64, r: f64) -> Option<bool> {
    Some(match op {
        BinopOp::Eq => l == r,
        BinopOp::Ne => l != r,
        BinopOp::Gt => l > r,
        BinopOp::Ge => l >= r,
        BinopOp::Lt => l < r,
        BinopOp::Le => l <= r,
        _ => return None,
    })
}

fn map(func: MapFn, v: f64, t_ns: i64, arg: impl Fn(usize) -> Option<f64>) -> Option<f64> {
    Some(match func {
        MapFn::Abs => v.abs(),
        MapFn::Ceil => v.ceil(),
        MapFn::Floor => v.floor(),
        MapFn::Round => {
            let inverse = 1.0 / arg(0).unwrap_or(1.0);
            (v * inverse + 0.5).floor() / inverse
        }
        MapFn::Sqrt => v.sqrt(),
        MapFn::Exp => v.exp(),
        MapFn::Ln => v.ln(),
        MapFn::Log2 => v.log2(),
        MapFn::Log10 => v.log10(),
        MapFn::Sgn if v > 0.0 => 1.0,
        MapFn::Sgn if v < 0.0 => -1.0,
        MapFn::Sgn => v,
        MapFn::Clamp => {
            let (min, max) = (arg(0)?, arg(1)?);
            if max < min {
                return None;
            }
            keep_nan(v, v.min(max).max(min))
        }
        MapFn::ClampMin => keep_nan(v, v.max(arg(0)?)),
        MapFn::ClampMax => keep_nan(v, v.min(arg(0)?)),
        MapFn::Timestamp => t_ns as f64 / NS_PER_SEC,
        calendar => return Some(calendar_part(calendar, v).unwrap_or(f64::NAN)),
    })
}

/// NaN stays NaN through a clamp, as through Go's `math.Min`/`math.Max`.
fn keep_nan(v: f64, clamped: f64) -> f64 {
    if v.is_nan() { v } else { clamped }
}

/// A calendar field (UTC) of `v` seconds since the epoch.
fn calendar_part(func: MapFn, v: f64) -> Option<f64> {
    if !v.is_finite() {
        return None;
    }
    let t = DateTime::from_timestamp(v as i64, 0)?;
    Some(f64::from(match func {
        MapFn::DayOfMonth => t.day(),
        MapFn::DayOfWeek => t.weekday().num_days_from_sunday(),
        MapFn::DayOfYear => t.ordinal(),
        MapFn::DaysInMonth => {
            let first = NaiveDate::from_ymd_opt(t.year(), t.month(), 1)?;
            let next = first.checked_add_months(Months::new(1))?;
            u32::try_from(next.signed_duration_since(first).num_days()).ok()?
        }
        MapFn::Hour => t.hour(),
        MapFn::Minute => t.minute(),
        MapFn::Month => t.month(),
        MapFn::Year => return Some(f64::from(t.year())),
        _ => return None,
    }))
}

/// `op(value, bucket)`: the new value, null where the row is dropped.
pub(super) fn value_expr(op: ValueOp) -> Expr {
    ScalarUDF::new_from_impl(ValueUdf {
        op,
        signature: Signature::any(2, Volatility::Immutable),
    })
    .call(vec![col("value"), col("bucket")])
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ValueUdf {
    op: ValueOp,
    signature: Signature,
}

impl ScalarUDFImpl for ValueUdf {
    fn name(&self) -> &str {
        "series_value"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let values = cast(&args.args[0].to_array(rows)?, &DataType::Float64)?;
        let values = values.as_primitive::<Float64Type>();
        let buckets = cast(&args.args[1].to_array(rows)?, &DataType::Int64)?;
        let buckets = buckets.as_primitive::<Int64Type>();
        let mut out = Float64Builder::with_capacity(rows);
        for row in 0..rows {
            let value = (values.is_valid(row) && buckets.is_valid(row))
                .then(|| self.op.apply(values.value(row), buckets.value(row)))
                .flatten();
            out.append_option(value);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(func: MapFn, args: &[f64], v: f64) -> Option<f64> {
        ValueOp::map(func, args).apply(v, 0)
    }

    #[test]
    fn math_follows_ieee_and_promql() {
        assert!(m(MapFn::Ln, &[], -1.0).is_some_and(f64::is_nan));
        assert_eq!(m(MapFn::Ln, &[], 0.0), Some(f64::NEG_INFINITY));
        assert_eq!(m(MapFn::Sqrt, &[], f64::INFINITY), Some(f64::INFINITY));
        assert_eq!(m(MapFn::Log2, &[], 8.0), Some(3.0));
        assert_eq!(m(MapFn::Exp, &[], f64::NEG_INFINITY), Some(0.0));
        assert!(m(MapFn::Sgn, &[], f64::NAN).is_some_and(f64::is_nan));
        assert_eq!(
            m(MapFn::Sgn, &[], -0.0).map(f64::to_bits),
            Some((-0.0f64).to_bits())
        );
        assert_eq!(m(MapFn::Sgn, &[], -3.0), Some(-1.0));
        // Round half up, to the nearest multiple of the argument.
        assert_eq!(m(MapFn::Round, &[], -2.5), Some(-2.0));
        assert_eq!(m(MapFn::Round, &[0.5], 1.3), Some(1.5));
        assert_eq!(m(MapFn::Clamp, &[0.0, 10.0], 12.0), Some(10.0));
        assert!(m(MapFn::Clamp, &[0.0, 10.0], f64::NAN).is_some_and(f64::is_nan));
        assert_eq!(m(MapFn::Clamp, &[10.0, 0.0], 5.0), None);
        assert_eq!(m(MapFn::ClampMin, &[1.0], f64::NEG_INFINITY), Some(1.0));
        assert!(m(MapFn::ClampMax, &[1.0], f64::NAN).is_some_and(f64::is_nan));
    }

    #[test]
    fn calendar_functions_read_utc_seconds() {
        // 2024-02-29T13:45:00Z, a Thursday.
        let t = 1_709_214_300.0;
        let got: Vec<_> = [
            MapFn::DayOfMonth,
            MapFn::DayOfWeek,
            MapFn::DayOfYear,
            MapFn::DaysInMonth,
            MapFn::Hour,
            MapFn::Minute,
            MapFn::Month,
            MapFn::Year,
        ]
        .into_iter()
        .map(|f| m(f, &[], t).unwrap_or(-1.0))
        .collect();
        assert_eq!(got, [29.0, 4.0, 60.0, 29.0, 13.0, 45.0, 2.0, 2024.0]);
        assert!(m(MapFn::Hour, &[], f64::NAN).is_some_and(f64::is_nan));
        assert_eq!(
            ValueOp::map(MapFn::Timestamp, &[]).apply(7.0, 90_000_000_000),
            Some(90.0)
        );
    }

    #[test]
    fn numbers_compare_with_ieee_semantics() {
        let n =
            |op, number, reverse, bool, v| ValueOp::number(op, number, reverse, bool).apply(v, 0);
        assert_eq!(n(BinopOp::Gt, 0.0, false, false, f64::NAN), None);
        assert!(n(BinopOp::Ne, 0.0, false, false, f64::NAN).is_some_and(f64::is_nan));
        assert_eq!(n(BinopOp::Eq, 0.0, false, true, -0.0), Some(1.0));
        // `2 < v` keeps v's value; `bool` yields 0/1.
        assert_eq!(n(BinopOp::Lt, 2.0, true, false, 3.0), Some(3.0));
        assert_eq!(n(BinopOp::Lt, 2.0, true, true, 1.0), Some(0.0));
        assert_eq!(n(BinopOp::Sub, 2.0, true, false, 3.0), Some(-1.0));
        assert_eq!(n(BinopOp::Div, 0.0, false, false, 1.0), Some(f64::INFINITY));
    }
}
