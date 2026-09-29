//! Evaluation-instant helpers: which instants cover a point, and timestamp normalisation.

use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, Int64Builder, ListBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, col,
    lit,
};

use crate::query::error::QuerierError;

/// Most evaluation instants one query may have (Prometheus' 11k-point limit).
const MAX_INSTANTS: i64 = 11_000;

pub(crate) fn invalid(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::External(Box::new(QuerierError::InvalidInput(msg.into())))
}

/// Int64 nanoseconds from an Int64 or any-unit Timestamp column.
pub(super) fn as_ns(a: &ArrayRef) -> Result<Int64Array> {
    let a = match a.data_type() {
        DataType::Timestamp(_, tz) => {
            cast(a, &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()))?
        }
        _ => a.clone(),
    };
    Ok(cast(&a, &DataType::Int64)?
        .as_primitive::<Int64Type>()
        .clone())
}

/// [`covering_instants_udf`] over the `timestamp` column.
pub(super) fn covering_instants(first: i64, last: i64, step: i64, window: i64) -> Expr {
    covering_instants_udf().call(vec![
        col("timestamp"),
        lit(first),
        lit(last),
        lit(step),
        lit(window),
    ])
}

/// Rejects a non-positive `step`/`window` and more than 11 000 evaluation
/// instants `first + k·step <= last` (Prometheus' points-per-series limit).
pub(crate) fn check_instants(
    first: i64,
    last: i64,
    step: i64,
    window: i64,
) -> Result<(), QuerierError> {
    if step <= 0 || window <= 0 {
        return Err(QuerierError::InvalidInput(
            "step and range window must be positive".into(),
        ));
    }
    if last.saturating_sub(first) / step >= MAX_INSTANTS {
        return Err(QuerierError::InvalidInput(format!(
            "the query spans more than {MAX_INSTANTS} evaluation instants; increase the step or shrink the range"
        )));
    }
    Ok(())
}

/// Scalar UDF `covering_instants(ts, first_instant, last_instant, step_ns, window_ns) -> List<Int64>`:
/// every evaluation instant `t = first + k*step` (`t <= last`) whose window
/// `(t - window, t]` contains `ts`. The bounds must be constants; they are
/// checked by [`check_instants`].
pub fn covering_instants_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CoveringInstants {
        signature: Signature::any(5, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CoveringInstants {
    signature: Signature,
}

/// Instants `first + k*step <= last` inside `(ts - .., ts + window)`; `None` on overflow.
fn covering(ts: i64, first: i64, last: i64, step: i64, window: i64) -> Option<Vec<i64>> {
    let k0 = if ts > first {
        (ts.checked_sub(first)?.checked_add(step - 1)?) / step
    } else {
        0
    };
    let mut t = first.checked_add(k0.checked_mul(step)?)?;
    let mut out = Vec::new();
    while t <= last && t.checked_sub(window).is_none_or(|lo| lo < ts) {
        out.push(t);
        match t.checked_add(step) {
            Some(next) => t = next,
            None => break,
        }
    }
    Some(out)
}

impl ScalarUDFImpl for CoveringInstants {
    fn name(&self) -> &str {
        "covering_instants"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Int64,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let bound = |i: usize| -> Result<Option<i64>> {
            let ColumnarValue::Scalar(s) = &args.args[i] else {
                return Err(DataFusionError::Internal(
                    "covering_instants bounds must be constants".into(),
                ));
            };
            let a = as_ns(&s.to_array()?)?;
            Ok(a.is_valid(0).then(|| a.value(0)))
        };
        let ts = as_ns(&args.args[0].to_array(args.number_rows)?)?;
        let mut out = ListBuilder::new(Int64Builder::new());
        let (Some(first), Some(last), Some(step), Some(window)) =
            (bound(1)?, bound(2)?, bound(3)?, bound(4)?)
        else {
            (0..args.number_rows).for_each(|_| out.append_null());
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        };
        check_instants(first, last, step, window)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for row in 0..args.number_rows {
            if ts.is_null(row) {
                out.append_null();
                continue;
            }
            out.values().append_slice(
                &covering(ts.value(row), first, last, step, window).unwrap_or_default(),
            );
            out.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::scalar::ScalarValue;

    use super::*;

    const S: i64 = 1_000_000_000;

    #[test]
    fn non_nanosecond_timestamps_are_normalised() {
        let us: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(3), None]));
        let out = as_ns(&us).unwrap();
        assert_eq!((out.value(0), out.is_null(1)), (3000, true));
    }

    fn invoke(args: [Option<i64>; 5]) -> Result<Vec<Option<Vec<i64>>>> {
        let udf = covering_instants_udf();
        let scalar = |v: Option<i64>| ColumnarValue::Scalar(ScalarValue::Int64(v));
        let out = udf.invoke_with_args(ScalarFunctionArgs {
            args: args.map(scalar).to_vec(),
            arg_fields: (0..5)
                .map(|_| Arc::new(Field::new("a", DataType::Int64, true)))
                .collect(),
            number_rows: 1,
            return_field: Arc::new(Field::new("r", udf.return_type(&[])?, true)),
            config_options: Arc::new(Default::default()),
        })?;
        let ColumnarValue::Array(arr) = out else {
            panic!("expected array")
        };
        let list = arr.as_list::<i32>();
        Ok((0..list.len())
            .map(|i| {
                list.is_valid(i)
                    .then(|| list.value(i).as_primitive::<Int64Type>().values().to_vec())
            })
            .collect())
    }

    fn secs(
        ts: i64,
        first: i64,
        last: i64,
        step: i64,
        window: i64,
    ) -> Result<Vec<Option<Vec<i64>>>> {
        invoke([ts, first, last, step, window].map(|v| Some(v * S)))
    }

    #[test]
    fn covering_instants_follow_half_open_window() {
        // (ts, first, last, step, window) in seconds -> instants in seconds
        let cases = [
            ((65, 0, 180, 60, 120), vec![120, 180]),
            ((60, 0, 180, 60, 120), vec![60, 120]),
            ((120, 0, 180, 60, 120), vec![120, 180]),
            ((0, 0, 180, 60, 120), vec![0, 60]),
            ((65, 0, 120, 60, 120), vec![120]),
            ((-5, 0, 120, 60, 120), vec![0, 60]),
            ((500, 0, 120, 60, 120), vec![]),
            ((65, 300, 100, 60, 120), vec![]),
        ];
        for ((ts, first, last, step, window), want) in cases {
            let got = secs(ts, first, last, step, window).unwrap();
            let got: Vec<i64> = got[0].clone().unwrap().iter().map(|v| v / S).collect();
            assert_eq!(got, want, "{:?}", (ts, first, last, step, window));
        }
    }

    #[test]
    fn covering_instants_guard_nulls_overflow_and_bad_arguments() {
        assert_eq!(
            invoke([Some(1), None, Some(9), Some(1), Some(5)]).unwrap(),
            vec![None]
        );
        let big = i64::MAX;
        assert!(invoke([Some(big - 5), Some(big - 20), Some(big), Some(10), Some(30)]).is_ok());
        assert!(invoke([Some(big), Some(big - 100), Some(big), Some(10), Some(30)]).is_ok());
        for bad in [
            [1, 0, 9, 0, 5],
            [1, 0, 9, 1, 0],
            [1, 0, 9, -1, 5],
            [1, 0, 11_000, 1, 5],
            [1, i64::MIN, i64::MAX, 1, 5],
        ] {
            let err = QuerierError::from(invoke(bad.map(Some)).unwrap_err());
            assert!(matches!(err, QuerierError::InvalidInput(_)), "{bad:?}");
        }
        assert!(invoke([1, 0, 10_999, 1, 11_000].map(Some)).is_ok());
        // A wide window over a short query range covers few instants.
        assert!(invoke([1, 0, 9, 1, 11_001].map(Some)).is_ok());
        // A window of many steps is bounded by `last`.
        let long = invoke([1, 0, 9, 1, 1_000_000].map(Some)).unwrap();
        assert_eq!(long, vec![Some((1..=9).collect())]);
    }

    #[test]
    fn check_instants_caps_the_evaluation_instants() {
        assert!(check_instants(0, 10_999, 1, 1).is_ok());
        assert!(check_instants(0, 11_000, 1, 1).is_err());
        assert!(check_instants(5, 0, 1, 1).is_ok());
        assert!(check_instants(0, 1, 1, 0).is_err());
    }
}
