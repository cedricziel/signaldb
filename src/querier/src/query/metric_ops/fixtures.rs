//! Wide `metrics` table fixtures shared by the IR and PromQL histogram tests.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{Field, Float64Type, Int64Type, Schema};

/// Cumulative points of metric `lat` from service `svc`, as `(series_id, ts, counts)`.
/// `histogram` rows use bounds `[1, 2, 4]`; `exponential_histogram` rows hold
/// `counts` as positive buckets at scale 0, offset 0.
pub(crate) fn histogram_points(kind: &str, rows: &[(&str, i64, &[i64])]) -> RecordBatch {
    let n = rows.len();
    let exp = kind == "exponential_histogram";
    let list = |on: bool| -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            rows.iter()
                .map(|r| on.then(|| r.2.iter().map(|c| Some(*c)).collect::<Vec<_>>())),
        ))
    };
    let exp_i32 = || Arc::new(Int32Array::from(vec![exp.then_some(0); n])) as ArrayRef;
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "timestamp",
            Arc::new(TimestampNanosecondArray::from_iter_values(
                rows.iter().map(|r| r.1),
            )),
        ),
        ("service_name", Arc::new(StringArray::from(vec!["svc"; n]))),
        ("metric_name", Arc::new(StringArray::from(vec!["lat"; n]))),
        ("metric_type", Arc::new(StringArray::from(vec![kind; n]))),
        (
            "series_id",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
        ),
        (
            "aggregation_temporality",
            Arc::new(Int32Array::from(vec![2; n])),
        ),
        (
            "explicit_bounds",
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                (0..n).map(|_| (!exp).then(|| vec![Some(1.0), Some(2.0), Some(4.0)])),
            )),
        ),
        ("bucket_counts", list(!exp)),
        ("scale", exp_i32()),
        (
            "zero_count",
            Arc::new(Int64Array::from(vec![exp.then_some(0); n])),
        ),
        ("positive_offset", exp_i32()),
        ("positive_bucket_counts", list(exp)),
    ];
    batch(columns)
}

/// A batch of nullable columns named as given.
fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.into_iter().map(|(_, a)| a).collect(),
    )
    .unwrap()
}
