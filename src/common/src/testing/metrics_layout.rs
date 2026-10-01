//! Converts a legacy per-type metrics table fixture into the wide `metrics`
//! table (otel-native-schema layer 7, D10) shape, for querier tests that
//! exercise both physical layouts from one set of source data.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};

/// Converts a batch shaped like a legacy per-type metrics table into the wide
/// `metrics` table shape: appends a `metric_type` column set to
/// `metric_type` for every row, and — where present — converts the legacy
/// JSON-string `bucket_counts`/`explicit_bounds` columns into typed
/// `List<Int64>`/`List<Float64>` (a JSON `"+Inf"` bound becomes a real
/// `f64::INFINITY`).
pub fn to_wide(batch: &RecordBatch, metric_type: &str) -> RecordBatch {
    let n = batch.num_rows();
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    let parse_json = |raw: &str| -> Vec<f64> {
        let value: serde_json::Value = serde_json::from_str(raw).expect("valid JSON fixture");
        value
            .as_array()
            .expect("array fixture")
            .iter()
            .map(|v| crate::flight::conversion::json_to_f64(v).expect("numeric fixture"))
            .collect()
    };
    for name in ["bucket_counts", "explicit_bounds"] {
        let Some(idx) = fields.iter().position(|f| f.name() == name) else {
            continue;
        };
        let strings = columns[idx]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("legacy fixture column is Utf8");
        let rows: Vec<Option<Vec<f64>>> = (0..n)
            .map(|i| (!strings.is_null(i)).then(|| parse_json(strings.value(i))))
            .collect();
        let list: ArrayRef = if name == "bucket_counts" {
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                rows.iter()
                    .map(|row| {
                        row.as_ref()
                            .map(|v| v.iter().map(|f| Some(*f as i64)).collect::<Vec<_>>())
                    })
                    .collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                rows.iter()
                    .map(|row| {
                        row.as_ref()
                            .map(|v| v.iter().map(|f| Some(*f)).collect::<Vec<_>>())
                    })
                    .collect::<Vec<_>>(),
            ))
        };
        fields[idx] = Field::new(name, list.data_type().clone(), true);
        columns[idx] = list;
    }

    fields.push(Field::new("metric_type", DataType::Utf8, false));
    columns.push(Arc::new(StringArray::from(vec![metric_type; n])));

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).expect("to_wide produces a valid batch")
}
