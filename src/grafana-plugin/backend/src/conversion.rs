//! Conversion utilities from Arrow RecordBatch to Grafana Frame.

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use grafana_plugin_sdk::data::{self, Field};
use grafana_plugin_sdk::prelude::IntoField;

/// Error type for conversion operations.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum ConversionError {
    #[error("Unsupported data type: {0}")]
    UnsupportedType(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Data extraction failed: {0}")]
    DataExtraction(String),
}

/// Convert Arrow RecordBatches to a Grafana Frame.
#[allow(dead_code)]
pub fn batches_to_frame(
    batches: &[RecordBatch],
    schema: &Schema,
    frame_name: &str,
) -> Result<data::Frame, ConversionError> {
    if batches.is_empty() {
        return Ok(data::Frame::new(frame_name));
    }

    let mut fields: Vec<Field> = Vec::new();

    // Process each column in the schema
    for field in schema.fields() {
        let field_name = field.name();
        let data_type = field.data_type();

        // Collect column data from all batches
        let arrays: Vec<&dyn Array> = batches
            .iter()
            .filter_map(|batch| batch.column_by_name(field_name).map(|c| c.as_ref()))
            .collect();

        if arrays.is_empty() {
            continue;
        }

        // Convert based on data type
        if let Some(grafana_field) = convert_column_to_field(&arrays, field_name, data_type) {
            fields.push(grafana_field);
        }
    }

    Ok(data::Frame::new(frame_name).with_fields(fields))
}

/// Convert Arrow RecordBatches to a Grafana Frame with a time field.
///
/// This function adds a time field by converting a nanosecond timestamp column
/// to a DateTime field, placing it first in the frame.
pub fn batches_to_frame_with_time(
    batches: &[RecordBatch],
    schema: &Schema,
    frame_name: &str,
    timestamp_column: &str,
    time_field_name: &str,
) -> Result<data::Frame, ConversionError> {
    if batches.is_empty() {
        return Ok(data::Frame::new(frame_name));
    }

    let mut fields: Vec<Field> = Vec::new();

    // First, add the time field from the timestamp column
    let time_field =
        convert_timestamp_column_to_time_field(batches, timestamp_column, time_field_name)?;
    fields.push(time_field);

    // Process each column in the schema
    for field in schema.fields() {
        let field_name = field.name();
        let data_type = field.data_type();

        // Skip the timestamp column as we've already converted it to time
        if field_name == timestamp_column {
            continue;
        }

        // Collect column data from all batches
        let arrays: Vec<&dyn Array> = batches
            .iter()
            .filter_map(|batch| batch.column_by_name(field_name).map(|c| c.as_ref()))
            .collect();

        if arrays.is_empty() {
            continue;
        }

        // Convert based on data type
        if let Some(grafana_field) = convert_column_to_field(&arrays, field_name, data_type) {
            fields.push(grafana_field);
        }
    }

    Ok(data::Frame::new(frame_name).with_fields(fields))
}

/// Convert Arrow column arrays to a Grafana Field.
///
/// Returns None if any array fails to downcast to the expected type,
/// logging a warning in that case.
fn convert_column_to_field(
    arrays: &[&dyn Array],
    field_name: &str,
    data_type: &DataType,
) -> Option<Field> {
    match data_type {
        DataType::Utf8 => {
            // Pre-check: verify all arrays can be downcast to StringArray
            let typed_arrays: Vec<&StringArray> = arrays
                .iter()
                .filter_map(|arr| arr.as_any().downcast_ref::<StringArray>())
                .collect();

            if typed_arrays.len() != arrays.len() {
                tracing::warn!(
                    "Failed to downcast some arrays to StringArray for field '{field_name}'"
                );
                return None;
            }

            let values: Vec<Option<String>> = typed_arrays
                .iter()
                .flat_map(|string_arr| {
                    (0..string_arr.len()).map(move |i| {
                        if string_arr.is_null(i) {
                            None
                        } else {
                            Some(string_arr.value(i).to_string())
                        }
                    })
                })
                .collect();
            Some(values.into_field(field_name))
        }

        DataType::UInt64 => {
            // Pre-check: verify all arrays can be downcast to UInt64Array
            let typed_arrays: Vec<&UInt64Array> = arrays
                .iter()
                .filter_map(|arr| arr.as_any().downcast_ref::<UInt64Array>())
                .collect();

            if typed_arrays.len() != arrays.len() {
                tracing::warn!(
                    "Failed to downcast some arrays to UInt64Array for field '{field_name}'"
                );
                return None;
            }

            let values: Vec<Option<u64>> = typed_arrays
                .iter()
                .flat_map(|uint_arr| {
                    (0..uint_arr.len()).map(move |i| {
                        if uint_arr.is_null(i) {
                            None
                        } else {
                            Some(uint_arr.value(i))
                        }
                    })
                })
                .collect();
            Some(values.into_field(field_name))
        }

        DataType::UInt32 => {
            // Pre-check: verify all arrays can be downcast to UInt32Array
            let typed_arrays: Vec<&UInt32Array> = arrays
                .iter()
                .filter_map(|arr| arr.as_any().downcast_ref::<UInt32Array>())
                .collect();

            if typed_arrays.len() != arrays.len() {
                tracing::warn!(
                    "Failed to downcast some arrays to UInt32Array for field '{field_name}'"
                );
                return None;
            }

            let values: Vec<Option<u64>> = typed_arrays
                .iter()
                .flat_map(|uint_arr| {
                    (0..uint_arr.len()).map(move |i| {
                        if uint_arr.is_null(i) {
                            None
                        } else {
                            Some(uint_arr.value(i) as u64)
                        }
                    })
                })
                .collect();
            Some(values.into_field(field_name))
        }

        DataType::Int64 => {
            // Pre-check: verify all arrays can be downcast to Int64Array
            let typed_arrays: Vec<&Int64Array> = arrays
                .iter()
                .filter_map(|arr| arr.as_any().downcast_ref::<Int64Array>())
                .collect();

            if typed_arrays.len() != arrays.len() {
                tracing::warn!(
                    "Failed to downcast some arrays to Int64Array for field '{field_name}'"
                );
                return None;
            }

            let values: Vec<Option<i64>> = typed_arrays
                .iter()
                .flat_map(|int_arr| {
                    (0..int_arr.len()).map(move |i| {
                        if int_arr.is_null(i) {
                            None
                        } else {
                            Some(int_arr.value(i))
                        }
                    })
                })
                .collect();
            Some(values.into_field(field_name))
        }

        DataType::Int32 => {
            // Pre-check: verify all arrays can be downcast to Int32Array
            let typed_arrays: Vec<&Int32Array> = arrays
                .iter()
                .filter_map(|arr| arr.as_any().downcast_ref::<Int32Array>())
                .collect();

            if typed_arrays.len() != arrays.len() {
                tracing::warn!(
                    "Failed to downcast some arrays to Int32Array for field '{field_name}'"
                );
                return None;
            }

            let values: Vec<Option<i64>> = typed_arrays
                .iter()
                .flat_map(|int_arr| {
                    (0..int_arr.len()).map(move |i| {
                        if int_arr.is_null(i) {
                            None
                        } else {
                            Some(int_arr.value(i) as i64)
                        }
                    })
                })
                .collect();
            Some(values.into_field(field_name))
        }

        DataType::Float64 => {
            // Pre-check: verify all arrays can be downcast to Float64Array
            let typed_arrays: Vec<&Float64Array> = arrays
                .iter()
                .filter_map(|arr| arr.as_any().downcast_ref::<Float64Array>())
                .collect();

            if typed_arrays.len() != arrays.len() {
                tracing::warn!(
                    "Failed to downcast some arrays to Float64Array for field '{field_name}'"
                );
                return None;
            }

            let values: Vec<Option<f64>> = typed_arrays
                .iter()
                .flat_map(|float_arr| {
                    (0..float_arr.len()).map(move |i| {
                        if float_arr.is_null(i) {
                            None
                        } else {
                            Some(float_arr.value(i))
                        }
                    })
                })
                .collect();
            Some(values.into_field(field_name))
        }

        DataType::Boolean => {
            // Pre-check: verify all arrays can be downcast to BooleanArray
            let typed_arrays: Vec<&BooleanArray> = arrays
                .iter()
                .filter_map(|arr| arr.as_any().downcast_ref::<BooleanArray>())
                .collect();

            if typed_arrays.len() != arrays.len() {
                tracing::warn!(
                    "Failed to downcast some arrays to BooleanArray for field '{field_name}'"
                );
                return None;
            }

            let values: Vec<Option<bool>> = typed_arrays
                .iter()
                .flat_map(|bool_arr| {
                    (0..bool_arr.len()).map(move |i| {
                        if bool_arr.is_null(i) {
                            None
                        } else {
                            Some(bool_arr.value(i))
                        }
                    })
                })
                .collect();
            Some(values.into_field(field_name))
        }

        // Skip unsupported types
        _ => None,
    }
}

/// Convert nanosecond timestamp to DateTime<Utc>.
pub fn nanos_to_datetime(nanos: u64) -> DateTime<Utc> {
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs).single().unwrap_or_default()
}

/// Convert Arrow timestamp column to Grafana time field.
pub fn convert_timestamp_column_to_time_field(
    batches: &[RecordBatch],
    column_name: &str,
    field_name: &str,
) -> Result<Field, ConversionError> {
    let values: Vec<DateTime<Utc>> = batches
        .iter()
        .filter_map(|batch| batch.column_by_name(column_name))
        .flat_map(|col| {
            let uint_arr = col.as_any().downcast_ref::<UInt64Array>();
            match uint_arr {
                Some(arr) => (0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            Utc.timestamp_opt(0, 0).single().unwrap_or_default()
                        } else {
                            nanos_to_datetime(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>(),
                None => vec![],
            }
        })
        .collect();

    Ok(values.into_field(field_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
        UInt64Array,
    };
    use datafusion::arrow::datatypes::Field as ArrowField;
    use std::sync::Arc;

    /// Helper to create a simple test schema
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            ArrowField::new("trace_id", DataType::Utf8, false),
            ArrowField::new("span_id", DataType::Utf8, false),
            ArrowField::new("start_time_unix_nano", DataType::UInt64, false),
            ArrowField::new("duration_nano", DataType::UInt64, false),
            ArrowField::new("name", DataType::Utf8, false),
        ])
    }

    /// Helper to create a test RecordBatch
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(create_test_schema());

        let trace_ids: ArrayRef = Arc::new(StringArray::from(vec!["trace1", "trace2", "trace3"]));
        let span_ids: ArrayRef = Arc::new(StringArray::from(vec!["span1", "span2", "span3"]));
        let start_times: ArrayRef = Arc::new(UInt64Array::from(vec![
            1_704_067_200_000_000_000_u64, // 2024-01-01 00:00:00
            1_704_067_201_000_000_000_u64, // 2024-01-01 00:00:01
            1_704_067_202_000_000_000_u64, // 2024-01-01 00:00:02
        ]));
        let durations: ArrayRef = Arc::new(UInt64Array::from(vec![
            100_000_000_u64,
            200_000_000,
            300_000_000,
        ]));
        let names: ArrayRef = Arc::new(StringArray::from(vec!["op1", "op2", "op3"]));

        RecordBatch::try_new(
            schema,
            vec![trace_ids, span_ids, start_times, durations, names],
        )
        .unwrap()
    }

    #[test]
    fn test_nanos_to_datetime() {
        let nanos = 1_704_067_200_000_000_000_u64; // 2024-01-01 00:00:00 UTC
        let dt = nanos_to_datetime(nanos);
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
    }

    #[test]
    fn test_nanos_to_datetime_with_subseconds() {
        let nanos = 1_704_067_200_500_000_000_u64; // 2024-01-01 00:00:00.5 UTC
        let dt = nanos_to_datetime(nanos);
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.timestamp_subsec_millis(), 500);
    }

    #[test]
    fn test_batches_to_frame_empty() {
        let schema = create_test_schema();
        let batches: Vec<RecordBatch> = vec![];

        let result = batches_to_frame(&batches, &schema, "test_frame");
        assert!(result.is_ok());
    }

    #[test]
    fn test_batches_to_frame_with_data() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let frame = batches_to_frame(&[batch], &schema, "traces").unwrap();
        // Should have 5 fields: trace_id, span_id, start_time_unix_nano, duration_nano, name
        assert_eq!(frame.fields().len(), 5);
    }

    #[test]
    fn test_batches_to_frame_with_time() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let frame =
            batches_to_frame_with_time(&[batch], &schema, "traces", "start_time_unix_nano", "time")
                .unwrap();

        // Should have time field + 4 other fields (start_time_unix_nano is converted to time)
        assert_eq!(frame.fields().len(), 5);
    }

    #[test]
    fn test_batches_to_frame_with_time_empty() {
        let schema = create_test_schema();
        let batches: Vec<RecordBatch> = vec![];

        let result =
            batches_to_frame_with_time(&batches, &schema, "traces", "start_time_unix_nano", "time");

        // Should succeed with empty batches
        assert!(result.is_ok());
    }

    #[test]
    fn test_convert_timestamp_column_to_time_field() {
        let batch = create_test_batch();

        let result =
            convert_timestamp_column_to_time_field(&[batch], "start_time_unix_nano", "time");

        assert!(result.is_ok());
    }

    #[test]
    fn test_convert_string_column() {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "message",
            DataType::Utf8,
            false,
        )]));
        let messages: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![messages]).unwrap();

        let frame = batches_to_frame(&[batch], &schema, "test").unwrap();
        assert_eq!(frame.fields().len(), 1);
    }

    #[test]
    fn test_convert_int64_column() {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "count",
            DataType::Int64,
            false,
        )]));
        let counts: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![counts]).unwrap();

        let frame = batches_to_frame(&[batch], &schema, "test").unwrap();
        assert_eq!(frame.fields().len(), 1);
    }

    #[test]
    fn test_convert_int32_column() {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "count",
            DataType::Int32,
            false,
        )]));
        let counts: ArrayRef = Arc::new(Int32Array::from(vec![1_i32, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![counts]).unwrap();

        let frame = batches_to_frame(&[batch], &schema, "test").unwrap();
        assert_eq!(frame.fields().len(), 1);
    }

    #[test]
    fn test_convert_uint32_column() {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "count",
            DataType::UInt32,
            false,
        )]));
        let counts: ArrayRef = Arc::new(UInt32Array::from(vec![1_u32, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![counts]).unwrap();

        let frame = batches_to_frame(&[batch], &schema, "test").unwrap();
        assert_eq!(frame.fields().len(), 1);
    }

    #[test]
    fn test_convert_float64_column() {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();

        let frame = batches_to_frame(&[batch], &schema, "test").unwrap();
        assert_eq!(frame.fields().len(), 1);
    }

    #[test]
    fn test_convert_boolean_column() {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "is_root",
            DataType::Boolean,
            false,
        )]));
        let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        let batch = RecordBatch::try_new(schema.clone(), vec![flags]).unwrap();

        let frame = batches_to_frame(&[batch], &schema, "test").unwrap();
        assert_eq!(frame.fields().len(), 1);
    }

    #[test]
    fn test_convert_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", DataType::Utf8, false),
            ArrowField::new("value", DataType::UInt64, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![3, 4])) as ArrayRef,
            ],
        )
        .unwrap();

        let frame = batches_to_frame(&[batch1, batch2], &schema, "test").unwrap();
        assert_eq!(frame.fields().len(), 2);
    }

    #[test]
    fn test_unsupported_type_skipped() {
        // List type is not supported, should be skipped
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new(
                "tags",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));

        let names: ArrayRef = Arc::new(StringArray::from(vec!["test"]));
        // Create a batch with just the string field
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![ArrowField::new(
                "name",
                DataType::Utf8,
                false,
            )])),
            vec![names],
        )
        .unwrap();

        let frame = batches_to_frame(&[batch], &schema, "test").unwrap();
        // Should only have the name field since tags (List type) is not in the batch
        assert_eq!(frame.fields().len(), 1);
    }

    #[test]
    fn test_convert_timestamp_column_empty_batches() {
        let batches: Vec<RecordBatch> = vec![];

        let result = convert_timestamp_column_to_time_field(&batches, "timestamp", "time");
        assert!(result.is_ok());
    }

    #[test]
    fn test_nanos_to_datetime_zero() {
        let nanos = 0_u64;
        let dt = nanos_to_datetime(nanos);
        assert_eq!(dt.year(), 1970);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
    }
}
