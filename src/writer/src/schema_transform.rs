use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, Timelike};
use common::schema::SCHEMA_DEFINITIONS;
use common::schema::schema_parser::ResolvedSchema;
use datafusion::arrow::{
    array::{
        Array, ArrayRef, Date32Array, Int32Array, Int64Array, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use std::sync::Arc;

/// Extract schema version from Flight metadata
pub fn extract_schema_version(metadata: &[u8]) -> Result<String> {
    let metadata_str =
        std::str::from_utf8(metadata).map_err(|e| anyhow!("Invalid UTF-8 in metadata: {}", e))?;

    let metadata_json: serde_json::Value = serde_json::from_str(metadata_str)
        .map_err(|e| anyhow!("Invalid JSON in metadata: {}", e))?;

    metadata_json
        .get("schema_version")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("Missing schema_version in metadata"))
}

/// Transform a trace RecordBatch from v1 to v2 schema
pub fn transform_trace_v1_to_v2(batch: RecordBatch) -> Result<RecordBatch> {
    let v2_schema = SCHEMA_DEFINITIONS.resolve_trace_schema("v2")?;
    let arrow_schema = create_arrow_schema_from_resolved(&v2_schema)?;

    // Debug logging to understand the schema mismatch
    log::debug!(
        "Transforming v1 batch with {} columns to v2 with {} expected fields",
        batch.num_columns(),
        v2_schema.fields.len()
    );
    log::debug!(
        "v1 columns: {:?}",
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );
    log::debug!(
        "v2 expected fields: {:?}",
        v2_schema.fields.iter().map(|f| &f.name).collect::<Vec<_>>()
    );

    let mut new_columns: Vec<ArrayRef> = Vec::new();

    // Process each v2 field
    for field in &v2_schema.fields {
        let column = match field.name.as_str() {
            // Direct mappings (same name in v1 and v2)
            "trace_id" | "span_id" | "parent_span_id" | "service_name" | "span_kind"
            | "status_code" | "status_message" | "is_root" => {
                get_column_by_name(&batch, &field.name)?
            }

            // UInt64 fields that need to be converted to Int64 for Iceberg compatibility
            "start_time_unix_nano" | "end_time_unix_nano" => {
                let col = get_column_by_name(&batch, &field.name)?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("{} is not UInt64Array", field.name))?;

                let int_values: Vec<Option<i64>> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            None
                        } else {
                            Some(uint_array.value(i) as i64)
                        }
                    })
                    .collect();

                Arc::new(Int64Array::from(int_values))
            }

            // Complex types converted to JSON strings
            "events" | "links" => {
                // For now, convert these to JSON string representation
                // TODO: Properly serialize complex Arrow structures
                let len = batch.num_rows();
                Arc::new(StringArray::from(vec![Some("[]"); len]))
            }

            // Renamed fields
            "span_name" => get_column_by_name(&batch, "name")?,
            "duration_nanos" => {
                // Convert UInt64 to Int64 for Iceberg compatibility
                let col = get_column_by_name(&batch, "duration_nano")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("duration_nano is not UInt64Array"))?;

                let int_values: Vec<Option<i64>> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            None
                        } else {
                            Some(uint_array.value(i) as i64)
                        }
                    })
                    .collect();

                Arc::new(Int64Array::from(int_values))
            }
            "span_attributes" => get_column_by_name(&batch, "attributes_json")?,
            "resource_attributes" => get_column_by_name(&batch, "resource_json")?,

            // New computed fields
            "timestamp" => {
                // Copy from start_time_unix_nano as TimestampNanosecondArray
                let start_times = get_column_by_name(&batch, "start_time_unix_nano")?;
                let uint_array = start_times
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("start_time_unix_nano is not UInt64Array"))?;

                let timestamps: Vec<Option<i64>> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            None
                        } else {
                            // Unix nanoseconds should fit in i64
                            Some(uint_array.value(i) as i64)
                        }
                    })
                    .collect();

                Arc::new(TimestampNanosecondArray::from(timestamps))
            }

            "date_day" => {
                // Extract date from start_time_unix_nano
                let start_times = get_column_by_name(&batch, "start_time_unix_nano")?;
                let uint_array = start_times
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("start_time_unix_nano is not UInt64Array"))?;

                let dates: Vec<Option<i32>> = (0..uint_array.len())
                    .map(|i| {
                        let nanos = uint_array.value(i);
                        let secs = (nanos / 1_000_000_000) as i64;
                        let dt = DateTime::from_timestamp(secs, 0)?;
                        // Days since Unix epoch
                        Some((dt.naive_utc().date().num_days_from_ce() - 719163) as i32)
                    })
                    .collect();

                Arc::new(Date32Array::from(dates))
            }

            "hour" => {
                // Extract hour from start_time_unix_nano
                let start_times = get_column_by_name(&batch, "start_time_unix_nano")?;
                let uint_array = start_times
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("start_time_unix_nano is not UInt64Array"))?;

                let hours: Vec<Option<i32>> = (0..uint_array.len())
                    .map(|i| {
                        let nanos = uint_array.value(i);
                        let secs = (nanos / 1_000_000_000) as i64;
                        let dt = DateTime::from_timestamp(secs, 0)?;
                        Some(dt.hour() as i32)
                    })
                    .collect();

                Arc::new(Int32Array::from(hours))
            }

            // New optional fields with default empty values
            "trace_state"
            | "resource_schema_url"
            | "scope_name"
            | "scope_version"
            | "scope_schema_url"
            | "scope_attributes" => {
                // Create array of nulls for these new optional fields
                let len = batch.num_rows();
                Arc::new(StringArray::from(vec![None as Option<&str>; len]))
            }

            _ => return Err(anyhow!("Unknown field in v2 schema: {}", field.name)),
        };

        new_columns.push(column);
    }

    let result = RecordBatch::try_new(arrow_schema, new_columns)
        .map_err(|e| anyhow!("Failed to create transformed RecordBatch: {}", e))?;

    log::debug!(
        "Transformation complete: created v2 batch with {} columns",
        result.num_columns()
    );

    Ok(result)
}

/// Get column by name from RecordBatch
fn get_column_by_name(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    batch
        .schema()
        .column_with_name(name)
        .map(|(idx, _)| batch.column(idx).clone())
        .ok_or_else(|| anyhow!("Column '{}' not found in batch", name))
}

/// Create Arrow schema from resolved schema
fn create_arrow_schema_from_resolved(resolved: &ResolvedSchema) -> Result<Arc<Schema>> {
    let mut fields = Vec::new();

    for field in &resolved.fields {
        let data_type = match field.field_type.as_str() {
            "string" => DataType::Utf8,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "uint64" => DataType::Int64, // Map uint64 to Int64 for Iceberg compatibility
            "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "timestamp_ns" => {
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
            }
            "date" => DataType::Date32,
            "list<struct>" => {
                // For now, treat as string (will be handled properly later)
                DataType::Utf8
            }
            _ => return Err(anyhow!("Unsupported field type: {}", field.field_type)),
        };

        fields.push(Field::new(&field.name, data_type, !field.required));
    }

    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_schema_version() {
        let metadata = r#"{"schema_version": "v1", "signal_type": "traces"}"#;
        let version = extract_schema_version(metadata.as_bytes()).unwrap();
        assert_eq!(version, "v1");
    }

    #[test]
    fn test_extract_schema_version_missing() {
        let metadata = r#"{"signal_type": "traces"}"#;
        let result = extract_schema_version(metadata.as_bytes());
        assert!(result.is_err());
    }
}
