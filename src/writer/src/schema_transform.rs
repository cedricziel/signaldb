use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, Timelike};
use common::schema::SCHEMA_DEFINITIONS;
use common::schema::schema_parser::ResolvedSchema;
use datafusion::arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array,
        Int64Array, ListArray, StringArray, StructArray, TimestampNanosecondArray, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::Arc;

/// Struct to hold all extracted metadata from Flight messages
#[derive(Debug, Clone)]
pub struct FlightMetadata {
    pub schema_version: String,
    pub signal_type: Option<String>,
    pub target_table: Option<String>,
    pub tenant_id: Option<String>,
    pub dataset_id: Option<String>,
}

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

/// Extract all metadata from Flight metadata bytes
/// Returns FlightMetadata with schema_version, signal_type, and target_table (if present)
pub fn extract_flight_metadata(metadata: &[u8]) -> Result<FlightMetadata> {
    let metadata_str =
        std::str::from_utf8(metadata).map_err(|e| anyhow!("Invalid UTF-8 in metadata: {}", e))?;

    let metadata_json: serde_json::Value = serde_json::from_str(metadata_str)
        .map_err(|e| anyhow!("Invalid JSON in metadata: {}", e))?;

    let schema_version = metadata_json
        .get("schema_version")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("Missing schema_version in metadata"))?;

    let signal_type = metadata_json
        .get("signal_type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let target_table = metadata_json
        .get("target_table")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let tenant_id = metadata_json
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let dataset_id = metadata_json
        .get("dataset_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(FlightMetadata {
        schema_version,
        signal_type,
        target_table,
        tenant_id,
        dataset_id,
    })
}

/// Determine WalOperation from signal_type metadata
pub fn determine_wal_operation(signal_type: Option<&str>) -> common::wal::WalOperation {
    match signal_type {
        Some("traces") => common::wal::WalOperation::WriteTraces,
        Some("logs") => common::wal::WalOperation::WriteLogs,
        Some("metrics") => common::wal::WalOperation::WriteMetrics,
        _ => {
            log::warn!("Unknown signal_type: {signal_type:?}, defaulting to WriteTraces");
            common::wal::WalOperation::WriteTraces // Default fallback
        }
    }
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
            "events" => {
                let col = get_column_by_name(&batch, "events")?;
                serialize_list_array_to_json_strings(&col, batch.num_rows(), "events")?
            }
            "links" => {
                let col = get_column_by_name(&batch, "links")?;
                serialize_list_array_to_json_strings(&col, batch.num_rows(), "links")?
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

            // Scope and resource metadata fields - now present in v1 schema
            "trace_state"
            | "resource_schema_url"
            | "scope_name"
            | "scope_version"
            | "scope_schema_url"
            | "scope_attributes" => get_column_by_name(&batch, &field.name)?,

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

/// Serialize a ListArray (containing StructArrays) to JSON string arrays
/// Each row's list of structs becomes a JSON array string like '[{"name":"event1",...},...]'
fn serialize_list_array_to_json_strings(
    col: &ArrayRef,
    num_rows: usize,
    field_name: &str,
) -> Result<ArrayRef> {
    let list_array = col
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow!("{field_name} is not a ListArray"))?;

    let mut json_strings: Vec<Option<String>> = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        if list_array.is_null(row) {
            json_strings.push(Some("[]".to_string()));
            continue;
        }

        let list_values = list_array.value(row);
        let struct_array = list_values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| anyhow!("{field_name} list items are not StructArray"))?;

        let mut items = Vec::new();
        for i in 0..struct_array.len() {
            let mut obj = serde_json::Map::new();
            for (col_idx, field) in struct_array.fields().iter().enumerate() {
                let col_array = struct_array.column(col_idx);
                let value = if col_array.is_null(i) {
                    serde_json::Value::Null
                } else if let Some(str_arr) = col_array.as_any().downcast_ref::<StringArray>() {
                    serde_json::Value::String(str_arr.value(i).to_string())
                } else if let Some(uint_arr) = col_array.as_any().downcast_ref::<UInt64Array>() {
                    serde_json::Value::Number(uint_arr.value(i).into())
                } else {
                    serde_json::Value::Null
                };
                obj.insert(field.name().clone(), value);
            }
            items.push(serde_json::Value::Object(obj));
        }

        json_strings.push(Some(
            serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string()),
        ));
    }

    Ok(Arc::new(StringArray::from(json_strings)))
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

pub fn transform_logs_v1_to_iceberg(batch: RecordBatch) -> Result<RecordBatch> {
    let v1_schema = SCHEMA_DEFINITIONS.resolve_log_schema("v1")?;
    let arrow_schema = create_arrow_schema_from_resolved(&v1_schema)?;

    let num_rows = batch.num_rows();
    let mut new_columns: Vec<ArrayRef> = Vec::new();

    for field in &v1_schema.fields {
        let column: ArrayRef = match field.name.as_str() {
            "timestamp" => {
                let col = get_column_by_name(&batch, "time_unix_nano")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("time_unix_nano is not UInt64Array"))?;
                let values: Vec<Option<i64>> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            None
                        } else {
                            Some(uint_array.value(i) as i64)
                        }
                    })
                    .collect();
                Arc::new(TimestampNanosecondArray::from(values))
            }
            "observed_timestamp" => {
                let col = get_column_by_name(&batch, "observed_time_unix_nano")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("observed_time_unix_nano is not UInt64Array"))?;
                let values: Vec<Option<i64>> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            None
                        } else {
                            Some(uint_array.value(i) as i64)
                        }
                    })
                    .collect();
                Arc::new(TimestampNanosecondArray::from(values))
            }
            "trace_id" => {
                let col = get_column_by_name(&batch, "trace_id")?;
                let bin_array = col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow!("trace_id is not BinaryArray"))?;
                let values: Vec<Option<String>> = (0..bin_array.len())
                    .map(|i| {
                        if bin_array.is_null(i) {
                            None
                        } else {
                            let bytes = bin_array.value(i);
                            if bytes.is_empty() {
                                None
                            } else {
                                Some(hex::encode(bytes))
                            }
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "span_id" => {
                let col = get_column_by_name(&batch, "span_id")?;
                let bin_array = col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow!("span_id is not BinaryArray"))?;
                let values: Vec<Option<String>> = (0..bin_array.len())
                    .map(|i| {
                        if bin_array.is_null(i) {
                            None
                        } else {
                            let bytes = bin_array.value(i);
                            if bytes.is_empty() {
                                None
                            } else {
                                Some(hex::encode(bytes))
                            }
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "trace_flags" => {
                let col = get_column_by_name(&batch, "flags")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| anyhow!("flags is not UInt32Array"))?;
                let values: Vec<Option<i32>> = (0..uint_array.len())
                    .map(|i| {
                        if uint_array.is_null(i) {
                            None
                        } else {
                            Some(uint_array.value(i) as i32)
                        }
                    })
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            "severity_text" | "severity_number" | "service_name" | "body" => {
                get_column_by_name(&batch, &field.name)?
            }
            "resource_schema_url" => {
                let col = get_column_by_name(&batch, "resource_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("resource_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        let json_str = str_array.value(i);
                        serde_json::from_str::<serde_json::Value>(json_str)
                            .ok()
                            .and_then(|v| {
                                v.get("schema_url")
                                    .and_then(|s| s.as_str())
                                    .map(String::from)
                            })
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "resource_attributes" => {
                let col = get_column_by_name(&batch, "resource_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("resource_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        let json_str = str_array.value(i);
                        match serde_json::from_str::<serde_json::Value>(json_str) {
                            Ok(v) => {
                                if let Some(attrs) = v.get("attributes") {
                                    Some(attrs.to_string())
                                } else {
                                    Some(json_str.to_string())
                                }
                            }
                            Err(_) => Some(json_str.to_string()),
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "scope_schema_url" => {
                let col = get_column_by_name(&batch, "scope_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("scope_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        serde_json::from_str::<serde_json::Value>(str_array.value(i))
                            .ok()
                            .and_then(|v| {
                                v.get("schema_url")
                                    .and_then(|s| s.as_str())
                                    .map(String::from)
                            })
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "scope_name" => {
                let col = get_column_by_name(&batch, "scope_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("scope_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        serde_json::from_str::<serde_json::Value>(str_array.value(i))
                            .ok()
                            .and_then(|v| v.get("name").and_then(|s| s.as_str()).map(String::from))
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "scope_version" => {
                let col = get_column_by_name(&batch, "scope_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("scope_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        serde_json::from_str::<serde_json::Value>(str_array.value(i))
                            .ok()
                            .and_then(|v| {
                                v.get("version").and_then(|s| s.as_str()).map(String::from)
                            })
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "scope_attributes" => {
                let col = get_column_by_name(&batch, "scope_json")?;
                let str_array = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("scope_json is not StringArray"))?;
                let values: Vec<Option<String>> = (0..str_array.len())
                    .map(|i| {
                        if str_array.is_null(i) {
                            return None;
                        }
                        serde_json::from_str::<serde_json::Value>(str_array.value(i))
                            .ok()
                            .and_then(|v| v.get("attributes").map(|a| a.to_string()))
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            "log_attributes" => get_column_by_name(&batch, "attributes_json")?,
            "date_day" => {
                let col = get_column_by_name(&batch, "time_unix_nano")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("time_unix_nano is not UInt64Array"))?;
                let dates: Vec<Option<i32>> = (0..uint_array.len())
                    .map(|i| {
                        let nanos = uint_array.value(i);
                        let secs = (nanos / 1_000_000_000) as i64;
                        let dt = DateTime::from_timestamp(secs, 0)?;
                        Some((dt.naive_utc().date().num_days_from_ce() - 719163) as i32)
                    })
                    .collect();
                Arc::new(Date32Array::from(dates))
            }
            "hour" => {
                let col = get_column_by_name(&batch, "time_unix_nano")?;
                let uint_array = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("time_unix_nano is not UInt64Array"))?;
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
            _ => return Err(anyhow!("Unknown field in logs schema: {}", field.name)),
        };
        new_columns.push(column);
    }

    let result = RecordBatch::try_new(arrow_schema, new_columns)
        .map_err(|e| anyhow!("Failed to create transformed log RecordBatch: {}", e))?;

    log::debug!(
        "Log transformation complete: {} input rows -> {} output columns",
        num_rows,
        result.num_columns()
    );

    Ok(result)
}

#[derive(Clone, Default)]
struct ResourceContext {
    service_name: Option<String>,
    resource_schema_url: Option<String>,
    resource_attributes: Option<String>,
}

#[derive(Clone, Default)]
struct ScopeContext {
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_schema_url: Option<String>,
    scope_attributes: Option<String>,
    scope_dropped_attr_count: i32,
}

fn create_metrics_gauge_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
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
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]))
}

fn create_metrics_sum_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
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
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("aggregation_temporality", DataType::Int32, false),
        Field::new("is_monotonic", DataType::Boolean, false),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]))
}

fn create_metrics_histogram_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
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
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("count", DataType::Int64, false),
        Field::new("sum", DataType::Float64, true),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
        Field::new("bucket_counts", DataType::Utf8, true),
        Field::new("explicit_bounds", DataType::Utf8, true),
        Field::new("flags", DataType::Int32, true),
        Field::new("aggregation_temporality", DataType::Int32, false),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]))
}

fn get_typed_column<'a, T>(batch: &'a RecordBatch, name: &str) -> Result<&'a T>
where
    T: Array + 'static,
{
    let (idx, _) = batch
        .schema()
        .column_with_name(name)
        .ok_or_else(|| anyhow!("Column '{}' not found in batch", name))?;

    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("Column '{}' has unexpected type", name))
}

fn string_value_ref(array: &StringArray, row: usize) -> Option<&str> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

fn string_value(array: &StringArray, row: usize) -> Option<String> {
    string_value_ref(array, row).map(ToString::to_string)
}

fn int32_value(array: &Int32Array, row: usize) -> Option<i32> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

fn bool_value(array: &BooleanArray, row: usize) -> Option<bool> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

fn parse_data_points(data_json: Option<&str>) -> Vec<serde_json::Value> {
    let Some(data_json) = data_json else {
        return Vec::new();
    };

    match serde_json::from_str::<serde_json::Value>(data_json) {
        Ok(serde_json::Value::Array(points)) => points,
        _ => Vec::new(),
    }
}

fn json_to_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    value.and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
    })
}

fn json_to_i64(value: Option<&serde_json::Value>) -> Option<i64> {
    value.and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_u64().and_then(|n| i64::try_from(n).ok()))
    })
}

fn json_to_i32(value: Option<&serde_json::Value>) -> Option<i32> {
    value.and_then(|v| {
        v.as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .or_else(|| v.as_u64().and_then(|n| i32::try_from(n).ok()))
    })
}

fn json_to_f64(value: Option<&serde_json::Value>) -> Option<f64> {
    value.and_then(|v| {
        v.as_f64()
            .or_else(|| v.as_i64().map(|n| n as f64))
            .or_else(|| v.as_u64().map(|n| n as f64))
    })
}

fn serialize_json(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(|v| {
        if v.is_null() {
            None
        } else {
            serde_json::to_string(v).ok()
        }
    })
}

fn serialize_json_array(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(|v| {
        if v.is_array() {
            serde_json::to_string(v).ok()
        } else {
            None
        }
    })
}

fn temporal_from_nanos(nanos: Option<u64>) -> (Option<i64>, Option<i32>, Option<i32>) {
    let Some(nanos) = nanos else {
        return (None, None, None);
    };

    let nanos_i64 = i64::try_from(nanos).ok();
    let secs = i64::try_from(nanos / 1_000_000_000).ok();

    let Some(secs) = secs else {
        return (nanos_i64, None, None);
    };

    let Some(dt) = DateTime::from_timestamp(secs, 0) else {
        return (nanos_i64, None, None);
    };

    (
        nanos_i64,
        Some(dt.naive_utc().date().num_days_from_ce() - 719163),
        Some(dt.hour() as i32),
    )
}

fn extract_service_name(
    resource_obj: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    if let Some(service_name) = resource_obj
        .get("service.name")
        .and_then(|value| value.as_str())
    {
        return Some(service_name.to_string());
    }

    resource_obj
        .get("attributes")
        .and_then(|value| value.as_object())
        .and_then(|attributes| attributes.get("service.name"))
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
}

fn extract_resource_context(resource_json: Option<&str>) -> ResourceContext {
    let Some(resource_json) = resource_json else {
        return ResourceContext::default();
    };

    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(resource_json) else {
        return ResourceContext {
            service_name: None,
            resource_schema_url: None,
            resource_attributes: Some(resource_json.to_string()),
        };
    };

    let serde_json::Value::Object(obj) = parsed else {
        return ResourceContext {
            service_name: None,
            resource_schema_url: None,
            resource_attributes: Some(resource_json.to_string()),
        };
    };

    let resource_attributes = if obj.get("attributes").is_some() {
        serialize_json(obj.get("attributes"))
    } else {
        Some(resource_json.to_string())
    };

    ResourceContext {
        service_name: extract_service_name(&obj),
        resource_schema_url: obj
            .get("schema_url")
            .and_then(|value| value.as_str())
            .map(ToString::to_string),
        resource_attributes,
    }
}

fn extract_scope_context(scope_json: Option<&str>) -> ScopeContext {
    let Some(scope_json) = scope_json else {
        return ScopeContext {
            scope_dropped_attr_count: 0,
            ..ScopeContext::default()
        };
    };

    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(scope_json) else {
        return ScopeContext {
            scope_dropped_attr_count: 0,
            ..ScopeContext::default()
        };
    };

    let serde_json::Value::Object(obj) = parsed else {
        return ScopeContext {
            scope_dropped_attr_count: 0,
            ..ScopeContext::default()
        };
    };

    ScopeContext {
        scope_name: obj
            .get("name")
            .and_then(|value| value.as_str())
            .map(ToString::to_string),
        scope_version: obj
            .get("version")
            .and_then(|value| value.as_str())
            .map(ToString::to_string),
        scope_schema_url: obj
            .get("schema_url")
            .and_then(|value| value.as_str())
            .map(ToString::to_string),
        scope_attributes: serialize_json(obj.get("attributes")),
        scope_dropped_attr_count: json_to_i32(obj.get("dropped_attributes_count")).unwrap_or(0),
    }
}

pub fn transform_metrics_gauge_v1_to_iceberg(batch: RecordBatch) -> Result<RecordBatch> {
    let output_schema = create_metrics_gauge_arrow_schema();

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let description_array = get_typed_column::<StringArray>(&batch, "description")?;
    let unit_array = get_typed_column::<StringArray>(&batch, "unit")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;

    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut start_timestamps: Vec<Option<i64>> = Vec::new();
    let mut service_names: Vec<Option<String>> = Vec::new();
    let mut metric_names: Vec<Option<String>> = Vec::new();
    let mut metric_descriptions: Vec<Option<String>> = Vec::new();
    let mut metric_units: Vec<Option<String>> = Vec::new();
    let mut values: Vec<Option<f64>> = Vec::new();
    let mut flags: Vec<Option<i32>> = Vec::new();
    let mut resource_schema_urls: Vec<Option<String>> = Vec::new();
    let mut resource_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_names: Vec<Option<String>> = Vec::new();
    let mut scope_versions: Vec<Option<String>> = Vec::new();
    let mut scope_schema_urls: Vec<Option<String>> = Vec::new();
    let mut scope_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_dropped_attr_counts: Vec<Option<i32>> = Vec::new();
    let mut attributes: Vec<Option<String>> = Vec::new();
    let mut exemplars: Vec<Option<String>> = Vec::new();
    let mut date_days: Vec<Option<i32>> = Vec::new();
    let mut hours: Vec<Option<i32>> = Vec::new();

    for row in 0..batch.num_rows() {
        let metric_name = string_value(name_array, row);
        let metric_description = string_value(description_array, row);
        let metric_unit = string_value(unit_array, row);

        let resource_context = extract_resource_context(string_value_ref(resource_json_array, row));
        let scope_context = extract_scope_context(string_value_ref(scope_json_array, row));
        let data_points = parse_data_points(string_value_ref(data_json_array, row));

        for point in data_points {
            let (timestamp, date_day, hour) =
                temporal_from_nanos(json_to_u64(point.get("time_unix_nano")));
            let (start_timestamp, _, _) =
                temporal_from_nanos(json_to_u64(point.get("start_time_unix_nano")));

            timestamps.push(timestamp);
            start_timestamps.push(start_timestamp);
            service_names.push(resource_context.service_name.clone());
            metric_names.push(metric_name.clone());
            metric_descriptions.push(metric_description.clone());
            metric_units.push(metric_unit.clone());
            values.push(json_to_f64(point.get("value")));
            flags.push(json_to_i32(point.get("flags")));
            resource_schema_urls.push(resource_context.resource_schema_url.clone());
            resource_attributes.push(resource_context.resource_attributes.clone());
            scope_names.push(scope_context.scope_name.clone());
            scope_versions.push(scope_context.scope_version.clone());
            scope_schema_urls.push(scope_context.scope_schema_url.clone());
            scope_attributes.push(scope_context.scope_attributes.clone());
            scope_dropped_attr_counts.push(Some(scope_context.scope_dropped_attr_count));
            attributes.push(serialize_json(point.get("attributes")));
            exemplars.push(serialize_json(point.get("exemplars")));
            date_days.push(date_day);
            hours.push(hour);
        }
    }

    RecordBatch::try_new(
        output_schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(start_timestamps)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(StringArray::from(metric_names)),
            Arc::new(StringArray::from(metric_descriptions)),
            Arc::new(StringArray::from(metric_units)),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int32Array::from(flags)),
            Arc::new(StringArray::from(resource_schema_urls)),
            Arc::new(StringArray::from(resource_attributes)),
            Arc::new(StringArray::from(scope_names)),
            Arc::new(StringArray::from(scope_versions)),
            Arc::new(StringArray::from(scope_schema_urls)),
            Arc::new(StringArray::from(scope_attributes)),
            Arc::new(Int32Array::from(scope_dropped_attr_counts)),
            Arc::new(StringArray::from(attributes)),
            Arc::new(StringArray::from(exemplars)),
            Arc::new(Date32Array::from(date_days)),
            Arc::new(Int32Array::from(hours)),
        ],
    )
    .map_err(|e| {
        anyhow!(
            "Failed to create transformed metrics_gauge RecordBatch: {}",
            e
        )
    })
}

pub fn transform_metrics_sum_v1_to_iceberg(batch: RecordBatch) -> Result<RecordBatch> {
    let output_schema = create_metrics_sum_arrow_schema();

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let description_array = get_typed_column::<StringArray>(&batch, "description")?;
    let unit_array = get_typed_column::<StringArray>(&batch, "unit")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;
    let aggregation_temporality_array =
        get_typed_column::<Int32Array>(&batch, "aggregation_temporality")?;
    let is_monotonic_array = get_typed_column::<BooleanArray>(&batch, "is_monotonic")?;

    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut start_timestamps: Vec<Option<i64>> = Vec::new();
    let mut service_names: Vec<Option<String>> = Vec::new();
    let mut metric_names: Vec<Option<String>> = Vec::new();
    let mut metric_descriptions: Vec<Option<String>> = Vec::new();
    let mut metric_units: Vec<Option<String>> = Vec::new();
    let mut values: Vec<Option<f64>> = Vec::new();
    let mut flags: Vec<Option<i32>> = Vec::new();
    let mut aggregation_temporalities: Vec<Option<i32>> = Vec::new();
    let mut is_monotonics: Vec<Option<bool>> = Vec::new();
    let mut resource_schema_urls: Vec<Option<String>> = Vec::new();
    let mut resource_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_names: Vec<Option<String>> = Vec::new();
    let mut scope_versions: Vec<Option<String>> = Vec::new();
    let mut scope_schema_urls: Vec<Option<String>> = Vec::new();
    let mut scope_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_dropped_attr_counts: Vec<Option<i32>> = Vec::new();
    let mut attributes: Vec<Option<String>> = Vec::new();
    let mut exemplars: Vec<Option<String>> = Vec::new();
    let mut date_days: Vec<Option<i32>> = Vec::new();
    let mut hours: Vec<Option<i32>> = Vec::new();

    for row in 0..batch.num_rows() {
        let metric_name = string_value(name_array, row);
        let metric_description = string_value(description_array, row);
        let metric_unit = string_value(unit_array, row);
        let aggregation_temporality = int32_value(aggregation_temporality_array, row);
        let is_monotonic = bool_value(is_monotonic_array, row);

        let resource_context = extract_resource_context(string_value_ref(resource_json_array, row));
        let scope_context = extract_scope_context(string_value_ref(scope_json_array, row));
        let data_points = parse_data_points(string_value_ref(data_json_array, row));

        for point in data_points {
            let (timestamp, date_day, hour) =
                temporal_from_nanos(json_to_u64(point.get("time_unix_nano")));
            let (start_timestamp, _, _) =
                temporal_from_nanos(json_to_u64(point.get("start_time_unix_nano")));

            timestamps.push(timestamp);
            start_timestamps.push(start_timestamp);
            service_names.push(resource_context.service_name.clone());
            metric_names.push(metric_name.clone());
            metric_descriptions.push(metric_description.clone());
            metric_units.push(metric_unit.clone());
            values.push(json_to_f64(point.get("value")));
            flags.push(json_to_i32(point.get("flags")));
            aggregation_temporalities.push(aggregation_temporality);
            is_monotonics.push(is_monotonic);
            resource_schema_urls.push(resource_context.resource_schema_url.clone());
            resource_attributes.push(resource_context.resource_attributes.clone());
            scope_names.push(scope_context.scope_name.clone());
            scope_versions.push(scope_context.scope_version.clone());
            scope_schema_urls.push(scope_context.scope_schema_url.clone());
            scope_attributes.push(scope_context.scope_attributes.clone());
            scope_dropped_attr_counts.push(Some(scope_context.scope_dropped_attr_count));
            attributes.push(serialize_json(point.get("attributes")));
            exemplars.push(serialize_json(point.get("exemplars")));
            date_days.push(date_day);
            hours.push(hour);
        }
    }

    RecordBatch::try_new(
        output_schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(start_timestamps)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(StringArray::from(metric_names)),
            Arc::new(StringArray::from(metric_descriptions)),
            Arc::new(StringArray::from(metric_units)),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int32Array::from(flags)),
            Arc::new(Int32Array::from(aggregation_temporalities)),
            Arc::new(BooleanArray::from(is_monotonics)),
            Arc::new(StringArray::from(resource_schema_urls)),
            Arc::new(StringArray::from(resource_attributes)),
            Arc::new(StringArray::from(scope_names)),
            Arc::new(StringArray::from(scope_versions)),
            Arc::new(StringArray::from(scope_schema_urls)),
            Arc::new(StringArray::from(scope_attributes)),
            Arc::new(Int32Array::from(scope_dropped_attr_counts)),
            Arc::new(StringArray::from(attributes)),
            Arc::new(StringArray::from(exemplars)),
            Arc::new(Date32Array::from(date_days)),
            Arc::new(Int32Array::from(hours)),
        ],
    )
    .map_err(|e| {
        anyhow!(
            "Failed to create transformed metrics_sum RecordBatch: {}",
            e
        )
    })
}

pub fn transform_metrics_histogram_v1_to_iceberg(batch: RecordBatch) -> Result<RecordBatch> {
    let output_schema = create_metrics_histogram_arrow_schema();

    let name_array = get_typed_column::<StringArray>(&batch, "name")?;
    let description_array = get_typed_column::<StringArray>(&batch, "description")?;
    let unit_array = get_typed_column::<StringArray>(&batch, "unit")?;
    let resource_json_array = get_typed_column::<StringArray>(&batch, "resource_json")?;
    let scope_json_array = get_typed_column::<StringArray>(&batch, "scope_json")?;
    let data_json_array = get_typed_column::<StringArray>(&batch, "data_json")?;
    let aggregation_temporality_array =
        get_typed_column::<Int32Array>(&batch, "aggregation_temporality")?;

    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut start_timestamps: Vec<Option<i64>> = Vec::new();
    let mut service_names: Vec<Option<String>> = Vec::new();
    let mut metric_names: Vec<Option<String>> = Vec::new();
    let mut metric_descriptions: Vec<Option<String>> = Vec::new();
    let mut metric_units: Vec<Option<String>> = Vec::new();
    let mut counts: Vec<Option<i64>> = Vec::new();
    let mut sums: Vec<Option<f64>> = Vec::new();
    let mut mins: Vec<Option<f64>> = Vec::new();
    let mut maxes: Vec<Option<f64>> = Vec::new();
    let mut bucket_counts: Vec<Option<String>> = Vec::new();
    let mut explicit_bounds: Vec<Option<String>> = Vec::new();
    let mut flags: Vec<Option<i32>> = Vec::new();
    let mut aggregation_temporalities: Vec<Option<i32>> = Vec::new();
    let mut resource_schema_urls: Vec<Option<String>> = Vec::new();
    let mut resource_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_names: Vec<Option<String>> = Vec::new();
    let mut scope_versions: Vec<Option<String>> = Vec::new();
    let mut scope_schema_urls: Vec<Option<String>> = Vec::new();
    let mut scope_attributes: Vec<Option<String>> = Vec::new();
    let mut scope_dropped_attr_counts: Vec<Option<i32>> = Vec::new();
    let mut attributes: Vec<Option<String>> = Vec::new();
    let mut exemplars: Vec<Option<String>> = Vec::new();
    let mut date_days: Vec<Option<i32>> = Vec::new();
    let mut hours: Vec<Option<i32>> = Vec::new();

    for row in 0..batch.num_rows() {
        let metric_name = string_value(name_array, row);
        let metric_description = string_value(description_array, row);
        let metric_unit = string_value(unit_array, row);
        let aggregation_temporality = int32_value(aggregation_temporality_array, row);

        let resource_context = extract_resource_context(string_value_ref(resource_json_array, row));
        let scope_context = extract_scope_context(string_value_ref(scope_json_array, row));
        let data_points = parse_data_points(string_value_ref(data_json_array, row));

        for point in data_points {
            let (timestamp, date_day, hour) =
                temporal_from_nanos(json_to_u64(point.get("time_unix_nano")));
            let (start_timestamp, _, _) =
                temporal_from_nanos(json_to_u64(point.get("start_time_unix_nano")));

            timestamps.push(timestamp);
            start_timestamps.push(start_timestamp);
            service_names.push(resource_context.service_name.clone());
            metric_names.push(metric_name.clone());
            metric_descriptions.push(metric_description.clone());
            metric_units.push(metric_unit.clone());
            counts.push(json_to_i64(point.get("count")));
            sums.push(json_to_f64(point.get("sum")));
            mins.push(json_to_f64(point.get("min")));
            maxes.push(json_to_f64(point.get("max")));
            bucket_counts.push(serialize_json_array(point.get("bucket_counts")));
            explicit_bounds.push(serialize_json_array(point.get("explicit_bounds")));
            flags.push(json_to_i32(point.get("flags")));
            aggregation_temporalities.push(aggregation_temporality);
            resource_schema_urls.push(resource_context.resource_schema_url.clone());
            resource_attributes.push(resource_context.resource_attributes.clone());
            scope_names.push(scope_context.scope_name.clone());
            scope_versions.push(scope_context.scope_version.clone());
            scope_schema_urls.push(scope_context.scope_schema_url.clone());
            scope_attributes.push(scope_context.scope_attributes.clone());
            scope_dropped_attr_counts.push(Some(scope_context.scope_dropped_attr_count));
            attributes.push(serialize_json(point.get("attributes")));
            exemplars.push(serialize_json(point.get("exemplars")));
            date_days.push(date_day);
            hours.push(hour);
        }
    }

    RecordBatch::try_new(
        output_schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(start_timestamps)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(StringArray::from(metric_names)),
            Arc::new(StringArray::from(metric_descriptions)),
            Arc::new(StringArray::from(metric_units)),
            Arc::new(Int64Array::from(counts)),
            Arc::new(Float64Array::from(sums)),
            Arc::new(Float64Array::from(mins)),
            Arc::new(Float64Array::from(maxes)),
            Arc::new(StringArray::from(bucket_counts)),
            Arc::new(StringArray::from(explicit_bounds)),
            Arc::new(Int32Array::from(flags)),
            Arc::new(Int32Array::from(aggregation_temporalities)),
            Arc::new(StringArray::from(resource_schema_urls)),
            Arc::new(StringArray::from(resource_attributes)),
            Arc::new(StringArray::from(scope_names)),
            Arc::new(StringArray::from(scope_versions)),
            Arc::new(StringArray::from(scope_schema_urls)),
            Arc::new(StringArray::from(scope_attributes)),
            Arc::new(Int32Array::from(scope_dropped_attr_counts)),
            Arc::new(StringArray::from(attributes)),
            Arc::new(StringArray::from(exemplars)),
            Arc::new(Date32Array::from(date_days)),
            Arc::new(Int32Array::from(hours)),
        ],
    )
    .map_err(|e| {
        anyhow!(
            "Failed to create transformed metrics_histogram RecordBatch: {}",
            e
        )
    })
}

pub fn transform_for_signal(
    signal_type: Option<&str>,
    target_table: Option<&str>,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    match (signal_type, target_table) {
        (Some("traces"), _) => transform_trace_v1_to_v2(batch),
        (Some("logs"), _) => transform_logs_v1_to_iceberg(batch),
        (Some("metrics"), Some("metrics_gauge")) => transform_metrics_gauge_v1_to_iceberg(batch),
        (Some("metrics"), Some("metrics_sum")) => transform_metrics_sum_v1_to_iceberg(batch),
        (Some("metrics"), Some("metrics_histogram")) => {
            transform_metrics_histogram_v1_to_iceberg(batch)
        }
        (Some("metrics"), Some(other)) => {
            log::warn!("No transform for metrics target_table={other}, passing through");
            Ok(batch)
        }
        _ => Ok(batch),
    }
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
