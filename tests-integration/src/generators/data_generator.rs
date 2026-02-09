//! Data generator for creating time-partitioned test data
//!
//! Provides utilities for generating realistic traces, logs, and metrics
//! with controlled time distribution for testing retention policies.

use anyhow::Result;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use writer::IcebergTableWriter;

use crate::fixtures::{DataGeneratorConfig, PartitionInfo};

/// Generates time-partitioned trace data
pub async fn generate_traces(
    writer: &mut IcebergTableWriter,
    config: &DataGeneratorConfig,
) -> Result<Vec<PartitionInfo>> {
    let mut partitions = Vec::new();
    let partition_duration = config.partition_granularity.to_millis();

    for partition_idx in 0..config.partition_count {
        let partition_start = config.base_timestamp + (partition_idx as i64 * partition_duration);
        let partition_end = partition_start + partition_duration;

        let mut total_rows = 0;

        for file_idx in 0..config.files_per_partition {
            let batch = create_trace_batch(
                partition_start,
                partition_end,
                config.rows_per_file,
                partition_idx,
                file_idx,
            )?;

            writer.write_batch(batch).await?;
            total_rows += config.rows_per_file;
        }

        let partition_id = format_partition_id(partition_start, config.partition_granularity);
        partitions.push(PartitionInfo {
            partition_id,
            timestamp_range: (partition_start, partition_end),
            file_count: config.files_per_partition,
            row_count: total_rows,
        });
    }

    Ok(partitions)
}

/// Generates time-partitioned log data
pub async fn generate_logs(
    writer: &mut IcebergTableWriter,
    config: &DataGeneratorConfig,
) -> Result<Vec<PartitionInfo>> {
    let mut partitions = Vec::new();
    let partition_duration = config.partition_granularity.to_millis();

    for partition_idx in 0..config.partition_count {
        let partition_start = config.base_timestamp + (partition_idx as i64 * partition_duration);
        let partition_end = partition_start + partition_duration;

        let mut total_rows = 0;

        for file_idx in 0..config.files_per_partition {
            let batch = create_log_batch(
                partition_start,
                partition_end,
                config.rows_per_file,
                partition_idx,
                file_idx,
            )?;

            writer.write_batch(batch).await?;
            total_rows += config.rows_per_file;
        }

        let partition_id = format_partition_id(partition_start, config.partition_granularity);
        partitions.push(PartitionInfo {
            partition_id,
            timestamp_range: (partition_start, partition_end),
            file_count: config.files_per_partition,
            row_count: total_rows,
        });
    }

    Ok(partitions)
}

/// Generates time-partitioned metrics data
pub async fn generate_metrics(
    writer: &mut IcebergTableWriter,
    config: &DataGeneratorConfig,
) -> Result<Vec<PartitionInfo>> {
    let mut partitions = Vec::new();
    let partition_duration = config.partition_granularity.to_millis();

    for partition_idx in 0..config.partition_count {
        let partition_start = config.base_timestamp + (partition_idx as i64 * partition_duration);
        let partition_end = partition_start + partition_duration;

        let mut total_rows = 0;

        for file_idx in 0..config.files_per_partition {
            let batch = create_metric_batch(
                partition_start,
                partition_end,
                config.rows_per_file,
                partition_idx,
                file_idx,
            )?;

            writer.write_batch(batch).await?;
            total_rows += config.rows_per_file;
        }

        let partition_id = format_partition_id(partition_start, config.partition_granularity);
        partitions.push(PartitionInfo {
            partition_id,
            timestamp_range: (partition_start, partition_end),
            file_count: config.files_per_partition,
            row_count: total_rows,
        });
    }

    Ok(partitions)
}

/// Creates a trace batch with specified parameters (v1 schema format)
fn create_trace_batch(
    start_ts: i64,
    end_ts: i64,
    num_rows: usize,
    partition_idx: usize,
    file_idx: usize,
) -> Result<RecordBatch> {
    use datafusion::arrow::array::{BooleanArray, ListArray, StructArray, UInt64Array};
    use datafusion::arrow::buffer::OffsetBuffer;

    // Create v1 schema matching what the acceptor produces
    // Based on schemas.toml traces.v1
    // Note: events and links are simplified as empty ListArrays for tests
    let events_struct_fields: Vec<Arc<Field>> = vec![]; // Empty struct for simplicity
    let events_field = Field::new(
        "events",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(events_struct_fields.into()),
            true,
        ))),
        true,
    );

    let links_struct_fields: Vec<Arc<Field>> = vec![]; // Empty struct for simplicity
    let links_field = Field::new(
        "links",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(links_struct_fields.into()),
            true,
        ))),
        true,
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::UInt64, false),
        Field::new("end_time_unix_nano", DataType::UInt64, false),
        Field::new("duration_nano", DataType::UInt64, false),
        Field::new("span_kind", DataType::Utf8, false),
        Field::new("status_code", DataType::Utf8, false),
        Field::new("status_message", DataType::Utf8, true),
        Field::new("is_root", DataType::Boolean, false),
        Field::new("attributes_json", DataType::Utf8, true),
        Field::new("resource_json", DataType::Utf8, true),
        events_field,
        links_field,
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
    ]));

    let time_step = (end_ts - start_ts) / num_rows as i64;
    let base_trace_id = format!("trace-p{}-f{}", partition_idx, file_idx);

    let mut trace_ids: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut span_ids: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut parent_span_ids: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut names: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut service_names: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut start_times: Vec<Option<u64>> = Vec::with_capacity(num_rows);
    let mut end_times: Vec<Option<u64>> = Vec::with_capacity(num_rows);
    let mut durations: Vec<Option<u64>> = Vec::with_capacity(num_rows);
    let mut span_kinds: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut status_codes: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut status_messages: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut is_roots: Vec<Option<bool>> = Vec::with_capacity(num_rows);
    let mut attributes_jsons: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut resource_jsons: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut trace_states: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut resource_schema_urls: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_names: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_versions: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_schema_urls: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_attributes: Vec<Option<String>> = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        let ts_millis = start_ts + (i as i64 * time_step);
        let ts_nanos = (ts_millis * 1_000_000) as u64; // Convert milliseconds to nanoseconds
        let duration_ns = 1_000_000u64 + (i as u64 * 100);

        trace_ids.push(Some(format!("{}-{}", base_trace_id, i)));
        span_ids.push(Some(format!("span-{}", i)));
        parent_span_ids.push(if i > 0 {
            Some(format!("span-{}", i - 1))
        } else {
            None
        });
        names.push(Some(format!("operation-{}", i % 10)));
        service_names.push(Some(format!("test-service-{}", i % 3)));
        start_times.push(Some(ts_nanos));
        end_times.push(Some(ts_nanos + duration_ns));
        durations.push(Some(duration_ns));
        span_kinds.push(Some("SPAN_KIND_INTERNAL".to_string()));
        status_codes.push(Some("STATUS_CODE_OK".to_string()));
        status_messages.push(None);
        is_roots.push(Some(i == 0));
        attributes_jsons.push(Some("{}".to_string()));
        resource_jsons.push(Some(r#"{"service.name":"test-service"}"#.to_string()));
        trace_states.push(None);
        resource_schema_urls.push(None);
        scope_names.push(Some("test-scope".to_string()));
        scope_versions.push(Some("1.0.0".to_string()));
        scope_schema_urls.push(None);
        scope_attributes.push(Some("{}".to_string()));
    }

    // Create empty ListArrays for events and links
    // Each row has an empty list
    let empty_fields: Vec<Arc<Field>> = vec![];
    let empty_struct = StructArray::new_empty_fields(0, None);
    let offsets = OffsetBuffer::from_lengths(vec![0; num_rows]);
    let empty_events = ListArray::new(
        Arc::new(Field::new(
            "item",
            DataType::Struct(empty_fields.clone().into()),
            true,
        )),
        offsets.clone(),
        Arc::new(empty_struct.clone()),
        None,
    );
    let empty_links = ListArray::new(
        Arc::new(Field::new(
            "item",
            DataType::Struct(empty_fields.into()),
            true,
        )),
        offsets,
        Arc::new(empty_struct),
        None,
    );

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(trace_ids)),
            Arc::new(StringArray::from(span_ids)),
            Arc::new(StringArray::from(parent_span_ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(UInt64Array::from(start_times)),
            Arc::new(UInt64Array::from(end_times)),
            Arc::new(UInt64Array::from(durations)),
            Arc::new(StringArray::from(span_kinds)),
            Arc::new(StringArray::from(status_codes)),
            Arc::new(StringArray::from(status_messages)),
            Arc::new(BooleanArray::from(is_roots)),
            Arc::new(StringArray::from(attributes_jsons)),
            Arc::new(StringArray::from(resource_jsons)),
            Arc::new(empty_events),
            Arc::new(empty_links),
            Arc::new(StringArray::from(trace_states)),
            Arc::new(StringArray::from(resource_schema_urls)),
            Arc::new(StringArray::from(scope_names)),
            Arc::new(StringArray::from(scope_versions)),
            Arc::new(StringArray::from(scope_schema_urls)),
            Arc::new(StringArray::from(scope_attributes)),
        ],
    )?;

    Ok(batch)
}

/// Creates a log batch with specified parameters (v1 schema format)
fn create_log_batch(
    start_ts: i64,
    end_ts: i64,
    num_rows: usize,
    partition_idx: usize,
    file_idx: usize,
) -> Result<RecordBatch> {
    use chrono::{DateTime, Datelike, Timelike};
    use datafusion::arrow::array::{Date32Array, Int32Array, TimestampNanosecondArray};

    // Create v1 schema matching schemas.toml logs.v1
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_timestamp",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("trace_flags", DataType::Int32, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("log_attributes", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]));

    let time_step = (end_ts - start_ts) / num_rows as i64;
    let severities = ["INFO", "WARN", "ERROR", "DEBUG"];

    let mut timestamps: Vec<Option<i64>> = Vec::with_capacity(num_rows);
    let mut observed_timestamps: Vec<Option<i64>> = Vec::with_capacity(num_rows);
    let mut trace_ids: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut span_ids: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut trace_flags: Vec<Option<i32>> = Vec::with_capacity(num_rows);
    let mut severity_texts: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut severity_numbers: Vec<Option<i32>> = Vec::with_capacity(num_rows);
    let mut service_names: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut bodies: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut resource_schema_urls: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut resource_attributes: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_schema_urls: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_names: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_versions: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_attributes: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut log_attributes: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut date_days: Vec<Option<i32>> = Vec::with_capacity(num_rows);
    let mut hours: Vec<Option<i32>> = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        let ts_millis = start_ts + (i as i64 * time_step);
        let ts_nanos = ts_millis * 1_000_000; // Convert milliseconds to nanoseconds

        timestamps.push(Some(ts_nanos));
        observed_timestamps.push(Some(ts_nanos));
        trace_ids.push(Some(format!("trace-{}", i)));
        span_ids.push(Some(format!("span-{}", i)));
        trace_flags.push(Some(1));
        severity_texts.push(Some(severities[i % severities.len()].to_string()));
        severity_numbers.push(Some(((i % severities.len()) as i32 + 1) * 4)); // Rough mapping
        service_names.push(Some(format!("service-{}", i % 5)));
        bodies.push(Some(format!(
            "Log message {} from partition {} file {}",
            i, partition_idx, file_idx
        )));
        resource_schema_urls.push(None);
        resource_attributes.push(Some("{}".to_string()));
        scope_schema_urls.push(None);
        scope_names.push(Some("test-scope".to_string()));
        scope_versions.push(Some("1.0.0".to_string()));
        scope_attributes.push(Some("{}".to_string()));
        log_attributes.push(Some("{}".to_string()));

        // Calculate date and hour from timestamp
        let secs = ts_nanos / 1_000_000_000;
        if let Some(dt) = DateTime::from_timestamp(secs, 0) {
            date_days.push(Some(dt.naive_utc().date().num_days_from_ce() - 719163));
            hours.push(Some(dt.hour() as i32));
        } else {
            date_days.push(None);
            hours.push(None);
        }
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(observed_timestamps)),
            Arc::new(StringArray::from(trace_ids)),
            Arc::new(StringArray::from(span_ids)),
            Arc::new(Int32Array::from(trace_flags)),
            Arc::new(StringArray::from(severity_texts)),
            Arc::new(Int32Array::from(severity_numbers)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(StringArray::from(bodies)),
            Arc::new(StringArray::from(resource_schema_urls)),
            Arc::new(StringArray::from(resource_attributes)),
            Arc::new(StringArray::from(scope_schema_urls)),
            Arc::new(StringArray::from(scope_names)),
            Arc::new(StringArray::from(scope_versions)),
            Arc::new(StringArray::from(scope_attributes)),
            Arc::new(StringArray::from(log_attributes)),
            Arc::new(Date32Array::from(date_days)),
            Arc::new(Int32Array::from(hours)),
        ],
    )?;

    Ok(batch)
}

/// Creates a metric batch with specified parameters
fn create_metric_batch(
    start_ts: i64,
    end_ts: i64,
    num_rows: usize,
    partition_idx: usize,
    file_idx: usize,
) -> Result<RecordBatch> {
    use chrono::{DateTime, Datelike, Timelike};
    use datafusion::arrow::array::{
        Date32Array, Float64Array, Int32Array, TimestampNanosecondArray,
    };

    // Use the writer's schema
    let schema = writer::schema_transform::create_metrics_gauge_arrow_schema();

    let time_step = (end_ts - start_ts) / num_rows as i64;
    let metric_names = ["cpu_usage", "memory_usage", "request_count", "error_rate"];

    // Build arrays for all 19 fields
    let mut timestamps: Vec<Option<i64>> = Vec::with_capacity(num_rows);
    let mut start_timestamps: Vec<Option<i64>> = Vec::with_capacity(num_rows);
    let mut service_names: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut metric_name_arr: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut metric_descriptions: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut metric_units: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut values: Vec<Option<f64>> = Vec::with_capacity(num_rows);
    let mut flags: Vec<Option<i32>> = Vec::with_capacity(num_rows);
    let mut resource_schema_urls: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut resource_attributes: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_names: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_versions: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_schema_urls: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_attributes: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut scope_dropped_attr_counts: Vec<Option<i32>> = Vec::with_capacity(num_rows);
    let mut attributes: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut exemplars: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut date_days: Vec<Option<i32>> = Vec::with_capacity(num_rows);
    let mut hours: Vec<Option<i32>> = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        let ts_millis = start_ts + (i as i64 * time_step);
        let ts_nanos = ts_millis * 1_000_000; // Convert milliseconds to nanoseconds

        timestamps.push(Some(ts_nanos));
        start_timestamps.push(Some(ts_nanos)); // Same as timestamp for gauges
        service_names.push(Some(format!("test-service-{}", i % 3)));
        metric_name_arr.push(Some(metric_names[i % metric_names.len()].to_string()));
        metric_descriptions.push(Some(format!("Test metric {}", i)));
        metric_units.push(Some("units".to_string()));
        values.push(Some((partition_idx * 1000 + file_idx * 100 + i) as f64));
        flags.push(Some(0));
        resource_schema_urls.push(None);
        resource_attributes.push(Some("{}".to_string()));
        scope_names.push(Some("test-scope".to_string()));
        scope_versions.push(Some("1.0.0".to_string()));
        scope_schema_urls.push(None);
        scope_attributes.push(Some("{}".to_string()));
        scope_dropped_attr_counts.push(Some(0));
        attributes.push(Some("{}".to_string()));
        exemplars.push(None);

        // Calculate date and hour from timestamp
        let secs = ts_nanos / 1_000_000_000;
        if let Some(dt) = DateTime::from_timestamp(secs, 0) {
            date_days.push(Some(dt.naive_utc().date().num_days_from_ce() - 719163));
            hours.push(Some(dt.hour() as i32));
        } else {
            date_days.push(None);
            hours.push(None);
        }
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(start_timestamps)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(StringArray::from(metric_name_arr)),
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
    )?;

    Ok(batch)
}

/// Formats a partition ID from a timestamp
fn format_partition_id(
    timestamp: i64,
    granularity: crate::fixtures::PartitionGranularity,
) -> String {
    use chrono::{DateTime, Utc};

    let dt: DateTime<Utc> =
        DateTime::from_timestamp_millis(timestamp).unwrap_or(DateTime::UNIX_EPOCH);

    match granularity {
        crate::fixtures::PartitionGranularity::Hour => dt.format("%Y-%m-%d-%H").to_string(),
        crate::fixtures::PartitionGranularity::Day => dt.format("%Y-%m-%d").to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_trace_batch() -> Result<()> {
        let batch = create_trace_batch(1700000000000, 1700003600000, 100, 0, 0)?;
        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 22); // Updated for v1 schema with 22 fields
        Ok(())
    }

    #[test]
    fn test_create_log_batch() -> Result<()> {
        let batch = create_log_batch(1700000000000, 1700003600000, 50, 0, 0)?;
        assert_eq!(batch.num_rows(), 50);
        assert_eq!(batch.num_columns(), 18); // Updated for v1 schema with 18 fields
        Ok(())
    }

    #[test]
    fn test_create_metric_batch() -> Result<()> {
        let batch = create_metric_batch(1700000000000, 1700003600000, 75, 0, 0)?;
        assert_eq!(batch.num_rows(), 75);
        assert_eq!(batch.num_columns(), 19); // Updated for metrics gauge schema with 19 fields
        Ok(())
    }

    #[test]
    fn test_format_partition_id_day() {
        use crate::fixtures::PartitionGranularity;
        let partition_id = format_partition_id(1700000000000, PartitionGranularity::Day);
        assert_eq!(partition_id, "2023-11-14");
    }

    #[test]
    fn test_format_partition_id_hour() {
        use crate::fixtures::PartitionGranularity;
        let partition_id = format_partition_id(1700000000000, PartitionGranularity::Hour);
        assert_eq!(partition_id, "2023-11-14-22");
    }
}
