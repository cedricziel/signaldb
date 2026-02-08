//! Data generator for creating time-partitioned test data
//!
//! Provides utilities for generating realistic traces, logs, and metrics
//! with controlled time distribution for testing retention policies.

use anyhow::Result;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
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

/// Creates a trace batch with specified parameters
fn create_trace_batch(
    start_ts: i64,
    end_ts: i64,
    num_rows: usize,
    partition_idx: usize,
    file_idx: usize,
) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("duration_ns", DataType::Int64, false),
    ]));

    let time_step = (end_ts - start_ts) / num_rows as i64;
    let base_trace_id = format!("trace-p{}-f{}", partition_idx, file_idx);

    let trace_ids: Vec<String> = (0..num_rows)
        .map(|i| format!("{}-{}", base_trace_id, i))
        .collect();
    let span_ids: Vec<String> = (0..num_rows).map(|i| format!("span-{}", i)).collect();
    let span_names: Vec<String> = (0..num_rows)
        .map(|i| format!("operation-{}", i % 10))
        .collect();
    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| start_ts + (i as i64 * time_step))
        .collect();
    let durations: Vec<i64> = (0..num_rows).map(|i| 1000000 + (i as i64 * 100)).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(trace_ids)),
            Arc::new(StringArray::from(span_ids)),
            Arc::new(StringArray::from(span_names)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(durations)),
        ],
    )?;

    Ok(batch)
}

/// Creates a log batch with specified parameters
fn create_log_batch(
    start_ts: i64,
    end_ts: i64,
    num_rows: usize,
    partition_idx: usize,
    file_idx: usize,
) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
    ]));

    let time_step = (end_ts - start_ts) / num_rows as i64;
    let severities = ["INFO", "WARN", "ERROR", "DEBUG"];

    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| start_ts + (i as i64 * time_step))
        .collect();
    let severity_values: Vec<String> = (0..num_rows)
        .map(|i| severities[i % severities.len()].to_string())
        .collect();
    let messages: Vec<String> = (0..num_rows)
        .map(|i| {
            format!(
                "Log message {} from partition {} file {}",
                i, partition_idx, file_idx
            )
        })
        .collect();
    let service_names: Vec<String> = (0..num_rows)
        .map(|i| format!("service-{}", i % 5))
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringArray::from(severity_values)),
            Arc::new(StringArray::from(messages)),
            Arc::new(StringArray::from(service_names)),
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("labels", DataType::Utf8, false),
    ]));

    let time_step = (end_ts - start_ts) / num_rows as i64;
    let metric_names = ["cpu_usage", "memory_usage", "request_count", "error_rate"];

    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| start_ts + (i as i64 * time_step))
        .collect();
    let names: Vec<String> = (0..num_rows)
        .map(|i| metric_names[i % metric_names.len()].to_string())
        .collect();
    let values: Vec<i64> = (0..num_rows)
        .map(|i| (partition_idx * 1000 + file_idx * 100 + i) as i64)
        .collect();
    let labels: Vec<String> = (0..num_rows)
        .map(|i| format!("{{instance=\"host-{}\"}}", i % 3))
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(labels)),
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
        assert_eq!(batch.num_columns(), 5);
        Ok(())
    }

    #[test]
    fn test_create_log_batch() -> Result<()> {
        let batch = create_log_batch(1700000000000, 1700003600000, 50, 0, 0)?;
        assert_eq!(batch.num_rows(), 50);
        assert_eq!(batch.num_columns(), 4);
        Ok(())
    }

    #[test]
    fn test_create_metric_batch() -> Result<()> {
        let batch = create_metric_batch(1700000000000, 1700003600000, 75, 0, 0)?;
        assert_eq!(batch.num_rows(), 75);
        assert_eq!(batch.num_columns(), 4);
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
