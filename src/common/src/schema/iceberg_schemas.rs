use crate::config::DefaultSchemas;
use crate::schema::SCHEMA_DEFINITIONS;
use anyhow::Result;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::spec::{PartitionSpec, Transform};
use std::sync::Arc;

/// Create Iceberg schema for traces table using TOML definitions
pub fn create_traces_schema() -> Result<Schema> {
    // Get the current trace schema version from TOML
    let current_version = SCHEMA_DEFINITIONS.current_trace_version();
    let resolved_schema = SCHEMA_DEFINITIONS.resolve_trace_schema(current_version)?;

    // Convert resolved schema to Iceberg schema
    resolved_schema.to_iceberg_schema()
}

/// Create Iceberg schema for logs table using TOML definitions
pub fn create_logs_schema() -> Result<Schema> {
    // Get the current log schema version from TOML
    let current_version = SCHEMA_DEFINITIONS.metadata.current_log_version.as_str();
    let resolved_schema = SCHEMA_DEFINITIONS.resolve_log_schema(current_version)?;

    // Convert resolved schema to Iceberg schema
    resolved_schema.to_iceberg_schema()
}

/// Create Iceberg schema for metrics gauge table
/// Based on ClickHouse metrics_gauge_table.sql schema but adapted for Iceberg
pub fn create_metrics_gauge_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        Arc::new(NestedField::required(
            1,
            "timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        Arc::new(NestedField::optional(
            2,
            "start_timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        // Metric identification
        Arc::new(NestedField::required(
            3,
            "service_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            4,
            "metric_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            5,
            "metric_description",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            6,
            "metric_unit",
            Type::Primitive(PrimitiveType::String),
        )),
        // Value
        Arc::new(NestedField::required(
            7,
            "value",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            8,
            "flags",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Resource and scope information
        Arc::new(NestedField::optional(
            9,
            "resource_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            10,
            "resource_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            11,
            "scope_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            12,
            "scope_version",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            13,
            "scope_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            14,
            "scope_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            15,
            "scope_dropped_attr_count",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Metric attributes
        Arc::new(NestedField::optional(
            16,
            "attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Exemplars - stored as JSON string for simplicity
        Arc::new(NestedField::optional(
            17,
            "exemplars",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Additional fields for query optimization
        Arc::new(NestedField::required(
            18,
            "date_day",
            Type::Primitive(PrimitiveType::Date),
        )), // Partition key
        Arc::new(NestedField::required(
            19,
            "hour",
            Type::Primitive(PrimitiveType::Int),
        )), // Sub-partition key
    ];

    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create metrics gauge schema: {}", e))
}

/// Create Iceberg schema for metrics sum table
/// Based on ClickHouse metrics_sum_table.sql schema but adapted for Iceberg
pub fn create_metrics_sum_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        Arc::new(NestedField::required(
            1,
            "timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        Arc::new(NestedField::optional(
            2,
            "start_timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        // Metric identification
        Arc::new(NestedField::required(
            3,
            "service_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            4,
            "metric_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            5,
            "metric_description",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            6,
            "metric_unit",
            Type::Primitive(PrimitiveType::String),
        )),
        // Value and aggregation info
        Arc::new(NestedField::required(
            7,
            "value",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            8,
            "flags",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::required(
            9,
            "aggregation_temporality",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::required(
            10,
            "is_monotonic",
            Type::Primitive(PrimitiveType::Boolean),
        )),
        // Resource and scope information
        Arc::new(NestedField::optional(
            11,
            "resource_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            12,
            "resource_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            13,
            "scope_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            14,
            "scope_version",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            15,
            "scope_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            16,
            "scope_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            17,
            "scope_dropped_attr_count",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Metric attributes
        Arc::new(NestedField::optional(
            18,
            "attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Exemplars - stored as JSON string for simplicity
        Arc::new(NestedField::optional(
            19,
            "exemplars",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Additional fields for query optimization
        Arc::new(NestedField::required(
            20,
            "date_day",
            Type::Primitive(PrimitiveType::Date),
        )), // Partition key
        Arc::new(NestedField::required(
            21,
            "hour",
            Type::Primitive(PrimitiveType::Int),
        )), // Sub-partition key
    ];

    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create metrics sum schema: {}", e))
}

/// Create Iceberg schema for metrics histogram table
/// Based on ClickHouse metrics_histogram_table.sql schema but adapted for Iceberg
pub fn create_metrics_histogram_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        Arc::new(NestedField::required(
            1,
            "timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        Arc::new(NestedField::optional(
            2,
            "start_timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        // Metric identification
        Arc::new(NestedField::required(
            3,
            "service_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            4,
            "metric_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            5,
            "metric_description",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            6,
            "metric_unit",
            Type::Primitive(PrimitiveType::String),
        )),
        // Histogram data
        Arc::new(NestedField::required(
            7,
            "count",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            8,
            "sum",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            9,
            "min",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            10,
            "max",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            11,
            "bucket_counts",
            Type::Primitive(PrimitiveType::String),
        )), // JSON array string
        Arc::new(NestedField::optional(
            12,
            "explicit_bounds",
            Type::Primitive(PrimitiveType::String),
        )), // JSON array string
        Arc::new(NestedField::optional(
            13,
            "flags",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::required(
            14,
            "aggregation_temporality",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Resource and scope information
        Arc::new(NestedField::optional(
            15,
            "resource_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            16,
            "resource_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            17,
            "scope_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            18,
            "scope_version",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            19,
            "scope_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            20,
            "scope_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            21,
            "scope_dropped_attr_count",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Metric attributes
        Arc::new(NestedField::optional(
            22,
            "attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Exemplars - stored as JSON string for simplicity
        Arc::new(NestedField::optional(
            23,
            "exemplars",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Additional fields for query optimization
        Arc::new(NestedField::required(
            24,
            "date_day",
            Type::Primitive(PrimitiveType::Date),
        )), // Partition key
        Arc::new(NestedField::required(
            25,
            "hour",
            Type::Primitive(PrimitiveType::Int),
        )), // Sub-partition key
    ];

    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create metrics histogram schema: {}", e))
}

/// Create Iceberg schema for metrics exponential histogram table
/// Similar to histogram but with exponential bucketing for better precision
pub fn create_metrics_exponential_histogram_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        Arc::new(NestedField::required(
            1,
            "timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        Arc::new(NestedField::optional(
            2,
            "start_timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        // Metric identification
        Arc::new(NestedField::required(
            3,
            "service_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            4,
            "metric_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            5,
            "metric_description",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            6,
            "metric_unit",
            Type::Primitive(PrimitiveType::String),
        )),
        // Exponential histogram data
        Arc::new(NestedField::required(
            7,
            "count",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            8,
            "sum",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            9,
            "min",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            10,
            "max",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            11,
            "scale",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::optional(
            12,
            "zero_count",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            13,
            "positive_offset",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::optional(
            14,
            "positive_bucket_counts",
            Type::Primitive(PrimitiveType::String),
        )), // JSON array string
        Arc::new(NestedField::optional(
            15,
            "negative_offset",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::optional(
            16,
            "negative_bucket_counts",
            Type::Primitive(PrimitiveType::String),
        )), // JSON array string
        Arc::new(NestedField::optional(
            17,
            "flags",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::required(
            18,
            "aggregation_temporality",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::optional(
            19,
            "zero_threshold",
            Type::Primitive(PrimitiveType::Double),
        )),
        // Resource and scope information
        Arc::new(NestedField::optional(
            20,
            "resource_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            21,
            "resource_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            22,
            "scope_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            23,
            "scope_version",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            24,
            "scope_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            25,
            "scope_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            26,
            "scope_dropped_attr_count",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Metric attributes
        Arc::new(NestedField::optional(
            27,
            "attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Exemplars - stored as JSON string for simplicity
        Arc::new(NestedField::optional(
            28,
            "exemplars",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Additional fields for query optimization
        Arc::new(NestedField::required(
            29,
            "date_day",
            Type::Primitive(PrimitiveType::Date),
        )), // Partition key
        Arc::new(NestedField::required(
            30,
            "hour",
            Type::Primitive(PrimitiveType::Int),
        )), // Sub-partition key
    ];

    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create metrics exponential histogram schema: {e}"))
}

/// Create Iceberg schema for metrics summary table
/// Stores quantile values for summary metrics
pub fn create_metrics_summary_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        Arc::new(NestedField::required(
            1,
            "timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        Arc::new(NestedField::optional(
            2,
            "start_timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        // Metric identification
        Arc::new(NestedField::required(
            3,
            "service_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            4,
            "metric_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            5,
            "metric_description",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            6,
            "metric_unit",
            Type::Primitive(PrimitiveType::String),
        )),
        // Summary data
        Arc::new(NestedField::required(
            7,
            "count",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::required(
            8,
            "sum",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            9,
            "quantile_values",
            Type::Primitive(PrimitiveType::String),
        )), // JSON array of {quantile, value} objects
        Arc::new(NestedField::optional(
            10,
            "flags",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Resource and scope information
        Arc::new(NestedField::optional(
            11,
            "resource_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            12,
            "resource_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            13,
            "scope_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            14,
            "scope_version",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            15,
            "scope_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            16,
            "scope_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            17,
            "scope_dropped_attr_count",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Metric attributes
        Arc::new(NestedField::optional(
            18,
            "attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Exemplars - stored as JSON string for simplicity
        Arc::new(NestedField::optional(
            19,
            "exemplars",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Additional fields for query optimization
        Arc::new(NestedField::required(
            20,
            "date_day",
            Type::Primitive(PrimitiveType::Date),
        )), // Partition key
        Arc::new(NestedField::required(
            21,
            "hour",
            Type::Primitive(PrimitiveType::Int),
        )), // Sub-partition key
    ];

    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create metrics summary schema: {e}"))
}

/// Create partition specification for traces table
/// Partitions by date (daily) and sub-partitions by hour for better query performance
pub fn create_traces_partition_spec() -> Result<PartitionSpec> {
    let schema = create_traces_schema()?;
    let current_version = SCHEMA_DEFINITIONS.current_trace_version();
    let resolved_schema = SCHEMA_DEFINITIONS.resolve_trace_schema(current_version)?;

    let mut builder = PartitionSpec::builder(schema).with_spec_id(1);

    // Add partition fields from TOML definition
    for partition_field in &resolved_schema.partition_by {
        builder =
            builder.add_partition_field(partition_field, partition_field, Transform::Identity)?;
    }

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create traces partition spec: {}", e))
}

/// Create partition specification for logs table
/// Partitions by date (daily) and sub-partitions by hour for better query performance
pub fn create_logs_partition_spec() -> Result<PartitionSpec> {
    let schema = create_logs_schema()?;
    PartitionSpec::builder(schema)
        .with_spec_id(1)
        .add_partition_field("date_day", "date_day", Transform::Identity)?
        .add_partition_field("hour", "hour", Transform::Identity)?
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create logs partition spec: {}", e))
}

/// Create partition specification for metrics tables
/// Partitions by date (daily) and sub-partitions by hour for better query performance
pub fn create_metrics_partition_spec() -> Result<PartitionSpec> {
    // Use metrics gauge schema as the base (they all have the same partition fields)
    let schema = create_metrics_gauge_schema()?;
    PartitionSpec::builder(schema)
        .with_spec_id(1)
        .add_partition_field("date_day", "date_day", Transform::Identity)?
        .add_partition_field("hour", "hour", Transform::Identity)?
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create metrics partition spec: {}", e))
}

/// All available table schemas
#[derive(Debug, Clone)]
pub enum TableSchema {
    Traces,
    Logs,
    MetricsGauge,
    MetricsSum,
    MetricsHistogram,
    MetricsExponentialHistogram,
    MetricsSummary,
    Custom(String), // For custom schemas from configuration
}

impl TableSchema {
    /// Get the schema for this table type
    pub fn schema(&self) -> Result<Schema> {
        match self {
            TableSchema::Traces => create_traces_schema(),
            TableSchema::Logs => create_logs_schema(),
            TableSchema::MetricsGauge => create_metrics_gauge_schema(),
            TableSchema::MetricsSum => create_metrics_sum_schema(),
            TableSchema::MetricsHistogram => create_metrics_histogram_schema(),
            TableSchema::MetricsExponentialHistogram => {
                create_metrics_exponential_histogram_schema()
            }
            TableSchema::MetricsSummary => create_metrics_summary_schema(),
            TableSchema::Custom(_) => Err(anyhow::anyhow!(
                "Custom schemas must be loaded from configuration"
            )),
        }
    }

    /// Get the partition specification for this table type
    pub fn partition_spec(&self) -> Result<PartitionSpec> {
        match self {
            TableSchema::Traces => create_traces_partition_spec(),
            TableSchema::Logs => create_logs_partition_spec(),
            TableSchema::MetricsGauge
            | TableSchema::MetricsSum
            | TableSchema::MetricsHistogram
            | TableSchema::MetricsExponentialHistogram
            | TableSchema::MetricsSummary => create_metrics_partition_spec(),
            TableSchema::Custom(_) => Err(anyhow::anyhow!(
                "Custom partition specs must be defined in configuration"
            )),
        }
    }

    /// Get the table name for this schema
    pub fn table_name(&self) -> &str {
        match self {
            TableSchema::Traces => "traces",
            TableSchema::Logs => "logs",
            TableSchema::MetricsGauge => "metrics_gauge",
            TableSchema::MetricsSum => "metrics_sum",
            TableSchema::MetricsHistogram => "metrics_histogram",
            TableSchema::MetricsExponentialHistogram => "metrics_exponential_histogram",
            TableSchema::MetricsSummary => "metrics_summary",
            TableSchema::Custom(name) => name,
        }
    }

    /// Get all available table schemas based on configuration
    pub fn all_from_config(config: &DefaultSchemas) -> Vec<TableSchema> {
        let mut schemas = Vec::new();

        if config.traces_enabled {
            schemas.push(TableSchema::Traces);
        }

        if config.logs_enabled {
            schemas.push(TableSchema::Logs);
        }

        if config.metrics_enabled {
            schemas.push(TableSchema::MetricsGauge);
            schemas.push(TableSchema::MetricsSum);
            schemas.push(TableSchema::MetricsHistogram);
            schemas.push(TableSchema::MetricsExponentialHistogram);
            schemas.push(TableSchema::MetricsSummary);
        }

        // Add custom schemas
        for name in config.custom_schemas.keys() {
            schemas.push(TableSchema::Custom(name.clone()));
        }

        schemas
    }

    /// Get all available table schemas (legacy method for backwards compatibility)
    pub fn all() -> Vec<TableSchema> {
        vec![
            TableSchema::Traces,
            TableSchema::Logs,
            TableSchema::MetricsGauge,
            TableSchema::MetricsSum,
            TableSchema::MetricsHistogram,
            TableSchema::MetricsExponentialHistogram,
            TableSchema::MetricsSummary,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traces_schema_creation() {
        let schema = create_traces_schema().unwrap();
        // Verify schema was created successfully (we already know this from unwrap above)
        assert!(schema.field_by_name("trace_id").is_some());

        // Check for key fields
        assert!(schema.field_by_name("trace_id").is_some());
        assert!(schema.field_by_name("span_id").is_some());
        assert!(schema.field_by_name("timestamp").is_some());
        assert!(schema.field_by_name("service_name").is_some());
        assert!(schema.field_by_name("date_day").is_some());
    }

    #[test]
    fn test_logs_schema_creation() {
        let schema = create_logs_schema().unwrap();
        // Verify schema was created successfully (we already know this from unwrap above)
        assert!(schema.field_by_name("timestamp").is_some());

        // Check for key fields
        assert!(schema.field_by_name("timestamp").is_some());
        assert!(schema.field_by_name("service_name").is_some());
        assert!(schema.field_by_name("severity_text").is_some());
        assert!(schema.field_by_name("body").is_some());
        assert!(schema.field_by_name("date_day").is_some());
    }

    #[test]
    fn test_metrics_gauge_schema_creation() {
        let schema = create_metrics_gauge_schema().unwrap();
        // Verify schema was created successfully (we already know this from unwrap above)
        assert!(schema.field_by_name("timestamp").is_some());

        // Check for key fields
        assert!(schema.field_by_name("timestamp").is_some());
        assert!(schema.field_by_name("service_name").is_some());
        assert!(schema.field_by_name("metric_name").is_some());
        assert!(schema.field_by_name("value").is_some());
        assert!(schema.field_by_name("date_day").is_some());
    }

    #[test]
    fn test_metrics_sum_schema_creation() {
        let schema = create_metrics_sum_schema().unwrap();
        // Verify schema was created successfully (we already know this from unwrap above)
        assert!(schema.field_by_name("timestamp").is_some());

        // Check for key fields
        assert!(schema.field_by_name("timestamp").is_some());
        assert!(schema.field_by_name("service_name").is_some());
        assert!(schema.field_by_name("metric_name").is_some());
        assert!(schema.field_by_name("value").is_some());
        assert!(schema.field_by_name("aggregation_temporality").is_some());
        assert!(schema.field_by_name("is_monotonic").is_some());
        assert!(schema.field_by_name("date_day").is_some());
    }

    #[test]
    fn test_metrics_histogram_schema_creation() {
        let schema = create_metrics_histogram_schema().unwrap();
        // Verify schema was created successfully (we already know this from unwrap above)
        assert!(schema.field_by_name("timestamp").is_some());

        // Check for key fields
        assert!(schema.field_by_name("timestamp").is_some());
        assert!(schema.field_by_name("service_name").is_some());
        assert!(schema.field_by_name("metric_name").is_some());
        assert!(schema.field_by_name("count").is_some());
        assert!(schema.field_by_name("bucket_counts").is_some());
        assert!(schema.field_by_name("explicit_bounds").is_some());
        assert!(schema.field_by_name("date_day").is_some());
    }

    #[test]
    fn test_partition_specs_creation() {
        // Test all partition specs can be created
        assert!(create_traces_partition_spec().is_ok());
        assert!(create_logs_partition_spec().is_ok());
        assert!(create_metrics_partition_spec().is_ok());
    }

    #[test]
    fn test_table_schema_enum() {
        // Test all schema types
        for table_schema in TableSchema::all() {
            assert!(table_schema.schema().is_ok());
            assert!(table_schema.partition_spec().is_ok());
            assert!(!table_schema.table_name().is_empty());
        }
    }

    #[test]
    fn test_partition_field_ids() {
        // Get the traces schema
        let schema = create_traces_schema().unwrap();

        println!("Traces schema fields:");
        for field in schema.as_struct().fields() {
            println!("  Field ID: {}, Name: {}", field.id, field.name);
        }

        // Get the partition spec
        let partition_spec = create_traces_partition_spec().unwrap();

        println!("\nPartition spec:");
        println!("  Spec ID: {}", partition_spec.spec_id());
        for field in partition_spec.fields() {
            println!(
                "  Partition field: {} (field_id: {}, source_id: {})",
                field.name, field.field_id, field.source_id
            );
        }

        // Find date_day and hour field IDs
        let date_day_field = schema
            .field_by_name("date_day")
            .expect("date_day field not found");
        let hour_field = schema.field_by_name("hour").expect("hour field not found");

        println!("\ndate_day field ID: {}", date_day_field.id);
        println!("hour field ID: {}", hour_field.id);

        // Verify partition field source IDs match schema field IDs
        let date_day_partition = partition_spec
            .fields()
            .iter()
            .find(|f| f.name == "date_day")
            .expect("date_day partition field not found");
        let hour_partition = partition_spec
            .fields()
            .iter()
            .find(|f| f.name == "hour")
            .expect("hour partition field not found");

        assert_eq!(
            date_day_partition.source_id, date_day_field.id,
            "date_day partition source_id should match schema field id"
        );
        assert_eq!(
            hour_partition.source_id, hour_field.id,
            "hour partition source_id should match schema field id"
        );
    }
}
