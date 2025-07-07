use crate::config::DefaultSchemas;
use anyhow::Result;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::spec::{PartitionSpec, Transform};
use std::sync::Arc;

/// Create Iceberg schema for traces table
/// Based on ClickHouse traces_table.sql schema but adapted for Iceberg
pub fn create_traces_schema() -> Result<Schema> {
    let fields = vec![
        // Core span identification
        Arc::new(NestedField::required(
            1,
            "trace_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            2,
            "span_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            3,
            "parent_span_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            4,
            "trace_state",
            Type::Primitive(PrimitiveType::String),
        )),
        // Timing information
        Arc::new(NestedField::required(
            5,
            "timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        Arc::new(NestedField::required(
            6,
            "duration_nanos",
            Type::Primitive(PrimitiveType::Long),
        )),
        // Span metadata
        Arc::new(NestedField::required(
            7,
            "span_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            8,
            "span_kind",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            9,
            "service_name",
            Type::Primitive(PrimitiveType::String),
        )),
        // Status
        Arc::new(NestedField::required(
            10,
            "status_code",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            11,
            "status_message",
            Type::Primitive(PrimitiveType::String),
        )),
        // Resource and scope information
        Arc::new(NestedField::optional(
            12,
            "resource_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            13,
            "resource_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            14,
            "scope_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            15,
            "scope_version",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            16,
            "scope_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            17,
            "scope_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            18,
            "span_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Events - stored as JSON array string for simplicity
        Arc::new(NestedField::optional(
            19,
            "events",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Links - stored as JSON array string for simplicity
        Arc::new(NestedField::optional(
            20,
            "links",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Additional fields for query optimization
        Arc::new(NestedField::required(
            21,
            "date_day",
            Type::Primitive(PrimitiveType::Date),
        )), // Partition key
        Arc::new(NestedField::required(
            22,
            "hour",
            Type::Primitive(PrimitiveType::Int),
        )), // Sub-partition key
    ];

    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create traces schema: {}", e))
}

/// Create Iceberg schema for logs table
/// Based on ClickHouse logs_table.sql schema but adapted for Iceberg
pub fn create_logs_schema() -> Result<Schema> {
    let fields = vec![
        // Core log identification
        Arc::new(NestedField::required(
            1,
            "timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        Arc::new(NestedField::optional(
            2,
            "observed_timestamp",
            Type::Primitive(PrimitiveType::TimestampNs),
        )),
        // Trace context
        Arc::new(NestedField::optional(
            3,
            "trace_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            4,
            "span_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            5,
            "trace_flags",
            Type::Primitive(PrimitiveType::Int),
        )),
        // Log content
        Arc::new(NestedField::optional(
            6,
            "severity_text",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            7,
            "severity_number",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::required(
            8,
            "service_name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            9,
            "body",
            Type::Primitive(PrimitiveType::String),
        )),
        // Resource and scope information
        Arc::new(NestedField::optional(
            10,
            "resource_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            11,
            "resource_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            12,
            "scope_schema_url",
            Type::Primitive(PrimitiveType::String),
        )),
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
            "scope_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        Arc::new(NestedField::optional(
            16,
            "log_attributes",
            Type::Primitive(PrimitiveType::String),
        )), // JSON string
        // Additional fields for query optimization
        Arc::new(NestedField::required(
            17,
            "date_day",
            Type::Primitive(PrimitiveType::Date),
        )), // Partition key
        Arc::new(NestedField::required(
            18,
            "hour",
            Type::Primitive(PrimitiveType::Int),
        )), // Sub-partition key
    ];

    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create logs schema: {}", e))
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

/// Create partition specification for traces table
/// Partitions by date (daily) and sub-partitions by hour for better query performance
pub fn create_traces_partition_spec() -> Result<PartitionSpec> {
    let schema = create_traces_schema()?;
    PartitionSpec::builder(schema)
        .with_spec_id(1)
        .add_partition_field("date_day", "date_day", Transform::Identity)?
        .add_partition_field("hour", "hour", Transform::Identity)?
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
            TableSchema::MetricsGauge | TableSchema::MetricsSum | TableSchema::MetricsHistogram => {
                create_metrics_partition_spec()
            }
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
}
