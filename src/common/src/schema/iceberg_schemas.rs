use crate::config::DefaultSchemas;
use crate::schema::SCHEMA_DEFINITIONS;
use anyhow::Result;
use iceberg_rust::spec::partition::{
    PartitionField, PartitionSpec, PartitionSpecBuilder, Transform,
};
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};

/// Helper to create a required StructField
fn required_field(id: i32, name: &str, prim: PrimitiveType) -> StructField {
    StructField {
        id,
        name: name.to_string(),
        required: true,
        field_type: Type::Primitive(prim),
        doc: None,
    }
}

/// Helper to create an optional StructField
fn optional_field(id: i32, name: &str, prim: PrimitiveType) -> StructField {
    StructField {
        id,
        name: name.to_string(),
        required: false,
        field_type: Type::Primitive(prim),
        doc: None,
    }
}

/// Create an hour partition spec for a schema, partitioning on the given source field.
/// Uses the Iceberg convention: partition field_id = 1000 + source_id.
fn create_hour_partition_spec(
    schema: &Schema,
    source_field_name: &str,
    partition_name: &str,
) -> Result<PartitionSpec> {
    let source_field = schema
        .fields()
        .iter()
        .find(|f| f.name == source_field_name)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Field '{}' not found in schema for partition spec",
                source_field_name
            )
        })?;

    let partition_field = PartitionField::new(
        source_field.id,
        1000 + source_field.id,
        partition_name,
        Transform::Hour,
    );

    PartitionSpecBuilder::default()
        .with_spec_id(0)
        .with_partition_field(partition_field)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build partition spec: {}", e))
}

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
        required_field(1, "timestamp", PrimitiveType::Timestamp),
        optional_field(2, "start_timestamp", PrimitiveType::Timestamp),
        // Metric identification
        required_field(3, "service_name", PrimitiveType::String),
        required_field(4, "metric_name", PrimitiveType::String),
        optional_field(5, "metric_description", PrimitiveType::String),
        optional_field(6, "metric_unit", PrimitiveType::String),
        // Value
        required_field(7, "value", PrimitiveType::Double),
        optional_field(8, "flags", PrimitiveType::Int),
        // Resource and scope information
        optional_field(9, "resource_schema_url", PrimitiveType::String),
        optional_field(10, "resource_attributes", PrimitiveType::String), // JSON string
        optional_field(11, "scope_name", PrimitiveType::String),
        optional_field(12, "scope_version", PrimitiveType::String),
        optional_field(13, "scope_schema_url", PrimitiveType::String),
        optional_field(14, "scope_attributes", PrimitiveType::String), // JSON string
        optional_field(15, "scope_dropped_attr_count", PrimitiveType::Int),
        // Metric attributes
        optional_field(16, "attributes", PrimitiveType::String), // JSON string
        // Exemplars - stored as JSON string for simplicity
        optional_field(17, "exemplars", PrimitiveType::String), // JSON string
        // Additional fields for query optimization
        required_field(18, "date_day", PrimitiveType::Date), // Partition key
        required_field(19, "hour", PrimitiveType::Int),      // Sub-partition key
    ];

    Ok(Schema::from_struct_type(StructType::new(fields), 0, None))
}

/// Create Iceberg schema for metrics sum table
/// Based on ClickHouse metrics_sum_table.sql schema but adapted for Iceberg
pub fn create_metrics_sum_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        required_field(1, "timestamp", PrimitiveType::Timestamp),
        optional_field(2, "start_timestamp", PrimitiveType::Timestamp),
        // Metric identification
        required_field(3, "service_name", PrimitiveType::String),
        required_field(4, "metric_name", PrimitiveType::String),
        optional_field(5, "metric_description", PrimitiveType::String),
        optional_field(6, "metric_unit", PrimitiveType::String),
        // Value and aggregation info
        required_field(7, "value", PrimitiveType::Double),
        optional_field(8, "flags", PrimitiveType::Int),
        required_field(9, "aggregation_temporality", PrimitiveType::Int),
        required_field(10, "is_monotonic", PrimitiveType::Boolean),
        // Resource and scope information
        optional_field(11, "resource_schema_url", PrimitiveType::String),
        optional_field(12, "resource_attributes", PrimitiveType::String), // JSON string
        optional_field(13, "scope_name", PrimitiveType::String),
        optional_field(14, "scope_version", PrimitiveType::String),
        optional_field(15, "scope_schema_url", PrimitiveType::String),
        optional_field(16, "scope_attributes", PrimitiveType::String), // JSON string
        optional_field(17, "scope_dropped_attr_count", PrimitiveType::Int),
        // Metric attributes
        optional_field(18, "attributes", PrimitiveType::String), // JSON string
        // Exemplars - stored as JSON string for simplicity
        optional_field(19, "exemplars", PrimitiveType::String), // JSON string
        // Additional fields for query optimization
        required_field(20, "date_day", PrimitiveType::Date), // Partition key
        required_field(21, "hour", PrimitiveType::Int),      // Sub-partition key
    ];

    Ok(Schema::from_struct_type(StructType::new(fields), 0, None))
}

/// Create Iceberg schema for metrics histogram table
/// Based on ClickHouse metrics_histogram_table.sql schema but adapted for Iceberg
pub fn create_metrics_histogram_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        required_field(1, "timestamp", PrimitiveType::Timestamp),
        optional_field(2, "start_timestamp", PrimitiveType::Timestamp),
        // Metric identification
        required_field(3, "service_name", PrimitiveType::String),
        required_field(4, "metric_name", PrimitiveType::String),
        optional_field(5, "metric_description", PrimitiveType::String),
        optional_field(6, "metric_unit", PrimitiveType::String),
        // Histogram data
        required_field(7, "count", PrimitiveType::Long),
        optional_field(8, "sum", PrimitiveType::Double),
        optional_field(9, "min", PrimitiveType::Double),
        optional_field(10, "max", PrimitiveType::Double),
        optional_field(11, "bucket_counts", PrimitiveType::String), // JSON array string
        optional_field(12, "explicit_bounds", PrimitiveType::String), // JSON array string
        optional_field(13, "flags", PrimitiveType::Int),
        required_field(14, "aggregation_temporality", PrimitiveType::Int),
        // Resource and scope information
        optional_field(15, "resource_schema_url", PrimitiveType::String),
        optional_field(16, "resource_attributes", PrimitiveType::String), // JSON string
        optional_field(17, "scope_name", PrimitiveType::String),
        optional_field(18, "scope_version", PrimitiveType::String),
        optional_field(19, "scope_schema_url", PrimitiveType::String),
        optional_field(20, "scope_attributes", PrimitiveType::String), // JSON string
        optional_field(21, "scope_dropped_attr_count", PrimitiveType::Int),
        // Metric attributes
        optional_field(22, "attributes", PrimitiveType::String), // JSON string
        // Exemplars - stored as JSON string for simplicity
        optional_field(23, "exemplars", PrimitiveType::String), // JSON string
        // Additional fields for query optimization
        required_field(24, "date_day", PrimitiveType::Date), // Partition key
        required_field(25, "hour", PrimitiveType::Int),      // Sub-partition key
    ];

    Ok(Schema::from_struct_type(StructType::new(fields), 0, None))
}

/// Create Iceberg schema for metrics exponential histogram table
/// Similar to histogram but with exponential bucketing for better precision
pub fn create_metrics_exponential_histogram_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        required_field(1, "timestamp", PrimitiveType::Timestamp),
        optional_field(2, "start_timestamp", PrimitiveType::Timestamp),
        // Metric identification
        required_field(3, "service_name", PrimitiveType::String),
        required_field(4, "metric_name", PrimitiveType::String),
        optional_field(5, "metric_description", PrimitiveType::String),
        optional_field(6, "metric_unit", PrimitiveType::String),
        // Exponential histogram data
        required_field(7, "count", PrimitiveType::Long),
        optional_field(8, "sum", PrimitiveType::Double),
        optional_field(9, "min", PrimitiveType::Double),
        optional_field(10, "max", PrimitiveType::Double),
        optional_field(11, "scale", PrimitiveType::Int),
        optional_field(12, "zero_count", PrimitiveType::Long),
        optional_field(13, "positive_offset", PrimitiveType::Int),
        optional_field(14, "positive_bucket_counts", PrimitiveType::String), // JSON array string
        optional_field(15, "negative_offset", PrimitiveType::Int),
        optional_field(16, "negative_bucket_counts", PrimitiveType::String), // JSON array string
        optional_field(17, "flags", PrimitiveType::Int),
        required_field(18, "aggregation_temporality", PrimitiveType::Int),
        optional_field(19, "zero_threshold", PrimitiveType::Double),
        // Resource and scope information
        optional_field(20, "resource_schema_url", PrimitiveType::String),
        optional_field(21, "resource_attributes", PrimitiveType::String), // JSON string
        optional_field(22, "scope_name", PrimitiveType::String),
        optional_field(23, "scope_version", PrimitiveType::String),
        optional_field(24, "scope_schema_url", PrimitiveType::String),
        optional_field(25, "scope_attributes", PrimitiveType::String), // JSON string
        optional_field(26, "scope_dropped_attr_count", PrimitiveType::Int),
        // Metric attributes
        optional_field(27, "attributes", PrimitiveType::String), // JSON string
        // Exemplars - stored as JSON string for simplicity
        optional_field(28, "exemplars", PrimitiveType::String), // JSON string
        // Additional fields for query optimization
        required_field(29, "date_day", PrimitiveType::Date), // Partition key
        required_field(30, "hour", PrimitiveType::Int),      // Sub-partition key
    ];

    Ok(Schema::from_struct_type(StructType::new(fields), 0, None))
}

/// Create Iceberg schema for metrics summary table
/// Stores quantile values for summary metrics
pub fn create_metrics_summary_schema() -> Result<Schema> {
    let fields = vec![
        // Timing
        required_field(1, "timestamp", PrimitiveType::Timestamp),
        optional_field(2, "start_timestamp", PrimitiveType::Timestamp),
        // Metric identification
        required_field(3, "service_name", PrimitiveType::String),
        required_field(4, "metric_name", PrimitiveType::String),
        optional_field(5, "metric_description", PrimitiveType::String),
        optional_field(6, "metric_unit", PrimitiveType::String),
        // Summary data
        required_field(7, "count", PrimitiveType::Long),
        required_field(8, "sum", PrimitiveType::Double),
        optional_field(9, "quantile_values", PrimitiveType::String), // JSON array of {quantile, value} objects
        optional_field(10, "flags", PrimitiveType::Int),
        // Resource and scope information
        optional_field(11, "resource_schema_url", PrimitiveType::String),
        optional_field(12, "resource_attributes", PrimitiveType::String), // JSON string
        optional_field(13, "scope_name", PrimitiveType::String),
        optional_field(14, "scope_version", PrimitiveType::String),
        optional_field(15, "scope_schema_url", PrimitiveType::String),
        optional_field(16, "scope_attributes", PrimitiveType::String), // JSON string
        optional_field(17, "scope_dropped_attr_count", PrimitiveType::Int),
        // Metric attributes
        optional_field(18, "attributes", PrimitiveType::String), // JSON string
        // Exemplars - stored as JSON string for simplicity
        optional_field(19, "exemplars", PrimitiveType::String), // JSON string
        // Additional fields for query optimization
        required_field(20, "date_day", PrimitiveType::Date), // Partition key
        required_field(21, "hour", PrimitiveType::Int),      // Sub-partition key
    ];

    Ok(Schema::from_struct_type(StructType::new(fields), 0, None))
}

/// Create partition specification for traces table
/// Partitions by hour using Iceberg's built-in Hour transform on the timestamp column.
/// Hour-level partitioning also enables day/month/year pruning automatically.
pub fn create_traces_partition_spec() -> Result<PartitionSpec> {
    let schema = create_traces_schema()?;
    create_hour_partition_spec(&schema, "timestamp", "timestamp_hour")
}

/// Create partition specification for logs table
/// Partitions by hour using Iceberg's built-in Hour transform on the timestamp column.
/// Hour-level partitioning also enables day/month/year pruning automatically.
pub fn create_logs_partition_spec() -> Result<PartitionSpec> {
    let schema = create_logs_schema()?;
    create_hour_partition_spec(&schema, "timestamp", "timestamp_hour")
}

/// Create partition specification for metrics tables
/// Partitions by hour using Iceberg's built-in Hour transform on the timestamp column.
/// Hour-level partitioning also enables day/month/year pruning automatically.
pub fn create_metrics_partition_spec() -> Result<PartitionSpec> {
    // Use metrics gauge schema as the base (they all have the same timestamp column)
    let schema = create_metrics_gauge_schema()?;
    create_hour_partition_spec(&schema, "timestamp", "timestamp_hour")
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

    /// Helper: find a field by name in a schema
    fn has_field(schema: &Schema, name: &str) -> bool {
        schema.fields().iter().any(|f| f.name == name)
    }

    #[test]
    fn test_traces_schema_creation() {
        let schema = create_traces_schema().unwrap();

        // Check for key fields
        assert!(has_field(&schema, "trace_id"));
        assert!(has_field(&schema, "span_id"));
        assert!(has_field(&schema, "timestamp"));
        assert!(has_field(&schema, "service_name"));
        assert!(has_field(&schema, "date_day"));
    }

    #[test]
    fn test_logs_schema_creation() {
        let schema = create_logs_schema().unwrap();

        // Check for key fields
        assert!(has_field(&schema, "timestamp"));
        assert!(has_field(&schema, "service_name"));
        assert!(has_field(&schema, "severity_text"));
        assert!(has_field(&schema, "body"));
        assert!(has_field(&schema, "date_day"));
    }

    #[test]
    fn test_metrics_gauge_schema_creation() {
        let schema = create_metrics_gauge_schema().unwrap();

        // Check for key fields
        assert!(has_field(&schema, "timestamp"));
        assert!(has_field(&schema, "service_name"));
        assert!(has_field(&schema, "metric_name"));
        assert!(has_field(&schema, "value"));
        assert!(has_field(&schema, "date_day"));
    }

    #[test]
    fn test_metrics_sum_schema_creation() {
        let schema = create_metrics_sum_schema().unwrap();

        // Check for key fields
        assert!(has_field(&schema, "timestamp"));
        assert!(has_field(&schema, "service_name"));
        assert!(has_field(&schema, "metric_name"));
        assert!(has_field(&schema, "value"));
        assert!(has_field(&schema, "aggregation_temporality"));
        assert!(has_field(&schema, "is_monotonic"));
        assert!(has_field(&schema, "date_day"));
    }

    #[test]
    fn test_metrics_histogram_schema_creation() {
        let schema = create_metrics_histogram_schema().unwrap();

        // Check for key fields
        assert!(has_field(&schema, "timestamp"));
        assert!(has_field(&schema, "service_name"));
        assert!(has_field(&schema, "metric_name"));
        assert!(has_field(&schema, "count"));
        assert!(has_field(&schema, "bucket_counts"));
        assert!(has_field(&schema, "explicit_bounds"));
        assert!(has_field(&schema, "date_day"));
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
        for field in schema.fields().iter() {
            println!("  Field ID: {}, Name: {}", field.id, field.name);
        }

        // Get the partition spec
        let partition_spec = create_traces_partition_spec().unwrap();

        println!("\nPartition spec:");
        println!("  Spec ID: {}", partition_spec.spec_id());
        for field in partition_spec.fields() {
            println!(
                "  Partition field: {} (field_id: {}, source_id: {})",
                field.name(),
                field.field_id(),
                field.source_id()
            );
        }

        // Find timestamp field ID
        let timestamp_field = schema
            .fields()
            .iter()
            .find(|f| f.name == "timestamp")
            .expect("timestamp field not found");

        println!("\ntimestamp field ID: {}", timestamp_field.id);

        // Verify partition field source ID matches the timestamp schema field ID
        let hour_partition = partition_spec
            .fields()
            .iter()
            .find(|f| f.name() == "timestamp_hour")
            .expect("timestamp_hour partition field not found");

        assert_eq!(
            *hour_partition.source_id(),
            timestamp_field.id,
            "timestamp_hour partition source_id should match timestamp schema field id"
        );

        // Verify only one partition field (Hour subsumes Day)
        assert_eq!(
            partition_spec.fields().len(),
            1,
            "Should have exactly 1 partition field (Hour)"
        );
    }
}
