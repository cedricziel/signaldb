use crate::config::DefaultSchemas;
use crate::schema::SCHEMA_DEFINITIONS;
use anyhow::Result;
use iceberg_rust::spec::partition::{
    PartitionField, PartitionSpec, PartitionSpecBuilder, Transform,
};
use iceberg_rust::spec::schema::Schema;

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

/// Create Iceberg schema for traces table using TOML definitions, plus any
/// configured materialized-label columns.
pub fn create_traces_schema_with(labels: &[String]) -> Result<Schema> {
    // Get the current trace schema version from TOML
    let current_version = SCHEMA_DEFINITIONS.current_trace_version();
    let resolved_schema = SCHEMA_DEFINITIONS.resolve_trace_schema(current_version)?;

    resolved_schema.to_iceberg_schema_with_labels(labels)
}

/// Create Iceberg schema for logs table using TOML definitions, plus any
/// configured materialized-label columns and the derived `attr_tokens`
/// column (see [`crate::schema::ATTR_TOKENS_COLUMN`]).
pub fn create_logs_schema_with(labels: &[String]) -> Result<Schema> {
    // Get the current log schema version from TOML
    let current_version = SCHEMA_DEFINITIONS.metadata.current_log_version.as_str();
    let resolved_schema = SCHEMA_DEFINITIONS.resolve_log_schema(current_version)?;

    // Promote configured attribute keys to dedicated columns. The schema is
    // materialized once at table-creation time; the global config is the
    // source of truth for which labels are promoted (empty when unset).
    resolved_schema.to_iceberg_schema_with_labels_and_attr_tokens(labels)
}

/// Global-config variant of [`create_traces_schema_with`].
pub fn create_traces_schema() -> Result<Schema> {
    create_traces_schema_with(&materialized_labels_for("traces"))
}

/// Global-config variant of [`create_logs_schema_with`].
pub fn create_logs_schema() -> Result<Schema> {
    create_logs_schema_with(&materialized_labels_for("logs"))
}

/// Global-config variant of [`create_metrics_gauge_schema_with`].
pub fn create_metrics_gauge_schema() -> Result<Schema> {
    create_metrics_gauge_schema_with(&materialized_labels_for("metrics"))
}

/// Global-config variant of [`create_metrics_sum_schema_with`].
pub fn create_metrics_sum_schema() -> Result<Schema> {
    create_metrics_sum_schema_with(&materialized_labels_for("metrics"))
}

/// Global-config variant of [`create_metrics_histogram_schema_with`].
pub fn create_metrics_histogram_schema() -> Result<Schema> {
    create_metrics_histogram_schema_with(&materialized_labels_for("metrics"))
}

/// Global-config variant of [`create_metrics_exponential_histogram_schema_with`].
pub fn create_metrics_exponential_histogram_schema() -> Result<Schema> {
    create_metrics_exponential_histogram_schema_with(&materialized_labels_for("metrics"))
}

/// Global-config variant of [`create_metrics_summary_schema_with`].
pub fn create_metrics_summary_schema() -> Result<Schema> {
    create_metrics_summary_schema_with(&materialized_labels_for("metrics"))
}

/// Global-config variant of [`create_profiles_schema_with`].
pub fn create_profiles_schema() -> Result<Schema> {
    create_profiles_schema_with(&materialized_labels_for("profiles"))
}

/// The configured materialized labels for a signal, read from the global
/// config (empty when the config is not initialized, e.g. in unit tests).
fn materialized_labels_for(signal: &str) -> Vec<String> {
    crate::config::CONFIG
        .get()
        .map(|c| {
            let m = &c.schema.materialized_labels;
            match signal {
                "logs" => m.logs.clone(),
                "traces" => m.traces.clone(),
                "metrics" => m.metrics.clone(),
                "profiles" => m.profiles.clone(),
                _ => Vec::new(),
            }
        })
        .unwrap_or_default()
}

/// Create Iceberg schema for metrics gauge table
/// Based on ClickHouse metrics_gauge_table.sql schema but adapted for Iceberg
pub fn create_metrics_gauge_schema_with(labels: &[String]) -> Result<Schema> {
    SCHEMA_DEFINITIONS
        .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_gauge, "physical-v1")?
        .to_iceberg_schema_with_labels(labels)
}

/// Create Iceberg schema for metrics sum table
/// Based on ClickHouse metrics_sum_table.sql schema but adapted for Iceberg
pub fn create_metrics_sum_schema_with(labels: &[String]) -> Result<Schema> {
    SCHEMA_DEFINITIONS
        .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_sum, "physical-v1")?
        .to_iceberg_schema_with_labels(labels)
}

/// Create Iceberg schema for metrics histogram table
/// Based on ClickHouse metrics_histogram_table.sql schema but adapted for Iceberg
pub fn create_metrics_histogram_schema_with(labels: &[String]) -> Result<Schema> {
    SCHEMA_DEFINITIONS
        .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_histogram, "physical-v1")?
        .to_iceberg_schema_with_labels(labels)
}

/// Create Iceberg schema for metrics exponential histogram table
/// Similar to histogram but with exponential bucketing for better precision
pub fn create_metrics_exponential_histogram_schema_with(labels: &[String]) -> Result<Schema> {
    SCHEMA_DEFINITIONS
        .resolve_table_schema(
            &SCHEMA_DEFINITIONS.metrics_exponential_histogram,
            "physical-v1",
        )?
        .to_iceberg_schema_with_labels(labels)
}

/// Create Iceberg schema for metrics summary table
/// Stores quantile values for summary metrics
pub fn create_metrics_summary_schema_with(labels: &[String]) -> Result<Schema> {
    SCHEMA_DEFINITIONS
        .resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_summary, "physical-v1")?
        .to_iceberg_schema_with_labels(labels)
}

/// Create Iceberg schema for the profiles table
///
/// Storage format for OpenTelemetry profiles with the OTLP dictionary
/// resolved at ingest. Identifiers (profile_id, trace_id, span_id) are
/// stored as hex strings to stay joinable with the traces and logs tables.
pub fn create_profiles_schema_with(labels: &[String]) -> Result<Schema> {
    SCHEMA_DEFINITIONS
        .resolve_table_schema(&SCHEMA_DEFINITIONS.profiles, "physical-v1")?
        .to_iceberg_schema_with_labels(labels)
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

/// Create partition specification for profiles table
/// Partitions by hour using Iceberg's built-in Hour transform on the timestamp column.
/// Hour-level partitioning also enables day/month/year pruning automatically.
pub fn create_profiles_partition_spec() -> Result<PartitionSpec> {
    let schema = create_profiles_schema()?;
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
    Profiles,
    Custom(String), // For custom schemas from configuration
}

impl TableSchema {
    /// Get the schema for this table type
    /// Like [`Self::schema`], but with an explicit per-tenant
    /// materialized-labels resolution instead of the global config.
    pub fn schema_with_labels(&self, m: &crate::config::MaterializedLabels) -> Result<Schema> {
        match self {
            TableSchema::Traces => create_traces_schema_with(&m.traces),
            TableSchema::Logs => create_logs_schema_with(&m.logs),
            TableSchema::MetricsGauge => create_metrics_gauge_schema_with(&m.metrics),
            TableSchema::MetricsSum => create_metrics_sum_schema_with(&m.metrics),
            TableSchema::MetricsHistogram => create_metrics_histogram_schema_with(&m.metrics),
            TableSchema::MetricsExponentialHistogram => {
                create_metrics_exponential_histogram_schema_with(&m.metrics)
            }
            TableSchema::MetricsSummary => create_metrics_summary_schema_with(&m.metrics),
            TableSchema::Profiles => create_profiles_schema_with(&m.profiles),
            TableSchema::Custom(_) => Err(anyhow::anyhow!(
                "Custom schemas must be loaded from configuration"
            )),
        }
    }

    /// The materialized-label allowlist that applies to this table from a
    /// resolved per-tenant config: logs/traces/metrics/profiles map to their
    /// signal's list, custom tables to none. This is the same routing
    /// [`Self::schema_with_labels`] uses to inject `label_<key>` columns.
    pub fn materialized_labels_of<'a>(
        &self,
        m: &'a crate::config::MaterializedLabels,
    ) -> &'a [String] {
        match self {
            TableSchema::Traces => &m.traces,
            TableSchema::Logs => &m.logs,
            TableSchema::MetricsGauge
            | TableSchema::MetricsSum
            | TableSchema::MetricsHistogram
            | TableSchema::MetricsExponentialHistogram
            | TableSchema::MetricsSummary => &m.metrics,
            TableSchema::Profiles => &m.profiles,
            TableSchema::Custom(_) => &[],
        }
    }

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
            TableSchema::Profiles => create_profiles_schema(),
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
            TableSchema::Profiles => create_profiles_partition_spec(),
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
            TableSchema::Profiles => "profiles",
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

        if config.profiles_enabled {
            schemas.push(TableSchema::Profiles);
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
            TableSchema::Profiles,
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
    fn schema_with_labels_injects_the_given_allowlist() {
        use crate::config::MaterializedLabels;
        let m = MaterializedLabels {
            logs: vec!["namespace".to_string()],
            traces: vec!["http.method".to_string()],
            ..Default::default()
        };
        let logs = TableSchema::Logs.schema_with_labels(&m).unwrap();
        assert!(logs.fields().iter().any(|f| f.name == "label_namespace"));
        // The logs schema must not pick up the traces list.
        assert!(!logs.fields().iter().any(|f| f.name == "label_http_method"));
        let traces = TableSchema::Traces.schema_with_labels(&m).unwrap();
        assert!(
            traces
                .fields()
                .iter()
                .any(|f| f.name == "label_http_method")
        );
    }

    #[test]
    fn metrics_and_profiles_schemas_inject_labels_now_that_theyre_schemas_toml_sourced() {
        // Regression coverage for the schemas.toml migration: metrics and
        // profiles used to build their own label columns via
        // append_materialized_label_fields (now deleted, dead code); they
        // now go through the same ResolvedSchema::to_iceberg_schema_with_labels
        // path traces/logs already used, which orders label columns after
        // map key/value ids rather than before -- a harmless field-id
        // ordering difference for brand-new tables, not a behavior change.
        use crate::config::MaterializedLabels;
        let m = MaterializedLabels {
            metrics: vec!["region".to_string()],
            profiles: vec!["deployment.environment".to_string()],
            ..Default::default()
        };

        let gauge = TableSchema::MetricsGauge.schema_with_labels(&m).unwrap();
        assert!(gauge.fields().iter().any(|f| f.name == "label_region"));

        let profiles = TableSchema::Profiles.schema_with_labels(&m).unwrap();
        assert!(
            profiles
                .fields()
                .iter()
                .any(|f| f.name == "label_deployment_environment")
        );

        // No duplicate field ids anywhere in either schema.
        for schema in [&gauge, &profiles] {
            let mut ids: Vec<i32> = schema.fields().iter().map(|f| f.id).collect();
            ids.sort_unstable();
            ids.dedup();
            assert_eq!(ids.len(), schema.fields().iter().count());
        }
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
    fn test_profiles_schema_creation() {
        let schema = create_profiles_schema().unwrap();

        // Check for key fields
        assert!(has_field(&schema, "profile_id"));
        assert!(has_field(&schema, "timestamp"));
        assert!(has_field(&schema, "sample_type"));
        assert!(has_field(&schema, "service_name"));
        assert!(has_field(&schema, "stacktraces_json"));
        assert!(has_field(&schema, "samples_json"));
        assert!(has_field(&schema, "trace_id"));
        assert!(has_field(&schema, "span_id"));
        assert!(has_field(&schema, "date_day"));
    }

    #[test]
    fn test_profiles_partition_spec() {
        let spec = create_profiles_partition_spec().unwrap();
        assert_eq!(spec.fields().len(), 1);
        assert_eq!(spec.fields()[0].name(), "timestamp_hour");
    }

    #[test]
    fn test_profiles_respects_config_toggle() {
        let mut config = DefaultSchemas::default();
        let names: Vec<&str> = TableSchema::all_from_config(&config)
            .iter()
            .map(|s| match s {
                TableSchema::Profiles => "profiles",
                _ => "",
            })
            .filter(|n| !n.is_empty())
            .collect();
        assert_eq!(names, vec!["profiles"]);

        config.profiles_enabled = false;
        assert!(
            !TableSchema::all_from_config(&config)
                .iter()
                .any(|s| matches!(s, TableSchema::Profiles))
        );
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
