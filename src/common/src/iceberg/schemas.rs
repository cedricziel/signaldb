use crate::config::DefaultSchemas;
use crate::schema::SCHEMA_DEFINITIONS;
use anyhow::Result;
use iceberg_rust::spec::partition::{
    PartitionField, PartitionSpec, PartitionSpecBuilder, Transform,
};
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::sort::{NullOrder, SortDirection, SortField, SortOrder, SortOrderBuilder};

/// Order id carried by every signal table's declared sort order.
///
/// Iceberg reserves order id `0` for the unsorted order, so a declared order
/// needs an id of its own. Every signal table uses the same id because every
/// signal table declares exactly one order; readers compare a data file's
/// `sort_order_id` against the table's default order id to decide whether the
/// file attests that order.
pub const SIGNAL_SORT_ORDER_ID: i32 = 1;

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

    /// The signal table this name refers to, or `None` for a name that is
    /// not one of SignalDB's own signal tables.
    ///
    /// Deliberately never returns [`TableSchema::Custom`]: callers use this
    /// to decide whether the built-in schema, partition spec and sort order
    /// apply, and none of those are defined for a custom table.
    pub fn from_table_name(table_name: &str) -> Option<TableSchema> {
        match table_name {
            "traces" => Some(TableSchema::Traces),
            "logs" => Some(TableSchema::Logs),
            "metrics_gauge" => Some(TableSchema::MetricsGauge),
            "metrics_sum" => Some(TableSchema::MetricsSum),
            "metrics_histogram" => Some(TableSchema::MetricsHistogram),
            "metrics_exponential_histogram" => Some(TableSchema::MetricsExponentialHistogram),
            "metrics_summary" => Some(TableSchema::MetricsSummary),
            "profiles" => Some(TableSchema::Profiles),
            _ => None,
        }
    }

    /// The canonical sort key for this table, as column names in key order.
    ///
    /// Time-leading for every signal: `ORDER BY timestamp` with a limit is
    /// the dominant query shape, and hour partitioning already clusters on
    /// the same column. These are exactly the keys the compactor has always
    /// sorted its output by, so declaring them costs no rewrite of existing
    /// data. A custom table has no canonical key.
    ///
    /// This is the single source of truth for the ordering contract: every
    /// producer sorts by it and the query engine is only ever told about
    /// this order (see `openspec/specs/declared-data-ordering`).
    pub fn sort_key_columns(&self) -> &'static [&'static str] {
        match self {
            TableSchema::Traces => &["timestamp", "trace_id"],
            TableSchema::Logs => &["timestamp", "service_name", "severity_text"],
            TableSchema::MetricsGauge
            | TableSchema::MetricsSum
            | TableSchema::MetricsHistogram
            | TableSchema::MetricsExponentialHistogram
            | TableSchema::MetricsSummary => &["timestamp", "metric_name", "service_name"],
            TableSchema::Profiles => &["timestamp", "service_name"],
            TableSchema::Custom(_) => &[],
        }
    }

    /// The canonical sort order for this table, with each key column bound to
    /// its field id in `schema`.
    ///
    /// Binding against the schema that is actually being declared matters:
    /// two tables for the same signal can carry different field ids, because
    /// materialized-label columns are injected per tenant. Ascending with
    /// nulls first on every column, matching what the compactor has always
    /// produced.
    ///
    /// # Errors
    ///
    /// Returns an error if a key column is missing from `schema` — that means
    /// the sort key and the schema have drifted apart, and declaring a
    /// partial order would silently claim a different ordering than the one
    /// producers honor.
    pub fn sort_order_for(&self, schema: &Schema) -> Result<Option<SortOrder>> {
        let columns = self.sort_key_columns();
        if columns.is_empty() {
            return Ok(None);
        }

        let mut builder = SortOrderBuilder::default();
        builder.with_order_id(SIGNAL_SORT_ORDER_ID);
        for column in columns {
            let field = schema
                .fields()
                .iter()
                .find(|field| field.name == *column)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Sort key column '{column}' is missing from the {} schema",
                        self.table_name()
                    )
                })?;
            builder.with_sort_field(SortField {
                source_id: field.id,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            });
        }

        builder.build().map(Some).map_err(|e| {
            anyhow::anyhow!("Failed to build sort order for {}: {e}", self.table_name())
        })
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

#[cfg(test)]
mod sort_order_tests {
    use super::*;
    use crate::config::MaterializedLabels;

    /// Resolve a sort order back to the column names it names, so the
    /// assertions read as the contract rather than as field ids.
    fn key_column_names(schema: &Schema, order: &SortOrder) -> Vec<String> {
        order
            .fields
            .iter()
            .map(|field| {
                schema
                    .fields()
                    .iter()
                    .find(|f| f.id == field.source_id)
                    .unwrap_or_else(|| panic!("sort field {} not in schema", field.source_id))
                    .name
                    .clone()
            })
            .collect()
    }

    #[test]
    fn every_signal_declares_its_canonical_time_leading_key() {
        let expected: &[(TableSchema, &[&str])] = &[
            (TableSchema::Traces, &["timestamp", "trace_id"]),
            (
                TableSchema::Logs,
                &["timestamp", "service_name", "severity_text"],
            ),
            (
                TableSchema::MetricsGauge,
                &["timestamp", "metric_name", "service_name"],
            ),
            (
                TableSchema::MetricsSum,
                &["timestamp", "metric_name", "service_name"],
            ),
            (
                TableSchema::MetricsHistogram,
                &["timestamp", "metric_name", "service_name"],
            ),
            (
                TableSchema::MetricsExponentialHistogram,
                &["timestamp", "metric_name", "service_name"],
            ),
            (
                TableSchema::MetricsSummary,
                &["timestamp", "metric_name", "service_name"],
            ),
            (TableSchema::Profiles, &["timestamp", "service_name"]),
        ];

        for (table, key) in expected {
            let schema = table.schema().unwrap();
            let order = table
                .sort_order_for(&schema)
                .unwrap()
                .unwrap_or_else(|| panic!("{} declares no sort order", table.table_name()));

            assert_eq!(
                order.order_id,
                SIGNAL_SORT_ORDER_ID,
                "{} must not use the reserved unsorted order id",
                table.table_name()
            );
            assert_eq!(
                key_column_names(&schema, &order),
                *key,
                "unexpected sort key for {}",
                table.table_name()
            );
            for field in &order.fields {
                assert_eq!(field.transform, Transform::Identity);
                assert_eq!(field.direction, SortDirection::Ascending);
                assert_eq!(field.null_order, NullOrder::First);
            }
        }
    }

    #[test]
    fn sort_fields_bind_to_the_schema_they_are_declared_against() {
        // Materialized labels shift field ids, so an order resolved against
        // one tenant's schema would name the wrong columns in another's.
        let labels = MaterializedLabels {
            traces: vec!["http.method".to_string()],
            ..Default::default()
        };
        let plain = TableSchema::Traces.schema().unwrap();
        let with_labels = TableSchema::Traces.schema_with_labels(&labels).unwrap();

        for schema in [&plain, &with_labels] {
            let order = TableSchema::Traces
                .sort_order_for(schema)
                .unwrap()
                .expect("traces declares a sort order");
            assert_eq!(
                key_column_names(schema, &order),
                vec!["timestamp".to_string(), "trace_id".to_string()]
            );
        }
    }

    #[test]
    fn custom_tables_declare_no_order() {
        let custom = TableSchema::Custom("whatever".to_string());
        assert!(custom.sort_key_columns().is_empty());
        let schema = TableSchema::Traces.schema().unwrap();
        assert!(custom.sort_order_for(&schema).unwrap().is_none());
    }

    #[test]
    fn a_key_column_missing_from_the_schema_is_an_error() {
        // A sort key and a schema that have drifted apart must surface, not
        // silently declare a truncated order that producers would honor
        // differently from what the engine is told.
        use iceberg_rust::spec::types::StructType;

        let full = TableSchema::Traces.schema().unwrap();
        let without_trace_id: Vec<_> = full
            .fields()
            .iter()
            .filter(|f| f.name != "trace_id")
            .cloned()
            .collect();
        let drifted = Schema::from_struct_type(StructType::new(without_trace_id), 0, None);

        let error = TableSchema::Traces
            .sort_order_for(&drifted)
            .expect_err("a missing key column must not declare a partial order");
        assert!(
            error.to_string().contains("trace_id"),
            "error should name the missing column: {error}"
        );
    }

    #[test]
    fn table_names_round_trip_to_their_schema() {
        for table in TableSchema::all() {
            let resolved = TableSchema::from_table_name(table.table_name())
                .unwrap_or_else(|| panic!("{} does not resolve back", table.table_name()));
            assert_eq!(resolved.table_name(), table.table_name());
        }
        assert!(TableSchema::from_table_name("not_a_signal_table").is_none());
    }
}
