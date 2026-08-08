use crate::config::Configuration;
use crate::iceberg::{create_object_store_builder_from_config, create_sql_catalog_with_builder};
use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;

pub mod schema_parser;

// Re-export iceberg modules for backward compatibility
pub use crate::iceberg::schemas as iceberg_schemas;
pub use crate::iceberg::{
    create_catalog, create_catalog_with_config, create_catalog_with_object_store,
    create_default_catalog, create_sql_catalog,
};

use self::schema_parser::SchemaDefinitions;

/// The column name a materialized attribute label is stored under. The
/// label key is sanitized (non-alphanumeric → `_`) and prefixed with
/// `label_` so promoted attributes never collide with the built-in schema
/// columns and are valid SQL/Arrow identifiers (OTLP keys like
/// `http.method` contain dots that DataFusion would treat as field
/// access). Writer, schema generation, and querier all resolve a label to
/// its column through this one function.
pub fn materialized_column_name(label: &str) -> String {
    let mut out = String::with_capacity(label.len() + 6);
    out.push_str("label_");
    for ch in label.chars() {
        out.push(if ch.is_ascii_alphanumeric() { ch } else { '_' });
    }
    out
}

/// The derived `key=value` token column on logs tables. Each row carries
/// one token per attribute across resource, scope, and record scopes, so a
/// single bloom-filtered column can answer "does this file contain
/// `key=value` for *any* attribute" without one column per key.
pub const ATTR_TOKENS_COLUMN: &str = "attr_tokens";

/// The bloom-filter table property for the derived [`ATTR_TOKENS_COLUMN`].
///
/// Parquet addresses the tokens through the List leaf column: arrow-rs
/// writes a `List<Utf8>` with the 3-level encoding
/// `attr_tokens (LIST) > list (repeated group) > item`, and the
/// Iceberg-to-Arrow conversion names the element field `item`, so the leaf
/// path is `attr_tokens.list.item`. The pinned iceberg-rust writer splits
/// the property's column suffix on dots into exactly those path parts.
pub fn bloom_filter_property_for_attr_tokens() -> (String, String) {
    use iceberg_rust::spec::table_metadata::WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX;
    (
        format!("{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}{ATTR_TOKENS_COLUMN}.list.item"),
        "true".to_string(),
    )
}

/// The built-in traces columns that carry a Parquet bloom filter.
///
/// Both are flat top-level `Utf8` columns (`schemas.toml` traces.v1/v2), so
/// the property's column suffix is the bare column name — no `.list.item`
/// leaf path like [`bloom_filter_property_for_attr_tokens`]. `trace_id` is
/// the high-cardinality column single-trace lookups
/// (`GET /api/traces/{traceID}`) filter on, for which manifest / row-group
/// min/max statistics never prune (every time-ordered file spans the full
/// random id range); a bloom filter is the only structure that can skip row
/// groups for such a point lookup. `span_id` gets the same treatment for
/// span-level point lookups.
pub const BLOOM_FILTER_TRACE_COLUMNS: [&str; 2] = ["trace_id", "span_id"];

/// False-positive probability for the trace point-lookup bloom filters.
///
/// A false positive costs a row-group read that returns nothing, which for a
/// single-trace lookup is the entire cost of the query. Parquet's default
/// `0.05` means one row group in twenty is read for nothing; `0.01` cuts that
/// five-fold for a filter roughly 40% larger, which is a good trade on a
/// column whose whole purpose is point lookups.
pub const BLOOM_FILTER_TRACE_FPP: &str = "0.01";

/// Expected distinct-value count for `trace_id`'s bloom filter.
///
/// A filter is sized from fpp *and* ndv; fpp alone leaves ndv at parquet-rs's
/// default, which assumes one distinct value per row in a full row group
/// (its default ndv and default max row-group row count are the same
/// number). That default is right for `span_id` — effectively unique per
/// row, so left unset here — but wrong for `trace_id`: every span of a
/// trace repeats the same id, so a row group's distinct trace count is a
/// fraction of its row count. `50_000` assumes an average of roughly 20
/// spans per trace against a row group approaching parquet-rs's default
/// size; it's an estimate, not a measurement — revisit once SignalDB has
/// real trace-shape telemetry to size it from.
pub const BLOOM_FILTER_TRACE_ID_NDV: &str = "50000";

/// Per-column Parquet bloom-filter table properties for the built-in traces
/// point-lookup columns ([`BLOOM_FILTER_TRACE_COLUMNS`]).
///
/// Emits `write.parquet.bloom-filter-enabled.column.<col> = "true"` and
/// `write.parquet.bloom-filter-fpp.column.<col>` for each, plus
/// `write.parquet.bloom-filter-ndv.column.trace_id` (see
/// [`BLOOM_FILTER_TRACE_ID_NDV`]) — standard Iceberg properties the pinned
/// iceberg-rust Parquet writer honors per column. Set at table creation for
/// both [`schemas::TableSchema::Traces`] and [`schemas::TableSchema::Logs`]
/// (see [`bloom_filter_properties_for_table`]) so every new file (ingest and
/// compaction output) carries the filters.
pub fn bloom_filter_properties_for_trace_columns() -> Vec<(String, String)> {
    use iceberg_rust::spec::table_metadata::{
        WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX,
        WRITE_PARQUET_BLOOM_FILTER_FPP_COLUMN_PREFIX, WRITE_PARQUET_BLOOM_FILTER_NDV_COLUMN_PREFIX,
    };

    BLOOM_FILTER_TRACE_COLUMNS
        .iter()
        .flat_map(|column| {
            let mut properties = vec![
                (
                    format!("{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}{column}"),
                    "true".to_string(),
                ),
                (
                    format!("{WRITE_PARQUET_BLOOM_FILTER_FPP_COLUMN_PREFIX}{column}"),
                    BLOOM_FILTER_TRACE_FPP.to_string(),
                ),
            ];
            if *column == "trace_id" {
                properties.push((
                    format!("{WRITE_PARQUET_BLOOM_FILTER_NDV_COLUMN_PREFIX}{column}"),
                    BLOOM_FILTER_TRACE_ID_NDV.to_string(),
                ));
            }
            properties
        })
        .collect()
}

/// Assembles every Parquet bloom-filter table property for a table's
/// columns, dispatching by table type and its materialized labels.
///
/// [`schemas::TableSchema::Logs`] gets a filter over the derived
/// `attr_tokens` column (for `key=value` containment checks) in addition to
/// every materialized label; both `Logs` and `Traces` get the `trace_id`/
/// `span_id` point-lookup filters ([`bloom_filter_properties_for_trace_columns`])
/// since `logs.v1` carries those same columns (optional, but named
/// identically) for logs-for-a-trace correlation. Other table types get
/// only the materialized-label filters.
pub fn bloom_filter_properties_for_table(
    table_schema: &crate::iceberg::schemas::TableSchema,
    materialized_labels: &[String],
) -> Vec<(String, String)> {
    let mut properties = bloom_filter_properties_for_labels(materialized_labels);

    if matches!(table_schema, crate::iceberg::schemas::TableSchema::Logs) {
        properties.push(bloom_filter_property_for_attr_tokens());
    }
    if matches!(
        table_schema,
        crate::iceberg::schemas::TableSchema::Traces | crate::iceberg::schemas::TableSchema::Logs
    ) {
        properties.extend(bloom_filter_properties_for_trace_columns());
    }

    properties
}

/// Parquet compression properties recorded on every table.
///
/// `CreateTableBuilder` records `zstd` / level `3` on a new table, but the
/// writer hardcoded zstd level 1 -- so the metadata described a file that was
/// never written. Now that the writer honors these properties, leaving them
/// alone would silently move every write to level 3. Pin the level the files
/// have actually been written at, making the metadata true without changing a
/// byte; raising it is a separate decision, worth measuring against the
/// ingest path's CPU budget rather than inheriting by accident.
pub fn compression_properties() -> Vec<(String, String)> {
    use iceberg_rust::spec::table_metadata::{
        WRITE_PARQUET_COMPRESSION_CODEC, WRITE_PARQUET_COMPRESSION_LEVEL,
    };

    vec![
        (
            WRITE_PARQUET_COMPRESSION_CODEC.to_string(),
            "zstd".to_string(),
        ),
        (WRITE_PARQUET_COMPRESSION_LEVEL.to_string(), "1".to_string()),
    ]
}

/// Per-column Parquet bloom-filter table properties for a set of
/// materialized attribute labels.
///
/// For each label key this yields
/// `write.parquet.bloom-filter-enabled.column.label_<key> = "true"`, the
/// standard Iceberg property the pinned iceberg-rust Parquet writer honors
/// per column. Column names come from [`materialized_column_name`], so the
/// properties always target the promoted `label_<key>` columns. Duplicate
/// labels (after sanitization) collapse to a single property.
///
/// Shared by table creation and the compactor's attribute-promotion path,
/// so both set identical properties for a promoted label.
pub fn bloom_filter_properties_for_labels(labels: &[String]) -> Vec<(String, String)> {
    use iceberg_rust::spec::table_metadata::WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX;

    let mut seen = std::collections::HashSet::new();
    labels
        .iter()
        .filter_map(|label| {
            let column = materialized_column_name(label);
            seen.insert(column.clone()).then(|| {
                (
                    format!("{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}{column}"),
                    "true".to_string(),
                )
            })
        })
        .collect()
}

/// Free-text columns whose min/max bounds can never prune a scan.
///
/// Iceberg stores a column's bounds inline in the manifest entry of every data
/// file, so a bound is a permanent per-file cost paid on every query plan. For
/// these columns nothing ever pays it back: no query compares them by range.
/// `body` and `status_message` are matched by substring or regex, and
/// `exemplars` is a JSON blob read whole or not at all.
///
/// The columns are listed per signal because the schemas do not share names;
/// a column absent from a table simply has no effect there.
pub const UNBOUNDED_FREE_TEXT_COLUMNS: [&str; 3] = ["body", "status_message", "exemplars"];

/// Metrics-mode table properties for the free-text columns of a signal.
///
/// Emits `write.metadata.metrics.column.<col> = "counts"` for each column in
/// [`UNBOUNDED_FREE_TEXT_COLUMNS`] present in `columns`: value and null counts
/// are still collected — the planner uses those for cardinality estimates —
/// but the useless bounds are dropped.
///
/// Every other column keeps the default `truncate(16)`, which iceberg-rust
/// applies without a property.
pub fn metrics_properties_for_free_text_columns(columns: &[String]) -> Vec<(String, String)> {
    use iceberg_rust::spec::table_metadata::WRITE_METADATA_METRICS_COLUMN_PREFIX;

    UNBOUNDED_FREE_TEXT_COLUMNS
        .iter()
        .filter(|column| columns.iter().any(|present| present == *column))
        .map(|column| {
            (
                format!("{WRITE_METADATA_METRICS_COLUMN_PREFIX}{column}"),
                "counts".to_string(),
            )
        })
        .collect()
}

/// Embedded schema definitions from schemas.toml
pub const SCHEMA_DEFINITIONS_TOML: &str = include_str!("../../../../schemas.toml");

/// Parsed schema definitions
pub static SCHEMA_DEFINITIONS: Lazy<SchemaDefinitions> = Lazy::new(|| {
    SchemaDefinitions::from_toml(SCHEMA_DEFINITIONS_TOML)
        .expect("Failed to load built-in schema definitions")
});

/// Tenant-aware schema registry for managing catalogs and schemas per tenant
pub struct TenantSchemaRegistry {
    pub(crate) config: Configuration,
    catalogs: HashMap<String, Arc<dyn IcebergCatalog>>,
    /// Optional database catalog used as an additional tenant source, so
    /// admin-API tenants resolve alongside config-defined ones.
    tenant_source: Option<Arc<crate::catalog::Catalog>>,
}

impl TenantSchemaRegistry {
    /// Create a new tenant schema registry
    pub fn new(config: Configuration) -> Self {
        Self {
            config,
            catalogs: HashMap::new(),
            tenant_source: None,
        }
    }

    /// Attach a database catalog as an additional tenant source.
    pub fn with_tenant_source(mut self, tenant_source: Arc<crate::catalog::Catalog>) -> Self {
        self.tenant_source = Some(tenant_source);
        self
    }

    /// Get or create a catalog for the specified tenant
    pub async fn get_catalog_for_tenant(
        &mut self,
        tenant_id: &str,
    ) -> Result<Arc<dyn IcebergCatalog>> {
        // Check if tenant is enabled
        if !self.config.is_tenant_enabled(tenant_id) {
            return Err(anyhow::anyhow!("Tenant '{}' is not enabled", tenant_id));
        }

        // Return cached catalog if available
        if let Some(catalog) = self.catalogs.get(tenant_id) {
            return Ok(catalog.clone());
        }

        // Create new catalog for tenant
        let tenant_schema_config = self.config.get_tenant_schema_config(tenant_id);

        // Create object store builder from storage config
        let object_store_builder = create_object_store_builder_from_config(&self.config.storage)?;

        // Create catalog for actual operations
        let catalog = create_sql_catalog_with_builder(
            &tenant_schema_config.catalog_uri,
            "signaldb",
            object_store_builder,
        )
        .await?;

        // Cache the catalog
        self.catalogs.insert(tenant_id.to_string(), catalog.clone());

        Ok(catalog)
    }

    /// Get custom schemas for a tenant
    pub fn get_custom_schemas(&self, tenant_id: &str) -> Option<&HashMap<String, String>> {
        self.config.get_tenant_custom_schemas(tenant_id)
    }

    /// Check if tenant is enabled
    pub fn is_tenant_enabled(&self, tenant_id: &str) -> bool {
        self.config.is_tenant_enabled(tenant_id)
    }

    /// Get the default tenant
    pub fn get_default_tenant(&self) -> &str {
        self.config.get_default_tenant()
    }

    /// Get all configured tenants
    pub fn get_configured_tenants(&self) -> Vec<String> {
        let mut tenants: Vec<String> = self.config.tenants.tenants.keys().cloned().collect();

        // Always include the default tenant if it's not explicitly configured
        let default_tenant = self.get_default_tenant().to_string();
        if !tenants.contains(&default_tenant) {
            tenants.push(default_tenant);
        }

        tenants
    }

    /// Remove a cached catalog (useful for invalidation)
    pub fn invalidate_tenant_catalog(&mut self, tenant_id: &str) {
        self.catalogs.remove(tenant_id);
    }

    /// Get schema definitions for a tenant
    pub fn get_schema_definitions(
        &self,
        _tenant_id: &str,
    ) -> Result<HashMap<String, iceberg_rust::spec::schema::Schema>> {
        let mut schemas = HashMap::new();
        let default_schemas = &self.config.schema.default_schemas;

        for table_schema in iceberg_schemas::TableSchema::all_from_config(default_schemas) {
            match &table_schema {
                iceberg_schemas::TableSchema::Custom(_) => {
                    // Skip custom schemas for now
                    // TODO: Parse custom schema from JSON configuration
                }
                _ => {
                    let schema = table_schema.schema()?;
                    schemas.insert(table_schema.table_name().to_string(), schema);
                }
            }
        }

        Ok(schemas)
    }

    /// Get partition specifications for a tenant  
    pub fn get_partition_specifications(
        &self,
        _tenant_id: &str,
    ) -> Result<HashMap<String, iceberg_rust::spec::partition::PartitionSpec>> {
        let mut partition_specs = HashMap::new();
        let default_schemas = &self.config.schema.default_schemas;

        for table_schema in iceberg_schemas::TableSchema::all_from_config(default_schemas) {
            match &table_schema {
                iceberg_schemas::TableSchema::Custom(_) => {
                    // Skip custom schemas for now
                    // TODO: Parse custom partition spec from configuration
                }
                _ => {
                    let partition_spec = table_schema.partition_spec()?;
                    partition_specs.insert(table_schema.table_name().to_string(), partition_spec);
                }
            }
        }

        Ok(partition_specs)
    }

    /// Provision every signal table enabled for a tenant, across all of its
    /// datasets, before returning.
    ///
    /// The manual counterpart to the writer's periodic reconciler: it gives an
    /// operator an immediate trigger rather than waiting for the next pass.
    /// Both go through [`CatalogManager::ensure_dataset_tables`], so a table
    /// created here is what the write path would have created.
    ///
    /// Errors if any table could not be provisioned — reporting success
    /// without having created them is not permitted.
    pub async fn create_default_tables_for_tenant(&mut self, tenant_id: &str) -> Result<()> {
        let manager = self.catalog_manager().await?;

        let datasets = self.datasets_for_tenant(&manager, tenant_id).await?;
        if datasets.is_empty() {
            return Err(anyhow::anyhow!(
                "Tenant '{tenant_id}' has no datasets to provision"
            ));
        }

        let mut failures: Vec<String> = Vec::new();
        for dataset in &datasets {
            let report = manager.ensure_dataset_tables(tenant_id, dataset).await;
            tracing::info!(
                tenant_id = %tenant_id,
                dataset = %dataset,
                created = report.created.len(),
                already_present = report.already_present.len(),
                failed = report.failed.len(),
                "Provisioned tenant tables on request"
            );
            failures.extend(
                report
                    .failed
                    .iter()
                    .map(|(table, reason)| format!("{dataset}.{table}: {reason}")),
            );
        }

        if !failures.is_empty() {
            return Err(anyhow::anyhow!(
                "Failed to create tables for tenant '{tenant_id}': {}",
                failures.join("; ")
            ));
        }
        Ok(())
    }

    /// The datasets to provision for a tenant: everything the registry knows
    /// about it, falling back to its `default_dataset`.
    ///
    /// A tenant created through the admin API carries `default_dataset` as a
    /// column on its tenant row and no dataset row at all, so the fallback is
    /// the common case rather than the exception.
    async fn datasets_for_tenant(
        &self,
        manager: &crate::CatalogManager,
        tenant_id: &str,
    ) -> Result<Vec<String>> {
        let slug = manager.get_tenant_slug(tenant_id);
        let Some(tenant) = manager.resolve_tenant_by_slug(&slug).await? else {
            return Err(anyhow::anyhow!("Tenant '{tenant_id}' is not registered"));
        };
        // The id -> slug -> tenant round-trip is not identity-preserving:
        // `resolve_tenant_by_slug` matches config tenants first, so a database
        // tenant whose id equals a different config tenant's slug resolves to
        // that config tenant. Provisioning its dataset names under this
        // tenant's namespace would breach tenant isolation.
        if tenant.id != tenant_id {
            return Err(anyhow::anyhow!(
                "Tenant '{tenant_id}' resolves to a different tenant ('{}') by slug '{slug}'; \
                 refusing to provision across tenants",
                tenant.id
            ));
        }

        // The registry guarantees `default_dataset` is among the resolved
        // datasets even when no dataset row names it, so this is a plain
        // projection.
        Ok(tenant.datasets.iter().map(|d| d.id.clone()).collect())
    }

    /// Build a `CatalogManager` over this registry's configuration, carrying
    /// the tenant source when one is attached so database-created tenants
    /// resolve alongside config-defined ones.
    async fn catalog_manager(&self) -> Result<crate::CatalogManager> {
        let manager = crate::CatalogManager::new(self.config.clone()).await?;
        Ok(match &self.tenant_source {
            Some(source) => manager.with_tenant_source(source.clone()),
            None => manager,
        })
    }

    /// List all tables for a tenant
    pub async fn list_tables_for_tenant(&mut self, tenant_id: &str) -> Result<Vec<String>> {
        // Get the catalog
        let _ = self.get_catalog_for_tenant(tenant_id).await?;

        // For now, return empty list
        // TODO: Implement table listing
        tracing::info!("Would list tables for tenant '{tenant_id}' using Iceberg catalog");
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DefaultSchemas, SchemaConfig, TenantSchemaConfig, TenantsConfig};
    use std::collections::HashMap;

    /// A point-lookup filter must be sized for point lookups: Parquet's default
    /// 0.05 reads one row group in twenty for nothing, which for a single-trace
    /// lookup is the entire cost of the query.
    #[test]
    fn trace_bloom_filters_are_sized_tighter_than_the_parquet_default() {
        let properties = bloom_filter_properties_for_trace_columns();

        for column in BLOOM_FILTER_TRACE_COLUMNS {
            assert!(
                properties.contains(&(
                    format!("write.parquet.bloom-filter-enabled.column.{column}"),
                    "true".to_string()
                )),
                "{column} must have a bloom filter"
            );

            let fpp = properties
                .iter()
                .find(|(key, _)| key == &format!("write.parquet.bloom-filter-fpp.column.{column}"))
                .map(|(_, value)| value.parse::<f64>().expect("fpp must be a number"))
                .unwrap_or_else(|| panic!("{column} must have an fpp"));

            assert!(
                fpp > 0.0 && fpp < 0.05,
                "{column} fpp must be tighter than Parquet's 0.05 default, got {fpp}"
            );
        }
    }

    /// `trace_id` repeats across every span of a trace, so its real
    /// cardinality is a fraction of the row count; leaving ndv unset would
    /// size its filter as if every row were a distinct trace. `span_id` is
    /// effectively unique per row, matching parquet-rs's own default, so it
    /// must stay unset rather than duplicate that default as a magic number.
    #[test]
    fn trace_id_bloom_filter_has_an_explicit_ndv_but_span_id_does_not() {
        let properties = bloom_filter_properties_for_trace_columns();

        assert!(
            properties.contains(&(
                "write.parquet.bloom-filter-ndv.column.trace_id".to_string(),
                BLOOM_FILTER_TRACE_ID_NDV.to_string()
            )),
            "trace_id must have an explicit ndv"
        );
        assert!(
            !properties
                .iter()
                .any(|(key, _)| key == "write.parquet.bloom-filter-ndv.column.span_id"),
            "span_id must not override parquet-rs's default ndv"
        );
    }

    /// `logs.v1` carries `trace_id`/`span_id` for logs-for-a-trace
    /// correlation, the same point-lookup problem traces has, so logs must
    /// get the same filters. It must also keep its `attr_tokens` filter.
    /// Traces has no `attr_tokens` column and must not get one.
    #[test]
    fn logs_and_traces_get_trace_columns_but_only_logs_gets_attr_tokens() {
        use crate::iceberg::schemas::TableSchema;

        let logs = bloom_filter_properties_for_table(&TableSchema::Logs, &[]);
        for column in BLOOM_FILTER_TRACE_COLUMNS {
            assert!(
                logs.contains(&(
                    format!("write.parquet.bloom-filter-enabled.column.{column}"),
                    "true".to_string()
                )),
                "logs must have a bloom filter on {column}"
            );
        }
        let (attr_tokens_key, attr_tokens_value) = bloom_filter_property_for_attr_tokens();
        assert!(
            logs.contains(&(attr_tokens_key, attr_tokens_value)),
            "logs must keep its attr_tokens filter"
        );

        let traces = bloom_filter_properties_for_table(&TableSchema::Traces, &[]);
        for column in BLOOM_FILTER_TRACE_COLUMNS {
            assert!(
                traces.contains(&(
                    format!("write.parquet.bloom-filter-enabled.column.{column}"),
                    "true".to_string()
                )),
                "traces must have a bloom filter on {column}"
            );
        }
        assert!(
            !traces.iter().any(|(key, _)| key.contains("attr_tokens")),
            "traces has no attr_tokens column and must not get a filter for one"
        );
    }

    /// A table type with no point-lookup id columns and no materialized
    /// labels gets no bloom filter properties at all.
    #[test]
    fn a_metrics_table_gets_no_bloom_filter_properties() {
        use crate::iceberg::schemas::TableSchema;

        assert!(bloom_filter_properties_for_table(&TableSchema::MetricsGauge, &[]).is_empty());
    }

    /// The writer wrote zstd level 1 while table metadata claimed level 3. Now
    /// that the writer honors the property, it must say what the files are, or
    /// every write silently moves to level 3.
    #[test]
    fn compression_properties_pin_the_level_the_files_are_written_at() {
        let properties = compression_properties();

        assert!(properties.contains(&(
            "write.parquet.compression-codec".to_string(),
            "zstd".to_string()
        )));
        assert!(properties.contains(&(
            "write.parquet.compression-level".to_string(),
            "1".to_string()
        )));
    }

    /// Bounds on a free-text column are dead weight in every manifest entry,
    /// so those columns opt down to counts. Counts stay: the planner uses them.
    #[test]
    fn metrics_properties_drop_bounds_for_free_text_columns() {
        let columns = vec![
            "timestamp".to_string(),
            "body".to_string(),
            "service_name".to_string(),
        ];
        let properties = metrics_properties_for_free_text_columns(&columns);

        assert_eq!(
            properties,
            vec![(
                "write.metadata.metrics.column.body".to_string(),
                "counts".to_string()
            )]
        );
    }

    /// A column the signal does not have must not produce a property; a
    /// property naming an absent column is noise in the table metadata.
    #[test]
    fn metrics_properties_skip_columns_the_table_lacks() {
        let traces = vec!["trace_id".to_string(), "status_message".to_string()];
        let properties = metrics_properties_for_free_text_columns(&traces);

        assert_eq!(
            properties,
            vec![(
                "write.metadata.metrics.column.status_message".to_string(),
                "counts".to_string()
            )]
        );
        assert!(metrics_properties_for_free_text_columns(&[]).is_empty());
    }

    /// Columns that queries actually prune on must keep their bounds — this is
    /// the whole reason the list is explicit rather than "every string column".
    #[test]
    fn metrics_properties_leave_prunable_columns_alone() {
        let columns = vec![
            "timestamp".to_string(),
            "service_name".to_string(),
            "trace_id".to_string(),
            "label_http_method".to_string(),
        ];
        assert!(metrics_properties_for_free_text_columns(&columns).is_empty());
    }

    #[test]
    fn materialized_column_name_sanitizes_and_prefixes() {
        assert_eq!(materialized_column_name("namespace"), "label_namespace");
        // Dots and other non-alphanumerics become underscores.
        assert_eq!(materialized_column_name("http.method"), "label_http_method");
        assert_eq!(
            materialized_column_name("k8s.pod/name"),
            "label_k8s_pod_name"
        );
    }

    #[test]
    fn attr_tokens_bloom_property_targets_the_list_leaf() {
        assert_eq!(
            bloom_filter_property_for_attr_tokens(),
            (
                "write.parquet.bloom-filter-enabled.column.attr_tokens.list.item".to_string(),
                "true".to_string()
            )
        );
    }

    #[test]
    fn trace_column_bloom_properties_target_flat_id_columns() {
        assert_eq!(
            bloom_filter_properties_for_trace_columns(),
            vec![
                (
                    "write.parquet.bloom-filter-enabled.column.trace_id".to_string(),
                    "true".to_string()
                ),
                (
                    "write.parquet.bloom-filter-fpp.column.trace_id".to_string(),
                    BLOOM_FILTER_TRACE_FPP.to_string()
                ),
                (
                    "write.parquet.bloom-filter-ndv.column.trace_id".to_string(),
                    BLOOM_FILTER_TRACE_ID_NDV.to_string()
                ),
                (
                    "write.parquet.bloom-filter-enabled.column.span_id".to_string(),
                    "true".to_string()
                ),
                (
                    "write.parquet.bloom-filter-fpp.column.span_id".to_string(),
                    BLOOM_FILTER_TRACE_FPP.to_string()
                ),
            ]
        );
    }

    #[test]
    fn bloom_filter_properties_target_materialized_columns() {
        let labels = vec![
            "namespace".to_string(),
            "http.method".to_string(),
            // Sanitizes to the same column as `http.method` → collapsed.
            "http_method".to_string(),
        ];
        assert_eq!(
            bloom_filter_properties_for_labels(&labels),
            vec![
                (
                    "write.parquet.bloom-filter-enabled.column.label_namespace".to_string(),
                    "true".to_string()
                ),
                (
                    "write.parquet.bloom-filter-enabled.column.label_http_method".to_string(),
                    "true".to_string()
                ),
            ]
        );
        assert!(bloom_filter_properties_for_labels(&[]).is_empty());
    }

    #[test]
    fn materialized_labels_default_is_empty_and_parses_from_toml() {
        // Default: no materialized labels for any signal.
        let cfg = SchemaConfig::default();
        assert!(cfg.materialized_labels.logs.is_empty());

        // A `[schema]` block with a materialized-labels table parses.
        let toml = r#"
catalog_type = "sql"
catalog_uri = "sqlite::memory:"
[materialized_labels]
logs = ["namespace", "pod"]
traces = ["http.method"]
"#;
        let parsed: SchemaConfig = toml::from_str(toml).expect("parse schema config");
        assert_eq!(parsed.materialized_labels.logs, vec!["namespace", "pod"]);
        assert_eq!(parsed.materialized_labels.traces, vec!["http.method"]);
        assert!(parsed.materialized_labels.metrics.is_empty());
    }

    #[tokio::test]
    async fn test_tenant_schema_registry_default() {
        let config = Configuration::default();
        let mut registry = TenantSchemaRegistry::new(config);

        // Should work with default tenant
        let catalog = registry.get_catalog_for_tenant("default").await;
        assert!(catalog.is_ok());

        // Should fail for unknown tenant
        let unknown_catalog = registry.get_catalog_for_tenant("unknown-tenant").await;
        assert!(unknown_catalog.is_err());

        // Default tenant should be "default"
        assert_eq!(registry.get_default_tenant(), "default");

        // Should return the default tenant
        let tenants = registry.get_configured_tenants();
        assert_eq!(tenants.len(), 1);
        assert_eq!(tenants[0], "default");

        // No custom schemas for default tenant
        assert!(registry.get_custom_schemas("default").is_none());
    }

    #[tokio::test]
    async fn test_tenant_schema_registry_with_custom_tenant() {
        let tenant_config = TenantSchemaConfig {
            schema: Some(SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: DefaultSchemas::default(),
                materialized_labels: Default::default(),
            }),
            custom_schemas: Some({
                let mut schemas = HashMap::new();
                schemas.insert("traces".to_string(), "custom_traces".to_string());
                schemas
            }),
            ..Default::default()
        };

        let mut tenants = HashMap::new();
        tenants.insert("test-tenant".to_string(), tenant_config);

        let config = Configuration {
            tenants: TenantsConfig {
                default_tenant: "test-tenant".to_string(),
                tenants,
            },
            ..Default::default()
        };

        let mut registry = TenantSchemaRegistry::new(config);

        // Should create catalog for configured tenant
        let catalog = registry.get_catalog_for_tenant("test-tenant").await;
        assert!(catalog.is_ok());

        // Should fail for unknown tenant
        let unknown_catalog = registry.get_catalog_for_tenant("unknown").await;
        assert!(unknown_catalog.is_err());

        // Should return custom schemas
        let custom_schemas = registry.get_custom_schemas("test-tenant");
        assert!(custom_schemas.is_some());
        assert_eq!(
            custom_schemas.unwrap().get("traces"),
            Some(&"custom_traces".to_string())
        );

        // Should cache catalogs
        let catalog2 = registry.get_catalog_for_tenant("test-tenant").await;
        assert!(catalog2.is_ok());

        // Should be the same instance (cached)
        assert!(Arc::ptr_eq(&catalog.unwrap(), &catalog2.unwrap()));

        // Should return configured tenants
        let tenants = registry.get_configured_tenants();
        assert_eq!(tenants.len(), 1);
        assert_eq!(tenants[0], "test-tenant");
    }

    #[tokio::test]
    async fn test_tenant_schema_registry_invalidation() {
        let tenant_config = TenantSchemaConfig {
            schema: Some(SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: DefaultSchemas::default(),
                materialized_labels: Default::default(),
            }),
            ..Default::default()
        };

        let mut tenants = HashMap::new();
        tenants.insert("test-tenant".to_string(), tenant_config);

        let config = Configuration {
            tenants: TenantsConfig {
                default_tenant: "test-tenant".to_string(),
                tenants,
            },
            ..Default::default()
        };

        let mut registry = TenantSchemaRegistry::new(config);

        // Create catalog
        let catalog1 = registry.get_catalog_for_tenant("test-tenant").await;
        assert!(catalog1.is_ok());

        // Invalidate cache
        registry.invalidate_tenant_catalog("test-tenant");

        // Create catalog again - should be a new instance
        let catalog2 = registry.get_catalog_for_tenant("test-tenant").await;
        assert!(catalog2.is_ok());

        // Should not be the same instance (cache was invalidated)
        assert!(!Arc::ptr_eq(&catalog1.unwrap(), &catalog2.unwrap()));
    }

    #[test]
    fn test_get_schema_definitions() {
        let config = Configuration::default();
        let registry = TenantSchemaRegistry::new(config);

        // Get schema definitions for the default tenant
        let schemas = registry.get_schema_definitions("default").unwrap();
        assert!(schemas.len() >= 5); // Should have at least traces, logs, and 3 metrics tables

        // Verify specific schemas exist
        assert!(schemas.contains_key("traces"));
        assert!(schemas.contains_key("logs"));
        assert!(schemas.contains_key("metrics_gauge"));
        assert!(schemas.contains_key("metrics_sum"));
        assert!(schemas.contains_key("metrics_histogram"));
    }

    #[test]
    fn test_get_partition_specifications() {
        let config = Configuration::default();
        let registry = TenantSchemaRegistry::new(config);

        // Get partition specifications for the default tenant
        let partition_specs = registry.get_partition_specifications("default").unwrap();
        assert!(partition_specs.len() >= 5); // Should have at least traces, logs, and 3 metrics tables

        // Verify specific partition specs exist
        assert!(partition_specs.contains_key("traces"));
        assert!(partition_specs.contains_key("logs"));
        assert!(partition_specs.contains_key("metrics_gauge"));
        assert!(partition_specs.contains_key("metrics_sum"));
        assert!(partition_specs.contains_key("metrics_histogram"));
    }

    #[test]
    fn test_schema_definitions_consistency() {
        let config = Configuration::default();
        let registry = TenantSchemaRegistry::new(config);

        // Schema definitions should be the same for all tenants
        let schemas1 = registry.get_schema_definitions("tenant1").unwrap();
        let schemas2 = registry.get_schema_definitions("tenant2").unwrap();

        assert_eq!(schemas1.len(), schemas2.len());

        // Verify all schema keys are the same
        let keys1: std::collections::BTreeSet<_> = schemas1.keys().collect();
        let keys2: std::collections::BTreeSet<_> = schemas2.keys().collect();
        assert_eq!(keys1, keys2);
    }

    #[tokio::test]
    async fn test_create_default_tables_for_tenant() {
        let mut config = Configuration::default();
        // A file-backed catalog: a named in-memory database lives only while a
        // connection to it is open, and the code under test builds and drops
        // its own pool. Holding a second manager open does not help — sqlx
        // pools are lazy and reap idle connections, so a held manager is not a
        // held connection.
        let temp_catalog = crate::testing::TempCatalog::new();
        config.schema.catalog_uri = temp_catalog.uri().to_string();
        config.auth.tenants = vec![crate::config::TenantConfig {
            id: "acme".to_string(),
            slug: "acme".to_string(),
            name: "Acme".to_string(),
            default_dataset: Some("production".to_string()),
            datasets: vec![],
            api_keys: vec![],
            schema_config: None,
            limits: None,
        }];
        let mut registry = TenantSchemaRegistry::new(config.clone());

        let manager = crate::CatalogManager::new(config).await.unwrap();

        registry
            .create_default_tables_for_tenant("acme")
            .await
            .expect("provisioning must succeed");

        // The tables are really there — this used to only log
        // "Would create table ...".
        let namespace = manager.build_namespace("acme", "production").unwrap();
        let tables = manager.catalog().list_tabulars(&namespace).await.unwrap();
        assert_eq!(tables.len(), 8, "{tables:?}");
    }

    /// Tenant isolation: `resolve_tenant_by_slug` matches config tenants
    /// first, so a database tenant whose id equals a *different* config
    /// tenant's slug resolves to that config tenant. Provisioning must not
    /// then create tables under one tenant's namespace using another
    /// tenant's dataset names.
    #[tokio::test]
    async fn create_default_tables_refuses_a_cross_tenant_slug_collision() {
        let mut config = Configuration::default();
        // A file-backed catalog: a named in-memory database lives only while a
        // connection to it is open, and the code under test builds and drops
        // its own pool. Holding a second manager open does not help — sqlx
        // pools are lazy and reap idle connections, so a held manager is not a
        // held connection.
        let temp_catalog = crate::testing::TempCatalog::new();
        config.schema.catalog_uri = temp_catalog.uri().to_string();
        // Config tenant `team-a` owns slug `shared`.
        config.auth.tenants = vec![crate::config::TenantConfig {
            id: "team-a".to_string(),
            slug: "shared".to_string(),
            name: "Team A".to_string(),
            default_dataset: Some("team-a-private".to_string()),
            datasets: vec![],
            api_keys: vec![],
            schema_config: None,
            limits: None,
        }];

        // A database-only tenant whose *id* is `shared`.
        let source = Arc::new(crate::catalog::Catalog::new_in_memory().await.unwrap());
        source
            .upsert_tenant("shared", "Shared", Some("shared-default"), "database")
            .await
            .unwrap();

        let mut registry =
            TenantSchemaRegistry::new(config.clone()).with_tenant_source(source.clone());

        let manager = crate::CatalogManager::new(config).await.unwrap();

        let err = registry
            .create_default_tables_for_tenant("shared")
            .await
            .expect_err("must refuse to provision across tenants");
        assert!(
            err.to_string().contains("different tenant"),
            "unexpected error: {err}"
        );

        // Nothing was created under either tenant's namespace.
        for (tenant, dataset) in [("shared", "team-a-private"), ("shared", "shared-default")] {
            let namespace = manager.build_namespace(tenant, dataset).unwrap();
            assert!(
                manager
                    .catalog()
                    .list_tabulars(&namespace)
                    .await
                    .unwrap_or_default()
                    .is_empty(),
                "{tenant}/{dataset} must hold no tables"
            );
        }
    }

    #[tokio::test]
    async fn create_default_tables_for_an_unregistered_tenant_errors() {
        // Reporting success without having created anything is not permitted.
        let mut registry = TenantSchemaRegistry::new(Configuration::default());
        assert!(
            registry
                .create_default_tables_for_tenant("nosuchtenant")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_list_tables_for_tenant_empty() {
        let config = Configuration::default();
        let mut registry = TenantSchemaRegistry::new(config);

        // List tables for a non-existent tenant should fail
        let result = registry.list_tables_for_tenant("non-existent-tenant").await;
        assert!(result.is_err());

        // List tables for the default tenant should work (even if empty)
        let tables = registry.list_tables_for_tenant("default").await.unwrap();
        assert_eq!(tables.len(), 0); // Should return empty list for default tenant with no tables
    }
}
