use crate::config::Configuration;
use crate::iceberg::{create_object_store_builder_from_config, create_sql_catalog_with_builder};
use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::spec::schema::Schema as IcebergSchema;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;

pub mod logical;
pub mod resource_identity;
pub mod schema_parser;
pub mod series_id;
pub mod type_authority;
pub mod typed_attributes;

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

/// The column name a per-level promoted attribute (`otel-native-schema`
/// layer 6) is stored under: `attr_<level>_<key>`. Unlike
/// [`materialized_column_name`], this is reversible for the common case — a
/// "clean" key (`^[a-z0-9]+([._][a-z0-9]+)*$`, ASCII lowercase/digits with
/// single `.`/`_` separators) round-trips through `.` → `_` and `_` → `__`,
/// so two clean keys never collide (a clean name never contains `___`, the
/// hashed form's separator). Anything else — mixed case, other punctuation,
/// or a clean name that would exceed 120 characters — falls back to a
/// lowercased, sanitized stem plus an 8-hex-digit FNV-1a hash of the exact
/// key bytes, so distinct keys stay distinct even when their stems collide.
pub fn promoted_attr_column(level: crate::schema::logical::AttributeLevel, key: &str) -> String {
    let prefix = format!("attr_{}_", level.as_str());
    if is_clean_attr_key(key) {
        let mut cleaned = String::with_capacity(key.len() * 2);
        for ch in key.chars() {
            match ch {
                '.' => cleaned.push('_'),
                '_' => cleaned.push_str("__"),
                other => cleaned.push(other),
            }
        }
        let name = format!("{prefix}{cleaned}");
        if name.len() <= 120 {
            return name;
        }
    }
    format!(
        "{prefix}{}___{}",
        attr_key_stem(key),
        fnv1a32_hex(key.as_bytes())
    )
}

/// Whether `key` matches `^[a-z0-9]+([._][a-z0-9]+)*$`: one or more
/// lowercase-ASCII-alphanumeric segments joined by single `.` or `_`
/// separators, with no leading/trailing/doubled separator.
fn is_clean_attr_key(key: &str) -> bool {
    !key.is_empty()
        && key.split(['.', '_']).all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}

/// The hashed fallback's human-readable stem: `key` lowercased, every
/// non-`[a-z0-9]` byte mapped to `_`, runs of `_` collapsed to one,
/// leading/trailing `_` trimmed, truncated to 64 bytes (then re-trimmed).
fn attr_key_stem(key: &str) -> String {
    let mut collapsed = String::with_capacity(key.len());
    let mut last_was_underscore = false;
    for ch in key.to_lowercase().chars() {
        let is_alnum = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        last_was_underscore = match (is_alnum, last_was_underscore) {
            (true, _) => {
                collapsed.push(ch);
                false
            }
            (false, false) => {
                collapsed.push('_');
                true
            }
            (false, true) => true,
        };
    }
    collapsed
        .trim_matches('_')
        .chars()
        .take(64)
        .collect::<String>()
        .trim_end_matches('_')
        .to_string()
}

/// FNV-1a, 32-bit, as 8 lowercase hex digits — implemented inline (not
/// pulled from a crate) so [`promoted_attr_column`]'s hashed fallback stays
/// stable across platforms and dependency versions.
fn fnv1a32_hex(bytes: &[u8]) -> String {
    let mut hash: u32 = 0x811c_9dc5;
    for &b in bytes {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{hash:08x}")
}

/// Stopgap for #1533: two distinct label keys can sanitize to the same
/// [`materialized_column_name`] (e.g. `http.method` and `http_method` both
/// map to `label_http_method`); the writer resolves the collision by
/// suffixing the later key's column (`label_http_method_2`). A resolver
/// that blindly matches `base` against the scanned schema would then
/// silently read the first key's column for the second key's queries.
/// Callers that pick a materialized column by name must check this first:
/// if a suffixed variant (`<base>_<n>`, `n` a positive integer) also exists
/// in the scanned schema, treat `base` as ambiguous and fall back to the
/// JSON/attribute-map extraction path instead of trusting the materialized
/// column.
pub fn has_colliding_materialized_variant<'a>(
    base: &str,
    columns: impl IntoIterator<Item = &'a str>,
) -> bool {
    let prefix = format!("{base}_");
    columns.into_iter().any(|column| {
        column
            .strip_prefix(prefix.as_str())
            .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
    })
}

/// Convenience wrapper around [`has_colliding_materialized_variant`] for the
/// common case: `columns` is the full set of materialized column names
/// found on the scanned table, and the caller only needs a yes/no "is
/// `base` present and safe to use" answer.
pub fn is_materialized_and_unambiguous(
    base: &str,
    columns: &std::collections::HashSet<String>,
) -> bool {
    columns.contains(base)
        && !has_colliding_materialized_variant(base, columns.iter().map(String::as_str))
}

/// Table property recording the warm index's token-encoding version, so a
/// reader can tell how to decode `attr_index` bytes without inferring it
/// from the column type alone.
pub const WARM_INDEX_ENCODING_PROPERTY: &str = "signaldb.warm-index.encoding";

/// The current [`WARM_INDEX_ENCODING_PROPERTY`] value.
pub const WARM_INDEX_ENCODING_VERSION: &str = "1";

/// Parquet bloom-filter and encoding table properties for the derived
/// [`crate::attrs::warm_index::WARM_INDEX_COLUMN`], sized from `cfg`.
///
/// NDV is derived, never measured: `spike/results.md` found the bloom filter
/// must be sized explicitly from `rows_per_row_group * attrs_per_row` (a
/// row group's expected count of distinct `key=value` tokens), capped at
/// `max_bloom_ndv` so a misconfigured row-group size cannot blow the filter
/// past a sane byte budget.
pub fn warm_index_properties(cfg: &crate::config::WarmIndexConfig) -> Vec<(String, String)> {
    use crate::attrs::warm_index::WARM_INDEX_COLUMN;
    use iceberg_rust::spec::table_metadata::{
        WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX,
        WRITE_PARQUET_BLOOM_FILTER_FPP_COLUMN_PREFIX, WRITE_PARQUET_BLOOM_FILTER_NDV_COLUMN_PREFIX,
    };

    let ndv = cfg
        .rows_per_row_group
        .saturating_mul(cfg.attrs_per_row)
        .min(cfg.max_bloom_ndv);
    let leaf = format!("{WARM_INDEX_COLUMN}.list.item");

    vec![
        (
            format!("{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}{leaf}"),
            "true".to_string(),
        ),
        (
            format!("{WRITE_PARQUET_BLOOM_FILTER_FPP_COLUMN_PREFIX}{leaf}"),
            cfg.fpp.to_string(),
        ),
        (
            format!("{WRITE_PARQUET_BLOOM_FILTER_NDV_COLUMN_PREFIX}{leaf}"),
            ndv.to_string(),
        ),
        (
            WARM_INDEX_ENCODING_PROPERTY.to_string(),
            WARM_INDEX_ENCODING_VERSION.to_string(),
        ),
    ]
}

/// The built-in traces columns that carry a Parquet bloom filter.
///
/// Both are flat top-level `Utf8` columns (`schemas.toml` traces.v1/v2), so
/// the property's column suffix is the bare column name rather than a
/// `.list.item` leaf path. `trace_id` is the high-cardinality column
/// single-trace lookups
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
/// columns, dispatching by table type and its already-built `schema`.
///
/// Every table type gets a filter for each materialized label; `Logs` and
/// `Traces` additionally get the `trace_id`/`span_id` point-lookup filters
/// ([`bloom_filter_properties_for_trace_columns`]) since `logs.v1` carries
/// those same columns (optional, but named identically) for
/// logs-for-a-trace correlation.
pub fn bloom_filter_properties_for_table(
    table_schema: &crate::iceberg::schemas::TableSchema,
    schema: &IcebergSchema,
) -> Vec<(String, String)> {
    let mut properties = bloom_filter_properties_for_labels(schema);

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

/// Per-column Parquet bloom-filter table properties for `schema`'s
/// materialized label columns.
///
/// For each column carrying a materialized-label `doc` (see
/// [`crate::iceberg::evolution::origin_key_of`]) this yields
/// `write.parquet.bloom-filter-enabled.column.label_<key> = "true"`, the
/// standard Iceberg property the pinned iceberg-rust Parquet writer honors
/// per column.
///
/// Reads the columns back from `schema` itself rather than independently
/// re-resolving them from a raw key list: `schema` is built by
/// [`crate::schema_parser::ResolvedSchema::build_iceberg_schema`], which
/// seeds its resolution from the table's own base fields (so a label
/// colliding with a base column is suffixed, not dropped). A caller with
/// only the key list, not the schema `build_iceberg_schema` actually
/// produced from it, cannot always reproduce that seeding and would risk
/// targeting a bloom filter at the wrong column under a base-column
/// collision (#1448) -- reading the columns back removes that divergence
/// entirely rather than keeping two resolutions in sync by convention.
///
/// Called at table creation only today (the compactor's attribute-promotion
/// path does not yet set bloom-filter properties for the columns it
/// evolves, tracked as #731).
pub fn bloom_filter_properties_for_labels(schema: &IcebergSchema) -> Vec<(String, String)> {
    use iceberg_rust::spec::table_metadata::WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX;

    schema
        .fields()
        .iter()
        .filter(|field| crate::iceberg::evolution::origin_key_of(field.doc.as_deref()).is_some())
        .map(|field| {
            (
                format!(
                    "{WRITE_PARQUET_BLOOM_FILTER_ENABLED_COLUMN_PREFIX}{}",
                    field.name
                ),
                "true".to_string(),
            )
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
    /// A long-lived manager to reuse instead of building one (and its
    /// connection pool) per call.
    catalog_manager: Option<Arc<crate::CatalogManager>>,
}

impl TenantSchemaRegistry {
    /// Create a new tenant schema registry
    pub fn new(config: Configuration) -> Self {
        Self {
            config,
            catalogs: HashMap::new(),
            tenant_source: None,
            catalog_manager: None,
        }
    }

    /// Attach a database catalog as an additional tenant source.
    pub fn with_tenant_source(mut self, tenant_source: Arc<crate::catalog::Catalog>) -> Self {
        self.tenant_source = Some(tenant_source);
        self
    }

    /// Reuse a shared `CatalogManager` rather than building one per call.
    /// It must already carry the tenant source, if any.
    pub fn with_catalog_manager(mut self, manager: Arc<crate::CatalogManager>) -> Self {
        self.catalog_manager = Some(manager);
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

    /// Build a `table_name -> T` map over every configured, non-custom table
    /// schema, via an accessor shared by [`Self::get_schema_definitions`] and
    /// [`Self::get_partition_specifications`] (custom schemas are skipped for
    /// both — TODO: parse them from JSON configuration).
    fn collect_table_schema_map<T>(
        &self,
        accessor: impl Fn(&iceberg_schemas::TableSchema) -> Result<T>,
    ) -> Result<HashMap<String, T>> {
        let mut out = HashMap::new();
        let default_schemas = &self.config.schema.default_schemas;

        for table_schema in iceberg_schemas::TableSchema::all_from_config(default_schemas) {
            if matches!(table_schema, iceberg_schemas::TableSchema::Custom(_)) {
                continue;
            }
            let value = accessor(&table_schema)?;
            out.insert(table_schema.table_name().to_string(), value);
        }

        Ok(out)
    }

    /// Get schema definitions for a tenant
    pub fn get_schema_definitions(
        &self,
        _tenant_id: &str,
    ) -> Result<HashMap<String, iceberg_rust::spec::schema::Schema>> {
        self.collect_table_schema_map(iceberg_schemas::TableSchema::schema)
    }

    /// Get partition specifications for a tenant
    pub fn get_partition_specifications(
        &self,
        _tenant_id: &str,
    ) -> Result<HashMap<String, iceberg_rust::spec::partition::PartitionSpec>> {
        self.collect_table_schema_map(iceberg_schemas::TableSchema::partition_spec)
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
    async fn catalog_manager(&self) -> Result<Arc<crate::CatalogManager>> {
        if let Some(manager) = &self.catalog_manager {
            return Ok(manager.clone());
        }
        let manager = crate::CatalogManager::new(self.config.clone()).await?;
        Ok(Arc::new(match &self.tenant_source {
            Some(source) => manager.with_tenant_source(source.clone()),
            None => manager,
        }))
    }

    /// List every table actually provisioned for a tenant, across all of its
    /// datasets, as one entry per dataset with the table names found in it.
    ///
    /// Every dataset the tenant is known to have is present — including one
    /// with nothing provisioned yet, listed with an empty table vector — so
    /// a caller can show "this dataset has no tables" rather than silently
    /// omitting it. Reads from the same place
    /// [`Self::create_default_tables_for_tenant`] writes to — the Iceberg
    /// catalog, per dataset namespace — so a listing agrees with what
    /// provisioning created. Never invents table entries: a dataset whose
    /// namespace has nothing in it (or does not exist yet) lists with an
    /// empty table vector, not as an error.
    ///
    /// A tenant that resolves only through the legacy
    /// [`Configuration::is_tenant_enabled`] fallback (the unconfigured
    /// `"default"` tenant, or one named only in `[tenants.tenants]`) but is
    /// absent from `datasets_for_tenant`'s registry has no known datasets to
    /// list yet, which is an empty result rather than an error. A tenant
    /// absent from *both* registries is genuinely unknown and stays an
    /// error.
    pub async fn list_tables_for_tenant(
        &mut self,
        tenant_id: &str,
    ) -> Result<Vec<(String, Vec<String>)>> {
        let manager = self.catalog_manager().await?;

        let datasets = match self.datasets_for_tenant(&manager, tenant_id).await {
            Ok(datasets) => datasets,
            Err(err) => {
                if self.config.is_tenant_enabled(tenant_id) {
                    Vec::new()
                } else {
                    return Err(err);
                }
            }
        };

        let mut result = Vec::with_capacity(datasets.len());
        for dataset in &datasets {
            let tables = match manager.build_namespace(tenant_id, dataset) {
                Ok(namespace) => manager
                    .catalog()
                    .list_tabulars(&namespace)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|identifier| identifier.name().to_string())
                    .collect(),
                Err(_) => Vec::new(),
            };
            result.push((dataset.clone(), tables));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SchemaConfig, TenantSchemaConfig, TenantsConfig};
    use crate::schema::logical::AttributeLevel;
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
    /// get the same filters.
    #[test]
    fn logs_and_traces_get_trace_columns() {
        use crate::iceberg::schemas::TableSchema;

        let logs = bloom_filter_properties_for_table(
            &TableSchema::Logs,
            &TableSchema::Logs.schema().unwrap(),
        );
        for column in BLOOM_FILTER_TRACE_COLUMNS {
            assert!(
                logs.contains(&(
                    format!("write.parquet.bloom-filter-enabled.column.{column}"),
                    "true".to_string()
                )),
                "logs must have a bloom filter on {column}"
            );
        }

        let traces = bloom_filter_properties_for_table(
            &TableSchema::Traces,
            &TableSchema::Traces.schema().unwrap(),
        );
        for column in BLOOM_FILTER_TRACE_COLUMNS {
            assert!(
                traces.contains(&(
                    format!("write.parquet.bloom-filter-enabled.column.{column}"),
                    "true".to_string()
                )),
                "traces must have a bloom filter on {column}"
            );
        }
    }

    /// A table type with no point-lookup id columns and no materialized
    /// labels gets no bloom filter properties at all.
    #[test]
    fn a_metrics_table_gets_no_bloom_filter_properties() {
        use crate::iceberg::schemas::TableSchema;

        assert!(
            bloom_filter_properties_for_table(
                &TableSchema::MetricsGauge,
                &TableSchema::MetricsGauge.schema().unwrap()
            )
            .is_empty()
        );
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
    fn colliding_materialized_variant_detected() {
        let columns = ["label_http_method", "label_http_method_2"];
        assert!(has_colliding_materialized_variant(
            "label_http_method",
            columns
        ));
    }

    #[test]
    fn no_colliding_materialized_variant_without_suffix() {
        let columns = ["label_http_method"];
        assert!(!has_colliding_materialized_variant(
            "label_http_method",
            columns
        ));
        // A different base sharing the prefix isn't a numeric suffix collision.
        let columns = ["label_http_method_status"];
        assert!(!has_colliding_materialized_variant(
            "label_http_method",
            columns
        ));
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
    fn promoted_attr_column_encodes_clean_keys_reversibly() {
        assert_eq!(
            promoted_attr_column(AttributeLevel::Record, "http.request.method"),
            "attr_record_http_request_method"
        );
        assert_eq!(
            promoted_attr_column(AttributeLevel::Record, "http.response.status_code"),
            "attr_record_http_response_status__code"
        );
        assert_eq!(
            promoted_attr_column(AttributeLevel::Record, "http_method"),
            "attr_record_http__method"
        );
    }

    #[test]
    fn promoted_attr_column_distinguishes_dot_and_underscore_spellings() {
        assert_ne!(
            promoted_attr_column(AttributeLevel::Record, "http.method"),
            promoted_attr_column(AttributeLevel::Record, "http_method")
        );
        // Neither is clean (an empty segment from the adjacent separators),
        // so both fall back to the hashed form — still distinct.
        assert_ne!(
            promoted_attr_column(AttributeLevel::Record, "a._b"),
            promoted_attr_column(AttributeLevel::Record, "a_.b")
        );
    }

    #[test]
    fn promoted_attr_column_hash_is_pinned() {
        assert_eq!(
            promoted_attr_column(AttributeLevel::Record, "MyApp-Version"),
            "attr_record_myapp_version___8241949b"
        );
    }

    #[test]
    fn promoted_attr_column_prefixes_by_level() {
        for (level, prefix) in [
            (AttributeLevel::Resource, "attr_resource_"),
            (AttributeLevel::Scope, "attr_scope_"),
            (AttributeLevel::Record, "attr_record_"),
        ] {
            assert!(
                promoted_attr_column(level, "k").starts_with(prefix),
                "level {level:?}"
            );
        }
    }

    #[test]
    fn promoted_attr_column_falls_back_when_the_clean_name_exceeds_120_chars() {
        let key = "a".repeat(130);
        let name = promoted_attr_column(AttributeLevel::Resource, &key);
        assert!(name.len() <= 120, "{name} ({} chars)", name.len());
        assert!(name.contains("___"), "{name}");
        assert!(name.starts_with("attr_resource_"), "{name}");
    }

    /// Builds the `Schema` [`bloom_filter_properties_for_labels`] reads back
    /// from -- a minimal base schema with `labels` appended the same way
    /// [`crate::schema_parser::ResolvedSchema::build_iceberg_schema`] does,
    /// so these tests exercise the real doc-tagging, not a hand-rolled
    /// stand-in for it.
    fn schema_with_labels(labels: &[String]) -> IcebergSchema {
        use crate::schema::schema_parser::{ResolvedField, ResolvedSchema};

        let base = ResolvedSchema {
            version: "test-only".to_string(),
            description: "fixture".to_string(),
            fields: vec![ResolvedField {
                name: "timestamp".to_string(),
                field_type: "timestamp_ns".to_string(),
                required: true,
                computed: None,
                physical_only: false,
                field_id: 1,
            }],
            partition_by: vec![],
        };
        base.to_iceberg_schema_with_labels(labels).unwrap()
    }

    #[test]
    fn bloom_filter_properties_target_the_schemas_actual_suffixed_column_under_a_base_collision() {
        // A label whose candidate name collides with a base column gets
        // suffixed at schema creation (#1448). The bloom filter must follow
        // that suffixed column -- reading it back from the schema itself,
        // rather than independently re-resolving from the raw key list
        // against an empty (base-blind) schema, is what guarantees this: an
        // empty-seeded resolution has no way to know the base collision
        // happened at all, and would target the wrong (unsuffixed) name.
        use crate::schema::schema_parser::{ResolvedField, ResolvedSchema};

        let base = ResolvedSchema {
            version: "test-only".to_string(),
            description: "fixture".to_string(),
            fields: vec![ResolvedField {
                name: "label_namespace".to_string(),
                field_type: "string".to_string(),
                required: false,
                computed: None,
                physical_only: false,
                field_id: 1,
            }],
            partition_by: vec![],
        };
        let labels = vec!["namespace".to_string()];
        let schema = base.to_iceberg_schema_with_labels(&labels).unwrap();

        assert_eq!(
            bloom_filter_properties_for_labels(&schema),
            vec![(
                "write.parquet.bloom-filter-enabled.column.label_namespace_2".to_string(),
                "true".to_string()
            )]
        );
    }

    #[test]
    fn bloom_filter_properties_for_labels_gives_colliding_keys_distinct_properties() {
        // `http.method` and `http_method` sanitize to the same candidate
        // column name; both must get their own bloom-filter property, not
        // just the first (#1448).
        let labels = vec!["http.method".to_string(), "http_method".to_string()];
        let properties = bloom_filter_properties_for_labels(&schema_with_labels(&labels));
        assert_eq!(
            properties,
            vec![
                (
                    "write.parquet.bloom-filter-enabled.column.label_http_method".to_string(),
                    "true".to_string()
                ),
                (
                    "write.parquet.bloom-filter-enabled.column.label_http_method_2".to_string(),
                    "true".to_string()
                ),
            ]
        );
    }

    #[test]
    fn warm_index_properties_are_sized_and_carry_the_encoding_property() {
        let cfg = crate::config::WarmIndexConfig {
            signals: vec![],
            datasets: None,
            fpp: 0.02,
            rows_per_row_group: 10_000,
            attrs_per_row: 16,
            max_bloom_ndv: 2_000_000,
        };
        assert_eq!(
            warm_index_properties(&cfg),
            vec![
                (
                    "write.parquet.bloom-filter-enabled.column.attr_index.list.item".to_string(),
                    "true".to_string()
                ),
                (
                    "write.parquet.bloom-filter-fpp.column.attr_index.list.item".to_string(),
                    "0.02".to_string()
                ),
                (
                    "write.parquet.bloom-filter-ndv.column.attr_index.list.item".to_string(),
                    "160000".to_string()
                ),
                (
                    WARM_INDEX_ENCODING_PROPERTY.to_string(),
                    WARM_INDEX_ENCODING_VERSION.to_string()
                ),
            ]
        );
    }

    #[test]
    fn warm_index_ndv_is_capped_by_max_bloom_ndv() {
        let cfg = crate::config::WarmIndexConfig {
            signals: vec![],
            datasets: None,
            fpp: 0.01,
            rows_per_row_group: 1_000_000,
            attrs_per_row: 1_000,
            max_bloom_ndv: 2_000_000,
        };
        let (_, ndv) = warm_index_properties(&cfg)
            .into_iter()
            .find(|(k, _)| k.contains("bloom-filter-ndv"))
            .expect("ndv property present");
        assert_eq!(ndv, "2000000");
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
            // Sanitizes to the same candidate name as `http.method` — gets
            // its own suffixed column and property, not collapsed (#1448).
            "http_method".to_string(),
        ];
        assert_eq!(
            bloom_filter_properties_for_labels(&schema_with_labels(&labels)),
            vec![
                (
                    "write.parquet.bloom-filter-enabled.column.label_http_method".to_string(),
                    "true".to_string()
                ),
                (
                    "write.parquet.bloom-filter-enabled.column.label_http_method_2".to_string(),
                    "true".to_string()
                ),
                (
                    "write.parquet.bloom-filter-enabled.column.label_namespace".to_string(),
                    "true".to_string()
                ),
            ]
        );
        assert!(bloom_filter_properties_for_labels(&schema_with_labels(&[])).is_empty());
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

    #[test]
    fn warm_index_config_default_is_off_and_parses_from_toml() {
        let cfg = SchemaConfig::default();
        assert!(cfg.warm_index.signals.is_empty());
        assert_eq!(cfg.warm_index.fpp, 0.01);
        assert_eq!(cfg.warm_index.rows_per_row_group, 10_000);
        assert_eq!(cfg.warm_index.attrs_per_row, 16);
        assert_eq!(cfg.warm_index.max_bloom_ndv, 2_000_000);

        let toml = r#"
catalog_type = "sql"
catalog_uri = "sqlite::memory:"
[warm_index]
signals = ["logs", "traces"]
datasets = ["prod"]
fpp = 0.02
rows_per_row_group = 5000
attrs_per_row = 8
max_bloom_ndv = 1000000
"#;
        let parsed: SchemaConfig = toml::from_str(toml).expect("parse schema config");
        assert_eq!(
            parsed.warm_index.signals,
            vec![
                crate::config::AttributeTypeSignal::Logs,
                crate::config::AttributeTypeSignal::Traces
            ]
        );
        assert_eq!(parsed.warm_index.datasets, Some(vec!["prod".to_string()]));
        assert_eq!(parsed.warm_index.fpp, 0.02);
        assert_eq!(parsed.warm_index.rows_per_row_group, 5000);
        assert_eq!(parsed.warm_index.attrs_per_row, 8);
        assert_eq!(parsed.warm_index.max_bloom_ndv, 1_000_000);
    }

    #[test]
    fn attribute_type_overrides_default_is_empty_and_parses_from_toml() {
        use crate::schema::logical::{AttributeLevel, LogicalFieldId};
        use crate::schema::type_authority::CanonicalType;

        let cfg = SchemaConfig::default();
        assert!(cfg.attribute_types.is_empty());

        let toml = r#"
catalog_type = "sql"
catalog_uri = "sqlite::memory:"
[[attribute_types]]
signal = "logs"
level = "record"
key = "retry.count"
type = "int64"

[[attribute_types]]
signal = "logs"
level = "record"
key = "retry.count"
type = "string"
dataset = "prod"
"#;
        let parsed: SchemaConfig = toml::from_str(toml).expect("parse schema config");
        assert_eq!(parsed.attribute_types.len(), 2);

        let field = LogicalFieldId {
            source: "logs".to_string(),
            level: Some(AttributeLevel::Record),
            name: "retry.count".to_string(),
        };

        // No dataset given: falls back to the global (no-dataset) entry.
        assert_eq!(
            parsed.attribute_type_override("staging", &field),
            Some(CanonicalType::Int64)
        );

        // A dataset-specific entry beats the entry with no dataset.
        assert_eq!(
            parsed.attribute_type_override("prod", &field),
            Some(CanonicalType::String)
        );
    }

    #[test]
    fn attribute_type_override_rejects_unknown_signal() {
        let toml = r#"
catalog_type = "sql"
catalog_uri = "sqlite::memory:"
[[attribute_types]]
signal = "spans"
level = "record"
key = "retry.count"
type = "int64"
"#;
        assert!(toml::from_str::<SchemaConfig>(toml).is_err());
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
                ..Default::default()
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
                ..Default::default()
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

    /// `list_tables_for_tenant` reads the same place `ensure_dataset_tables`
    /// writes to: a dataset with nothing provisioned lists nothing, and a
    /// provisioned dataset lists exactly its own tables tagged with its
    /// dataset id — the other dataset stays empty until it too is
    /// provisioned.
    #[tokio::test]
    async fn list_tables_for_tenant_groups_by_dataset_and_reflects_provisioning() {
        let mut config = Configuration::default();
        // A file-backed catalog: a named in-memory database lives only while
        // a connection to it is open, and the code under test builds and
        // drops its own pool.
        let temp_catalog = crate::testing::TempCatalog::new();
        config.schema.catalog_uri = temp_catalog.uri().to_string();
        config.auth.tenants = vec![crate::config::TenantConfig {
            id: "acme".to_string(),
            slug: "acme".to_string(),
            name: "Acme".to_string(),
            default_dataset: Some("alpha".to_string()),
            datasets: vec![
                crate::config::DatasetConfig {
                    id: "alpha".to_string(),
                    slug: "alpha".to_string(),
                    is_default: true,
                    storage: None,
                },
                crate::config::DatasetConfig {
                    id: "beta".to_string(),
                    slug: "beta".to_string(),
                    is_default: false,
                    storage: None,
                },
            ],
            api_keys: vec![],
            schema_config: None,
            limits: None,
        }];
        let mut registry = TenantSchemaRegistry::new(config.clone());
        let manager = crate::CatalogManager::new(config).await.unwrap();

        // Both known datasets are present even though nothing has been
        // provisioned yet — each with an empty table list, not omitted.
        let tables = registry.list_tables_for_tenant("acme").await.unwrap();
        let by_dataset: std::collections::BTreeMap<&str, &[String]> = tables
            .iter()
            .map(|(dataset, names)| (dataset.as_str(), names.as_slice()))
            .collect();
        assert_eq!(by_dataset.len(), 2, "{tables:?}");
        assert!(by_dataset["alpha"].is_empty());
        assert!(by_dataset["beta"].is_empty());

        // Provision "alpha" only.
        manager.ensure_dataset_tables("acme", "alpha").await;
        let tables = registry.list_tables_for_tenant("acme").await.unwrap();
        let by_dataset: std::collections::BTreeMap<&str, &[String]> = tables
            .iter()
            .map(|(dataset, names)| (dataset.as_str(), names.as_slice()))
            .collect();
        assert!(
            by_dataset["alpha"].iter().any(|name| name == "traces"),
            "'alpha' must be provisioned: {tables:?}"
        );
        assert!(
            by_dataset["beta"].is_empty(),
            "'beta' has not been provisioned yet: {tables:?}"
        );

        // Provision "beta" too; both datasets now hold tables.
        manager.ensure_dataset_tables("acme", "beta").await;
        let tables = registry.list_tables_for_tenant("acme").await.unwrap();
        let by_dataset: std::collections::BTreeMap<&str, &[String]> = tables
            .iter()
            .map(|(dataset, names)| (dataset.as_str(), names.as_slice()))
            .collect();
        assert!(by_dataset["alpha"].iter().any(|name| name == "traces"));
        assert!(by_dataset["beta"].iter().any(|name| name == "traces"));
    }
}
