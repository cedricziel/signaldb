//! Manages Iceberg table lifecycle -- loading and creating tables.

use std::sync::Arc;

use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::table::Table;

use super::evolution;
use super::names;
use super::schemas;
use crate::schema::SCHEMA_DEFINITIONS;

/// Standard Iceberg property: delete aged-out metadata files after commit.
const DELETE_AFTER_COMMIT_KEY: &str = "write.metadata.delete-after-commit.enabled";
/// Standard Iceberg property: how many previous metadata files to retain.
const PREVIOUS_VERSIONS_MAX_KEY: &str = "write.metadata.previous-versions-max";

/// Manages the lifecycle of Iceberg tables.
///
/// Provides `ensure_table()` which loads an existing table or creates it
/// if it doesn't exist. Every call loads fresh metadata from the catalog:
/// a `Table` handle carries table metadata as of load time, so handing out
/// long-lived cached handles would hide snapshots committed by other
/// writers (issue #537). Callers that need a current view must call
/// `ensure_table` again rather than hold on to an old handle.
pub struct IcebergTableManager {
    catalog: Arc<dyn IcebergCatalog>,
    /// `write.metadata.previous-versions-max` applied to tables at creation.
    metadata_previous_versions_max: usize,
}

impl IcebergTableManager {
    /// Create from catalog reference with the metadata-retention window applied
    /// to newly-created tables.
    pub fn new(catalog: Arc<dyn IcebergCatalog>, metadata_previous_versions_max: usize) -> Self {
        Self {
            catalog,
            metadata_previous_versions_max,
        }
    }

    /// Commit the metadata pruning properties onto tables that lack them.
    ///
    /// Tables created before #895 have no `write.metadata.*` properties, so
    /// the catalog's delete-after-commit never fires for them and superseded
    /// metadata files accumulate forever (#959). Only absent keys are added
    /// -- operator-set values are never overwritten -- so this commits at
    /// most once per table and is a no-op afterwards. A failed commit (e.g.
    /// losing a CAS race to a concurrent writer) is logged and skipped: the
    /// loaded handle stays valid and the next `ensure_table` call retries.
    async fn backfill_metadata_pruning_properties(&self, table: &mut Table) {
        let properties = &table.metadata().properties;
        let mut missing = Vec::new();
        if !properties.contains_key(DELETE_AFTER_COMMIT_KEY) {
            missing.push((DELETE_AFTER_COMMIT_KEY.to_string(), "true".to_string()));
        }
        if !properties.contains_key(PREVIOUS_VERSIONS_MAX_KEY) {
            missing.push((
                PREVIOUS_VERSIONS_MAX_KEY.to_string(),
                self.metadata_previous_versions_max.to_string(),
            ));
        }
        if missing.is_empty() {
            return;
        }

        let ident = table.identifier().clone();
        if let Err(e) = table
            .new_transaction(None)
            .update_properties(missing)
            .commit()
            .await
        {
            tracing::warn!(
                error = %e,
                table = %ident,
                "Failed to backfill metadata pruning properties; will retry on next load"
            );
        } else {
            tracing::info!(
                table = %ident,
                "Backfilled metadata pruning properties on pre-existing table"
            );
        }
    }

    /// Bring `table_name`'s schema forward to its current `schemas.toml`
    /// version via [`evolution::ensure_schema_current`].
    ///
    /// Scoped to `traces` and `logs` only: those are the only signals whose
    /// physical schema is actually sourced from `schemas.toml` today.
    /// Metrics (all five representations) and profiles are hand-written in
    /// `iceberg::schemas` with no versioned definition to evolve against —
    /// see `openspec/changes/iceberg-schema-evolution`'s scope correction
    /// and `unified-table-schema`, which owns migrating them onto
    /// `schemas.toml`. A no-op for any other table name.
    async fn ensure_schema_evolved(&self, table_name: &str, ident: &Identifier) -> Result<()> {
        let (schemas_map, current_version) = match table_name {
            "traces" => (
                &SCHEMA_DEFINITIONS.traces,
                SCHEMA_DEFINITIONS.current_trace_version(),
            ),
            "logs" => (
                &SCHEMA_DEFINITIONS.logs,
                SCHEMA_DEFINITIONS.metadata.current_log_version.as_str(),
            ),
            _ => return Ok(()),
        };
        evolution::ensure_schema_current(
            self.catalog.clone(),
            ident,
            &SCHEMA_DEFINITIONS,
            schemas_map,
            current_version,
        )
        .await
        .map(|_| ())
    }

    /// Brings an already-existing table's metadata up to date: backfills
    /// pruning properties, evolves its schema to the current `schemas.toml`
    /// version, and reloads on success. Used both for the common
    /// load-existing path and for a lost create race (see [`Self::ensure_table`]),
    /// since a race winner's table is just as likely to predate this
    /// writer's schema expectations as one loaded on a later call.
    async fn reconcile_existing_table(
        &self,
        table_name: &str,
        ident: &Identifier,
        mut table: Table,
    ) -> Table {
        self.backfill_metadata_pruning_properties(&mut table).await;

        if let Err(e) = self.ensure_schema_evolved(table_name, ident).await {
            tracing::warn!(
                error = %e,
                table = %ident,
                "Failed to evolve table schema to current version; will retry on next load"
            );
        } else if let Ok(Tabular::Table(refreshed)) = self.catalog.clone().load_tabular(ident).await
        {
            table = refreshed;
        }

        table
    }

    /// Load an existing table or create it if it doesn't exist.
    ///
    /// This method:
    /// 1. Tries `load_tabular()` -- single catalog round-trip, fresh metadata
    /// 2. On NotFound -> creates the table with schema from `schemas::TableSchema`
    /// 3. Handles `AlreadyExists` gracefully (concurrent callers)
    pub async fn ensure_table(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
        table_name: &str,
        labels: &crate::config::MaterializedLabels,
    ) -> Result<Table> {
        let ident = names::build_table_identifier(tenant_slug, dataset_slug, table_name);
        if let Ok(tabular) = self.catalog.clone().load_tabular(&ident).await {
            let table = match tabular {
                Tabular::Table(table) => table,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Expected table but found different tabular type for {}",
                        ident
                    ));
                }
            };

            return Ok(self
                .reconcile_existing_table(table_name, &ident, table)
                .await);
        }

        let table_schema = match table_name {
            "traces" => schemas::TableSchema::Traces,
            "logs" => schemas::TableSchema::Logs,
            "metrics_gauge" => schemas::TableSchema::MetricsGauge,
            "metrics_sum" => schemas::TableSchema::MetricsSum,
            "metrics_histogram" => schemas::TableSchema::MetricsHistogram,
            "metrics_exponential_histogram" => schemas::TableSchema::MetricsExponentialHistogram,
            "metrics_summary" => schemas::TableSchema::MetricsSummary,
            "profiles" => schemas::TableSchema::Profiles,
            _ => return Err(anyhow::anyhow!("Unknown table name: {table_name}")),
        };

        // Ensure namespace exists before creating table
        let namespace = names::build_namespace(tenant_slug, dataset_slug)?;
        // Try to create namespace - idempotent, will succeed if already exists
        match self
            .catalog
            .clone()
            .create_namespace(&namespace, None)
            .await
        {
            Ok(_) => {
                // Namespace created successfully
            }
            Err(e) => {
                let message = e.to_string().to_lowercase();
                // Ignore "already exists" errors from concurrent creation attempts.
                // SQLite surfaces this as "unique constraint failed" rather than "already exists".
                if !message.contains("already exists")
                    && !message.contains("conflict")
                    && !message.contains("unique constraint")
                {
                    return Err(anyhow::anyhow!(
                        "Failed to create namespace {namespace}: {e}"
                    ));
                }
            }
        }

        // Enable a Parquet bloom filter for every materialized label column,
        // plus (for logs) the derived `attr_tokens` column and (for traces
        // and logs) the `trace_id`/`span_id` point-lookup columns. The
        // pinned iceberg-rust Parquet writer reads these standard Iceberg
        // properties from the table metadata on every write.
        let bloom_properties = crate::schema::bloom_filter_properties_for_table(
            &table_schema,
            table_schema.materialized_labels_of(labels),
        );

        // Drop the min/max bounds of the free-text columns. Bounds ride along
        // in every data file's manifest entry, and no query compares these
        // columns by range, so they are permanent cost for no pruning. Every
        // other column keeps iceberg-rust's default `truncate(16)`.
        let schema = table_schema.schema_with_labels(labels)?;
        let column_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect();
        let metrics_properties =
            crate::schema::metrics_properties_for_free_text_columns(&column_names);

        let mut builder = CreateTableBuilder::default();
        builder
            .with_name(table_name.to_string())
            .with_schema(schema)
            .with_partition_spec(table_schema.partition_spec()?)
            .with_location(names::build_table_location(
                tenant_slug,
                dataset_slug,
                table_name,
            ));
        // Bound accumulated metadata: keep a window of previous metadata files
        // and reclaim the rest on commit. Without this the catalog leaves one
        // orphaned `metadata.json` per commit, growing without bound under
        // continuous ingestion (#888). Honored by the SQL catalog's
        // delete-after-commit support (JanKaul/iceberg-rust#382).
        let mut properties: std::collections::HashMap<String, String> =
            bloom_properties.into_iter().collect();
        properties.extend(metrics_properties);
        properties.extend(crate::schema::compression_properties());
        properties.insert(DELETE_AFTER_COMMIT_KEY.to_string(), "true".to_string());
        properties.insert(
            PREVIOUS_VERSIONS_MAX_KEY.to_string(),
            self.metadata_previous_versions_max.to_string(),
        );
        // A table created fresh already IS the current version -- record it
        // now so `ensure_schema_evolved` never treats a brand-new table as
        // pre-dating this mechanism. Only for signals evolution actually
        // covers today (see `ensure_schema_evolved`'s doc comment).
        let current_version = match table_name {
            "traces" => Some(SCHEMA_DEFINITIONS.current_trace_version()),
            "logs" => Some(SCHEMA_DEFINITIONS.metadata.current_log_version.as_str()),
            _ => None,
        };
        if let Some(version) = current_version {
            properties.insert(
                evolution::SCHEMA_VERSION_PROPERTY.to_string(),
                version.to_string(),
            );
        }
        builder.with_properties(properties);
        let table_create = builder
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to build CreateTable for {ident}: {e}"))?;

        let table = match self
            .catalog
            .clone()
            .create_table(ident.clone(), table_create)
            .await
        {
            Ok(table) => table,
            Err(create_err) => {
                let message = create_err.to_string().to_lowercase();
                // SQLite surfaces concurrent duplicate-create as "unique constraint failed"
                // rather than "already exists", so treat both the same way.
                if message.contains("already exists") || message.contains("unique constraint") {
                    let tabular = self
                        .catalog
                        .clone()
                        .load_tabular(&ident)
                        .await
                        .map_err(|load_err| {
                        anyhow::anyhow!(
                            "Table {} already existed but reload failed: {}; original create error: {}",
                            ident,
                            load_err,
                            create_err
                        )
                    })?;

                    let table = match tabular {
                        Tabular::Table(table) => table,
                        _ => {
                            return Err(anyhow::anyhow!(
                                "Expected table but found different tabular type for {}",
                                ident
                            ));
                        }
                    };

                    // Lost the create race to another writer -- that writer's
                    // table may predate this one's schema (e.g. a pre-upgrade
                    // Writer). Reconcile it the same way the load-existing
                    // path above does, rather than handing back whatever
                    // schema happened to win the race.
                    self.reconcile_existing_table(table_name, &ident, table)
                        .await
                } else {
                    return Err(create_err.into());
                }
            }
        };

        Ok(table)
    }
}

/// Reconcile a table's `write.target-file-size-bytes` property with the
/// caller's compaction target, committing only when they differ.
///
/// The pinned iceberg-rust Parquet writer rolls output files on its own real
/// bytes-written feedback (JanKaul/iceberg-rust#388), but only against this
/// standard Iceberg property -- [`IcebergTableManager::ensure_table`] never
/// sets it, so every table rolls at the writer's unset-property fallback
/// (512 MiB) no matter what `[compactor].target_file_size_mb` says. Setting
/// it once at table creation would still drift the moment an operator
/// changes that config, so this reconciles it from the compaction path
/// itself, immediately before the write that depends on it, every cycle.
///
/// Same idiom as [`IcebergTableManager::backfill_metadata_pruning_properties`]:
/// a metadata-only `update_properties` commit, a no-op once the value
/// matches, and a failed commit (e.g. losing a CAS race to a concurrent
/// writer) is logged and skipped rather than failing the caller -- the
/// rewrite proceeds under whatever target the table already has, and the
/// next cycle retries the reconciliation.
pub async fn ensure_target_file_size_property(table: &mut Table, target_file_size_bytes: u64) {
    use iceberg_rust::spec::table_metadata::WRITE_TARGET_FILE_SIZE_BYTES;

    let desired = target_file_size_bytes.to_string();
    if table
        .metadata()
        .properties
        .get(WRITE_TARGET_FILE_SIZE_BYTES)
        == Some(&desired)
    {
        return;
    }

    let ident = table.identifier().clone();
    match table
        .new_transaction(None)
        .update_properties(vec![(WRITE_TARGET_FILE_SIZE_BYTES.to_string(), desired)])
        .commit()
        .await
    {
        Ok(()) => {
            tracing::info!(
                table = %ident,
                target_file_size_bytes,
                "Updated write.target-file-size-bytes to match the configured compaction target"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                table = %ident,
                target_file_size_bytes,
                "Failed to update write.target-file-size-bytes; compaction output will roll \
                 at the table's previous target this cycle"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CatalogManager;
    use crate::config::MaterializedLabels;
    use iceberg_rust::spec::schema::Schema;
    use iceberg_rust::spec::types::StructType;

    /// Creates a "traces" table pre-populated with the current
    /// `physical-v2` schema minus one field and no
    /// `signaldb.schema.version` property -- simulating a table that
    /// predates the schema-evolution mechanism.
    async fn create_stale_traces_table(catalog: &Arc<dyn IcebergCatalog>) -> anyhow::Result<()> {
        let resolved =
            SCHEMA_DEFINITIONS.resolve_trace_schema(SCHEMA_DEFINITIONS.current_trace_version())?;
        let full = resolved.to_iceberg_schema()?;
        let fields: Vec<_> = full
            .fields()
            .iter()
            .filter(|f| f.name != "span_kind")
            .cloned()
            .collect();
        let stale = Schema::from_struct_type(StructType::new(fields), 0, None);

        let namespace = names::build_namespace("evo_tenant", "evo_dataset")?;
        let _ = catalog.clone().create_namespace(&namespace, None).await;
        let identifier = names::build_table_identifier("evo_tenant", "evo_dataset", "traces");
        let create = CreateTableBuilder::default()
            .with_name("traces".to_string())
            .with_schema(stale)
            .with_location(names::build_table_location(
                "evo_tenant",
                "evo_dataset",
                "traces",
            ))
            .create()
            .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
        catalog.clone().create_table(identifier, create).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ensure_table_evolves_an_existing_table_behind_the_current_version()
    -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        create_stale_traces_table(&catalog).await?;

        let table_manager = IcebergTableManager::new(catalog.clone(), 5);
        let table = table_manager
            .ensure_table(
                "evo_tenant",
                "evo_dataset",
                "traces",
                &MaterializedLabels::default(),
            )
            .await?;

        let schema = table.current_schema()?;
        assert!(
            schema.fields().iter().any(|f| f.name == "span_kind"),
            "missing field should have been added by evolution"
        );
        assert_eq!(
            table
                .metadata()
                .properties
                .get(evolution::SCHEMA_VERSION_PROPERTY),
            Some(&SCHEMA_DEFINITIONS.current_trace_version().to_string()),
            "schema version property should be stamped to current after evolving"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_existing_table_evolves_a_table_handed_back_after_a_lost_create_race()
    -> anyhow::Result<()> {
        // `ensure_table`'s AlreadyExists branch (another writer won the
        // create race) hands its reloaded `Table` to the same
        // `reconcile_existing_table` helper the load-existing path uses --
        // exercise that helper directly against a stale table to prove a
        // race loser's handle gets evolved too, not returned as-is.
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        create_stale_traces_table(&catalog).await?;

        let table_manager = IcebergTableManager::new(catalog.clone(), 5);
        let ident = names::build_table_identifier("evo_tenant", "evo_dataset", "traces");
        let loaded = match catalog.clone().load_tabular(&ident).await? {
            Tabular::Table(table) => table,
            _ => panic!("expected a table"),
        };

        let table = table_manager
            .reconcile_existing_table("traces", &ident, loaded)
            .await;

        let schema = table.current_schema()?;
        assert!(
            schema.fields().iter().any(|f| f.name == "span_kind"),
            "missing field should have been added by evolution"
        );
        assert_eq!(
            table
                .metadata()
                .properties
                .get(evolution::SCHEMA_VERSION_PROPERTY),
            Some(&SCHEMA_DEFINITIONS.current_trace_version().to_string()),
            "schema version property should be stamped to current after evolving"
        );
        Ok(())
    }

    #[tokio::test]
    async fn ensure_table_on_a_table_already_current_does_not_error() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let table_manager = IcebergTableManager::new(catalog.clone(), 5);

        // First call creates the table fresh, at the current version.
        table_manager
            .ensure_table(
                "evo_tenant2",
                "evo_dataset2",
                "traces",
                &MaterializedLabels::default(),
            )
            .await?;

        // Second call hits the existing-table branch; evolution should be
        // a no-op (already at current) rather than erroring or looping.
        let table = table_manager
            .ensure_table(
                "evo_tenant2",
                "evo_dataset2",
                "traces",
                &MaterializedLabels::default(),
            )
            .await?;
        assert_eq!(
            table
                .metadata()
                .properties
                .get(evolution::SCHEMA_VERSION_PROPERTY),
            Some(&SCHEMA_DEFINITIONS.current_trace_version().to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn ensure_table_does_not_attempt_evolution_for_metrics_tables() -> anyhow::Result<()> {
        // Metrics tables are hand-written, not schemas.toml-sourced (see
        // `ensure_schema_evolved`'s doc comment) -- this must not panic or
        // error trying to resolve a schemas.toml version for them.
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let table_manager = IcebergTableManager::new(catalog.clone(), 5);

        table_manager
            .ensure_table(
                "evo_tenant3",
                "evo_dataset3",
                "metrics_gauge",
                &MaterializedLabels::default(),
            )
            .await?;
        let table = table_manager
            .ensure_table(
                "evo_tenant3",
                "evo_dataset3",
                "metrics_gauge",
                &MaterializedLabels::default(),
            )
            .await?;
        assert!(
            !table
                .metadata()
                .properties
                .contains_key(evolution::SCHEMA_VERSION_PROPERTY),
            "metrics tables are not versioned by this mechanism yet"
        );
        Ok(())
    }
}
