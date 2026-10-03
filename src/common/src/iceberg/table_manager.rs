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
use crate::schema::schema_parser::TableSchemaDefinition;
use crate::schema::typed_attributes;

/// Standard Iceberg property: delete aged-out metadata files after commit.
const DELETE_AFTER_COMMIT_KEY: &str = "write.metadata.delete-after-commit.enabled";
/// Standard Iceberg property: how many previous metadata files to retain.
const PREVIOUS_VERSIONS_MAX_KEY: &str = "write.metadata.previous-versions-max";

/// Parameters for creating a signal table fresh, bundled to keep
/// [`IcebergTableManager::create_fresh_table`]/[`IcebergTableManager::recreate_as_typed`]
/// under clippy's argument-count limit.
struct NewTableRequest<'a> {
    tenant_slug: &'a str,
    dataset_slug: &'a str,
    table_name: &'a str,
    labels: &'a crate::config::MaterializedLabels,
    /// Opts the table into the warm containment index when `Some` (task 4.3).
    warm_index: Option<crate::config::WarmIndexConfig>,
}

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
    /// Per-table-identifier mutex serializing [`Self::recreate_as_typed`]
    /// within this process, so two tasks racing the same identifier (e.g.
    /// a writer and the table reconciler, or a writer and the compactor in
    /// microservices mode, both loading the table while it is still
    /// legacy) can never interleave their drop-then-create. Keyed by the
    /// identifier's string form since [`Identifier`] itself isn't `Eq`+`Hash`
    /// in a form `DashMap` can use directly. Entries are never evicted --
    /// bounded by the number of distinct tables this process ever
    /// recreates, not by ongoing load.
    recreation_locks: dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Service catalog holding the advisory attribute statistics, cleared
    /// for a table [`Self::recreate_as_typed`] drops (#1826).
    stats_catalog: Option<Arc<crate::catalog::Catalog>>,
}

impl IcebergTableManager {
    /// Create from catalog reference with the metadata-retention window applied
    /// to newly-created tables.
    pub fn new(catalog: Arc<dyn IcebergCatalog>, metadata_previous_versions_max: usize) -> Self {
        Self {
            catalog,
            metadata_previous_versions_max,
            recreation_locks: dashmap::DashMap::new(),
            stats_catalog: None,
        }
    }

    /// Clear a recreated table's attribute statistics in `catalog`.
    pub fn with_stats_catalog(mut self, catalog: Arc<crate::catalog::Catalog>) -> Self {
        self.stats_catalog = Some(catalog);
        self
    }

    /// The mutex serializing [`Self::recreate_as_typed`] calls for `ident`
    /// within this process, created on first use.
    fn recreation_lock_for(&self, ident: &Identifier) -> Arc<tokio::sync::Mutex<()>> {
        self.recreation_locks
            .entry(ident.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
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

    /// Declare the canonical sort order on a table that does not already
    /// carry it.
    ///
    /// Tables created before the ordering contract landed have the reserved
    /// unsorted order as their default, so nothing they hold can ever be
    /// attributed an ordering. Declaring the order is metadata-only and says
    /// nothing about the files already in the table: a file is only claimed
    /// as ordered when its own manifest entry attests the order id, so old
    /// unsorted files stay unattributed and keep their explicit sorts.
    ///
    /// Idempotent: the order is resolved against the table's *current*
    /// schema (field ids differ per tenant because of materialized-label
    /// columns) and compared against the declared default, so this commits
    /// at most once per table and is a no-op afterwards. A failed commit —
    /// typically losing a CAS race to a concurrent writer — is logged and
    /// skipped; the loaded handle stays valid and the next `ensure_table`
    /// call retries.
    async fn backfill_sort_order(&self, table_name: &str, table: &mut Table) {
        let Some(table_schema) = schemas::TableSchema::from_table_name(table_name) else {
            return;
        };

        let current_schema = match table.metadata().current_schema() {
            Ok(schema) => schema.clone(),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    table = %table.identifier(),
                    "Cannot read current schema; skipping sort-order declaration"
                );
                return;
            }
        };

        let desired = match table_schema.sort_order_for(&current_schema) {
            Ok(Some(order)) => order,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    table = %table.identifier(),
                    "Cannot resolve the canonical sort order against this table's schema; \
                     leaving it undeclared"
                );
                return;
            }
        };

        if table
            .metadata()
            .default_sort_order()
            .is_ok_and(|declared| *declared == desired)
        {
            return;
        }

        let ident = table.identifier().clone();
        if let Err(e) = table
            .new_transaction(None)
            .replace_sort_order(desired)
            .commit()
            .await
        {
            tracing::warn!(
                error = %e,
                table = %ident,
                "Failed to declare the canonical sort order; will retry on next load"
            );
        } else {
            tracing::info!(
                table = %ident,
                "Declared the canonical sort order on a pre-existing table"
            );
        }
    }

    /// The `schemas.toml` schema map and current version for `table_name`,
    /// shared by [`Self::ensure_schema_evolved`] (which walks a table
    /// forward within that map) and [`Self::target_is_typed`] (which asks
    /// whether the destination of that walk is the typed layout). `None`
    /// for any table name `schemas.toml` doesn't source.
    fn schema_target_for(
        table_name: &str,
    ) -> Option<(
        &'static std::collections::HashMap<String, TableSchemaDefinition>,
        &'static str,
    )> {
        match table_name {
            "traces" => Some((
                &SCHEMA_DEFINITIONS.traces,
                SCHEMA_DEFINITIONS.current_trace_version(),
            )),
            "logs" => Some((
                &SCHEMA_DEFINITIONS.logs,
                SCHEMA_DEFINITIONS.metadata.current_log_version.as_str(),
            )),
            "profiles" => Some((
                &SCHEMA_DEFINITIONS.profiles,
                SCHEMA_DEFINITIONS.metadata.current_profile_version.as_str(),
            )),
            "metrics" => Some((&SCHEMA_DEFINITIONS.metrics, schemas::TYPED_METRIC_VERSION)),
            "metric_exemplars" => Some((
                &SCHEMA_DEFINITIONS.metric_exemplars,
                schemas::TYPED_METRIC_VERSION,
            )),
            _ => None,
        }
    }

    /// Whether `table_name`'s current `schemas.toml` version realizes the
    /// typed attribute layout (see `typed_attributes`) rather than the
    /// legacy single map/JSON column per container.
    ///
    /// `false` both for a table name `schemas.toml` doesn't source and for
    /// one whose current version genuinely predates the typed layout --
    /// either way there is nothing to cut over.
    fn target_is_typed(table_name: &str) -> Result<bool> {
        let Some((schemas_map, current_version)) = Self::schema_target_for(table_name) else {
            return Ok(false);
        };
        let resolved = SCHEMA_DEFINITIONS.resolve_table_schema(schemas_map, current_version)?;
        Ok(typed_attributes::is_typed_layout(
            resolved.fields.iter().map(|f| f.name.as_str()),
        ))
    }

    /// Bring `table_name`'s schema forward to its current `schemas.toml`
    /// version via [`evolution::ensure_schema_current`].
    ///
    /// Covers every `schemas.toml`-sourced signal: traces, logs, all five
    /// metrics representations, and profiles. A no-op for any other table
    /// name. Must never be reached for a legacy table whose target version
    /// is typed -- [`evolution::ensure_schema_current`] can only add/remove
    /// scalar columns, never the map/binary columns the typed layout needs
    /// (see [`Self::ensure_table`]'s recreate-as-typed gate, which
    /// intercepts that case first).
    async fn ensure_schema_evolved(&self, table_name: &str, ident: &Identifier) -> Result<()> {
        let Some((schemas_map, current_version)) = Self::schema_target_for(table_name) else {
            return Ok(());
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
        self.backfill_sort_order(table_name, &mut table).await;

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

    /// Drops `ident` and creates it fresh at the current (typed)
    /// `schemas.toml` version -- the one-shot cutover's data-loss step for a
    /// table still in the legacy `map<string,string>` layout. Intentional:
    /// pre-cutover data in this table is not migrated (breaking-changes
    /// policy). Logged once per recreated table. `request` is forwarded
    /// to [`Self::create_fresh_table`] unchanged, same as any other fresh
    /// creation, so an opted-in warm index survives the recreation.
    ///
    /// `expected_table_uuid` is the `table_uuid` of the legacy table the
    /// *caller* loaded and decided needed recreating. Two tasks can load
    /// the same identifier while it is still legacy and both reach this
    /// method (e.g. a writer and the table reconciler, or a writer and the
    /// compactor in microservices mode) -- without re-checking, the second
    /// caller would drop the table the first one just created and is
    /// already committing to, losing every write in between. This method
    /// closes that window three ways:
    ///
    /// 1. Serializes every call for `ident` within this process via
    ///    [`Self::recreation_lock_for`] -- the whole check-drop-create
    ///    sequence below runs under the lock, so a same-process racer
    ///    always observes the *other* racer's outcome before acting.
    /// 2. Immediately before dropping, reloads `ident` and re-checks: a
    ///    table that no longer exists or is already typed is never
    ///    touched -- the caller's stale legacy view is simply superseded.
    /// 3. Only drops when the freshly reloaded table is still legacy
    ///    *and* its `table_uuid` still equals `expected_table_uuid` -- the
    ///    literal table the caller observed, not a same-identifier
    ///    successor. A legacy table with a different uuid (a same-identity
    ///    table this call didn't expect) is left alone; a later
    ///    [`Self::ensure_table`] call retries recreation if it still needs
    ///    it.
    ///
    /// This closes every *within-process* race. A residual, much narrower
    /// window remains *across processes*: the reload-then-drop gap itself
    /// is not atomic (the underlying `Catalog::drop_table` has no
    /// compare-and-delete by uuid), so a different process could still
    /// drop+recreate `ident` in the instant between this call's reload and
    /// its own `drop_table`, and this call's `drop_table` would then remove
    /// that process's new typed table. Unlike the pre-reload window this
    /// replaces, that requires two processes to race the exact same
    /// still-legacy identifier within microseconds of each other, not
    /// merely within the same recreation cycle.
    async fn recreate_as_typed(
        &self,
        request: NewTableRequest<'_>,
        ident: &Identifier,
        expected_table_uuid: uuid::Uuid,
        from_version: Option<&str>,
    ) -> Result<Table> {
        let lock = self.recreation_lock_for(ident);
        let _guard = lock.lock().await;

        let reloaded = match self.catalog.clone().load_tabular(ident).await {
            Ok(Tabular::Table(table)) => Some(table),
            Ok(_) => {
                return Err(anyhow::anyhow!(
                    "Expected table but found different tabular type for {ident}"
                ));
            }
            Err(_) => None,
        };

        // Re-check under the lock before touching anything: a table that
        // vanished, was already cut over, or is a different table than the
        // one our caller observed is superseded, not ours to drop.
        match reloaded {
            None => {}
            Some(table) => {
                let already_typed = table.current_schema().is_ok_and(|schema| {
                    typed_attributes::is_typed_layout(
                        schema.fields().iter().map(|f| f.name.as_str()),
                    )
                });
                let same_table = table.metadata().table_uuid == expected_table_uuid;
                if already_typed || !same_table {
                    return Ok(self
                        .reconcile_existing_table(request.table_name, ident, table)
                        .await);
                }

                let to_version =
                    Self::schema_target_for(request.table_name).map(|(_, version)| version);
                tracing::warn!(
                    table = %ident,
                    from_version = from_version.unwrap_or("unrecorded"),
                    to_version = to_version.unwrap_or("unknown"),
                    "Recreating table in the typed attribute layout for the one-shot cutover; \
                     pre-cutover data in this table is dropped and not migrated"
                );

                if let Err(e) = self.catalog.drop_table(ident).await {
                    let message = e.to_string().to_lowercase();
                    let already_gone = message.contains("not found")
                        || message.contains("does not exist")
                        || message.contains("no such");
                    if !already_gone {
                        return Err(anyhow::anyhow!(
                            "Failed to drop legacy table {ident} for typed-layout recreation: {e}"
                        ));
                    }
                }
                self.clear_attribute_stats(&request, ident).await;
            }
        }

        self.create_fresh_table(request).await
    }

    /// Drop the advisory attribute statistics of a table that was just
    /// dropped, so discovery and promotion stop describing its data. Failures
    /// are logged: the statistics are advisory and must not fail recreation.
    async fn clear_attribute_stats(&self, request: &NewTableRequest<'_>, ident: &Identifier) {
        let Some(catalog) = &self.stats_catalog else {
            return;
        };
        let signal = crate::catalog::attribute_stats_signal(request.table_name);
        if let Err(e) = catalog
            .clear_attribute_stats(request.tenant_slug, request.dataset_slug, signal)
            .await
        {
            tracing::warn!(
                error = %e,
                table = %ident,
                "Failed to clear attribute statistics of a recreated table"
            );
        }
    }

    /// Load an existing table or create it if it doesn't exist.
    ///
    /// This method:
    /// 1. Tries `load_tabular()` -- single catalog round-trip, fresh metadata
    /// 2. On NotFound -> creates the table with schema from `schemas::TableSchema`
    /// 3. Handles `AlreadyExists` gracefully (concurrent callers)
    ///
    /// A loaded table still in the legacy layout, whose current
    /// `schemas.toml` version is typed, is dropped and recreated rather
    /// than reconciled (one-shot cutover; see [`Self::recreate_as_typed`]):
    /// [`evolution::ensure_schema_current`] can only add/remove scalar
    /// columns, so it can never carry a table from the legacy
    /// `map<string,string>` layout to the typed one.
    pub async fn ensure_table(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
        table_name: &str,
        labels: &crate::config::MaterializedLabels,
    ) -> Result<Table> {
        self.ensure_table_with_warm_index(tenant_slug, dataset_slug, table_name, labels, None)
            .await
    }

    /// Like [`Self::ensure_table`], additionally opting a brand-new table
    /// into the warm containment index when `warm_index` is `Some`. Only
    /// affects table *creation*: an already-existing table is loaded and
    /// reconciled exactly as [`Self::ensure_table`] does, since evolving a
    /// live table's derived columns is out of scope (task 4.3).
    pub async fn ensure_table_with_warm_index(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
        table_name: &str,
        labels: &crate::config::MaterializedLabels,
        warm_index: Option<crate::config::WarmIndexConfig>,
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

            let already_typed = table.current_schema().is_ok_and(|schema| {
                typed_attributes::is_typed_layout(schema.fields().iter().map(|f| f.name.as_str()))
            });
            if !already_typed && Self::target_is_typed(table_name)? {
                let from_version = table
                    .metadata()
                    .properties
                    .get(evolution::SCHEMA_VERSION_PROPERTY)
                    .map(String::as_str);
                let expected_table_uuid = table.metadata().table_uuid;
                let request = NewTableRequest {
                    tenant_slug,
                    dataset_slug,
                    table_name,
                    labels,
                    warm_index,
                };
                return self
                    .recreate_as_typed(request, &ident, expected_table_uuid, from_version)
                    .await;
            }

            return Ok(self
                .reconcile_existing_table(table_name, &ident, table)
                .await);
        }

        self.create_fresh_table(NewTableRequest {
            tenant_slug,
            dataset_slug,
            table_name,
            labels,
            warm_index,
        })
        .await
    }

    /// Creates `table_name` fresh at its current `schemas.toml` version:
    /// schema (with materialized labels), partitioning, sort order, bloom
    /// and metrics properties, and metadata-pruning properties. Shared by
    /// [`Self::ensure_table`]'s missing-table path and
    /// [`Self::recreate_as_typed`]'s post-drop recreation.
    ///
    /// A lost create race (`AlreadyExists`) reloads the winner's table and
    /// reconciles it, same as the load-existing path.
    async fn create_fresh_table(&self, request: NewTableRequest<'_>) -> Result<Table> {
        let NewTableRequest {
            tenant_slug,
            dataset_slug,
            table_name,
            labels,
            warm_index,
        } = request;
        let ident = names::build_table_identifier(tenant_slug, dataset_slug, table_name);
        let table_schema = schemas::TableSchema::from_table_name(table_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown table name: {table_name}"))?;

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

        // Drop the min/max bounds of the free-text columns. Bounds ride along
        // in every data file's manifest entry, and no query compares these
        // columns by range, so they are permanent cost for no pruning. Every
        // other column keeps iceberg-rust's default `truncate(16)`.
        let schema =
            table_schema.schema_with_labels_and_warm_index(labels, warm_index.is_some())?;
        let column_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect();

        // Enable a Parquet bloom filter for every materialized label column,
        // plus (for traces and logs) the `trace_id`/`span_id` point-lookup
        // columns. The pinned iceberg-rust Parquet writer reads these standard Iceberg
        // properties from the table metadata on every write. Read back from
        // `schema` itself (built just above) rather than independently
        // re-resolved from `labels`, so the two can never target different
        // columns under a collision (#1448).
        let bloom_properties =
            crate::schema::bloom_filter_properties_for_table(&table_schema, &schema);
        let metrics_properties =
            crate::schema::metrics_properties_for_free_text_columns(&column_names);

        // Declare the canonical per-signal sort order. Bound to this
        // schema's field ids, since materialized-label columns shift them
        // per tenant. Declaring it here is what lets producers attest their
        // files and the query engine trust the attestation.
        let sort_order = table_schema.sort_order_for(&schema)?;

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
        if let Some(sort_order) = sort_order {
            builder.with_sort_order(sort_order);
        }
        // Bound accumulated metadata: keep a window of previous metadata files
        // and reclaim the rest on commit. Without this the catalog leaves one
        // orphaned `metadata.json` per commit, growing without bound under
        // continuous ingestion (#888). Honored by the SQL catalog's
        // delete-after-commit support (JanKaul/iceberg-rust#382).
        let mut properties: std::collections::HashMap<String, String> =
            bloom_properties.into_iter().collect();
        properties.extend(metrics_properties);
        properties.extend(crate::schema::compression_properties());
        // Only set when the schema actually gained the column: a legacy
        // version silently drops the request (see `DerivedColumns::warm_index`).
        if let Some(cfg) = &warm_index
            && column_names
                .iter()
                .any(|name| name == crate::attrs::warm_index::WARM_INDEX_COLUMN)
        {
            properties.extend(crate::schema::warm_index_properties(cfg));
        }
        properties.insert(DELETE_AFTER_COMMIT_KEY.to_string(), "true".to_string());
        properties.insert(
            PREVIOUS_VERSIONS_MAX_KEY.to_string(),
            self.metadata_previous_versions_max.to_string(),
        );
        // A table created fresh already IS the current version -- record it
        // now so `ensure_schema_evolved` never treats a brand-new table as
        // pre-dating this mechanism. Only for signals evolution actually
        // covers today (see `ensure_schema_evolved`'s doc comment).
        let current_version = Self::schema_target_for(table_name).map(|(_, version)| version);
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

    #[test]
    fn schema_target_for_resolves_metrics_and_metric_exemplars_at_the_typed_version() {
        let (_, version) = IcebergTableManager::schema_target_for("metrics")
            .expect("metrics should be schemas.toml-sourced");
        assert_eq!(version, schemas::TYPED_METRIC_VERSION);

        let (_, version) = IcebergTableManager::schema_target_for("metric_exemplars")
            .expect("metric_exemplars should be schemas.toml-sourced");
        assert_eq!(version, schemas::TYPED_METRIC_VERSION);
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

    /// Creates `table_name` (must be a real table name `ensure_schema_evolved`
    /// dispatches on: "metrics_gauge", "profiles", etc.) directly at its real
    /// `physical-v1` shape -- map-typed attribute columns, the shape every
    /// live pre-#1340 table has -- with no `signaldb.schema.version`
    /// property, the same way [`create_stale_traces_table`] simulates a
    /// pre-mechanism traces table.
    async fn create_v1_table(
        catalog: &Arc<dyn IcebergCatalog>,
        schemas_map: &std::collections::HashMap<
            String,
            crate::schema::schema_parser::TableSchemaDefinition,
        >,
        tenant_slug: &str,
        dataset_slug: &str,
        table_name: &str,
    ) -> anyhow::Result<()> {
        let schema = SCHEMA_DEFINITIONS
            .resolve_table_schema(schemas_map, "physical-v1")?
            .to_iceberg_schema()?;

        let namespace = names::build_namespace(tenant_slug, dataset_slug)?;
        let _ = catalog.clone().create_namespace(&namespace, None).await;
        let identifier = names::build_table_identifier(tenant_slug, dataset_slug, table_name);
        let create = CreateTableBuilder::default()
            .with_name(table_name.to_string())
            .with_schema(schema)
            .with_location(names::build_table_location(
                tenant_slug,
                dataset_slug,
                table_name,
            ))
            .create()
            .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
        catalog.clone().create_table(identifier, create).await?;
        Ok(())
    }

    /// A `physical-v1` profiles table (legacy `map<string,string>`
    /// attributes) is behind a *typed* current version, so `ensure_table`
    /// cannot evolve it -- `diff_schema` doesn't support adding/removing map
    /// columns. It is dropped and recreated fresh instead
    /// (`recreate_as_typed`), landing directly on the current typed schema.
    #[tokio::test]
    async fn ensure_table_recreates_a_stale_v1_profiles_table_as_typed() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        create_v1_table(
            &catalog,
            &SCHEMA_DEFINITIONS.profiles,
            "evo_tenant4",
            "evo_dataset4",
            "profiles",
        )
        .await?;

        let table_manager = IcebergTableManager::new(catalog.clone(), 5);
        let table = table_manager
            .ensure_table(
                "evo_tenant4",
                "evo_dataset4",
                "profiles",
                &MaterializedLabels::default(),
            )
            .await?;

        let schema = table.current_schema()?;
        assert!(
            typed_attributes::is_typed_layout(schema.fields().iter().map(|f| f.name.as_str())),
            "recreated table should be in the typed attribute layout"
        );
        assert!(
            schema
                .fields()
                .iter()
                .any(|f| f.name == "resource_identity"),
            "resource_identity should be present on the recreated table"
        );
        assert_eq!(
            table
                .metadata()
                .properties
                .get(evolution::SCHEMA_VERSION_PROPERTY),
            Some(&SCHEMA_DEFINITIONS.metadata.current_profile_version),
            "schema version property should be stamped to current after recreation"
        );
        Ok(())
    }

    /// The declared default order, as the column names it names in key order.
    fn declared_key(table: &Table) -> anyhow::Result<Vec<String>> {
        let schema = table.current_schema()?;
        let order = table
            .metadata()
            .default_sort_order()
            .map_err(|e| anyhow::anyhow!("no default sort order: {e}"))?;
        Ok(order
            .fields
            .iter()
            .map(|field| {
                schema
                    .fields()
                    .iter()
                    .find(|f| f.id == field.source_id)
                    .map(|f| f.name.clone())
                    .unwrap_or_else(|| format!("<unknown field {}>", field.source_id))
            })
            .collect())
    }

    #[tokio::test]
    async fn a_new_signal_table_declares_its_canonical_sort_order() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let table_manager = IcebergTableManager::new(catalog.clone(), 5);

        for (table_name, expected) in [
            ("traces", vec!["timestamp", "trace_id"]),
            ("logs", vec!["timestamp", "service_name", "severity_text"]),
            ("metrics", vec!["timestamp", "metric_name", "service_name"]),
            ("profiles", vec!["timestamp", "service_name"]),
        ] {
            let table = table_manager
                .ensure_table(
                    "sort_tenant",
                    "sort_dataset",
                    table_name,
                    &MaterializedLabels::default(),
                )
                .await?;

            assert_eq!(
                table.metadata().default_sort_order_id,
                schemas::SIGNAL_SORT_ORDER_ID,
                "{table_name} must declare its order as the default, not the unsorted order"
            );
            assert_eq!(declared_key(&table)?, expected, "sort key of {table_name}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_pre_existing_table_gets_the_sort_order_declared_once() -> anyhow::Result<()> {
        // A table created before the ordering contract: same schema, but no
        // declared order at all.
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let namespace = names::build_namespace("legacy_tenant", "legacy_dataset")?;
        let _ = catalog.clone().create_namespace(&namespace, None).await;
        let ident = names::build_table_identifier("legacy_tenant", "legacy_dataset", "traces");
        let create = CreateTableBuilder::default()
            .with_name("traces".to_string())
            .with_schema(schemas::TableSchema::Traces.schema()?)
            .with_location(names::build_table_location(
                "legacy_tenant",
                "legacy_dataset",
                "traces",
            ))
            .create()
            .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
        let legacy = catalog.clone().create_table(ident.clone(), create).await?;
        assert!(
            legacy
                .metadata()
                .default_sort_order()
                .is_ok_and(|order| order.fields.is_empty()),
            "the legacy table must start out with no declared order"
        );

        let table_manager = IcebergTableManager::new(catalog.clone(), 5);
        let upgraded = table_manager
            .ensure_table(
                "legacy_tenant",
                "legacy_dataset",
                "traces",
                &MaterializedLabels::default(),
            )
            .await?;
        assert_eq!(declared_key(&upgraded)?, vec!["timestamp", "trace_id"]);
        // The SQL catalog appends one metadata-log entry per commit, so this
        // counts commits without depending on wall-clock timestamps.
        let commits_after_first = upgraded.metadata().metadata_log.len();

        // Idempotent: a second pass declares nothing further.
        let again = table_manager
            .ensure_table(
                "legacy_tenant",
                "legacy_dataset",
                "traces",
                &MaterializedLabels::default(),
            )
            .await?;
        assert_eq!(declared_key(&again)?, vec!["timestamp", "trace_id"]);
        assert_eq!(
            again.metadata().sort_orders.len(),
            2,
            "only the unsorted order and the declared one should exist"
        );
        assert_eq!(
            again.metadata().metadata_log.len(),
            commits_after_first,
            "a second reconcile must not commit the sort order again"
        );
        Ok(())
    }

    /// A traces table created explicitly at `physical-v4` (legacy
    /// `map<string,string>` attributes, the shape every table predates the
    /// typed-layout cutover has) is a genuinely different table -- not an
    /// evolved version of it -- after `ensure_table`: a new `table_uuid`,
    /// the typed layout, and the current (typed) version property.
    #[tokio::test]
    async fn ensure_table_drops_and_recreates_a_legacy_traces_table_as_typed() -> anyhow::Result<()>
    {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();

        let namespace = names::build_namespace("cutover_tenant", "cutover_dataset")?;
        let _ = catalog.clone().create_namespace(&namespace, None).await;
        let ident = names::build_table_identifier("cutover_tenant", "cutover_dataset", "traces");
        let legacy_schema = SCHEMA_DEFINITIONS
            .resolve_trace_schema("physical-v4")?
            .to_iceberg_schema()?;
        let create = CreateTableBuilder::default()
            .with_name("traces".to_string())
            .with_schema(legacy_schema)
            .with_location(names::build_table_location(
                "cutover_tenant",
                "cutover_dataset",
                "traces",
            ))
            .with_properties(std::collections::HashMap::from([(
                evolution::SCHEMA_VERSION_PROPERTY.to_string(),
                "physical-v4".to_string(),
            )]))
            .create()
            .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
        let legacy = catalog.clone().create_table(ident.clone(), create).await?;
        assert!(
            !typed_attributes::is_typed_layout(
                legacy
                    .current_schema()?
                    .fields()
                    .iter()
                    .map(|f| f.name.as_str())
            ),
            "the seeded table must start out in the legacy layout"
        );
        let legacy_uuid = legacy.metadata().table_uuid;

        let table_manager = IcebergTableManager::new(catalog.clone(), 5);
        let recreated = table_manager
            .ensure_table(
                "cutover_tenant",
                "cutover_dataset",
                "traces",
                &MaterializedLabels::default(),
            )
            .await?;

        assert_ne!(
            recreated.metadata().table_uuid,
            legacy_uuid,
            "recreation must produce a genuinely new table, not an evolved one"
        );
        let schema = recreated.current_schema()?;
        assert!(
            typed_attributes::is_typed_layout(schema.fields().iter().map(|f| f.name.as_str())),
            "recreated table should be in the typed attribute layout"
        );
        assert_eq!(
            recreated
                .metadata()
                .properties
                .get(evolution::SCHEMA_VERSION_PROPERTY),
            Some(&SCHEMA_DEFINITIONS.current_trace_version().to_string())
        );
        Ok(())
    }

    /// A table already in the typed layout is left alone by `ensure_table`
    /// -- same `table_uuid`, same `metadata_location` -- even though its
    /// current version is the typed one, which is what would otherwise
    /// trigger recreation.
    #[tokio::test]
    async fn ensure_table_never_drops_an_already_typed_table() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let table_manager = IcebergTableManager::new(catalog.clone(), 5);

        let created = table_manager
            .ensure_table(
                "typed_tenant",
                "typed_dataset",
                "traces",
                &MaterializedLabels::default(),
            )
            .await?;
        let created_uuid = created.metadata().table_uuid;
        let created_location = created.metadata().location.clone();

        let reconciled = table_manager
            .ensure_table(
                "typed_tenant",
                "typed_dataset",
                "traces",
                &MaterializedLabels::default(),
            )
            .await?;

        assert_eq!(
            reconciled.metadata().table_uuid,
            created_uuid,
            "an already-typed table must never be dropped and recreated"
        );
        assert_eq!(reconciled.metadata().location, created_location);
        Ok(())
    }

    /// A legacy table opted into the warm containment index recreates
    /// through the same [`IcebergTableManager::create_fresh_table`] path a
    /// brand-new table uses, so the recreated table carries the index
    /// column exactly as if it had never existed before -- the recreate
    /// path must not bypass warm-index opt-in.
    #[tokio::test]
    async fn ensure_table_with_warm_index_recreates_a_legacy_table_with_the_index_column()
    -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();

        let namespace = names::build_namespace("warm_tenant", "warm_dataset")?;
        let _ = catalog.clone().create_namespace(&namespace, None).await;
        let ident = names::build_table_identifier("warm_tenant", "warm_dataset", "traces");
        let legacy_schema = SCHEMA_DEFINITIONS
            .resolve_trace_schema("physical-v4")?
            .to_iceberg_schema()?;
        let create = CreateTableBuilder::default()
            .with_name("traces".to_string())
            .with_schema(legacy_schema)
            .with_location(names::build_table_location(
                "warm_tenant",
                "warm_dataset",
                "traces",
            ))
            .with_properties(std::collections::HashMap::from([(
                evolution::SCHEMA_VERSION_PROPERTY.to_string(),
                "physical-v4".to_string(),
            )]))
            .create()
            .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
        catalog.clone().create_table(ident.clone(), create).await?;

        let table_manager = IcebergTableManager::new(catalog.clone(), 5);
        let warm_index = crate::config::WarmIndexConfig {
            signals: vec![],
            datasets: None,
            fpp: 0.01,
            rows_per_row_group: 1_000,
            attrs_per_row: 8,
            max_bloom_ndv: 1_000_000,
        };
        let recreated = table_manager
            .ensure_table_with_warm_index(
                "warm_tenant",
                "warm_dataset",
                "traces",
                &MaterializedLabels::default(),
                Some(warm_index),
            )
            .await?;

        let schema = recreated.current_schema()?;
        assert!(
            typed_attributes::is_typed_layout(schema.fields().iter().map(|f| f.name.as_str())),
            "recreated table should be in the typed attribute layout"
        );
        assert!(
            schema
                .fields()
                .iter()
                .any(|f| f.name == crate::attrs::warm_index::WARM_INDEX_COLUMN),
            "recreated table should carry the warm-index column since it was opted in"
        );
        assert!(
            recreated
                .metadata()
                .properties
                .contains_key(crate::schema::WARM_INDEX_ENCODING_PROPERTY),
            "recreated table should carry the warm-index bloom properties"
        );
        Ok(())
    }

    /// Creates a legacy `traces` table at `physical-v4` for the given
    /// tenant/dataset, returning its identifier and `table_uuid` -- the pair
    /// a caller needs to exercise [`IcebergTableManager::recreate_as_typed`]
    /// directly, the way the race tests below do.
    async fn create_legacy_traces_table(
        catalog: &Arc<dyn IcebergCatalog>,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> anyhow::Result<(Identifier, uuid::Uuid)> {
        let namespace = names::build_namespace(tenant_slug, dataset_slug)?;
        let _ = catalog.clone().create_namespace(&namespace, None).await;
        let ident = names::build_table_identifier(tenant_slug, dataset_slug, "traces");
        let legacy_schema = SCHEMA_DEFINITIONS
            .resolve_trace_schema("physical-v4")?
            .to_iceberg_schema()?;
        let create = CreateTableBuilder::default()
            .with_name("traces".to_string())
            .with_schema(legacy_schema)
            .with_location(names::build_table_location(
                tenant_slug,
                dataset_slug,
                "traces",
            ))
            .with_properties(std::collections::HashMap::from([(
                evolution::SCHEMA_VERSION_PROPERTY.to_string(),
                "physical-v4".to_string(),
            )]))
            .create()
            .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
        let table = catalog.clone().create_table(ident.clone(), create).await?;
        Ok((ident, table.metadata().table_uuid))
    }

    /// Two callers racing `recreate_as_typed` against the same legacy table
    /// (e.g. a writer and the table reconciler both discovering the same
    /// stale table): neither call errors, and -- because the in-process
    /// lock in `recreate_as_typed` serializes them -- the second caller
    /// reloads under the lock, observes the first caller's typed table, and
    /// reconciles onto it rather than dropping it. Both callers therefore
    /// end up holding the exact same table (same `table_uuid`), not merely
    /// "a" typed table.
    #[tokio::test]
    async fn recreate_as_typed_is_idempotent_under_a_concurrent_recreation() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let (ident, legacy_uuid) =
            create_legacy_traces_table(&catalog, "race_tenant", "race_dataset").await?;

        let table_manager = IcebergTableManager::new(catalog.clone(), 5);
        let labels = MaterializedLabels::default();
        let request = || NewTableRequest {
            tenant_slug: "race_tenant",
            dataset_slug: "race_dataset",
            table_name: "traces",
            labels: &labels,
            warm_index: None,
        };
        let (first, second) = tokio::join!(
            table_manager.recreate_as_typed(request(), &ident, legacy_uuid, Some("physical-v4")),
            table_manager.recreate_as_typed(request(), &ident, legacy_uuid, Some("physical-v4")),
        );
        let first = first?;
        let second = second?;

        assert!(typed_attributes::is_typed_layout(
            first
                .current_schema()?
                .fields()
                .iter()
                .map(|f| f.name.as_str())
        ));
        assert_eq!(
            first.metadata().table_uuid,
            second.metadata().table_uuid,
            "the in-process lock must serialize the racers onto one table, not two"
        );
        assert_ne!(
            first.metadata().table_uuid,
            legacy_uuid,
            "the surviving table must be a genuinely new one, not the legacy table reused"
        );
        Ok(())
    }

    /// Recreating a table forgets its attribute statistics, so discovery
    /// stops listing fields of the dropped data (#1826). Statistics of other
    /// signals and the canonical attribute types are kept.
    #[tokio::test]
    async fn recreate_as_typed_clears_the_tables_attribute_statistics() -> anyhow::Result<()> {
        use crate::schema::logical::AttributeLevel;

        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let (ident, legacy_uuid) =
            create_legacy_traces_table(&catalog, "stats_tenant", "stats_dataset").await?;

        let stats = Arc::new(crate::catalog::Catalog::new("sqlite::memory:").await?);
        let (tenant, dataset) = ("stats_tenant", "stats_dataset");
        for signal in ["traces", "logs"] {
            stats
                .upsert_attribute_scan_stats(
                    tenant, dataset, signal, "busy_ns", 10, 10, 3, false, None,
                )
                .await?;
            stats
                .upsert_attribute_level_scan_stats(
                    tenant,
                    dataset,
                    signal,
                    AttributeLevel::Record,
                    "busy_ns",
                    10,
                    10,
                )
                .await?;
            stats
                .replace_attribute_value_stats(
                    tenant,
                    dataset,
                    signal,
                    "busy_ns",
                    &[("42".to_string(), 10)],
                )
                .await?;
        }

        let table_manager =
            IcebergTableManager::new(catalog.clone(), 5).with_stats_catalog(stats.clone());
        let labels = MaterializedLabels::default();
        let request = NewTableRequest {
            tenant_slug: tenant,
            dataset_slug: dataset,
            table_name: "traces",
            labels: &labels,
            warm_index: None,
        };
        table_manager
            .recreate_as_typed(request, &ident, legacy_uuid, Some("physical-v4"))
            .await?;

        assert!(
            stats
                .get_attribute_stats(tenant, dataset, "traces")
                .await?
                .is_empty()
        );
        assert!(
            stats
                .list_attribute_level_stats(tenant, dataset, "traces")
                .await?
                .is_empty()
        );
        assert!(
            stats
                .get_attribute_value_stats(tenant, dataset, "traces", "busy_ns", 10)
                .await?
                .is_empty()
        );
        assert_eq!(
            stats
                .get_attribute_stats(tenant, dataset, "logs")
                .await?
                .len(),
            1
        );
        Ok(())
    }

    /// The scenario the race is actually about: caller A recreates the
    /// table and commits real data to it; caller B still holds its
    /// *original* stale legacy load (recorded before A ever ran) and only
    /// now gets around to calling `recreate_as_typed`. B must not drop A's
    /// table -- the reload-and-recheck inside `recreate_as_typed` sees the
    /// table is already typed and reconciles instead, so A's committed
    /// data survives.
    #[tokio::test]
    async fn a_stale_caller_never_drops_a_typed_table_another_caller_already_committed_to()
    -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let (ident, legacy_uuid) =
            create_legacy_traces_table(&catalog, "commit_tenant", "commit_dataset").await?;

        let table_manager = IcebergTableManager::new(catalog.clone(), 5);
        let labels = MaterializedLabels::default();
        let request = || NewTableRequest {
            tenant_slug: "commit_tenant",
            dataset_slug: "commit_dataset",
            table_name: "traces",
            labels: &labels,
            warm_index: None,
        };

        // Caller A recreates the table...
        let recreated = table_manager
            .recreate_as_typed(request(), &ident, legacy_uuid, Some("physical-v4"))
            .await?;
        // ...and commits real data to it (stood in for by a metadata-only
        // property commit -- a genuine drop would erase this along with
        // any real data, which is exactly what this test guards against).
        let mut recreated = recreated;
        recreated
            .new_transaction(None)
            .update_properties(vec![(
                "committed-marker".to_string(),
                "a-wrote-this".to_string(),
            )])
            .commit()
            .await?;
        let committed_uuid = recreated.metadata().table_uuid;

        // Caller B still has `legacy_uuid` from *before* A ever ran (it
        // never re-observes the table in between) and only now calls
        // `recreate_as_typed`.
        let after_b = table_manager
            .recreate_as_typed(request(), &ident, legacy_uuid, Some("physical-v4"))
            .await?;

        assert_eq!(
            after_b.metadata().table_uuid,
            committed_uuid,
            "B must reconcile onto A's table, not drop and replace it"
        );
        assert_eq!(
            after_b.metadata().properties.get("committed-marker"),
            Some(&"a-wrote-this".to_string()),
            "A's committed data must survive B's call"
        );
        Ok(())
    }
}
