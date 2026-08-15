//! # Iceberg schema evolution helpers
//!
//! Production schema-evolution path for attribute auto-promotion (epic
//! #737, #734): adding materialized `label_<key>` columns to an existing
//! table through the catalog's metadata commit path — `AddSchema` +
//! `SetCurrentSchema` via [`Catalog::update_table`] — without touching any
//! data files. Old files null-fill the new columns on read; the
//! rewrite-coupled promotion backfills them at the next compaction.
//!
//! Requires iceberg-rust rev >= 96f28c18: earlier revisions resolved
//! `current_schema` through the current snapshot's pinned schema id, so
//! the flip never took effect (JanKaul/iceberg-rust#378).

use anyhow::{Context, Result};
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::commit::{CommitTable, TableUpdate};
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};
use iceberg_rust::table::Table;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::schema::materialized_column_name;
use crate::schema::schema_parser::{
    ResolvedField, SchemaDefinitions, TableSchemaDefinition, version_chain,
};

/// The highest field id used anywhere in the schema tree: top-level
/// fields plus nested struct fields, list element ids, and map key/value
/// ids. New columns must continue from this id — nested ids are allocated
/// after the top-level ones (see `schema_parser`), so the top-level
/// maximum alone would collide with a map column's key/value ids.
fn max_field_id(schema: &Schema) -> i32 {
    fn walk(field_type: &Type, max: &mut i32) {
        match field_type {
            Type::Primitive(_) => {}
            Type::Struct(fields) => {
                for field in fields.iter() {
                    *max = (*max).max(field.id);
                    walk(&field.field_type, max);
                }
            }
            Type::List(list) => {
                *max = (*max).max(list.element_id);
                walk(&list.element, max);
            }
            Type::Map(map) => {
                *max = (*max).max(map.key_id).max(map.value_id);
                walk(&map.key, max);
                walk(&map.value, max);
            }
        }
    }
    let mut max = 0;
    for field in schema.fields().iter() {
        max = max.max(field.id);
        walk(&field.field_type, &mut max);
    }
    max
}

/// Load a table (never a view) from the catalog.
async fn load_table(catalog: &Arc<dyn Catalog>, identifier: &Identifier) -> Result<Table> {
    let tabular = catalog
        .clone()
        .load_tabular(identifier)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load table {identifier}: {e}"))?;
    match tabular {
        Tabular::Table(table) => Ok(table),
        _ => Err(anyhow::anyhow!("Expected table but got view: {identifier}")),
    }
}

/// Add one optional string `label_<key>` column per attribute key to the
/// table's current schema and make the evolved schema current.
///
/// Idempotent: keys whose materialized column (see
/// [`materialized_column_name`]) already exists are skipped, and when no
/// new columns remain the current schema is returned without a commit.
/// Field ids continue after the maximum id across the whole existing
/// schema tree (nested map/list ids included), and the new schema id is
/// one past the highest existing schema id.
///
/// The change is committed as `AddSchema` + `SetCurrentSchema` through
/// [`Catalog::update_table`], then the table is reloaded and the evolved
/// schema verified — the SQL catalog's compare-and-swap can silently lose
/// races, so a post-commit verification guards against dropped commits.
///
/// Returns the verified current schema (with the new columns).
pub async fn add_label_columns(
    catalog: Arc<dyn Catalog>,
    identifier: &Identifier,
    keys: &[String],
) -> Result<Schema> {
    let table = load_table(&catalog, identifier).await?;
    let current = table
        .current_schema()
        .map_err(|e| anyhow::anyhow!("Failed to resolve current schema of {identifier}: {e}"))?;

    // Resolve keys to column names, skipping columns that already exist
    // and collapsing duplicates (two keys can encode to the same column).
    let existing: Vec<&str> = current.fields().iter().map(|f| f.name.as_str()).collect();
    let mut new_columns: Vec<(String, String)> = Vec::new(); // (key, column)
    for key in keys {
        let column = materialized_column_name(key);
        if existing.iter().any(|name| *name == column)
            || new_columns.iter().any(|(_, c)| *c == column)
        {
            continue;
        }
        new_columns.push((key.clone(), column));
    }
    if new_columns.is_empty() {
        return Ok(current.clone());
    }

    // Build the evolved schema: current fields plus one optional string
    // field per new key, ids continuing after the true maximum across the
    // whole schema tree AND the metadata's `last_column_id` — a column
    // dropped by [`remove_label_columns`] no longer appears in the current
    // schema, but its id must never be reused (old data files still map
    // it to the old column's values).
    let metadata = table.metadata();
    let mut fields: Vec<StructField> = current.fields().iter().cloned().collect();
    let mut next_id = max_field_id(current).max(metadata.last_column_id) + 1;
    for (key, column) in &new_columns {
        fields.push(StructField {
            id: next_id,
            name: column.clone(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: Some(format!("Materialized attribute label '{key}'")),
            initial_default: None,
            write_default: None,
        });
        next_id += 1;
    }
    let last_column_id = next_id - 1;

    let new_schema_id = metadata
        .schemas
        .keys()
        .max()
        .copied()
        .unwrap_or(*current.schema_id())
        + 1;
    let evolved = Schema::from_struct_type(StructType::new(fields), new_schema_id, None);

    catalog
        .clone()
        .update_table(CommitTable {
            identifier: identifier.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::AddSchema {
                    schema: evolved,
                    last_column_id: Some(last_column_id),
                },
                TableUpdate::SetCurrentSchema {
                    schema_id: new_schema_id,
                },
            ],
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit schema evolution for {identifier}: {e}"))?;

    // Post-commit verification: reload and confirm the evolved schema is
    // current and carries every requested column.
    let reloaded = load_table(&catalog, identifier)
        .await
        .context("Failed to reload table for post-evolution verification")?;
    let verified = reloaded.current_schema().map_err(|e| {
        anyhow::anyhow!("Failed to resolve current schema after evolution of {identifier}: {e}")
    })?;
    for (key, column) in &new_columns {
        anyhow::ensure!(
            verified.fields().iter().any(|f| &f.name == column),
            "Schema evolution of {identifier} did not take effect: column {column} (key '{key}') \
             missing from current schema; a concurrent commit likely won the race"
        );
    }

    tracing::info!(
        table = %identifier,
        schema_id = *verified.schema_id(),
        columns = ?new_columns.iter().map(|(_, c)| c.as_str()).collect::<Vec<_>>(),
        "Added materialized label columns via schema evolution"
    );

    Ok(verified.clone())
}

/// Remove the materialized `label_<key>` columns of the given attribute
/// keys from the table's current schema and make the pruned schema
/// current (the demotion half of #734).
///
/// Idempotent: keys whose materialized column (see
/// [`materialized_column_name`]) is already absent are skipped, and when
/// nothing remains to drop the current schema is returned without a
/// commit. Only `label_<key>` columns can ever be named — the key ->
/// column encoding always carries the `label_` prefix, so base columns
/// are unreachable by construction. Field ids of dropped columns are
/// never reused: the metadata's `last_column_id` is left untouched
/// (`AddSchema` with `last_column_id: None`) and [`add_label_columns`]
/// allocates past it.
///
/// The change is committed as `AddSchema` + `SetCurrentSchema` through
/// [`Catalog::update_table`], then the table is reloaded and the pruned
/// schema verified, mirroring [`add_label_columns`]. The attribute values
/// themselves stay in the map-typed attributes column — dropping the
/// label column loses nothing; queries fall back to map matching.
///
/// Returns the verified current schema (without the dropped columns).
pub async fn remove_label_columns(
    catalog: Arc<dyn Catalog>,
    identifier: &Identifier,
    keys: &[String],
) -> Result<Schema> {
    let table = load_table(&catalog, identifier).await?;
    let current = table
        .current_schema()
        .map_err(|e| anyhow::anyhow!("Failed to resolve current schema of {identifier}: {e}"))?;

    // Resolve keys to column names, keeping only columns that exist and
    // collapsing duplicates (two keys can encode to the same column).
    let mut drop_columns: Vec<String> = Vec::new();
    for key in keys {
        let column = materialized_column_name(key);
        if current.fields().iter().any(|f| f.name == column) && !drop_columns.contains(&column) {
            drop_columns.push(column);
        }
    }
    if drop_columns.is_empty() {
        return Ok(current.clone());
    }

    // Build the pruned schema: current fields minus the dropped columns,
    // ids untouched, new schema id one past the highest existing one.
    let fields: Vec<StructField> = current
        .fields()
        .iter()
        .filter(|f| !drop_columns.contains(&f.name))
        .cloned()
        .collect();
    let metadata = table.metadata();
    let new_schema_id = metadata
        .schemas
        .keys()
        .max()
        .copied()
        .unwrap_or(*current.schema_id())
        + 1;
    let pruned = Schema::from_struct_type(StructType::new(fields), new_schema_id, None);

    catalog
        .clone()
        .update_table(CommitTable {
            identifier: identifier.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::AddSchema {
                    schema: pruned,
                    // Keep `last_column_id` as is: dropped ids must never
                    // be handed out again.
                    last_column_id: None,
                },
                TableUpdate::SetCurrentSchema {
                    schema_id: new_schema_id,
                },
            ],
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit schema demotion for {identifier}: {e}"))?;

    // Post-commit verification: reload and confirm the pruned schema is
    // current and every named column is gone.
    let reloaded = load_table(&catalog, identifier)
        .await
        .context("Failed to reload table for post-demotion verification")?;
    let verified = reloaded.current_schema().map_err(|e| {
        anyhow::anyhow!("Failed to resolve current schema after demotion of {identifier}: {e}")
    })?;
    for column in &drop_columns {
        anyhow::ensure!(
            !verified.fields().iter().any(|f| &f.name == column),
            "Schema demotion of {identifier} did not take effect: column {column} still present \
             in current schema; a concurrent commit likely won the race"
        );
    }

    tracing::info!(
        table = %identifier,
        schema_id = *verified.schema_id(),
        columns = ?drop_columns,
        "Removed materialized label columns via schema evolution"
    );

    Ok(verified.clone())
}

/// The table property carrying the `schemas.toml` version label a table
/// was last reconciled to. Lives on the table itself (not a separate
/// migrations store) so the recorded version and the table's actual
/// columns can never diverge independently of the table's own commit
/// history. See `openspec/changes/iceberg-schema-evolution/design.md`.
pub const SCHEMA_VERSION_PROPERTY: &str = "signaldb.schema.version";

/// Maps a `schemas.toml` field type string to its Iceberg primitive type.
/// `map<string,string>`/`list<struct>` are schemas.toml constructs used at
/// table *creation* (see `schema_parser::build_iceberg_schema`) but are not
/// yet supported as live-table add/remove operations -- nothing in the
/// traces/logs version chain needs that today.
fn iceberg_type_for(field_type: &str) -> Result<Type> {
    Ok(match field_type {
        "string" => Type::Primitive(PrimitiveType::String),
        "int32" => Type::Primitive(PrimitiveType::Int),
        "int64" | "uint64" => Type::Primitive(PrimitiveType::Long),
        "double" => Type::Primitive(PrimitiveType::Double),
        "boolean" => Type::Primitive(PrimitiveType::Boolean),
        "timestamp_ns" => Type::Primitive(PrimitiveType::Timestamp),
        "date" => Type::Primitive(PrimitiveType::Date),
        other => anyhow::bail!(
            "schema evolution: unsupported field type '{other}' for a live-table add/remove \
             (map<string,string>/list<struct> are not supported as evolution operations yet)"
        ),
    })
}

/// One version hop's additive/removal diff between a table's live schema
/// and a target field set, computed by NAME. `ResolvedSchema::to_iceberg_schema`
/// assigns field ids by position on every call, which is only safe for a
/// table being created fresh -- diffing against a live table must never
/// use it, only the field name/type set (see its doc comment).
#[derive(Debug, Default, PartialEq)]
pub struct SchemaDiff {
    pub additions: Vec<StructField>,
    pub removed_field_ids: Vec<i32>,
    /// Live fields being renamed in place: same field id, new name, so the
    /// existing Parquet data (written under that id) stays reachable
    /// through the renamed column instead of going invisible under a
    /// removed id while a same-typed field with a fresh id takes its place.
    pub renamed: Vec<StructField>,
}

impl SchemaDiff {
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.removed_field_ids.is_empty() && self.renamed.is_empty()
    }
}

/// Computes the additive/removal diff between `live` and `target`. New
/// fields get fresh ids starting at `next_id` (the caller computes this
/// the same way [`add_label_columns`] does: past both [`max_field_id`] and
/// the metadata's `last_column_id`, so a dropped column's id is never
/// reused); every untouched field keeps its existing id; removed fields
/// are identified by their existing id in `live`, never recomputed.
/// Additions are always nullable regardless of the target's declared
/// `required`, so historical rows never need a rewrite.
///
/// `allow_removals: false` skips the removal half entirely (returns only
/// additions) -- used when the live schema's relationship to `target` is
/// not actually known (see [`ensure_schema_current`]'s handling of a table
/// with no recorded version): a field present in `live` but absent from an
/// inferred `target` might be a field the table legitimately has that the
/// inferred baseline simply doesn't mention, not a field that needs
/// dropping. Removal is only safe when the caller trusts `live` really
/// matches a known prior version's exact field set.
///
/// `renames` is this hop's own `field_renames` (the version being migrated
/// *to* -- see `schema_parser::resolve_table_schema`, which applies a
/// version's renames to its parent's inherited fields). A rename whose
/// `from` name is actually present in `live` is resolved as a rename in
/// place (same field id, new name) rather than as a removal of `from` plus
/// a fresh-id addition of `to` -- the latter would orphan every historical
/// value written under `from`'s id, since Iceberg readers resolve columns
/// by id, not name. Only used for known-baseline hops; pass `&[]` when
/// `target` is an inferred baseline (`allow_removals: false`).
pub fn diff_schema(
    live: &Schema,
    next_id: i32,
    target: &[ResolvedField],
    renames: &[crate::schema::schema_parser::FieldRename],
    allow_removals: bool,
) -> Result<SchemaDiff> {
    let live_by_name: HashMap<&str, &StructField> =
        live.fields().iter().map(|f| (f.name.as_str(), f)).collect();
    let target_names: HashSet<&str> = target.iter().map(|f| f.name.as_str()).collect();

    // Only trust a rename whose source field actually exists live -- a hop
    // replayed on a table that already carries the renamed name (e.g. an
    // idempotent retry) must not touch it again.
    let renamed_from: HashMap<&str, &str> = renames
        .iter()
        .filter(|r| live_by_name.contains_key(r.from.as_str()))
        .map(|r| (r.from.as_str(), r.to.as_str()))
        .collect();
    let renamed_to: HashSet<&str> = renamed_from.values().copied().collect();

    let mut id = next_id;
    let mut additions = Vec::new();
    for field in target {
        if renamed_to.contains(field.name.as_str())
            || live_by_name.contains_key(field.name.as_str())
        {
            continue;
        }
        additions.push(StructField {
            id,
            name: field.name.clone(),
            required: false,
            field_type: iceberg_type_for(&field.field_type)?,
            doc: None,
            initial_default: None,
            write_default: None,
        });
        id += 1;
    }

    let renamed: Vec<StructField> = renamed_from
        .iter()
        .filter_map(|(from, to)| {
            live_by_name.get(from).map(|live_field| StructField {
                id: live_field.id,
                name: (*to).to_string(),
                required: live_field.required,
                field_type: live_field.field_type.clone(),
                doc: live_field.doc.clone(),
                initial_default: live_field.initial_default.clone(),
                write_default: live_field.write_default.clone(),
            })
        })
        .collect();

    let removed_field_ids = if allow_removals {
        live.fields()
            .iter()
            .filter(|f| {
                !target_names.contains(f.name.as_str())
                    && !renamed_from.contains_key(f.name.as_str())
            })
            .map(|f| f.id)
            .collect()
    } else {
        Vec::new()
    };

    Ok(SchemaDiff {
        additions,
        removed_field_ids,
        renamed,
    })
}

/// Brings `identifier`'s current schema to include exactly `target_fields`
/// (adding missing columns, dropping columns not in the target set) and
/// stamps [`SCHEMA_VERSION_PROPERTY`] to `target_version`, all in one
/// commit. Mirrors [`add_label_columns`]/[`remove_label_columns`]:
/// `AddSchema` + `SetCurrentSchema` + `SetProperties` through
/// [`Catalog::update_table`], then reload-and-verify. Idempotent: if the
/// table is already at `target_version` with no diff, no commit happens.
///
/// A lost race (another writer evolving the same table concurrently)
/// surfaces as a verification failure, logged and returned as an error --
/// the caller is expected to be a periodic reconciler that retries on its
/// next pass, the same pattern [`IcebergTableManager::backfill_metadata_pruning_properties`]
/// already uses for a failed commit.
///
/// [`IcebergTableManager::backfill_metadata_pruning_properties`]: crate::iceberg::table_manager::IcebergTableManager
///
/// `allow_removals` is forwarded to [`diff_schema`] -- pass `false` when
/// `target_fields` is an inferred baseline rather than a version the table
/// is known to have actually passed through (see
/// [`ensure_schema_current`]). `renames` is also forwarded to
/// [`diff_schema`] -- pass `&[]` alongside `allow_removals: false`.
pub async fn apply_schema_migration(
    catalog: Arc<dyn Catalog>,
    identifier: &Identifier,
    target_fields: &[ResolvedField],
    renames: &[crate::schema::schema_parser::FieldRename],
    target_version: &str,
    allow_removals: bool,
) -> Result<Schema> {
    let table = load_table(&catalog, identifier).await?;
    let current = table
        .current_schema()
        .map_err(|e| anyhow::anyhow!("Failed to resolve current schema of {identifier}: {e}"))?;
    let metadata = table.metadata();

    let next_id = max_field_id(current).max(metadata.last_column_id) + 1;
    let diff = diff_schema(current, next_id, target_fields, renames, allow_removals)?;
    let already_current = metadata
        .properties
        .get(SCHEMA_VERSION_PROPERTY)
        .map(|v| v == target_version)
        .unwrap_or(false);
    if diff.is_empty() && already_current {
        return Ok(current.clone());
    }

    let mut fields: Vec<StructField> = current
        .fields()
        .iter()
        .filter(|f| !diff.removed_field_ids.contains(&f.id))
        .map(|f| {
            diff.renamed
                .iter()
                .find(|r| r.id == f.id)
                .cloned()
                .unwrap_or_else(|| f.clone())
        })
        .collect();
    fields.extend(diff.additions.iter().cloned());
    let last_column_id = if diff.additions.is_empty() {
        None
    } else {
        Some(next_id + diff.additions.len() as i32 - 1)
    };

    let new_schema_id = metadata
        .schemas
        .keys()
        .max()
        .copied()
        .unwrap_or(*current.schema_id())
        + 1;
    let evolved = Schema::from_struct_type(StructType::new(fields), new_schema_id, None);

    let mut properties = HashMap::new();
    properties.insert(
        SCHEMA_VERSION_PROPERTY.to_string(),
        target_version.to_string(),
    );

    catalog
        .clone()
        .update_table(CommitTable {
            identifier: identifier.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::AddSchema {
                    schema: evolved,
                    last_column_id,
                },
                TableUpdate::SetCurrentSchema {
                    schema_id: new_schema_id,
                },
                TableUpdate::SetProperties {
                    updates: properties,
                },
            ],
        })
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to commit schema migration for {identifier} to {target_version}: {e}"
            )
        })?;

    let reloaded = load_table(&catalog, identifier)
        .await
        .context("Failed to reload table for post-migration verification")?;
    let verified = reloaded.current_schema().map_err(|e| {
        anyhow::anyhow!("Failed to resolve current schema after migration of {identifier}: {e}")
    })?;
    for addition in &diff.additions {
        anyhow::ensure!(
            verified.fields().iter().any(|f| f.name == addition.name),
            "Schema migration of {identifier} to {target_version} did not take effect: column \
             {} missing from current schema; a concurrent commit likely won the race",
            addition.name
        );
    }
    anyhow::ensure!(
        reloaded.metadata().properties.get(SCHEMA_VERSION_PROPERTY)
            == Some(&target_version.to_string()),
        "Schema migration of {identifier} committed but {SCHEMA_VERSION_PROPERTY} was not \
         updated to {target_version}; a concurrent commit likely won the race"
    );

    tracing::info!(
        table = %identifier,
        schema_id = *verified.schema_id(),
        version = target_version,
        additions = diff.additions.len(),
        removals = diff.removed_field_ids.len(),
        "Applied schema migration"
    );

    Ok(verified.clone())
}

/// Brings `identifier`'s schema forward to `current_version`.
///
/// When the table has a recorded [`SCHEMA_VERSION_PROPERTY`], this walks
/// every intervening hop one at a time via [`version_chain`], removals
/// included -- a recorded version is trusted to be the table's true prior
/// shape, so a hop's removals are safe to apply.
///
/// When the table has **no** recorded version (it predates this
/// mechanism), its true prior shape relative to any given hop is unknown:
/// hop-by-hop removal could destroy fields the table legitimately already
/// has but an early hop simply doesn't mention (see [`diff_schema`]'s
/// `allow_removals` doc). So this case skips hop-walking and instead
/// migrates directly to `current_version` in one step, additions only --
/// safe regardless of the table's real history, at the cost of never
/// removing a field for a table whose baseline was never recorded.
///
/// A table already at `current_version` is a no-op either way.
pub async fn ensure_schema_current(
    catalog: Arc<dyn Catalog>,
    identifier: &Identifier,
    defs: &SchemaDefinitions,
    schemas_map: &HashMap<String, TableSchemaDefinition>,
    current_version: &str,
) -> Result<Schema> {
    let table = load_table(&catalog, identifier).await?;
    let recorded = table
        .metadata()
        .properties
        .get(SCHEMA_VERSION_PROPERTY)
        .cloned();
    let mut last = table
        .current_schema()
        .map_err(|e| anyhow::anyhow!("Failed to resolve current schema of {identifier}: {e}"))?
        .clone();

    // Untrusted baseline: jump straight to current, additions only, never
    // renaming or removing -- used both for a table with no recorded
    // version at all and for one whose recorded version isn't actually on
    // this chain (see `version_chain`'s doc comment).
    let untrusted_baseline_migration = || async {
        let resolved = defs.resolve_table_schema(schemas_map, current_version)?;
        apply_schema_migration(
            catalog.clone(),
            identifier,
            &resolved.fields,
            &[],
            current_version,
            false,
        )
        .await
    };

    match recorded {
        None => {
            last = untrusted_baseline_migration().await?;
        }
        Some(from) => match version_chain(schemas_map, Some(from.as_str()), current_version)? {
            Some(hops) => {
                for version in hops {
                    let schema_def = schemas_map
                        .get(&version)
                        .ok_or_else(|| anyhow::anyhow!("Schema version {version} not found"))?;
                    let resolved = defs.resolve_table_schema(schemas_map, &version)?;
                    last = apply_schema_migration(
                        catalog.clone(),
                        identifier,
                        &resolved.fields,
                        &schema_def.field_renames,
                        &version,
                        true, // recorded baseline: trusted, safe to rename/remove
                    )
                    .await?;
                }
            }
            None => {
                last = untrusted_baseline_migration().await?;
            }
        },
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CatalogManager;
    use iceberg_rust::catalog::create::CreateTableBuilder;
    use iceberg_rust::spec::partition::PartitionSpec;
    use iceberg_rust::spec::types::MapType;

    fn string_field(id: i32, name: &str, required: bool) -> StructField {
        StructField {
            id,
            name: name.to_string(),
            required,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    /// Base schema mirroring the real tables' shape: top-level ids 1..=3
    /// with a map-typed attributes column whose key/value ids (4, 5) are
    /// allocated AFTER the top-level ids, as `schema_parser` does.
    fn base_schema_with_map() -> Schema {
        let timestamp = StructField {
            id: 1,
            name: "timestamp".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Timestamp),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        let attributes = StructField {
            id: 3,
            name: "attributes".to_string(),
            required: false,
            field_type: Type::Map(MapType {
                key_id: 4,
                key: Box::new(Type::Primitive(PrimitiveType::String)),
                value_id: 5,
                value_required: false,
                value: Box::new(Type::Primitive(PrimitiveType::String)),
            }),
            doc: None,
            initial_default: None,
            write_default: None,
        };
        Schema::from_struct_type(
            StructType::new(vec![timestamp, string_field(2, "body", false), attributes]),
            0,
            None,
        )
    }

    async fn create_test_table(
        catalog: &Arc<dyn Catalog>,
        table_name: &str,
    ) -> anyhow::Result<Identifier> {
        let namespace = crate::iceberg::names::build_namespace("evo", "test")?;
        // Namespace may already exist when several tables share it.
        let _ = catalog.clone().create_namespace(&namespace, None).await;
        let identifier = crate::iceberg::names::build_table_identifier("evo", "test", table_name);
        let create = CreateTableBuilder::default()
            .with_name(table_name.to_string())
            .with_schema(base_schema_with_map())
            .with_partition_spec(PartitionSpec::default())
            .with_location(crate::iceberg::names::build_table_location(
                "evo", "test", table_name,
            ))
            .create()
            .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
        catalog
            .clone()
            .create_table(identifier.clone(), create)
            .await?;
        Ok(identifier)
    }

    fn field<'a>(schema: &'a Schema, name: &str) -> Option<&'a StructField> {
        schema.fields().iter().find(|f| f.name == name)
    }

    #[tokio::test]
    async fn adds_label_columns_with_ids_after_nested_map_ids() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        let schema = add_label_columns(
            catalog.clone(),
            &identifier,
            &["env".to_string(), "http.method".to_string()],
        )
        .await?;

        // Both columns present, optional strings.
        let env = field(&schema, "label_env").expect("label_env missing");
        let method = field(&schema, "label_http_method").expect("label_http_method missing");
        assert!(!env.required);
        assert_eq!(env.field_type, Type::Primitive(PrimitiveType::String));

        // Field ids continue after the map's nested key/value ids (4, 5),
        // not after the top-level maximum (3).
        assert_eq!(env.id, 6);
        assert_eq!(method.id, 7);

        // The evolved schema is current in the catalog.
        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(table.metadata().current_schema_id, 1);
        assert_eq!(table.metadata().schemas.len(), 2);
        assert_eq!(table.metadata().last_column_id, 7);
        assert!(field(table.current_schema()?, "label_env").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn rerun_is_idempotent_and_commits_nothing() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        let first = add_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;
        let second = add_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;
        assert_eq!(first, second, "re-run must return the same schema");

        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(
            table.metadata().schemas.len(),
            2,
            "idempotent re-run must not add another schema"
        );
        assert_eq!(
            table
                .current_schema()?
                .fields()
                .iter()
                .filter(|f| f.name == "label_env")
                .count(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn partial_overlap_adds_only_missing_columns() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        add_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;
        let schema = add_label_columns(
            catalog.clone(),
            &identifier,
            &["env".to_string(), "region".to_string()],
        )
        .await?;

        let env = field(&schema, "label_env").expect("label_env missing");
        let region = field(&schema, "label_region").expect("label_region missing");
        assert_eq!(env.id, 6, "existing column keeps its id");
        assert_eq!(region.id, 7, "new column continues the id sequence");

        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(table.metadata().current_schema_id, 2);
        assert_eq!(table.metadata().schemas.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_keys_collapse_to_one_column() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        // `http.method` and `http_method` encode to the same column name.
        let schema = add_label_columns(
            catalog.clone(),
            &identifier,
            &["http.method".to_string(), "http_method".to_string()],
        )
        .await?;
        assert_eq!(
            schema
                .fields()
                .iter()
                .filter(|f| f.name == "label_http_method")
                .count(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn removes_label_columns_and_flips_current_schema() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        add_label_columns(
            catalog.clone(),
            &identifier,
            &["env".to_string(), "pod".to_string()],
        )
        .await?;
        let schema =
            remove_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;

        assert!(field(&schema, "label_env").is_none(), "label_env dropped");
        assert!(field(&schema, "label_pod").is_some(), "label_pod kept");
        assert!(field(&schema, "attributes").is_some(), "map column kept");

        // The pruned schema is current in the catalog and the column id
        // budget did not regress.
        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(table.metadata().current_schema_id, 2);
        assert_eq!(table.metadata().schemas.len(), 3);
        assert_eq!(table.metadata().last_column_id, 7);
        assert!(field(table.current_schema()?, "label_env").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn remove_rerun_is_idempotent_and_commits_nothing() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        add_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;
        let first =
            remove_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;
        let second =
            remove_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;
        assert_eq!(first, second, "re-run must return the same schema");

        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(
            table.metadata().schemas.len(),
            3,
            "idempotent re-run must not add another schema"
        );
        Ok(())
    }

    #[tokio::test]
    async fn remove_of_absent_columns_is_a_no_op() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        // No label columns exist yet: nothing to remove, no commit. The
        // key -> column encoding also means base columns can never be
        // named for removal ("body" maps to "label_body").
        let schema = remove_label_columns(
            catalog.clone(),
            &identifier,
            &["env".to_string(), "body".to_string()],
        )
        .await?;
        assert!(field(&schema, "body").is_some());

        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(table.metadata().schemas.len(), 1, "no commit happened");
        Ok(())
    }

    #[tokio::test]
    async fn readd_after_remove_assigns_a_fresh_field_id() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        add_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;
        remove_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;
        let schema = add_label_columns(catalog.clone(), &identifier, &["env".to_string()]).await?;

        // Iceberg field ids must never be reused: the re-added column gets
        // a new id past the previous maximum (6 was the original).
        let env = field(&schema, "label_env").expect("label_env re-added");
        assert_eq!(env.id, 7);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_double_call_leaves_consistent_schema() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_test_table(&catalog, "events").await?;

        let keys = vec!["env".to_string()];
        let (a, b) = tokio::join!(
            add_label_columns(catalog.clone(), &identifier, &keys),
            add_label_columns(catalog.clone(), &identifier, &keys),
        );
        assert!(
            a.is_ok() || b.is_ok(),
            "at least one concurrent evolution must succeed: {a:?} / {b:?}"
        );

        let table = load_table(&catalog, &identifier).await?;
        let current = table.current_schema()?;
        assert_eq!(
            current
                .fields()
                .iter()
                .filter(|f| f.name == "label_env")
                .count(),
            1,
            "concurrent evolution must not duplicate the column"
        );
        // No duplicate field ids anywhere in the final schema.
        let mut ids: Vec<i32> = current.fields().iter().map(|f| f.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), current.fields().iter().count());
        Ok(())
    }

    fn resolved_field(name: &str, field_type: &str) -> ResolvedField {
        ResolvedField {
            name: name.to_string(),
            field_type: field_type.to_string(),
            required: true, // must be forced nullable by the diff regardless
            computed: None,
            physical_only: false,
            field_id: 0, // never trusted by diff_schema -- see its doc comment
        }
    }

    #[test]
    fn diff_schema_adds_one_field_with_id_past_the_given_next_id() {
        let live = base_schema_with_map(); // ids 1..=5 (map key/value at 4,5)
        let target = vec![
            resolved_field("timestamp", "timestamp_ns"),
            resolved_field("body", "string"),
            resolved_field("attributes", "map<string,string>"), // unaffected: already present
            resolved_field("new_column", "int32"),
        ];

        let diff = diff_schema(&live, 6, &target, &[], true).unwrap();

        assert_eq!(diff.additions.len(), 1);
        assert_eq!(diff.additions[0].name, "new_column");
        assert_eq!(diff.additions[0].id, 6);
        assert!(!diff.additions[0].required, "additions are always nullable");
        assert_eq!(
            diff.additions[0].field_type,
            Type::Primitive(PrimitiveType::Int)
        );
        assert!(diff.removed_field_ids.is_empty());
    }

    #[test]
    fn diff_schema_removes_a_field_missing_from_the_target_by_its_existing_id() {
        let live = base_schema_with_map();
        let attributes_id = field(&live, "attributes").unwrap().id;
        let target = vec![
            resolved_field("timestamp", "timestamp_ns"),
            resolved_field("body", "string"),
            // "attributes" intentionally absent from the target.
        ];

        let diff = diff_schema(&live, 6, &target, &[], true).unwrap();

        assert!(diff.additions.is_empty());
        assert_eq!(diff.removed_field_ids, vec![attributes_id]);

        // Every remaining field's id is untouched.
        for name in ["timestamp", "body"] {
            let before = field(&live, name).unwrap().id;
            assert!(!diff.removed_field_ids.contains(&before));
        }
    }

    #[test]
    fn diff_schema_with_removals_disallowed_never_reports_a_removal() {
        let live = base_schema_with_map();
        // Same target as the removal test above, but with removals
        // disallowed -- as `ensure_schema_current` does for a table with no
        // recorded baseline, since "attributes" might be a field the table
        // legitimately has that the inferred target just doesn't mention.
        let target = vec![
            resolved_field("timestamp", "timestamp_ns"),
            resolved_field("body", "string"),
        ];

        let diff = diff_schema(&live, 6, &target, &[], false).unwrap();

        assert!(diff.additions.is_empty());
        assert!(
            diff.removed_field_ids.is_empty(),
            "removals must be skipped entirely when allow_removals is false"
        );
    }

    #[test]
    fn diff_schema_renames_a_live_field_in_place_preserving_its_id() {
        // "body" -> "message": a target field by a new name, resolved via
        // this hop's own field_renames. Must NOT be reported as a removal
        // of "body" plus a fresh-id addition of "message" -- that would
        // orphan every historical value written under "body"'s id, since
        // Iceberg readers resolve columns by id, not name.
        let live = base_schema_with_map();
        let body_id = field(&live, "body").unwrap().id;
        let target = vec![
            resolved_field("timestamp", "timestamp_ns"),
            resolved_field("message", "string"),
            resolved_field("attributes", "map<string,string>"),
        ];
        let renames = vec![crate::schema::schema_parser::FieldRename {
            from: "body".to_string(),
            to: "message".to_string(),
        }];

        let diff = diff_schema(&live, 6, &target, &renames, true).unwrap();

        assert!(
            diff.additions.is_empty(),
            "the renamed field must not also show up as an addition: {:?}",
            diff.additions
        );
        assert!(
            !diff.removed_field_ids.contains(&body_id),
            "the renamed field's original id must not be removed"
        );
        assert_eq!(diff.renamed.len(), 1);
        assert_eq!(diff.renamed[0].id, body_id, "rename must preserve the id");
        assert_eq!(diff.renamed[0].name, "message");
    }

    #[test]
    fn diff_schema_ignores_a_rename_whose_source_field_is_not_actually_live() {
        // Idempotent replay: a hop already applied (live already has
        // "message", not "body") must not try to rename again or drop
        // anything.
        let live = Schema::from_struct_type(
            StructType::new(vec![
                string_field(1, "timestamp", true),
                string_field(2, "message", false),
            ]),
            0,
            None,
        );
        let target = vec![
            resolved_field("timestamp", "timestamp_ns"),
            resolved_field("message", "string"),
        ];
        let renames = vec![crate::schema::schema_parser::FieldRename {
            from: "body".to_string(),
            to: "message".to_string(),
        }];

        let diff = diff_schema(&live, 3, &target, &renames, true).unwrap();

        assert!(
            diff.is_empty(),
            "already-migrated hop must be a no-op: {diff:?}"
        );
    }

    async fn create_generic_test_table(
        catalog: &Arc<dyn Catalog>,
        table_name: &str,
    ) -> anyhow::Result<Identifier> {
        // Reuses the same fixture shape as the label-column tests -- a
        // generic evolution engine has no opinion on what a table's base
        // shape looks like.
        create_test_table(catalog, table_name).await
    }

    #[tokio::test]
    async fn apply_schema_migration_adds_columns_and_stamps_the_version_property()
    -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_generic_test_table(&catalog, "traces").await?;

        let target = vec![
            resolved_field("timestamp", "timestamp_ns"),
            resolved_field("body", "string"),
            resolved_field("attributes", "map<string,string>"),
            resolved_field("span_kind_number", "int32"),
        ];
        let schema = apply_schema_migration(
            catalog.clone(),
            &identifier,
            &target,
            &[],
            "physical-v3",
            true,
        )
        .await?;

        let added = field(&schema, "span_kind_number").expect("span_kind_number added");
        assert!(!added.required);
        assert_eq!(added.field_type, Type::Primitive(PrimitiveType::Int));

        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(
            table.metadata().properties.get(SCHEMA_VERSION_PROPERTY),
            Some(&"physical-v3".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_schema_migration_is_idempotent_when_already_at_target() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_generic_test_table(&catalog, "traces").await?;

        let target = vec![
            resolved_field("timestamp", "timestamp_ns"),
            resolved_field("body", "string"),
            resolved_field("attributes", "map<string,string>"),
        ];
        apply_schema_migration(
            catalog.clone(),
            &identifier,
            &target,
            &[],
            "physical-v1",
            true,
        )
        .await?;
        let table_after_first = load_table(&catalog, &identifier).await?;
        let schema_count_after_first = table_after_first.metadata().schemas.len();

        // Same target, same version: no diff and already stamped -> no commit.
        apply_schema_migration(
            catalog.clone(),
            &identifier,
            &target,
            &[],
            "physical-v1",
            true,
        )
        .await?;
        let table_after_second = load_table(&catalog, &identifier).await?;
        assert_eq!(
            table_after_second.metadata().schemas.len(),
            schema_count_after_first,
            "idempotent re-run must not add another schema"
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_apply_schema_migration_leaves_a_consistent_schema() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_generic_test_table(&catalog, "traces").await?;

        let target = vec![
            resolved_field("timestamp", "timestamp_ns"),
            resolved_field("body", "string"),
            resolved_field("attributes", "map<string,string>"),
            resolved_field("span_kind_number", "int32"),
        ];
        let (a, b) = tokio::join!(
            apply_schema_migration(
                catalog.clone(),
                &identifier,
                &target,
                &[],
                "physical-v3",
                true
            ),
            apply_schema_migration(
                catalog.clone(),
                &identifier,
                &target,
                &[],
                "physical-v3",
                true
            ),
        );
        assert!(
            a.is_ok() || b.is_ok(),
            "at least one concurrent migration must succeed: {a:?} / {b:?}"
        );

        let table = load_table(&catalog, &identifier).await?;
        let current = table.current_schema()?;
        assert_eq!(
            current
                .fields()
                .iter()
                .filter(|f| f.name == "span_kind_number")
                .count(),
            1,
            "concurrent migration must not duplicate the column"
        );
        let mut ids: Vec<i32> = current.fields().iter().map(|f| f.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), current.fields().iter().count());
        Ok(())
    }

    fn version_chain_fixture() -> HashMap<String, TableSchemaDefinition> {
        let mut schemas = HashMap::new();
        schemas.insert(
            "physical-v1".to_string(),
            TableSchemaDefinition {
                description: "root".to_string(),
                inherits: None,
                fields: vec![
                    crate::schema::schema_parser::FieldDefinition {
                        name: "timestamp".to_string(),
                        field_type: "timestamp_ns".to_string(),
                        required: true,
                        computed: None,
                        physical_only: false,
                    },
                    crate::schema::schema_parser::FieldDefinition {
                        name: "body".to_string(),
                        field_type: "string".to_string(),
                        required: false,
                        computed: None,
                        physical_only: false,
                    },
                    crate::schema::schema_parser::FieldDefinition {
                        name: "attributes".to_string(),
                        field_type: "map<string,string>".to_string(),
                        required: false,
                        computed: None,
                        physical_only: false,
                    },
                ],
                field_renames: vec![],
                field_additions: vec![],
                field_removals: vec![],
                partition_by: vec![],
            },
        );
        schemas.insert(
            "physical-v2".to_string(),
            TableSchemaDefinition {
                description: "adds span_kind_number".to_string(),
                inherits: Some("physical-v1".to_string()),
                fields: vec![],
                field_renames: vec![],
                field_additions: vec![crate::schema::schema_parser::FieldDefinition {
                    name: "span_kind_number".to_string(),
                    field_type: "int32".to_string(),
                    required: false,
                    computed: None,
                    physical_only: false,
                }],
                field_removals: vec![],
                partition_by: vec![],
            },
        );
        schemas.insert(
            "physical-v3".to_string(),
            TableSchemaDefinition {
                description: "adds status_code_number".to_string(),
                inherits: Some("physical-v2".to_string()),
                fields: vec![],
                field_renames: vec![],
                field_additions: vec![crate::schema::schema_parser::FieldDefinition {
                    name: "status_code_number".to_string(),
                    field_type: "int32".to_string(),
                    required: false,
                    computed: None,
                    physical_only: false,
                }],
                field_removals: vec![],
                partition_by: vec![],
            },
        );
        schemas
    }

    fn defs_for(schemas_map: HashMap<String, TableSchemaDefinition>) -> SchemaDefinitions {
        // `SchemaDefinitions::resolve_table_schema` only needs `&self` to
        // recurse through `inherits` on the SAME map passed in, so which
        // named field it lives under here doesn't matter for these tests.
        SchemaDefinitions {
            metadata: crate::schema::schema_parser::SchemaMetadata {
                description: "test".to_string(),
                current_trace_version: "physical-v3".to_string(),
                current_log_version: "physical-v1".to_string(),
                current_metric_version: "physical-v1".to_string(),
                logical_schema_version: "test".to_string(),
            },
            traces: schemas_map,
            logs: HashMap::new(),
            metrics_gauge: HashMap::new(),
            metrics_sum: HashMap::new(),
            metrics_histogram: HashMap::new(),
            metrics_exponential_histogram: HashMap::new(),
            metrics_summary: HashMap::new(),
            profiles: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn ensure_schema_current_walks_two_hops_one_at_a_time() -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_generic_test_table(&catalog, "traces").await?;
        let schemas_map = version_chain_fixture();
        let defs = defs_for(schemas_map.clone());

        // Land the table at v1 first (as if created there), then ask to
        // reach v3 -- two hops away.
        let v1 = defs.resolve_table_schema(&schemas_map, "physical-v1")?;
        apply_schema_migration(
            catalog.clone(),
            &identifier,
            &v1.fields,
            &[],
            "physical-v1",
            true,
        )
        .await?;

        ensure_schema_current(
            catalog.clone(),
            &identifier,
            &defs,
            &schemas_map,
            "physical-v3",
        )
        .await?;

        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(
            table.metadata().properties.get(SCHEMA_VERSION_PROPERTY),
            Some(&"physical-v3".to_string())
        );
        let current = table.current_schema()?;
        assert!(field(current, "span_kind_number").is_some());
        assert!(field(current, "status_code_number").is_some());

        // Both intermediate hops actually happened as distinct schema
        // versions (root create + v1 stamp + v2 hop + v3 hop = 4 schemas).
        assert_eq!(table.metadata().schemas.len(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn ensure_schema_current_on_a_table_with_no_recorded_version_jumps_straight_to_current()
    -> anyhow::Result<()> {
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_generic_test_table(&catalog, "traces").await?;
        let schemas_map = version_chain_fixture();
        let defs = defs_for(schemas_map.clone());

        // No prior `apply_schema_migration` call: the table has never
        // recorded a schema version, exactly like a pre-existing table
        // from before this mechanism.
        ensure_schema_current(
            catalog.clone(),
            &identifier,
            &defs,
            &schemas_map,
            "physical-v3",
        )
        .await?;

        let table = load_table(&catalog, &identifier).await?;
        assert_eq!(
            table.metadata().properties.get(SCHEMA_VERSION_PROPERTY),
            Some(&"physical-v3".to_string())
        );
        let current = table.current_schema()?;
        assert!(field(current, "span_kind_number").is_some());
        assert!(field(current, "status_code_number").is_some());

        // One direct commit to v3, not a root->v2->v3 walk: create(1) +
        // one migration(1) = 2 schemas.
        assert_eq!(table.metadata().schemas.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn ensure_schema_current_with_no_recorded_version_never_removes_extra_fields()
    -> anyhow::Result<()> {
        // Regression test: a table that already has MORE fields than an
        // early hop declares (e.g. created at v2 shape but never stamped)
        // must not have those fields stripped by naively walking the chain
        // from root. This is exactly the bug `allow_removals` fixes.
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_generic_test_table(&catalog, "traces").await?;
        let schemas_map = version_chain_fixture();
        let defs = defs_for(schemas_map.clone());

        // Table already has span_kind_number (v2-shaped) but was never
        // stamped with a schema version.
        let v2 = defs.resolve_table_schema(&schemas_map, "physical-v2")?;
        apply_schema_migration(
            catalog.clone(),
            &identifier,
            &v2.fields,
            &[],
            "__setup_only__",
            false,
        )
        .await?;
        // Erase the setup stamp so the table looks exactly like an
        // untracked pre-existing table again.
        catalog
            .clone()
            .update_table(CommitTable {
                identifier: identifier.clone(),
                requirements: vec![],
                updates: vec![TableUpdate::RemoveProperties {
                    removals: vec![SCHEMA_VERSION_PROPERTY.to_string()],
                }],
            })
            .await?;

        ensure_schema_current(
            catalog.clone(),
            &identifier,
            &defs,
            &schemas_map,
            "physical-v3",
        )
        .await?;

        let table = load_table(&catalog, &identifier).await?;
        let current = table.current_schema()?;
        assert!(
            field(current, "span_kind_number").is_some(),
            "field the table already had must survive an unknown-baseline migration"
        );
        assert!(field(current, "status_code_number").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn ensure_schema_current_with_an_unrecognized_recorded_version_never_removes_extra_fields()
    -> anyhow::Result<()> {
        // Same hazard as the unknown-baseline regression test above, but
        // for a recorded `signaldb.schema.version` that isn't actually on
        // this chain (corrupted property, or a retired version name) rather
        // than an entirely absent one -- `version_chain` returns `None` for
        // this case, and `ensure_schema_current` must fall back to the same
        // untrusted, additions-only path instead of trusting a fabricated
        // "full chain from root" with removals enabled.
        let manager = CatalogManager::new_in_memory().await?;
        let catalog = manager.catalog();
        let identifier = create_generic_test_table(&catalog, "traces").await?;
        let schemas_map = version_chain_fixture();
        let defs = defs_for(schemas_map.clone());

        // Table already has span_kind_number (v2-shaped) but is stamped
        // with a version name that doesn't exist in `schemas_map` at all.
        let v2 = defs.resolve_table_schema(&schemas_map, "physical-v2")?;
        apply_schema_migration(
            catalog.clone(),
            &identifier,
            &v2.fields,
            &[],
            "physical-v0-retired",
            false,
        )
        .await?;

        ensure_schema_current(
            catalog.clone(),
            &identifier,
            &defs,
            &schemas_map,
            "physical-v3",
        )
        .await?;

        let table = load_table(&catalog, &identifier).await?;
        let current = table.current_schema()?;
        assert!(
            field(current, "span_kind_number").is_some(),
            "field the table already had must survive an unrecognized-baseline migration"
        );
        assert!(field(current, "status_code_number").is_some());
        assert_eq!(
            table.metadata().properties.get(SCHEMA_VERSION_PROPERTY),
            Some(&"physical-v3".to_string())
        );
        Ok(())
    }
}
