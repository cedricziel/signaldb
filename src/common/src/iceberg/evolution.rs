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
use std::sync::Arc;

use crate::schema::materialized_column_name;

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
}
