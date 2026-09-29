//! Fixture for a table left behind by the retired per-type metrics layout.

use anyhow::Result;
use iceberg_rust::catalog::create::CreateTableBuilder;

use crate::CatalogManager;
use crate::iceberg::schemas::TableSchema;

/// Creates a bare table named `table_name` (e.g. one of
/// [`crate::iceberg::schemas::LEGACY_METRIC_TABLE_NAMES`]) directly in the
/// catalog. `schemas.toml` no longer sources those names, so
/// [`CatalogManager::ensure_table`] rejects them; the contents are
/// irrelevant to the purge and redirect tests that need them to exist.
pub async fn create_legacy_metric_table(
    manager: &CatalogManager,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<()> {
    let catalog = manager.catalog();
    let namespace = manager.build_namespace(tenant_id, dataset_id)?;
    let _ = catalog.clone().create_namespace(&namespace, None).await;
    let create = CreateTableBuilder::default()
        .with_name(table_name.to_string())
        .with_schema(TableSchema::Traces.schema()?)
        .with_location(manager.build_table_location(tenant_id, dataset_id, table_name))
        .create()
        .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
    catalog
        .clone()
        .create_table(
            manager.build_table_identifier(tenant_id, dataset_id, table_name),
            create,
        )
        .await?;
    Ok(())
}
