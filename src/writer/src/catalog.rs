use anyhow::Result;

use iceberg_rust::catalog::Catalog as IcebergRustCatalog;
use std::sync::Arc;

/// Create a SQL catalog for use with DataFusion
pub async fn create_sql_catalog(
    catalog_uri: &str,
    catalog_name: &str,
) -> Result<Arc<dyn IcebergRustCatalog>> {
    log::info!("Creating SQL catalog with URI: {catalog_uri}");

    use iceberg_rust::object_store::ObjectStoreBuilder;
    use iceberg_sql_catalog::SqlCatalog;

    // Create an in-memory object store builder
    let object_store_builder = ObjectStoreBuilder::memory();

    let catalog = SqlCatalog::new(catalog_uri, catalog_name, object_store_builder)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create SQL catalog: {}", e))?;

    let catalog_arc: Arc<dyn IcebergRustCatalog> = Arc::new(catalog);

    log::info!("Successfully created SQL catalog: {catalog_name}");
    Ok(catalog_arc)
}
