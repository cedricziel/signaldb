use crate::config::{Configuration, SchemaConfig, StorageConfig};
use crate::storage::create_object_store;
use anyhow::Result;
use iceberg::io::FileIOBuilder;
use iceberg_catalog_memory::MemoryCatalog;
use std::sync::Arc;

/// Create an Iceberg catalog from full configuration
pub async fn create_catalog_with_config(
    config: &Configuration,
) -> Result<Arc<dyn ::iceberg::Catalog>> {
    let schema_config = &config.schema;
    let object_store = create_object_store(&config.storage)?;

    create_catalog_with_object_store(schema_config, object_store).await
}

/// Create an Iceberg catalog with explicit object store
pub async fn create_catalog_with_object_store(
    schema_config: &SchemaConfig,
    _object_store: Arc<dyn object_store::ObjectStore>,
) -> Result<Arc<dyn ::iceberg::Catalog>> {
    // For now, we still use memory FileIO until we can properly integrate object_store
    // TODO: Integrate object_store with FileIO when iceberg-rust supports it
    let file_io = FileIOBuilder::new("memory").build()?;

    let catalog: Arc<dyn ::iceberg::Catalog> = match schema_config.catalog_type.as_str() {
        "sql" => {
            // For now, use memory catalog as foundation for SQL catalog
            // TODO: Implement proper SQL catalog with SQLite/PostgreSQL backend
            // when iceberg-catalog-sql version compatibility is resolved
            let catalog = MemoryCatalog::new(file_io, None);
            Arc::new(catalog)
        }
        "memory" => {
            let catalog = MemoryCatalog::new(file_io, None);
            Arc::new(catalog)
        }
        catalog_type => {
            return Err(anyhow::anyhow!(
                "Unsupported catalog type: {}. Supported: sql, memory",
                catalog_type
            ));
        }
    };
    Ok(catalog)
}

/// Create an Iceberg catalog from schema config with default storage
/// This is a convenience function for tests and simple use cases
pub async fn create_catalog(schema_config: SchemaConfig) -> Result<Arc<dyn ::iceberg::Catalog>> {
    let default_storage = StorageConfig::default();
    let object_store = create_object_store(&default_storage)?;

    create_catalog_with_object_store(&schema_config, object_store).await
}

/// Create an Iceberg catalog with default configuration
/// Uses default schema config and in-memory storage
pub async fn create_default_catalog() -> Result<Arc<dyn ::iceberg::Catalog>> {
    create_catalog(SchemaConfig::default()).await
}
