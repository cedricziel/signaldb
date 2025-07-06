use crate::config::SchemaConfig;
use anyhow::Result;
use iceberg::io::FileIOBuilder;
use iceberg_catalog_memory::MemoryCatalog;
use std::sync::Arc;

/// Create an Iceberg catalog from configuration
pub async fn create_catalog(config: SchemaConfig) -> Result<Arc<dyn ::iceberg::Catalog>> {
    let catalog: Arc<dyn ::iceberg::Catalog> = match config.catalog_type.as_str() {
        "sql" => {
            // For now, use memory catalog as foundation for SQL catalog
            // TODO: Implement proper SQL catalog with SQLite/PostgreSQL backend
            // when iceberg-catalog-sql version compatibility is resolved
            let file_io = FileIOBuilder::new("memory").build()?;
            let catalog = MemoryCatalog::new(file_io, Some(config.warehouse_path));
            Arc::new(catalog)
        }
        "memory" => {
            let file_io = FileIOBuilder::new("memory").build()?;
            let catalog = MemoryCatalog::new(file_io, Some(config.warehouse_path));
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

/// Create an Iceberg catalog with default configuration (SQL-compatible with in-memory storage)
pub async fn create_default_catalog() -> Result<Arc<dyn ::iceberg::Catalog>> {
    create_catalog(SchemaConfig::default()).await
}
