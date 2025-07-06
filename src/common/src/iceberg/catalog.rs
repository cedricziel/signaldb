use anyhow::Result;
use iceberg::io::FileIOBuilder;
use iceberg::{Catalog, NamespaceIdent};
use iceberg_catalog_memory::MemoryCatalog;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::IcebergConfig;

/// Wrapper around Iceberg catalog functionality
#[derive(Debug)]
pub struct IcebergCatalog {
    catalog: Arc<dyn Catalog>,
    #[allow(dead_code)]
    config: IcebergConfig,
}

impl IcebergCatalog {
    /// Create a new Iceberg catalog from configuration
    pub async fn new(config: IcebergConfig) -> Result<Self> {
        let catalog = match config.catalog_type.as_str() {
            "memory" => Self::create_memory_catalog(&config).await?,
            catalog_type => {
                return Err(anyhow::anyhow!(
                    "Unsupported catalog type: {}. Currently supported: memory",
                    catalog_type
                ));
            }
        };

        Ok(Self { catalog, config })
    }

    /// Create a memory-backed catalog (for testing and basic usage)
    async fn create_memory_catalog(config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
        // Create FileIO for memory storage
        let file_io = FileIOBuilder::new("memory").build()?;

        // Create memory catalog with warehouse location
        let catalog = MemoryCatalog::new(file_io, Some(config.warehouse_path.clone()));
        Ok(Arc::new(catalog))
    }

    /// Get a reference to the underlying catalog
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// List available namespaces
    pub async fn list_namespaces(&self) -> Result<Vec<NamespaceIdent>> {
        Ok(self.catalog.list_namespaces(None).await?)
    }

    /// Create a namespace if it doesn't exist
    pub async fn create_namespace_if_not_exists(&self, namespace: &str) -> Result<()> {
        let namespace_ident = NamespaceIdent::from_strs(vec![namespace])?;

        // Check if namespace exists
        let namespaces = self.list_namespaces().await?;
        if !namespaces.contains(&namespace_ident) {
            self.catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_memory_catalog_creation() {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().join("warehouse");

        let config = IcebergConfig {
            catalog_type: "memory".to_string(),
            catalog_uri: "memory://".to_string(),
            warehouse_path: warehouse_path.to_string_lossy().to_string(),
        };

        let catalog = IcebergCatalog::new(config).await;
        assert!(
            catalog.is_ok(),
            "Failed to create memory catalog: {:?}",
            catalog.err()
        );
    }

    #[tokio::test]
    async fn test_unsupported_catalog_type() {
        let config = IcebergConfig {
            catalog_type: "unsupported".to_string(),
            catalog_uri: "unsupported://test".to_string(),
            warehouse_path: "/tmp/warehouse".to_string(),
        };

        let result = IcebergCatalog::new(config).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported catalog type"));
    }
}
