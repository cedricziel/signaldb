//! Global catalog manager for SignalDB.
//!
//! This module provides a centralized catalog manager that holds the shared Iceberg catalog
//! instance. All SignalDB components (writer, querier, router) should use the same catalog
//! instance for:
//! - Consistent table metadata
//! - Proper caching
//! - Avoiding race conditions

use std::sync::Arc;

use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;

use crate::config::{Configuration, StorageConfig};
use crate::schema::create_catalog_with_config;

/// Global catalog manager holding the shared Iceberg catalog instance.
///
/// This ensures all SignalDB components use the same catalog for:
/// - Consistent table metadata
/// - Proper caching
/// - Avoiding race conditions
pub struct CatalogManager {
    catalog: Arc<dyn IcebergCatalog>,
    config: Configuration,
}

impl CatalogManager {
    /// Create a new catalog manager with the shared Iceberg catalog.
    pub async fn new(config: Configuration) -> Result<Self> {
        let catalog = create_catalog_with_config(&config).await?;
        Ok(Self { catalog, config })
    }

    /// Create an in-memory catalog manager for fast tests.
    ///
    /// This uses `Configuration::default()` which provides:
    /// - In-memory object storage (`memory://`)
    /// - In-memory SQLite catalog (`sqlite::memory:`)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use common::CatalogManager;
    ///
    /// let manager = CatalogManager::new_in_memory().await?;
    /// let catalog = manager.catalog();
    /// ```
    pub async fn new_in_memory() -> Result<Self> {
        Self::new(Configuration::default()).await
    }

    /// Get the shared Iceberg catalog.
    pub fn catalog(&self) -> Arc<dyn IcebergCatalog> {
        self.catalog.clone()
    }

    /// Get the configuration.
    pub fn config(&self) -> &Configuration {
        &self.config
    }

    /// Get effective storage config for a dataset (dataset -> tenant -> global fallback).
    ///
    /// Delegates to [`Configuration::get_dataset_storage_config`].
    pub fn get_dataset_storage_config(&self, tenant_id: &str, dataset_id: &str) -> &StorageConfig {
        self.config
            .get_dataset_storage_config(tenant_id, dataset_id)
    }

    /// Get the tenant slug for a given tenant ID.
    ///
    /// Delegates to [`Configuration::get_tenant_slug`].
    pub fn get_tenant_slug(&self, tenant_id: &str) -> String {
        self.config.get_tenant_slug(tenant_id)
    }

    /// Get the dataset slug for a given tenant and dataset ID.
    ///
    /// Delegates to [`Configuration::get_dataset_slug`].
    pub fn get_dataset_slug(&self, tenant_id: &str, dataset_id: &str) -> String {
        self.config.get_dataset_slug(tenant_id, dataset_id)
    }

    /// Get all enabled tenants.
    pub fn get_enabled_tenants(&self) -> Vec<&crate::config::TenantConfig> {
        self.config
            .auth
            .tenants
            .iter()
            .filter(|t| {
                // Check if tenant has schema_config with enabled field set to false
                if let Some(ref schema_config) = t.schema_config {
                    schema_config.enabled
                } else {
                    true
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, DatasetConfig, StorageConfig, TenantConfig};

    fn create_test_config() -> Configuration {
        Configuration {
            auth: AuthConfig {
                enabled: true,
                tenants: vec![
                    TenantConfig {
                        id: "acme".to_string(),
                        slug: "acme".to_string(),
                        name: "Acme Corp".to_string(),
                        default_dataset: Some("production".to_string()),
                        datasets: vec![
                            DatasetConfig {
                                id: "production".to_string(),
                                slug: "prod".to_string(),
                                is_default: true,
                                storage: None, // Uses global storage
                            },
                            DatasetConfig {
                                id: "archive".to_string(),
                                slug: "archive".to_string(),
                                is_default: false,
                                storage: Some(StorageConfig {
                                    dsn: "s3://acme-archive/signals".to_string(),
                                }),
                            },
                        ],
                        api_keys: vec![],
                        schema_config: None,
                    },
                    TenantConfig {
                        id: "beta".to_string(),
                        slug: "beta".to_string(),
                        name: "Beta Tenant".to_string(),
                        default_dataset: Some("staging".to_string()),
                        datasets: vec![DatasetConfig {
                            id: "staging".to_string(),
                            slug: "staging".to_string(),
                            is_default: true,
                            storage: Some(StorageConfig {
                                dsn: "file://.data/beta-staging".to_string(),
                            }),
                        }],
                        api_keys: vec![],
                        schema_config: None,
                    },
                ],
                admin_api_key: None,
            },
            storage: StorageConfig {
                dsn: "memory://".to_string(),
            },
            ..Configuration::default()
        }
    }

    async fn create_test_catalog_manager() -> CatalogManager {
        let config = create_test_config();
        CatalogManager::new(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_get_dataset_storage_config_with_global_fallback() {
        let manager = create_test_catalog_manager().await;

        // acme/production should use global storage (no override)
        let storage = manager.get_dataset_storage_config("acme", "production");
        assert_eq!(storage.dsn, "memory://");
    }

    #[tokio::test]
    async fn test_get_dataset_storage_config_with_dataset_override() {
        let manager = create_test_catalog_manager().await;

        // acme/archive should use S3 storage
        let storage = manager.get_dataset_storage_config("acme", "archive");
        assert_eq!(storage.dsn, "s3://acme-archive/signals");

        // beta/staging should use local file storage
        let storage = manager.get_dataset_storage_config("beta", "staging");
        assert_eq!(storage.dsn, "file://.data/beta-staging");
    }

    #[tokio::test]
    async fn test_get_dataset_storage_config_unknown_tenant() {
        let manager = create_test_catalog_manager().await;

        // Unknown tenant should fall back to global storage
        let storage = manager.get_dataset_storage_config("unknown", "dataset");
        assert_eq!(storage.dsn, "memory://");
    }

    #[tokio::test]
    async fn test_get_tenant_slug() {
        let manager = create_test_catalog_manager().await;
        assert_eq!(manager.get_tenant_slug("acme"), "acme");
        assert_eq!(manager.get_tenant_slug("unknown"), "unknown");
    }

    #[tokio::test]
    async fn test_get_dataset_slug() {
        let manager = create_test_catalog_manager().await;
        assert_eq!(manager.get_dataset_slug("acme", "production"), "prod");
        assert_eq!(manager.get_dataset_slug("acme", "archive"), "archive");
        assert_eq!(manager.get_dataset_slug("acme", "unknown"), "unknown");
    }
}
