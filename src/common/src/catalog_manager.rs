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
    /// This allows each dataset to optionally specify its own storage backend,
    /// falling back to the global storage configuration if not specified.
    pub fn get_dataset_storage_config(&self, tenant_id: &str, dataset_id: &str) -> &StorageConfig {
        // Check dataset-level storage
        if let Some(tenant) = self.config.auth.tenants.iter().find(|t| t.id == tenant_id)
            && let Some(dataset) = tenant.datasets.iter().find(|d| d.id == dataset_id)
            && let Some(ref storage) = dataset.storage
        {
            return storage;
        }
        // Future: Check tenant-level storage here if we add it
        // Fall back to global storage
        &self.config.storage
    }

    /// Get the tenant slug for a given tenant ID.
    ///
    /// Returns the tenant's slug if found, otherwise returns the tenant_id as-is.
    pub fn get_tenant_slug(&self, tenant_id: &str) -> String {
        self.config
            .auth
            .tenants
            .iter()
            .find(|t| t.id == tenant_id)
            .map(|t| t.slug.clone())
            .unwrap_or_else(|| tenant_id.to_string())
    }

    /// Get the dataset slug for a given tenant and dataset ID.
    ///
    /// Returns the dataset's slug if found, otherwise returns the dataset_id as-is.
    pub fn get_dataset_slug(&self, tenant_id: &str, dataset_id: &str) -> String {
        self.config
            .auth
            .tenants
            .iter()
            .find(|t| t.id == tenant_id)
            .and_then(|t| t.datasets.iter().find(|d| d.id == dataset_id))
            .map(|d| d.slug.clone())
            .unwrap_or_else(|| dataset_id.to_string())
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
                dsn: "file://.data/storage".to_string(),
            },
            ..Configuration::default()
        }
    }

    #[test]
    fn test_get_dataset_storage_config_with_global_fallback() {
        let config = create_test_config();

        // acme/production should use global storage (no override)
        let storage = config.get_dataset_storage_config("acme", "production");
        assert_eq!(storage.dsn, "file://.data/storage");
    }

    #[test]
    fn test_get_dataset_storage_config_with_dataset_override() {
        let config = create_test_config();

        // acme/archive should use S3 storage
        let storage = config.get_dataset_storage_config("acme", "archive");
        assert_eq!(storage.dsn, "s3://acme-archive/signals");

        // beta/staging should use local file storage
        let storage = config.get_dataset_storage_config("beta", "staging");
        assert_eq!(storage.dsn, "file://.data/beta-staging");
    }

    #[test]
    fn test_get_dataset_storage_config_unknown_tenant() {
        let config = create_test_config();

        // Unknown tenant should fall back to global storage
        let storage = config.get_dataset_storage_config("unknown", "dataset");
        assert_eq!(storage.dsn, "file://.data/storage");
    }

    #[test]
    fn test_get_tenant_slug() {
        // Uses Configuration::get_tenant_slug directly since it's now a method on Configuration
        let config = create_test_config();
        assert_eq!(config.get_tenant_slug("acme"), "acme");
        assert_eq!(config.get_tenant_slug("unknown"), "unknown");
    }

    #[test]
    fn test_get_dataset_slug() {
        // Uses Configuration::get_dataset_slug directly since it's now a method on Configuration
        let config = create_test_config();
        assert_eq!(config.get_dataset_slug("acme", "production"), "prod");
        assert_eq!(config.get_dataset_slug("acme", "archive"), "archive");
        assert_eq!(config.get_dataset_slug("acme", "unknown"), "unknown");
    }
}
