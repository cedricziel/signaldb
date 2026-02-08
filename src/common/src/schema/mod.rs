use crate::config::Configuration;
use crate::iceberg::{create_object_store_builder_from_config, create_sql_catalog_with_builder};
use anyhow::Result;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;

pub mod schema_parser;

// Re-export iceberg modules for backward compatibility
pub use crate::iceberg::schemas as iceberg_schemas;
pub use crate::iceberg::{
    create_catalog, create_catalog_with_config, create_catalog_with_object_store,
    create_default_catalog, create_sql_catalog,
};

use self::schema_parser::SchemaDefinitions;

/// Embedded schema definitions from schemas.toml
pub const SCHEMA_DEFINITIONS_TOML: &str = include_str!("../../../../schemas.toml");

/// Parsed schema definitions
pub static SCHEMA_DEFINITIONS: Lazy<SchemaDefinitions> = Lazy::new(|| {
    SchemaDefinitions::from_toml(SCHEMA_DEFINITIONS_TOML)
        .expect("Failed to load built-in schema definitions")
});

/// Tenant-aware schema registry for managing catalogs and schemas per tenant
pub struct TenantSchemaRegistry {
    pub(crate) config: Configuration,
    catalogs: HashMap<String, Arc<dyn IcebergCatalog>>,
}

impl TenantSchemaRegistry {
    /// Create a new tenant schema registry
    pub fn new(config: Configuration) -> Self {
        Self {
            config,
            catalogs: HashMap::new(),
        }
    }

    /// Get or create a catalog for the specified tenant
    pub async fn get_catalog_for_tenant(
        &mut self,
        tenant_id: &str,
    ) -> Result<Arc<dyn IcebergCatalog>> {
        // Check if tenant is enabled
        if !self.config.is_tenant_enabled(tenant_id) {
            return Err(anyhow::anyhow!("Tenant '{}' is not enabled", tenant_id));
        }

        // Return cached catalog if available
        if let Some(catalog) = self.catalogs.get(tenant_id) {
            return Ok(catalog.clone());
        }

        // Create new catalog for tenant
        let tenant_schema_config = self.config.get_tenant_schema_config(tenant_id);

        // Create object store builder from storage config
        let object_store_builder = create_object_store_builder_from_config(&self.config.storage)?;

        // Create catalog for actual operations
        let catalog = create_sql_catalog_with_builder(
            &tenant_schema_config.catalog_uri,
            "signaldb",
            object_store_builder,
        )
        .await?;

        // Cache the catalog
        self.catalogs.insert(tenant_id.to_string(), catalog.clone());

        Ok(catalog)
    }

    /// Get custom schemas for a tenant
    pub fn get_custom_schemas(&self, tenant_id: &str) -> Option<&HashMap<String, String>> {
        self.config.get_tenant_custom_schemas(tenant_id)
    }

    /// Check if tenant is enabled
    pub fn is_tenant_enabled(&self, tenant_id: &str) -> bool {
        self.config.is_tenant_enabled(tenant_id)
    }

    /// Get the default tenant
    pub fn get_default_tenant(&self) -> &str {
        self.config.get_default_tenant()
    }

    /// Get all configured tenants
    pub fn get_configured_tenants(&self) -> Vec<String> {
        let mut tenants: Vec<String> = self.config.tenants.tenants.keys().cloned().collect();

        // Always include the default tenant if it's not explicitly configured
        let default_tenant = self.get_default_tenant().to_string();
        if !tenants.contains(&default_tenant) {
            tenants.push(default_tenant);
        }

        tenants
    }

    /// Remove a cached catalog (useful for invalidation)
    pub fn invalidate_tenant_catalog(&mut self, tenant_id: &str) {
        self.catalogs.remove(tenant_id);
    }

    /// Get schema definitions for a tenant
    pub fn get_schema_definitions(
        &self,
        _tenant_id: &str,
    ) -> Result<HashMap<String, iceberg_rust::spec::schema::Schema>> {
        let mut schemas = HashMap::new();
        let default_schemas = &self.config.schema.default_schemas;

        for table_schema in iceberg_schemas::TableSchema::all_from_config(default_schemas) {
            match &table_schema {
                iceberg_schemas::TableSchema::Custom(_) => {
                    // Skip custom schemas for now
                    // TODO: Parse custom schema from JSON configuration
                }
                _ => {
                    let schema = table_schema.schema()?;
                    schemas.insert(table_schema.table_name().to_string(), schema);
                }
            }
        }

        Ok(schemas)
    }

    /// Get partition specifications for a tenant  
    pub fn get_partition_specifications(
        &self,
        _tenant_id: &str,
    ) -> Result<HashMap<String, iceberg_rust::spec::partition::PartitionSpec>> {
        let mut partition_specs = HashMap::new();
        let default_schemas = &self.config.schema.default_schemas;

        for table_schema in iceberg_schemas::TableSchema::all_from_config(default_schemas) {
            match &table_schema {
                iceberg_schemas::TableSchema::Custom(_) => {
                    // Skip custom schemas for now
                    // TODO: Parse custom partition spec from configuration
                }
                _ => {
                    let partition_spec = table_schema.partition_spec()?;
                    partition_specs.insert(table_schema.table_name().to_string(), partition_spec);
                }
            }
        }

        Ok(partition_specs)
    }

    /// Create default tables for a tenant
    pub async fn create_default_tables_for_tenant(&mut self, tenant_id: &str) -> Result<()> {
        // Get the catalog
        let _ = self.get_catalog_for_tenant(tenant_id).await?;

        // For now, log that we would create tables
        // TODO: Implement table creation
        log::info!("Would create default tables for tenant '{tenant_id}' using Iceberg catalog");

        // Get the default schemas configuration
        let default_schemas = &self.config.schema.default_schemas;

        // Create tables based on configuration
        for table_schema in iceberg_schemas::TableSchema::all_from_config(default_schemas) {
            let table_name = table_schema.table_name();

            // For catalog operations, we'll need to use writer's catalog module
            match &table_schema {
                iceberg_schemas::TableSchema::Custom(name) => {
                    log::info!(
                        "Would create custom table '{name}' from configuration for tenant {tenant_id}"
                    );
                }
                _ => {
                    let _schema = table_schema.schema()?;
                    let _partition_spec = table_schema.partition_spec()?;

                    log::info!(
                        "Would create table {table_name} with schema for tenant {tenant_id}"
                    );
                }
            }
        }

        Ok(())
    }

    /// List all tables for a tenant
    pub async fn list_tables_for_tenant(&mut self, tenant_id: &str) -> Result<Vec<String>> {
        // Get the catalog
        let _ = self.get_catalog_for_tenant(tenant_id).await?;

        // For now, return empty list
        // TODO: Implement table listing
        log::info!("Would list tables for tenant '{tenant_id}' using Iceberg catalog");
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DefaultSchemas, SchemaConfig, TenantSchemaConfig, TenantsConfig};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_tenant_schema_registry_default() {
        let config = Configuration::default();
        let mut registry = TenantSchemaRegistry::new(config);

        // Should work with default tenant
        let catalog = registry.get_catalog_for_tenant("default").await;
        assert!(catalog.is_ok());

        // Should fail for unknown tenant
        let unknown_catalog = registry.get_catalog_for_tenant("unknown-tenant").await;
        assert!(unknown_catalog.is_err());

        // Default tenant should be "default"
        assert_eq!(registry.get_default_tenant(), "default");

        // Should return the default tenant
        let tenants = registry.get_configured_tenants();
        assert_eq!(tenants.len(), 1);
        assert_eq!(tenants[0], "default");

        // No custom schemas for default tenant
        assert!(registry.get_custom_schemas("default").is_none());
    }

    #[tokio::test]
    async fn test_tenant_schema_registry_with_custom_tenant() {
        let tenant_config = TenantSchemaConfig {
            schema: Some(SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: DefaultSchemas::default(),
            }),
            custom_schemas: Some({
                let mut schemas = HashMap::new();
                schemas.insert("traces".to_string(), "custom_traces".to_string());
                schemas
            }),
            ..Default::default()
        };

        let mut tenants = HashMap::new();
        tenants.insert("test-tenant".to_string(), tenant_config);

        let config = Configuration {
            tenants: TenantsConfig {
                default_tenant: "test-tenant".to_string(),
                tenants,
            },
            ..Default::default()
        };

        let mut registry = TenantSchemaRegistry::new(config);

        // Should create catalog for configured tenant
        let catalog = registry.get_catalog_for_tenant("test-tenant").await;
        assert!(catalog.is_ok());

        // Should fail for unknown tenant
        let unknown_catalog = registry.get_catalog_for_tenant("unknown").await;
        assert!(unknown_catalog.is_err());

        // Should return custom schemas
        let custom_schemas = registry.get_custom_schemas("test-tenant");
        assert!(custom_schemas.is_some());
        assert_eq!(
            custom_schemas.unwrap().get("traces"),
            Some(&"custom_traces".to_string())
        );

        // Should cache catalogs
        let catalog2 = registry.get_catalog_for_tenant("test-tenant").await;
        assert!(catalog2.is_ok());

        // Should be the same instance (cached)
        assert!(Arc::ptr_eq(&catalog.unwrap(), &catalog2.unwrap()));

        // Should return configured tenants
        let tenants = registry.get_configured_tenants();
        assert_eq!(tenants.len(), 1);
        assert_eq!(tenants[0], "test-tenant");
    }

    #[tokio::test]
    async fn test_tenant_schema_registry_invalidation() {
        let tenant_config = TenantSchemaConfig {
            schema: Some(SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: DefaultSchemas::default(),
            }),
            ..Default::default()
        };

        let mut tenants = HashMap::new();
        tenants.insert("test-tenant".to_string(), tenant_config);

        let config = Configuration {
            tenants: TenantsConfig {
                default_tenant: "test-tenant".to_string(),
                tenants,
            },
            ..Default::default()
        };

        let mut registry = TenantSchemaRegistry::new(config);

        // Create catalog
        let catalog1 = registry.get_catalog_for_tenant("test-tenant").await;
        assert!(catalog1.is_ok());

        // Invalidate cache
        registry.invalidate_tenant_catalog("test-tenant");

        // Create catalog again - should be a new instance
        let catalog2 = registry.get_catalog_for_tenant("test-tenant").await;
        assert!(catalog2.is_ok());

        // Should not be the same instance (cache was invalidated)
        assert!(!Arc::ptr_eq(&catalog1.unwrap(), &catalog2.unwrap()));
    }

    #[test]
    fn test_get_schema_definitions() {
        let config = Configuration::default();
        let registry = TenantSchemaRegistry::new(config);

        // Get schema definitions for the default tenant
        let schemas = registry.get_schema_definitions("default").unwrap();
        assert!(schemas.len() >= 5); // Should have at least traces, logs, and 3 metrics tables

        // Verify specific schemas exist
        assert!(schemas.contains_key("traces"));
        assert!(schemas.contains_key("logs"));
        assert!(schemas.contains_key("metrics_gauge"));
        assert!(schemas.contains_key("metrics_sum"));
        assert!(schemas.contains_key("metrics_histogram"));
    }

    #[test]
    fn test_get_partition_specifications() {
        let config = Configuration::default();
        let registry = TenantSchemaRegistry::new(config);

        // Get partition specifications for the default tenant
        let partition_specs = registry.get_partition_specifications("default").unwrap();
        assert!(partition_specs.len() >= 5); // Should have at least traces, logs, and 3 metrics tables

        // Verify specific partition specs exist
        assert!(partition_specs.contains_key("traces"));
        assert!(partition_specs.contains_key("logs"));
        assert!(partition_specs.contains_key("metrics_gauge"));
        assert!(partition_specs.contains_key("metrics_sum"));
        assert!(partition_specs.contains_key("metrics_histogram"));
    }

    #[test]
    fn test_schema_definitions_consistency() {
        let config = Configuration::default();
        let registry = TenantSchemaRegistry::new(config);

        // Schema definitions should be the same for all tenants
        let schemas1 = registry.get_schema_definitions("tenant1").unwrap();
        let schemas2 = registry.get_schema_definitions("tenant2").unwrap();

        assert_eq!(schemas1.len(), schemas2.len());

        // Verify all schema keys are the same
        let keys1: std::collections::BTreeSet<_> = schemas1.keys().collect();
        let keys2: std::collections::BTreeSet<_> = schemas2.keys().collect();
        assert_eq!(keys1, keys2);
    }

    #[tokio::test]
    async fn test_create_default_tables_for_tenant() {
        let config = Configuration::default();
        let mut registry = TenantSchemaRegistry::new(config);

        // Create default tables for the default tenant
        let result = registry.create_default_tables_for_tenant("default").await;
        assert!(result.is_ok());

        // For now, just verify the call succeeds
        // TODO: Add verification once table creation API is implemented
    }

    #[tokio::test]
    async fn test_list_tables_for_tenant_empty() {
        let config = Configuration::default();
        let mut registry = TenantSchemaRegistry::new(config);

        // List tables for a non-existent tenant should fail
        let result = registry.list_tables_for_tenant("non-existent-tenant").await;
        assert!(result.is_err());

        // List tables for the default tenant should work (even if empty)
        let tables = registry.list_tables_for_tenant("default").await.unwrap();
        assert_eq!(tables.len(), 0); // Should return empty list for default tenant with no tables
    }
}
