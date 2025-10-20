use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::config::{Configuration, SchemaConfig};
use crate::schema::{TenantSchemaRegistry, iceberg_schemas};

/// API request to create or update a tenant
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTenantRequest {
    /// Tenant ID
    pub tenant_id: String,
    /// Tenant-specific schema configuration
    pub schema: Option<SchemaConfig>,
    /// Custom schema definitions
    pub custom_schemas: Option<HashMap<String, String>>,
    /// Whether tenant is enabled
    pub enabled: Option<bool>,
}

/// API request to update tenant configuration
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTenantRequest {
    /// Tenant-specific schema configuration
    pub schema: Option<SchemaConfig>,
    /// Custom schema definitions
    pub custom_schemas: Option<HashMap<String, String>>,
    /// Whether tenant is enabled
    pub enabled: Option<bool>,
}

/// API response for tenant information
#[derive(Debug, Serialize, Deserialize)]
pub struct TenantInfo {
    /// Tenant ID
    pub tenant_id: String,
    /// Tenant-specific schema configuration
    pub schema: Option<SchemaConfig>,
    /// Custom schema definitions
    pub custom_schemas: Option<HashMap<String, String>>,
    /// Whether tenant is enabled
    pub enabled: bool,
}

/// API response for listing tenants
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTenantsResponse {
    /// List of tenants
    pub tenants: Vec<TenantInfo>,
    /// Default tenant ID
    pub default_tenant: String,
}

/// API response for table information
#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Table schema type
    pub schema_type: String,
    /// Table description
    pub description: String,
}

/// API response for listing tables
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTablesResponse {
    /// List of tables for the tenant
    pub tables: Vec<TableInfo>,
    /// Tenant ID
    pub tenant_id: String,
}

/// Tenant management API
pub struct TenantApi {
    registry: TenantSchemaRegistry,
}

impl TenantApi {
    /// Create a new tenant API instance
    pub fn new(config: Configuration) -> Self {
        Self {
            registry: TenantSchemaRegistry::new(config),
        }
    }

    /// List all tenants
    pub fn list_tenants(&self) -> ListTenantsResponse {
        let mut tenants: Vec<TenantInfo> = self
            .registry
            .config
            .tenants
            .tenants
            .iter()
            .map(|(tenant_id, config)| TenantInfo {
                tenant_id: tenant_id.clone(),
                schema: config.schema.clone(),
                custom_schemas: config.custom_schemas.clone(),
                enabled: config.enabled,
            })
            .collect();

        // Always include the default tenant if it's not explicitly configured
        let default_tenant = self.registry.get_default_tenant().to_string();
        if !tenants.iter().any(|t| t.tenant_id == default_tenant) {
            tenants.push(TenantInfo {
                tenant_id: default_tenant.clone(),
                schema: Some(self.registry.config.schema.clone()),
                custom_schemas: None,
                enabled: true,
            });
        }

        ListTenantsResponse {
            tenants,
            default_tenant,
        }
    }

    /// Get tenant information
    pub fn get_tenant(&self, tenant_id: &str) -> Result<TenantInfo> {
        if let Some(config) = self.registry.config.tenants.tenants.get(tenant_id) {
            Ok(TenantInfo {
                tenant_id: tenant_id.to_string(),
                schema: config.schema.clone(),
                custom_schemas: config.custom_schemas.clone(),
                enabled: config.enabled,
            })
        } else if tenant_id == self.registry.get_default_tenant() {
            // Return default tenant info
            Ok(TenantInfo {
                tenant_id: tenant_id.to_string(),
                schema: Some(self.registry.config.schema.clone()),
                custom_schemas: None,
                enabled: true,
            })
        } else {
            Err(anyhow::anyhow!("Tenant '{}' not found", tenant_id))
        }
    }

    /// Get the schema registry for advanced operations
    pub fn get_registry(&mut self) -> &mut TenantSchemaRegistry {
        &mut self.registry
    }

    /// Validate a tenant creation request
    pub fn validate_create_tenant_request(&self, request: &CreateTenantRequest) -> Result<()> {
        if request.tenant_id.is_empty() {
            return Err(anyhow::anyhow!("Tenant ID cannot be empty"));
        }

        if self
            .registry
            .config
            .tenants
            .tenants
            .contains_key(&request.tenant_id)
        {
            return Err(anyhow::anyhow!(
                "Tenant '{}' already exists",
                request.tenant_id
            ));
        }

        Ok(())
    }

    /// Validate a tenant update request
    pub fn validate_update_tenant_request(
        &self,
        tenant_id: &str,
        _request: &UpdateTenantRequest,
    ) -> Result<()> {
        if !self.registry.config.tenants.tenants.contains_key(tenant_id)
            && tenant_id != self.registry.get_default_tenant()
        {
            return Err(anyhow::anyhow!("Tenant '{}' not found", tenant_id));
        }

        Ok(())
    }

    /// Get schema definitions for a tenant
    pub fn get_schema_definitions(
        &self,
        tenant_id: &str,
    ) -> Result<HashMap<String, iceberg::spec::Schema>> {
        self.registry.get_schema_definitions(tenant_id)
    }

    /// Get partition specifications for a tenant
    pub fn get_partition_specifications(
        &self,
        tenant_id: &str,
    ) -> Result<HashMap<String, iceberg::spec::PartitionSpec>> {
        self.registry.get_partition_specifications(tenant_id)
    }

    /// Create default tables for a tenant
    pub async fn create_default_tables(&mut self, tenant_id: &str) -> Result<()> {
        self.registry
            .create_default_tables_for_tenant(tenant_id)
            .await
    }

    /// List tables for a tenant
    pub async fn list_tables(&mut self, tenant_id: &str) -> Result<ListTablesResponse> {
        let table_names = self.registry.list_tables_for_tenant(tenant_id).await?;

        let tables = table_names
            .into_iter()
            .map(|name| {
                let (schema_type, description) = match name.as_str() {
                    "traces" => ("traces", "OpenTelemetry traces and spans"),
                    "logs" => ("logs", "OpenTelemetry log entries"),
                    "metrics_gauge" => ("metrics_gauge", "OpenTelemetry gauge metrics"),
                    "metrics_sum" => ("metrics_sum", "OpenTelemetry sum/counter metrics"),
                    "metrics_histogram" => ("metrics_histogram", "OpenTelemetry histogram metrics"),
                    _ => ("custom", "Custom table"),
                };

                TableInfo {
                    name,
                    schema_type: schema_type.to_string(),
                    description: description.to_string(),
                }
            })
            .collect();

        Ok(ListTablesResponse {
            tables,
            tenant_id: tenant_id.to_string(),
        })
    }

    /// List available table schemas for a tenant
    pub fn list_table_schemas(&self, tenant_id: &str) -> Result<ListTablesResponse> {
        let default_schemas = &self.registry.config.schema.default_schemas;
        let tables = iceberg_schemas::TableSchema::all_from_config(default_schemas)
            .into_iter()
            .map(|schema| {
                let description = match schema {
                    iceberg_schemas::TableSchema::Traces => "OpenTelemetry traces and spans",
                    iceberg_schemas::TableSchema::Logs => "OpenTelemetry log entries",
                    iceberg_schemas::TableSchema::MetricsGauge => "OpenTelemetry gauge metrics",
                    iceberg_schemas::TableSchema::MetricsSum => "OpenTelemetry sum/counter metrics",
                    iceberg_schemas::TableSchema::MetricsHistogram => {
                        "OpenTelemetry histogram metrics"
                    }
                    iceberg_schemas::TableSchema::MetricsExponentialHistogram => {
                        "OpenTelemetry exponential histogram metrics"
                    }
                    iceberg_schemas::TableSchema::MetricsSummary => "OpenTelemetry summary metrics",
                    iceberg_schemas::TableSchema::Custom(ref name) => {
                        // For custom schemas, use a generic description
                        return TableInfo {
                            name: name.clone(),
                            schema_type: "custom".to_string(),
                            description: format!("Custom table: {name}"),
                        };
                    }
                };

                TableInfo {
                    name: schema.table_name().to_string(),
                    schema_type: schema.table_name().to_string(),
                    description: description.to_string(),
                }
            })
            .collect();

        Ok(ListTablesResponse {
            tables,
            tenant_id: tenant_id.to_string(),
        })
    }

    /// Get available table schema types (uses default configuration)
    pub fn get_available_table_schemas() -> Vec<TableInfo> {
        // Use default configuration when called statically
        let default_config = crate::config::DefaultSchemas::default();
        iceberg_schemas::TableSchema::all_from_config(&default_config)
            .into_iter()
            .map(|schema| {
                let description = match schema {
                    iceberg_schemas::TableSchema::Traces => "OpenTelemetry traces and spans",
                    iceberg_schemas::TableSchema::Logs => "OpenTelemetry log entries",
                    iceberg_schemas::TableSchema::MetricsGauge => "OpenTelemetry gauge metrics",
                    iceberg_schemas::TableSchema::MetricsSum => "OpenTelemetry sum/counter metrics",
                    iceberg_schemas::TableSchema::MetricsHistogram => {
                        "OpenTelemetry histogram metrics"
                    }
                    iceberg_schemas::TableSchema::MetricsExponentialHistogram => {
                        "OpenTelemetry exponential histogram metrics"
                    }
                    iceberg_schemas::TableSchema::MetricsSummary => "OpenTelemetry summary metrics",
                    iceberg_schemas::TableSchema::Custom(ref name) => {
                        // For custom schemas, use a generic description
                        return TableInfo {
                            name: name.clone(),
                            schema_type: "custom".to_string(),
                            description: format!("Custom table: {name}"),
                        };
                    }
                };

                TableInfo {
                    name: schema.table_name().to_string(),
                    schema_type: schema.table_name().to_string(),
                    description: description.to_string(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Configuration, TenantSchemaConfig, TenantsConfig};

    #[test]
    fn test_tenant_api_default_configuration() {
        let config = Configuration::default();
        let api = TenantApi::new(config);

        // Should return default tenant info
        let tenant_info = api.get_tenant("default").unwrap();
        assert_eq!(tenant_info.tenant_id, "default");
        assert!(tenant_info.enabled);
        assert!(tenant_info.schema.is_some());

        // Should fail for unknown tenant
        assert!(api.get_tenant("unknown-tenant").is_err());

        // Should list default tenant
        let tenants = api.list_tenants();
        assert_eq!(tenants.default_tenant, "default");
        assert_eq!(tenants.tenants.len(), 1);
        assert_eq!(tenants.tenants[0].tenant_id, "default");

        // Should allow creating new tenants
        let create_request = CreateTenantRequest {
            tenant_id: "test".to_string(),
            schema: None,
            custom_schemas: None,
            enabled: Some(true),
        };
        assert!(api.validate_create_tenant_request(&create_request).is_ok());
    }

    #[test]
    fn test_tenant_api_with_custom_tenant() {
        let tenant_config = TenantSchemaConfig {
            schema: Some(SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: crate::config::DefaultSchemas::default(),
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

        let api = TenantApi::new(config);

        // Should return configured tenant info
        let tenant_info = api.get_tenant("test-tenant").unwrap();
        assert_eq!(tenant_info.tenant_id, "test-tenant");
        assert!(tenant_info.enabled);
        assert!(tenant_info.schema.is_some());

        // Should fail for unknown tenant
        assert!(api.get_tenant("unknown").is_err());

        // Should list configured tenants
        let tenants = api.list_tenants();
        assert_eq!(tenants.default_tenant, "test-tenant");
        assert_eq!(tenants.tenants.len(), 1);
        assert_eq!(tenants.tenants[0].tenant_id, "test-tenant");
    }

    #[test]
    fn test_create_tenant_validation() {
        let config = Configuration::default();
        let api = TenantApi::new(config);

        // Valid request should pass
        let valid_request = CreateTenantRequest {
            tenant_id: "new-tenant".to_string(),
            schema: None,
            custom_schemas: None,
            enabled: Some(true),
        };
        assert!(api.validate_create_tenant_request(&valid_request).is_ok());

        // Empty tenant ID should fail
        let empty_id_request = CreateTenantRequest {
            tenant_id: "".to_string(),
            schema: None,
            custom_schemas: None,
            enabled: Some(true),
        };
        assert!(
            api.validate_create_tenant_request(&empty_id_request)
                .is_err()
        );
    }

    #[test]
    fn test_update_tenant_validation() {
        let tenant_config = TenantSchemaConfig::default();
        let mut tenants = HashMap::new();
        tenants.insert("existing-tenant".to_string(), tenant_config);

        let config = Configuration {
            tenants: TenantsConfig {
                default_tenant: "existing-tenant".to_string(),
                tenants,
            },
            ..Default::default()
        };

        let api = TenantApi::new(config);

        let update_request = UpdateTenantRequest {
            schema: None,
            custom_schemas: None,
            enabled: Some(false),
        };

        // Should pass for existing tenant
        assert!(
            api.validate_update_tenant_request("existing-tenant", &update_request)
                .is_ok()
        );

        // Should fail for non-existing tenant
        assert!(
            api.validate_update_tenant_request("non-existing", &update_request)
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_create_default_tables() {
        let config = Configuration::default();
        let mut api = TenantApi::new(config);

        // Create default tables for default tenant
        let result = api.create_default_tables("default").await;
        assert!(result.is_ok());

        // For now, we just verify the call succeeds
        // TODO: Add table listing verification once table creation API is implemented
    }

    #[test]
    fn test_list_table_schemas() {
        let config = Configuration::default();
        let api = TenantApi::new(config);

        // List available table schemas
        let schemas_response = api.list_table_schemas("default").unwrap();
        assert_eq!(schemas_response.tenant_id, "default");
        assert!(schemas_response.tables.len() >= 5);

        // Verify specific schemas exist
        let schema_names: Vec<String> = schemas_response
            .tables
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert!(schema_names.contains(&"traces".to_string()));
        assert!(schema_names.contains(&"logs".to_string()));
        assert!(schema_names.contains(&"metrics_gauge".to_string()));
    }

    #[test]
    fn test_get_available_table_schemas() {
        let schemas = TenantApi::get_available_table_schemas();
        assert!(schemas.len() >= 5);

        let schema_names: Vec<String> = schemas.into_iter().map(|s| s.name).collect();
        assert!(schema_names.contains(&"traces".to_string()));
        assert!(schema_names.contains(&"logs".to_string()));
        assert!(schema_names.contains(&"metrics_gauge".to_string()));
        assert!(schema_names.contains(&"metrics_sum".to_string()));
        assert!(schema_names.contains(&"metrics_histogram".to_string()));
    }

    #[tokio::test]
    async fn test_list_tables_empty_tenant() {
        let config = Configuration::default();
        let mut api = TenantApi::new(config);

        // Try to list tables for a tenant that doesn't exist/have tables
        let result = api.list_tables("non-existent").await;
        assert!(result.is_err());
    }
}
