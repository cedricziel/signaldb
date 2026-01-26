//! Core authentication logic for multi-tenancy
//!
//! This module provides the Authenticator which handles API key validation,
//! tenant resolution, and dataset resolution across config-based and database-based tenants.

use super::{AuthError, TenantContext, TenantSource};
use crate::catalog::Catalog;
use crate::config::{AuthConfig, TenantConfig};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

/// Core authenticator for multi-tenant API key validation
pub struct Authenticator {
    /// Database catalog for dynamic tenant lookups
    catalog: Arc<Catalog>,
    /// Config-based tenants indexed by tenant_id
    config_tenants: HashMap<String, TenantConfig>,
    /// Config-based API keys indexed by key_hash -> (tenant_id, key_name)
    config_api_keys: HashMap<String, (String, Option<String>)>,
}

impl Authenticator {
    /// Create a new Authenticator from configuration and catalog
    pub fn new(auth_config: AuthConfig, catalog: Arc<Catalog>) -> Self {
        let mut config_tenants = HashMap::new();
        let mut config_api_keys = HashMap::new();

        // Index config-based tenants and API keys
        for tenant in auth_config.tenants {
            let tenant_id = tenant.id.clone();

            // Index API keys for this tenant
            for api_key in &tenant.api_keys {
                let key_hash = Self::hash_api_key(&api_key.key);
                config_api_keys.insert(key_hash, (tenant_id.clone(), api_key.name.clone()));
            }

            config_tenants.insert(tenant_id, tenant);
        }

        Self {
            catalog,
            config_tenants,
            config_api_keys,
        }
    }

    /// Authenticate a request using API key and tenant/dataset headers
    ///
    /// # Arguments
    /// * `api_key` - The raw API key from Authorization header
    /// * `tenant_id` - The tenant ID from X-Tenant-ID header
    /// * `dataset_id` - Optional dataset ID from X-Dataset-ID header
    ///
    /// # Returns
    /// * `Ok(TenantContext)` - Successfully authenticated with tenant context
    /// * `Err(AuthError)` - Authentication failed (400/401/403)
    pub async fn authenticate(
        &self,
        api_key: &str,
        tenant_id: &str,
        dataset_id: Option<&str>,
    ) -> Result<TenantContext, AuthError> {
        // Hash the API key for comparison
        let key_hash = Self::hash_api_key(api_key);

        // Try config-based authentication first (faster, no DB query)
        if let Some((config_tenant_id, api_key_name)) = self.config_api_keys.get(&key_hash) {
            // Verify tenant ID matches
            if config_tenant_id != tenant_id {
                return Err(AuthError::forbidden(format!(
                    "API key does not belong to tenant '{tenant_id}'"
                )));
            }

            // Get tenant config
            let tenant_config = self
                .config_tenants
                .get(tenant_id)
                .ok_or_else(|| AuthError::unauthorized("Tenant configuration not found"))?;

            // Resolve dataset
            let resolved_dataset = self.resolve_dataset(tenant_config, dataset_id)?;

            // Resolve slugs from config
            let tenant_slug = tenant_config.slug.clone();
            let dataset_slug = tenant_config
                .datasets
                .iter()
                .find(|d| d.id == resolved_dataset)
                .map(|d| d.slug.clone())
                .unwrap_or_else(|| resolved_dataset.clone());

            return Ok(TenantContext::new(
                tenant_id.to_string(),
                resolved_dataset,
                tenant_slug,
                dataset_slug,
                api_key_name.clone(),
                TenantSource::Config,
            ));
        }

        // Fall back to database-based authentication
        self.authenticate_from_database(&key_hash, tenant_id, dataset_id)
            .await
    }

    /// Authenticate using database catalog
    async fn authenticate_from_database(
        &self,
        key_hash: &str,
        tenant_id: &str,
        dataset_id: Option<&str>,
    ) -> Result<TenantContext, AuthError> {
        // Validate API key and get associated tenant
        let validation_result = self
            .catalog
            .validate_api_key(key_hash)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?;

        let (db_tenant_id, api_key_name) = validation_result
            .ok_or_else(|| AuthError::unauthorized("Invalid or revoked API key"))?;

        // Verify tenant ID matches
        if db_tenant_id != tenant_id {
            return Err(AuthError::forbidden(format!(
                "API key does not belong to tenant '{tenant_id}'"
            )));
        }

        // Get tenant from database
        let tenant_record = self
            .catalog
            .get_tenant(tenant_id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::forbidden(format!("Tenant '{tenant_id}' not found")))?;

        // Resolve dataset
        let resolved_dataset = match dataset_id {
            Some(id) => id.to_string(),
            None => tenant_record.default_dataset.clone().ok_or_else(|| {
                AuthError::bad_request(
                    "X-Dataset-ID header required (tenant has no default dataset)",
                )
            })?,
        };

        // Verify dataset exists for tenant
        let datasets = self
            .catalog
            .get_datasets(tenant_id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?;

        if !datasets.iter().any(|d| d.name == resolved_dataset) {
            return Err(AuthError::forbidden(format!(
                "Dataset '{resolved_dataset}' not found for tenant '{tenant_id}'"
            )));
        }

        // For DB-based tenants, use IDs as slugs (DB slug columns are a follow-up)
        Ok(TenantContext::new(
            tenant_id.to_string(),
            resolved_dataset.clone(),
            tenant_id.to_string(),
            resolved_dataset,
            api_key_name,
            TenantSource::Database,
        ))
    }

    /// Resolve dataset from config-based tenant
    fn resolve_dataset(
        &self,
        tenant_config: &TenantConfig,
        dataset_id: Option<&str>,
    ) -> Result<String, AuthError> {
        match dataset_id {
            Some(id) => {
                // Verify dataset exists in tenant config
                let dataset_exists = tenant_config.datasets.iter().any(|d| d.id == id);
                if !dataset_exists {
                    return Err(AuthError::forbidden(format!(
                        "Dataset '{id}' not found for tenant '{}'",
                        tenant_config.id
                    )));
                }
                Ok(id.to_string())
            }
            None => {
                // Use default dataset or find the first dataset marked as default
                if let Some(default_dataset) = &tenant_config.default_dataset {
                    return Ok(default_dataset.clone());
                }

                // Look for a dataset marked as default
                if let Some(default) = tenant_config.datasets.iter().find(|d| d.is_default) {
                    return Ok(default.id.clone());
                }

                // No default dataset configured
                Err(AuthError::bad_request(
                    "X-Dataset-ID header required (tenant has no default dataset)",
                ))
            }
        }
    }

    /// Hash an API key using SHA-256
    fn hash_api_key(api_key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(api_key.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiKeyConfig, DatasetConfig};

    #[test]
    fn test_hash_api_key() {
        let key = "test-api-key-12345";
        let hash1 = Authenticator::hash_api_key(key);
        let hash2 = Authenticator::hash_api_key(key);

        // Hashes should be deterministic
        assert_eq!(hash1, hash2);

        // Hash should be 64 hex characters (SHA-256)
        assert_eq!(hash1.len(), 64);

        // Different keys should have different hashes
        let different_hash = Authenticator::hash_api_key("different-key");
        assert_ne!(hash1, different_hash);
    }

    #[tokio::test]
    async fn test_config_based_authentication() {
        // Setup
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());

        let auth_config = AuthConfig {
            enabled: true,
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![
                    DatasetConfig {
                        id: "production".to_string(),
                        slug: "production".to_string(),
                        is_default: true,
                    },
                    DatasetConfig {
                        id: "staging".to_string(),
                        slug: "staging".to_string(),
                        is_default: false,
                    },
                ],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: Some("prod-key".to_string()),
                }],
                schema_config: None,
            }],
        };

        let authenticator = Authenticator::new(auth_config, catalog);

        // Test successful authentication with default dataset
        let result = authenticator
            .authenticate("test-key-123", "acme", None)
            .await;
        assert!(result.is_ok());
        let ctx = result.unwrap();
        assert_eq!(ctx.tenant_id, "acme");
        assert_eq!(ctx.dataset_id, "production");
        assert_eq!(ctx.tenant_slug, "acme");
        assert_eq!(ctx.dataset_slug, "production");
        assert_eq!(ctx.api_key_name, Some("prod-key".to_string()));
        assert_eq!(ctx.source, TenantSource::Config);

        // Test successful authentication with explicit dataset
        let result = authenticator
            .authenticate("test-key-123", "acme", Some("staging"))
            .await;
        assert!(result.is_ok());
        let ctx = result.unwrap();
        assert_eq!(ctx.dataset_id, "staging");
        assert_eq!(ctx.dataset_slug, "staging");
    }

    #[tokio::test]
    async fn test_invalid_api_key() {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
            enabled: true,
            tenants: vec![],
        };
        let authenticator = Authenticator::new(auth_config, catalog);

        let result = authenticator
            .authenticate("invalid-key", "acme", None)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 401);
    }

    #[tokio::test]
    async fn test_tenant_mismatch() {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
            enabled: true,
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![DatasetConfig {
                    id: "production".to_string(),
                    slug: "production".to_string(),
                    is_default: true,
                }],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: None,
                }],
                schema_config: None,
            }],
        };
        let authenticator = Authenticator::new(auth_config, catalog);

        // Try to use acme's API key for different tenant
        let result = authenticator
            .authenticate("test-key-123", "evil-corp", None)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 403);
        assert!(err.message.contains("does not belong to tenant"));
    }

    #[tokio::test]
    async fn test_invalid_dataset() {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
            enabled: true,
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![DatasetConfig {
                    id: "production".to_string(),
                    slug: "production".to_string(),
                    is_default: true,
                }],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: None,
                }],
                schema_config: None,
            }],
        };
        let authenticator = Authenticator::new(auth_config, catalog);

        let result = authenticator
            .authenticate("test-key-123", "acme", Some("nonexistent"))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 403);
        assert!(err.message.contains("Dataset"));
    }

    #[tokio::test]
    async fn test_missing_default_dataset() {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
            enabled: true,
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: None,
                datasets: vec![],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: None,
                }],
                schema_config: None,
            }],
        };
        let authenticator = Authenticator::new(auth_config, catalog);

        let result = authenticator
            .authenticate("test-key-123", "acme", None)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code, 400);
        assert!(err.message.contains("X-Dataset-ID header required"));
    }
}
