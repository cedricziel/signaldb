//! Core authentication logic for multi-tenancy
//!
//! This module provides the Authenticator which handles API key validation,
//! tenant resolution, and dataset resolution across config-based and database-based tenants.

use super::{AuthError, TenantContext, TenantSource, UserIdentity};
use crate::auth::password::hash_session_token;
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

            return self.resolve_config_context(tenant_config, dataset_id, api_key_name.clone());
        }

        // Fall back to database-based authentication
        self.authenticate_from_database(&key_hash, tenant_id, dataset_id)
            .await
    }

    /// Authenticate a browser request carrying a user session token
    /// (`sdbs_`-prefixed cookie value) instead of an API key.
    ///
    /// Validates the token against the catalog's `user_sessions` table
    /// (revoked/expired sessions and disabled users are already excluded by
    /// the lookup), then resolves the tenant from the user's memberships:
    ///
    /// - An explicit `tenant_id` (from `X-Tenant-ID`) must match one of the
    ///   user's memberships, otherwise 403.
    /// - Without an explicit tenant, a user with exactly one membership uses
    ///   it implicitly; multiple memberships require the header (400); no
    ///   memberships is 403.
    ///
    /// Dataset resolution then follows the same rules as API-key
    /// authentication for the resolved tenant (explicit `dataset_id`
    /// validated against the tenant's datasets, else the tenant default).
    ///
    /// The returned [`TenantContext`] carries the [`UserIdentity`]
    /// (user id, email, membership role) for downstream handlers.
    pub async fn authenticate_user_session(
        &self,
        raw_token: &str,
        tenant_id: Option<&str>,
        dataset_id: Option<&str>,
    ) -> Result<TenantContext, AuthError> {
        let token_hash = hash_session_token(raw_token);
        let session = self
            .catalog
            .get_valid_session(&token_hash)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::unauthorized("Invalid or expired session"))?;

        let user = self
            .catalog
            .get_user(&session.user_id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::unauthorized("Invalid or expired session"))?;

        let memberships = self
            .catalog
            .list_memberships_for_user(&user.id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?;

        let membership = match tenant_id {
            Some(requested) => memberships
                .iter()
                .find(|m| m.tenant_id == requested)
                .ok_or_else(|| {
                    AuthError::forbidden(format!("User is not a member of tenant '{requested}'"))
                })?,
            None => match memberships.as_slice() {
                [only] => only,
                [] => {
                    return Err(AuthError::forbidden("User has no tenant memberships"));
                }
                _ => {
                    return Err(AuthError::bad_request(
                        "X-Tenant-ID header required (user belongs to multiple tenants)",
                    ));
                }
            },
        };

        let context = self
            .resolve_tenant_context(&membership.tenant_id, dataset_id, None)
            .await?;

        Ok(context.with_user(UserIdentity {
            user_id: user.id,
            email: user.email,
            role: membership.role,
        }))
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

        self.resolve_database_context(tenant_id, dataset_id, api_key_name)
            .await
    }

    /// Resolve a [`TenantContext`] for an already-authorized tenant,
    /// mirroring the `authenticate` precedence: config-defined tenants
    /// first, then catalog (database) tenants.
    ///
    /// Callers must have verified the principal's right to act as
    /// `tenant_id` (API-key ownership or tenant membership) beforehand.
    async fn resolve_tenant_context(
        &self,
        tenant_id: &str,
        dataset_id: Option<&str>,
        api_key_name: Option<String>,
    ) -> Result<TenantContext, AuthError> {
        if let Some(tenant_config) = self.config_tenants.get(tenant_id) {
            return self.resolve_config_context(tenant_config, dataset_id, api_key_name);
        }
        self.resolve_database_context(tenant_id, dataset_id, api_key_name)
            .await
    }

    /// Resolve dataset and slugs for a config-defined tenant.
    fn resolve_config_context(
        &self,
        tenant_config: &TenantConfig,
        dataset_id: Option<&str>,
        api_key_name: Option<String>,
    ) -> Result<TenantContext, AuthError> {
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

        Ok(TenantContext::new(
            tenant_config.id.clone(),
            resolved_dataset,
            tenant_slug,
            dataset_slug,
            api_key_name,
            TenantSource::Config,
        ))
    }

    /// Resolve dataset and slugs for a database-defined tenant.
    async fn resolve_database_context(
        &self,
        tenant_id: &str,
        dataset_id: Option<&str>,
        api_key_name: Option<String>,
    ) -> Result<TenantContext, AuthError> {
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
    pub fn hash_api_key(api_key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(api_key.as_bytes());
        hex::encode(hasher.finalize())
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
                        storage: None,
                    },
                    DatasetConfig {
                        id: "staging".to_string(),
                        slug: "staging".to_string(),
                        is_default: false,
                        storage: None,
                    },
                ],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: Some("prod-key".to_string()),
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
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
            tenants: vec![],
            ..Default::default()
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
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![DatasetConfig {
                    id: "production".to_string(),
                    slug: "production".to_string(),
                    is_default: true,
                    storage: None,
                }],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: None,
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
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
            tenants: vec![TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme Corp".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![DatasetConfig {
                    id: "production".to_string(),
                    slug: "production".to_string(),
                    is_default: true,
                    storage: None,
                }],
                api_keys: vec![ApiKeyConfig {
                    key: "test-key-123".to_string(),
                    name: None,
                }],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
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

    mod user_sessions {
        use super::*;
        use crate::auth::password::generate_session_token;
        use crate::catalog::MembershipRole;
        use chrono::{Duration, Utc};

        /// Seed a database tenant with one dataset (set as default).
        async fn seed_tenant(catalog: &Catalog, tenant_id: &str, dataset: &str) {
            catalog
                .upsert_tenant(tenant_id, tenant_id, Some(dataset), "database")
                .await
                .unwrap();
            catalog.create_dataset(tenant_id, dataset).await.unwrap();
        }

        /// Seed a user with memberships and a valid session; returns the
        /// raw session token.
        async fn seed_user_with_session(
            catalog: &Catalog,
            email: &str,
            memberships: &[(&str, MembershipRole)],
        ) -> (String, String) {
            // Password hash is irrelevant for session auth; avoid Argon2 cost.
            let user = catalog
                .create_user(email, None, "unused-hash", false)
                .await
                .unwrap();
            for (tenant_id, role) in memberships {
                catalog
                    .upsert_tenant_membership(&user.id, tenant_id, *role)
                    .await
                    .unwrap();
            }
            let token = generate_session_token();
            catalog
                .create_user_session(
                    &user.id,
                    &hash_session_token(&token),
                    Utc::now() + Duration::hours(24),
                )
                .await
                .unwrap();
            (user.id, token)
        }

        fn empty_authenticator(catalog: Arc<Catalog>) -> Authenticator {
            Authenticator::new(AuthConfig::default(), catalog)
        }

        #[tokio::test]
        async fn single_membership_resolves_implicitly_with_user_identity() {
            let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
            seed_tenant(&catalog, "acme", "production").await;
            let (user_id, token) = seed_user_with_session(
                &catalog,
                "Alice@Example.com",
                &[("acme", MembershipRole::Member)],
            )
            .await;
            let auth = empty_authenticator(catalog);

            let ctx = auth
                .authenticate_user_session(&token, None, None)
                .await
                .unwrap();
            assert_eq!(ctx.tenant_id, "acme");
            assert_eq!(ctx.dataset_id, "production");
            // DB tenants use IDs as slugs, like API-key auth.
            assert_eq!(ctx.tenant_slug, "acme");
            assert_eq!(ctx.dataset_slug, "production");
            assert_eq!(ctx.source, TenantSource::Database);
            let user = ctx.user.expect("user identity present");
            assert_eq!(user.user_id, user_id);
            assert_eq!(user.email, "alice@example.com");
            assert_eq!(user.role, MembershipRole::Member);
        }

        #[tokio::test]
        async fn explicit_tenant_must_match_a_membership() {
            let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
            seed_tenant(&catalog, "acme", "production").await;
            seed_tenant(&catalog, "globex", "main").await;
            let (_, token) = seed_user_with_session(
                &catalog,
                "a@example.com",
                &[("acme", MembershipRole::Admin)],
            )
            .await;
            let auth = empty_authenticator(catalog);

            // Member tenant works with the explicit header.
            let ctx = auth
                .authenticate_user_session(&token, Some("acme"), None)
                .await
                .unwrap();
            assert_eq!(ctx.tenant_id, "acme");
            assert_eq!(ctx.user.unwrap().role, MembershipRole::Admin);

            // Non-member tenant is forbidden.
            let err = auth
                .authenticate_user_session(&token, Some("globex"), None)
                .await
                .unwrap_err();
            assert_eq!(err.status_code, 403);
            assert!(err.message.contains("not a member"));
        }

        #[tokio::test]
        async fn multiple_memberships_require_tenant_header() {
            let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
            seed_tenant(&catalog, "acme", "production").await;
            seed_tenant(&catalog, "globex", "main").await;
            let (_, token) = seed_user_with_session(
                &catalog,
                "a@example.com",
                &[
                    ("acme", MembershipRole::Member),
                    ("globex", MembershipRole::Viewer),
                ],
            )
            .await;
            let auth = empty_authenticator(catalog);

            let err = auth
                .authenticate_user_session(&token, None, None)
                .await
                .unwrap_err();
            assert_eq!(err.status_code, 400);
            assert!(err.message.contains("X-Tenant-ID"));

            // Selecting one explicitly works and carries that tenant's role.
            let ctx = auth
                .authenticate_user_session(&token, Some("globex"), None)
                .await
                .unwrap();
            assert_eq!(ctx.tenant_id, "globex");
            assert_eq!(ctx.dataset_id, "main");
            assert_eq!(ctx.user.unwrap().role, MembershipRole::Viewer);
        }

        #[tokio::test]
        async fn user_without_memberships_is_forbidden() {
            let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
            let (_, token) = seed_user_with_session(&catalog, "a@example.com", &[]).await;
            let auth = empty_authenticator(catalog);

            let err = auth
                .authenticate_user_session(&token, None, None)
                .await
                .unwrap_err();
            assert_eq!(err.status_code, 403);
        }

        #[tokio::test]
        async fn expired_revoked_or_unknown_sessions_are_unauthorized() {
            let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
            seed_tenant(&catalog, "acme", "production").await;
            let user = catalog
                .create_user("a@example.com", None, "unused-hash", false)
                .await
                .unwrap();
            catalog
                .upsert_tenant_membership(&user.id, "acme", MembershipRole::Member)
                .await
                .unwrap();

            // Expired session.
            let expired_token = generate_session_token();
            catalog
                .create_user_session(
                    &user.id,
                    &hash_session_token(&expired_token),
                    Utc::now() - Duration::hours(1),
                )
                .await
                .unwrap();

            // Revoked session.
            let revoked_token = generate_session_token();
            let revoked = catalog
                .create_user_session(
                    &user.id,
                    &hash_session_token(&revoked_token),
                    Utc::now() + Duration::hours(24),
                )
                .await
                .unwrap();
            catalog.revoke_session(&revoked.id).await.unwrap();

            let auth = empty_authenticator(catalog);
            for token in [
                expired_token,
                revoked_token,
                generate_session_token(), // never stored
            ] {
                let err = auth
                    .authenticate_user_session(&token, None, None)
                    .await
                    .unwrap_err();
                assert_eq!(err.status_code, 401);
                assert!(err.message.contains("session"));
            }
        }

        #[tokio::test]
        async fn config_tenant_membership_uses_config_slugs_and_datasets() {
            let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
            // Memberships FK to `tenants`, so config tenants must exist as
            // rows too — which is what `sync_config_tenants` does at startup.
            catalog
                .upsert_tenant("acme", "Acme Corp", Some("production"), "config")
                .await
                .unwrap();
            let (_, token) = seed_user_with_session(
                &catalog,
                "a@example.com",
                &[("acme", MembershipRole::Member)],
            )
            .await;
            let auth_config = AuthConfig {
                tenants: vec![TenantConfig {
                    id: "acme".to_string(),
                    slug: "acme-slug".to_string(),
                    name: "Acme Corp".to_string(),
                    default_dataset: Some("production".to_string()),
                    datasets: vec![DatasetConfig {
                        id: "production".to_string(),
                        slug: "prod-slug".to_string(),
                        is_default: true,
                        storage: None,
                    }],
                    api_keys: vec![],
                    schema_config: None,
                    limits: None,
                }],
                ..Default::default()
            };
            let auth = Authenticator::new(auth_config, catalog);

            let ctx = auth
                .authenticate_user_session(&token, None, None)
                .await
                .unwrap();
            assert_eq!(ctx.tenant_id, "acme");
            assert_eq!(ctx.tenant_slug, "acme-slug");
            assert_eq!(ctx.dataset_id, "production");
            assert_eq!(ctx.dataset_slug, "prod-slug");
            assert_eq!(ctx.source, TenantSource::Config);

            // An unknown dataset for the config tenant is rejected the same
            // way as for API-key auth.
            let err = auth
                .authenticate_user_session(&token, None, Some("nope"))
                .await
                .unwrap_err();
            assert_eq!(err.status_code, 403);
            assert!(err.message.contains("Dataset"));
        }
    }

    #[tokio::test]
    async fn test_missing_default_dataset() {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        let auth_config = AuthConfig {
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
                limits: None,
            }],
            ..Default::default()
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
