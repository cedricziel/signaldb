//! Core authentication logic for multi-tenancy
//!
//! This module provides the Authenticator which handles API key validation,
//! tenant resolution, and dataset resolution across config-based and database-based tenants.

use super::oauth::hash_oauth_token;
use super::{AuthError, TenantContext, TenantSource, UserContext, hash_session_token};
use crate::catalog::{Catalog, MembershipRole, UserRecord};
use crate::config::{AuthConfig, TenantConfig};
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
    /// The configured MCP resource URL, if any. OAuth access tokens are
    /// audience-bound to it (change: mcp-oauth-dcr).
    mcp_resource: Option<String>,
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
            mcp_resource: None,
        }
    }

    /// Set the MCP resource URL that OAuth access tokens must be audience-bound
    /// to (from `mcp.oauth.resource_url`). Used by
    /// [`authenticate_oauth_token`](Self::authenticate_oauth_token) via
    /// [`mcp_resource`](Self::mcp_resource).
    pub fn with_mcp_resource(mut self, resource: Option<String>) -> Self {
        self.mcp_resource = resource;
        self
    }

    /// The configured MCP resource URL OAuth tokens are bound to, if any.
    pub fn mcp_resource(&self) -> Option<&str> {
        self.mcp_resource.as_deref()
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

    /// Authenticate an opaque browser session and resolve its requested
    /// tenant/dataset through the user's tenant membership.
    pub async fn authenticate_session(
        &self,
        token: &str,
        tenant_id: &str,
        dataset_id: Option<&str>,
    ) -> Result<TenantContext, AuthError> {
        let session = self
            .catalog
            .get_valid_session(&hash_session_token(token))
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::unauthorized("Invalid or expired session"))?;
        let user = self
            .catalog
            .get_user(&session.user_id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::unauthorized("Session user not found"))?;
        let role = self.resolve_role(&user, tenant_id).await?;

        self.resolve_user_tenant(
            tenant_id,
            dataset_id,
            user.id,
            role,
            user.is_instance_admin,
            Some(session.id),
        )
        .await
    }

    /// Resolve a user's role within a tenant: their explicit membership role,
    /// or `Admin` for instance admins with no membership row. Instance admins
    /// can act on any tenant without being explicitly added to it.
    async fn resolve_role(
        &self,
        user: &UserRecord,
        tenant_id: &str,
    ) -> Result<MembershipRole, AuthError> {
        let membership = self
            .catalog
            .get_tenant_membership(&user.id, tenant_id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?;
        match membership {
            Some(membership) => Ok(membership.role),
            None if user.is_instance_admin => Ok(MembershipRole::Admin),
            None => Err(AuthError::forbidden(format!(
                "User is not a member of tenant '{tenant_id}'"
            ))),
        }
    }

    /// Authenticate an opaque OAuth 2.1 access token (change: mcp-oauth-dcr).
    ///
    /// The tenant and scopes come from the **token record**, never from an
    /// `X-Tenant-ID` header or a tool argument — an OAuth session cannot be
    /// pointed at a tenant it was not granted. `expected_resource` is this
    /// deployment's configured MCP resource URL; when both it and the token's
    /// recorded audience are present they must match (RFC 8707), so a token
    /// minted for another resource is rejected. An expired or revoked token
    /// is not found and surfaces as unauthorized.
    pub async fn authenticate_oauth_token(
        &self,
        access_token: &str,
        dataset_id: Option<&str>,
        expected_resource: Option<&str>,
    ) -> Result<TenantContext, AuthError> {
        let record = self
            .catalog
            .get_valid_access_token(&hash_oauth_token(access_token))
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::unauthorized("Invalid or expired access token"))?;

        // Audience binding (RFC 8707). When this deployment declares an MCP
        // resource, every token must be bound to exactly it — a token carrying
        // a different audience, or none at all, is refused. A token bound to a
        // resource is likewise refused where no resource is declared. Only when
        // neither side names a resource is the check skipped.
        let audience_ok = match (record.resource.as_deref(), expected_resource) {
            (Some(audience), Some(expected)) => audience == expected,
            (None, None) => true,
            _ => false,
        };
        if !audience_ok {
            return Err(AuthError::unauthorized(
                "access token audience does not match this resource",
            ));
        }

        let user = self
            .catalog
            .get_user(&record.user_id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::unauthorized("Access token user not found"))?;

        // Tenant is fixed by the token; the user's role in that tenant still
        // gates what the token may do.
        let role = self.resolve_role(&user, &record.tenant_id).await?;

        let context = self
            .resolve_user_tenant(
                &record.tenant_id,
                dataset_id,
                user.id,
                role,
                user.is_instance_admin,
                None,
            )
            .await?;
        // The OAuth grant's scopes are enforced exactly like API-key scopes.
        Ok(context.with_api_key_restrictions(Some(record.scopes), None))
    }

    /// Resolve an instance administrator from an opaque browser session.
    pub async fn authenticate_instance_admin_session(
        &self,
        token: &str,
    ) -> Result<UserContext, AuthError> {
        let session = self
            .catalog
            .get_valid_session(&hash_session_token(token))
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::unauthorized("Invalid or expired session"))?;
        let user = self
            .catalog
            .get_user(&session.user_id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::unauthorized("Session user not found"))?;
        if !user.is_instance_admin {
            return Err(AuthError::forbidden(
                "Instance administrator privileges required",
            ));
        }
        Ok(UserContext {
            user_id: user.id,
            email: user.email,
            is_instance_admin: true,
            session_id: session.id,
        })
    }

    async fn resolve_user_tenant(
        &self,
        tenant_id: &str,
        dataset_id: Option<&str>,
        user_id: String,
        role: MembershipRole,
        is_instance_admin: bool,
        session_id: Option<String>,
    ) -> Result<TenantContext, AuthError> {
        if let Some(tenant_config) = self.config_tenants.get(tenant_id) {
            let resolved_dataset = match self.resolve_dataset(tenant_config, dataset_id) {
                Ok(dataset) => dataset,
                Err(error) if dataset_id.is_some() => {
                    let requested = dataset_id.expect("guarded by is_some");
                    let catalog_datasets = self
                        .catalog
                        .get_datasets(tenant_id)
                        .await
                        .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?;
                    if catalog_datasets
                        .iter()
                        .any(|dataset| dataset.name == requested)
                    {
                        requested.to_string()
                    } else {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            };
            let dataset_slug = tenant_config
                .datasets
                .iter()
                .find(|d| d.id == resolved_dataset)
                .map(|d| d.slug.clone())
                .unwrap_or_else(|| resolved_dataset.clone());
            return Ok(TenantContext::new(
                tenant_id.to_string(),
                resolved_dataset,
                tenant_config.slug.clone(),
                dataset_slug,
                None,
                TenantSource::Config,
            )
            .with_user(user_id, role, is_instance_admin, session_id));
        }

        let tenant = self
            .catalog
            .get_tenant(tenant_id)
            .await
            .map_err(|e| AuthError::unauthorized(format!("Database error: {e}")))?
            .ok_or_else(|| AuthError::forbidden(format!("Tenant '{tenant_id}' not found")))?;
        let resolved_dataset = match dataset_id {
            Some(id) => id.to_string(),
            None => tenant.default_dataset.ok_or_else(|| {
                AuthError::bad_request(
                    "X-Dataset-ID header required (tenant has no default dataset)",
                )
            })?,
        };
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
        Ok(TenantContext::new(
            tenant_id.to_string(),
            resolved_dataset.clone(),
            tenant_id.to_string(),
            resolved_dataset,
            None,
            TenantSource::Database,
        )
        .with_user(user_id, role, is_instance_admin, session_id))
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

        let api_key = validation_result
            .ok_or_else(|| AuthError::unauthorized("Invalid or revoked API key"))?;

        // Verify tenant ID matches
        if api_key.tenant_id != tenant_id {
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
        if let (Some(bound), Some(requested)) = (&api_key.dataset_id, dataset_id)
            && bound != requested
        {
            return Err(AuthError::forbidden(format!(
                "API key is restricted to dataset '{bound}'"
            )));
        }
        let resolved_dataset = match dataset_id.or(api_key.dataset_id.as_deref()) {
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
            api_key.name,
            TenantSource::Database,
        )
        .with_api_key_restrictions(api_key.scopes, api_key.dataset_id))
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
        super::sha256_hex(api_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::oauth::{TokenKind, generate_oauth_token, hash_oauth_token};
    use crate::config::{ApiKeyConfig, DatasetConfig};
    use chrono::{Duration, Utc};

    /// An authenticator over a database tenant `acme` with dataset `production`,
    /// a member user, and a stored access token for that user/tenant. Returns
    /// the authenticator and the raw access token.
    async fn oauth_authenticator(
        scopes: &[String],
        resource: Option<&str>,
        expires_at: chrono::DateTime<Utc>,
    ) -> (Authenticator, String) {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        catalog.create_dataset("acme", "production").await.unwrap();
        let user = catalog
            .create_user("agent@example.com", None, Some("phc"), false)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user.id, "acme", MembershipRole::Member)
            .await
            .unwrap();
        let raw = generate_oauth_token(TokenKind::Access);
        catalog
            .create_access_token(
                &hash_oauth_token(&raw),
                "client-1",
                &user.id,
                "acme",
                scopes,
                resource,
                expires_at,
            )
            .await
            .unwrap();
        (Authenticator::new(AuthConfig::default(), catalog), raw)
    }

    #[tokio::test]
    async fn oauth_token_resolves_tenant_and_scopes_from_the_token() {
        let scopes = vec!["traces:read".to_string(), "logs:read".to_string()];
        let (auth, token) = oauth_authenticator(
            &scopes,
            Some("https://signaldb.example.com/mcp"),
            Utc::now() + Duration::hours(1),
        )
        .await;

        let ctx = auth
            .authenticate_oauth_token(&token, None, Some("https://signaldb.example.com/mcp"))
            .await
            .expect("valid token authenticates");
        // Tenant comes from the token; scopes are enforced like API-key scopes.
        assert_eq!(ctx.tenant_id, "acme");
        assert_eq!(ctx.dataset_id, "production");
        assert_eq!(ctx.api_key_scopes, Some(scopes));
        assert!(ctx.can_read("traces"));
        assert!(!ctx.can_read("metrics"));
        // No browser session backs an OAuth context.
        assert_eq!(ctx.session_id, None);
    }

    #[tokio::test]
    async fn oauth_token_with_wrong_audience_is_rejected() {
        let (auth, token) = oauth_authenticator(
            &["traces:read".to_string()],
            Some("https://signaldb.example.com/mcp"),
            Utc::now() + Duration::hours(1),
        )
        .await;
        let err = auth
            .authenticate_oauth_token(&token, None, Some("https://other.example.com/mcp"))
            .await
            .expect_err("audience mismatch is rejected");
        assert_eq!(err.status_code, 401);
    }

    #[tokio::test]
    async fn oauth_token_without_audience_is_rejected_when_resource_configured() {
        // A deployment that declares a resource must refuse a token that carries
        // no audience — it cannot have been minted for this resource.
        let (auth, token) = oauth_authenticator(
            &["traces:read".to_string()],
            None,
            Utc::now() + Duration::hours(1),
        )
        .await;
        let err = auth
            .authenticate_oauth_token(&token, None, Some("https://signaldb.example.com/mcp"))
            .await
            .expect_err("unbound token is rejected where a resource is configured");
        assert_eq!(err.status_code, 401);
    }

    #[tokio::test]
    async fn expired_oauth_token_is_unauthorized() {
        let (auth, token) = oauth_authenticator(
            &["traces:read".to_string()],
            Some("https://signaldb.example.com/mcp"),
            Utc::now() - Duration::seconds(1),
        )
        .await;
        let err = auth
            .authenticate_oauth_token(&token, None, Some("https://signaldb.example.com/mcp"))
            .await
            .expect_err("expired token is rejected");
        assert_eq!(err.status_code, 401);
    }

    #[tokio::test]
    async fn unknown_oauth_token_is_unauthorized() {
        let (auth, _token) = oauth_authenticator(
            &["traces:read".to_string()],
            Some("https://signaldb.example.com/mcp"),
            Utc::now() + Duration::hours(1),
        )
        .await;
        let err = auth
            .authenticate_oauth_token("sdb_at_nonexistent", None, None)
            .await
            .expect_err("unknown token is rejected");
        assert_eq!(err.status_code, 401);
    }

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

    #[tokio::test]
    async fn database_api_key_enforces_dataset_and_signal_scopes() {
        let catalog = Arc::new(Catalog::new("sqlite::memory:").await.unwrap());
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        catalog.create_dataset("acme", "production").await.unwrap();
        catalog.create_dataset("acme", "staging").await.unwrap();
        let raw_key = "sdbk_scoped";
        let scopes = vec!["metrics:write".to_string()];
        catalog
            .upsert_scoped_api_key(
                "acme",
                &Authenticator::hash_api_key(raw_key),
                Some("metrics"),
                Some("production"),
                Some(&scopes),
                Some("user-1"),
            )
            .await
            .unwrap();
        let authenticator = Authenticator::new(AuthConfig::default(), Arc::clone(&catalog));

        let context = authenticator
            .authenticate(raw_key, "acme", None)
            .await
            .unwrap();
        assert_eq!(context.dataset_id, "production");
        assert!(context.can_ingest("metrics"));
        assert!(!context.can_ingest("logs"));

        let error = authenticator
            .authenticate(raw_key, "acme", Some("staging"))
            .await
            .unwrap_err();
        assert_eq!(error.status_code, 403);
        assert!(error.message.contains("restricted to dataset"));
    }
}
