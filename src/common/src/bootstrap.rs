//! # First-boot bootstrap
//!
//! Auto-provisions a default tenant when SignalDB starts with no tenants at
//! all — none configured via `[[auth.tenants]]` and none present in the
//! catalog. This gives zero-config deployments a working ingest path out of
//! the box: the monolith prints the generated API key once at startup.
//!
//! The self-monitoring tenant (auto-injected by
//! [`Configuration::ensure_self_monitoring_tenant`](crate::config::Configuration::ensure_self_monitoring_tenant))
//! does not count as "configured": it exists for SignalDB's own telemetry,
//! not for user data.
//!
//! Guardrails:
//! - Runs only on truly-empty state; any user-defined tenant in config or
//!   catalog (including tenants created via the admin API) disables it.
//! - On the second boot the `default` tenant already exists in the catalog,
//!   so bootstrap is a no-op and the key is never re-printed.
//! - The key is stored hashed (same format as config-tenant sync); the
//!   plaintext is returned to the caller exactly once.

use crate::auth::Authenticator;
use crate::catalog::Catalog;
use crate::config::Configuration;
use uuid::Uuid;

/// Tenant ID provisioned on first boot.
pub const DEFAULT_TENANT_ID: &str = "default";
/// Dataset ID provisioned on first boot.
pub const DEFAULT_DATASET_ID: &str = "default";

/// Auto-provision the `default` tenant (with a `default` dataset and a fresh
/// API key) when no tenants exist in config or catalog.
///
/// Returns `Ok(Some(plaintext_key))` when provisioning happened; the caller
/// is responsible for printing the key once. Returns `Ok(None)` when any
/// tenant other than the self-monitoring tenant already exists (in config or
/// catalog), leaving existing deployments untouched.
///
/// # Errors
///
/// Returns an error if catalog reads or writes fail.
pub async fn bootstrap_default_tenant(
    catalog: &Catalog,
    config: &Configuration,
) -> Result<Option<String>, sqlx::Error> {
    let self_monitoring_tenant = &config.self_monitoring.tenant_id;

    // Any user-defined tenant in config disables bootstrap.
    if config
        .auth
        .tenants
        .iter()
        .any(|t| &t.id != self_monitoring_tenant)
    {
        return Ok(None);
    }

    // Any user-defined tenant in the catalog (admin API, previous bootstrap)
    // disables it too — this also makes second boots a no-op.
    if catalog
        .list_tenants()
        .await?
        .iter()
        .any(|t| &t.id != self_monitoring_tenant)
    {
        return Ok(None);
    }

    // Source "database" — same as admin-API-provisioned tenants (the
    // tenants.source column only allows 'config' or 'database').
    catalog
        .upsert_tenant(
            DEFAULT_TENANT_ID,
            "Default Tenant",
            Some(DEFAULT_DATASET_ID),
            "database",
        )
        .await?;
    catalog
        .create_dataset(DEFAULT_TENANT_ID, DEFAULT_DATASET_ID)
        .await?;

    // Same key shape as the admin API (`sk-{tenant}-{uuid_v4}`); Uuid::new_v4
    // draws from the OS CSPRNG. Stored hashed, exactly like config-tenant sync.
    let raw_key = format!("sk-{DEFAULT_TENANT_ID}-{}", Uuid::new_v4());
    let key_hash = Authenticator::hash_api_key(&raw_key);
    catalog
        .upsert_api_key(DEFAULT_TENANT_ID, &key_hash, Some("Bootstrap Key"))
        .await?;

    Ok(Some(raw_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiKeyConfig, AuthConfig, DatasetConfig, TenantConfig};
    use std::sync::Arc;

    fn tenant_config(id: &str) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: id.to_string(),
            default_dataset: Some("main".to_string()),
            datasets: vec![DatasetConfig {
                id: "main".to_string(),
                slug: "main".to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![ApiKeyConfig {
                key: format!("sk-{id}-test"),
                name: Some("test".to_string()),
            }],
            schema_config: None,
            limits: None,
        }
    }

    #[tokio::test]
    async fn empty_state_provisions_default_tenant_with_working_key() {
        let catalog = Catalog::new_in_memory().await.unwrap();
        let config = Configuration::default();

        let key = bootstrap_default_tenant(&catalog, &config)
            .await
            .unwrap()
            .expect("empty state must provision a key");
        assert!(key.starts_with("sk-default-"));

        // Tenant and dataset exist in the catalog.
        let tenant = catalog
            .get_tenant(DEFAULT_TENANT_ID)
            .await
            .unwrap()
            .expect("default tenant must exist");
        assert_eq!(tenant.default_dataset.as_deref(), Some(DEFAULT_DATASET_ID));
        assert_eq!(tenant.source, "database");
        let datasets = catalog.get_datasets(DEFAULT_TENANT_ID).await.unwrap();
        assert!(datasets.iter().any(|d| d.name == DEFAULT_DATASET_ID));

        // The key is stored hashed: it is only found under its hash, never
        // under the plaintext.
        let keys = catalog.list_api_keys(DEFAULT_TENANT_ID).await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].name.as_deref(), Some("Bootstrap Key"));
        assert!(
            catalog
                .validate_api_key(&Authenticator::hash_api_key(&key))
                .await
                .unwrap()
                .is_some()
        );
        assert!(catalog.validate_api_key(&key).await.unwrap().is_none());

        // The key authenticates through the normal auth path.
        let authenticator = Authenticator::new(AuthConfig::default(), Arc::new(catalog));
        let ctx = authenticator
            .authenticate(&key, DEFAULT_TENANT_ID, None)
            .await
            .expect("bootstrap key must authenticate");
        assert_eq!(ctx.tenant_id, DEFAULT_TENANT_ID);
        assert_eq!(ctx.dataset_id, DEFAULT_DATASET_ID);
    }

    #[tokio::test]
    async fn config_tenant_disables_bootstrap() {
        let catalog = Catalog::new_in_memory().await.unwrap();
        let mut config = Configuration::default();
        config.auth.tenants.push(tenant_config("acme"));

        let key = bootstrap_default_tenant(&catalog, &config).await.unwrap();
        assert!(key.is_none());
        assert!(catalog.list_tenants().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn catalog_tenant_disables_bootstrap() {
        let catalog = Catalog::new_in_memory().await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("main"), "database")
            .await
            .unwrap();
        let config = Configuration::default();

        let key = bootstrap_default_tenant(&catalog, &config).await.unwrap();
        assert!(key.is_none());
        assert!(
            catalog
                .get_tenant(DEFAULT_TENANT_ID)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn second_boot_is_noop_and_key_is_not_regenerated() {
        let catalog = Catalog::new_in_memory().await.unwrap();
        let config = Configuration::default();

        let first = bootstrap_default_tenant(&catalog, &config).await.unwrap();
        assert!(first.is_some());

        let second = bootstrap_default_tenant(&catalog, &config).await.unwrap();
        assert!(second.is_none(), "second boot must not reprint a key");

        let keys = catalog.list_api_keys(DEFAULT_TENANT_ID).await.unwrap();
        assert_eq!(keys.len(), 1, "second boot must not add another key");
    }

    #[tokio::test]
    async fn self_monitoring_tenant_does_not_count_as_configured() {
        let catalog = Catalog::new_in_memory().await.unwrap();
        let mut config = Configuration::default();
        // Simulate ensure_self_monitoring_tenant() having injected the
        // self-monitoring tenant, and a previous boot having synced it.
        let self_mon_id = config.self_monitoring.tenant_id.clone();
        config.auth.tenants.push(tenant_config(&self_mon_id));
        catalog
            .upsert_tenant(&self_mon_id, "System (Self-Monitoring)", None, "config")
            .await
            .unwrap();

        let key = bootstrap_default_tenant(&catalog, &config).await.unwrap();
        assert!(
            key.is_some(),
            "self-monitoring tenant alone must not disable bootstrap"
        );
    }
}
