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

use crate::catalog::Catalog;
use crate::config::{Configuration, StorageConfig};
use crate::iceberg::{self, create_catalog_with_config};

/// A dataset resolved from the tenant registry, source-agnostic.
///
/// Carries everything a consumer needs to register a catalog and locate
/// storage uniformly, regardless of whether the tenant came from config or
/// the database.
#[derive(Debug, Clone)]
pub struct ResolvedDataset {
    /// Dataset identifier as used on the read/write path (for database
    /// datasets this is the dataset *name*, matching the ingest resolver).
    pub id: String,
    /// URL-friendly slug used for the Iceberg namespace path.
    pub slug: String,
    /// Effective storage DSN (dataset → tenant → global fallback).
    pub storage_dsn: String,
    /// Whether this is the tenant's default dataset.
    pub is_default: bool,
}

/// A tenant resolved from the tenant registry, source-agnostic.
#[derive(Debug, Clone)]
pub struct ResolvedTenant {
    /// Tenant identifier.
    pub id: String,
    /// URL-friendly slug used for the DataFusion catalog / Iceberg namespace.
    pub slug: String,
    /// Default dataset identifier, if any.
    pub default_dataset: Option<String>,
    /// The tenant's datasets.
    pub datasets: Vec<ResolvedDataset>,
}

/// Global catalog manager holding the shared Iceberg catalog instance.
///
/// This ensures all SignalDB components use the same catalog for:
/// - Consistent table metadata
/// - Proper caching
/// - Avoiding race conditions
pub struct CatalogManager {
    catalog: Arc<dyn IcebergCatalog>,
    config: Configuration,
    table_manager: crate::iceberg::table_manager::IcebergTableManager,
    /// Optional database catalog used as the additional tenant source. When
    /// present, [`CatalogManager::list_active_tenants`] and
    /// [`CatalogManager::resolve_tenant_by_slug`] merge database-created
    /// tenants with the config-defined ones. When absent (pure in-memory /
    /// unit contexts), enumeration falls back to config only.
    tenant_source: Option<Arc<Catalog>>,
}

impl CatalogManager {
    /// Create a new catalog manager with the shared Iceberg catalog.
    pub async fn new(config: Configuration) -> Result<Self> {
        let catalog = create_catalog_with_config(&config).await?;
        let table_manager = crate::iceberg::table_manager::IcebergTableManager::new(
            catalog.clone(),
            config.writer.metadata_previous_versions_max,
        );
        Ok(Self {
            catalog,
            config,
            table_manager,
            tenant_source: None,
        })
    }

    /// Attach a database catalog as an additional tenant source.
    ///
    /// With a tenant source attached, the registry (`list_active_tenants` /
    /// `resolve_tenant_by_slug`) returns the union of config-defined and
    /// database-created tenants, so admin-API tenants are queryable and
    /// lifecycle-managed without a `[[auth.tenants]]` config block.
    pub fn with_tenant_source(mut self, tenant_source: Arc<Catalog>) -> Self {
        self.tenant_source = Some(tenant_source);
        self
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
        use std::sync::atomic::{AtomicU64, Ordering};
        static CATALOG_COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = CATALOG_COUNTER.fetch_add(1, Ordering::Relaxed);

        let mut config = Configuration::default();
        // Use a uniquely-named in-memory SQLite database so that concurrent test calls
        // each get an isolated catalog rather than sharing the unnamed global cache.
        config.schema.catalog_uri =
            format!("sqlite:file:signaldb_test_{id}?mode=memory&cache=shared");
        Self::new(config).await
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

    /// Build an Iceberg namespace for a tenant and dataset.
    pub fn build_namespace(
        &self,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<iceberg_rust::catalog::namespace::Namespace> {
        let tenant_slug = self.get_tenant_slug(tenant_id);
        let dataset_slug = self.get_dataset_slug(tenant_id, dataset_id);
        iceberg::names::build_namespace(&tenant_slug, &dataset_slug)
    }

    /// Build an Iceberg table identifier for a tenant, dataset, and table.
    pub fn build_table_identifier(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> iceberg_rust::catalog::identifier::Identifier {
        let tenant_slug = self.get_tenant_slug(tenant_id);
        let dataset_slug = self.get_dataset_slug(tenant_id, dataset_id);
        iceberg::names::build_table_identifier(&tenant_slug, &dataset_slug, table_name)
    }

    /// Build an object-store table location for a tenant, dataset, and table.
    pub fn build_table_location(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> String {
        let tenant_slug = self.get_tenant_slug(tenant_id);
        let dataset_slug = self.get_dataset_slug(tenant_id, dataset_id);
        iceberg::names::build_table_location(&tenant_slug, &dataset_slug, table_name)
    }

    /// Ensure an Iceberg table exists for the given tenant, dataset, and table name.
    /// Creates the table if it doesn't exist. Returns the loaded Table.
    pub async fn ensure_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<iceberg_rust::table::Table> {
        let tenant_slug = self.get_tenant_slug(tenant_id);
        let dataset_slug = self.get_dataset_slug(tenant_id, dataset_id);
        // Per-tenant materialized-label allowlists: a tenant schema
        // override replaces the global set wholesale.
        let labels = self
            .config
            .get_tenant_schema_config(tenant_id)
            .materialized_labels;
        self.table_manager
            .ensure_table(&tenant_slug, &dataset_slug, table_name, &labels)
            .await
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

    /// Build a source-agnostic descriptor for a config-defined tenant.
    fn config_tenant_descriptor(&self, tenant: &crate::config::TenantConfig) -> ResolvedTenant {
        let datasets = tenant
            .datasets
            .iter()
            .map(|d| ResolvedDataset {
                id: d.id.clone(),
                slug: d.slug.clone(),
                storage_dsn: self
                    .get_dataset_storage_config(&tenant.id, &d.id)
                    .dsn
                    .clone(),
                is_default: d.is_default,
            })
            .collect();
        ResolvedTenant {
            id: tenant.id.clone(),
            slug: tenant.slug.clone(),
            default_dataset: tenant.default_dataset.clone(),
            datasets,
        }
    }

    /// Build a source-agnostic descriptor for a database-created tenant.
    ///
    /// Database datasets carry no slug or storage override, so the slug is
    /// derived via the same functions the read/write path uses (identity for
    /// database tenants) and storage falls back to the tenant/global default.
    /// The dataset *name* — not the internal UUID — is the identifier the
    /// ingest resolver and namespace paths use, so it is used here too.
    fn db_tenant_descriptor(
        &self,
        record: &crate::catalog::TenantRecord,
        datasets: &[crate::catalog::DatasetRecord],
    ) -> ResolvedTenant {
        let resolved = datasets
            .iter()
            .map(|d| ResolvedDataset {
                id: d.name.clone(),
                slug: self.get_dataset_slug(&record.id, &d.name),
                storage_dsn: self
                    .get_dataset_storage_config(&record.id, &d.name)
                    .dsn
                    .clone(),
                is_default: record.default_dataset.as_deref() == Some(d.name.as_str()),
            })
            .collect();
        ResolvedTenant {
            id: record.id.clone(),
            slug: self.get_tenant_slug(&record.id),
            default_dataset: record.default_dataset.clone(),
            datasets: resolved,
        }
    }

    /// Merge database-only datasets (those added to a tenant at runtime, e.g.
    /// via the admin API) into an existing descriptor. Datasets already present
    /// (from config, keyed by name/id) keep their explicit values; new ones are
    /// appended with derived slug/storage.
    fn merge_db_datasets(
        &self,
        descriptor: &mut ResolvedTenant,
        tenant_id: &str,
        db_datasets: &[crate::catalog::DatasetRecord],
    ) {
        let existing: std::collections::HashSet<String> =
            descriptor.datasets.iter().map(|d| d.id.clone()).collect();
        for d in db_datasets {
            if existing.contains(&d.name) {
                continue;
            }
            descriptor.datasets.push(ResolvedDataset {
                id: d.name.clone(),
                slug: self.get_dataset_slug(tenant_id, &d.name),
                storage_dsn: self
                    .get_dataset_storage_config(tenant_id, &d.name)
                    .dsn
                    .clone(),
                // The config-defined default (if any) already governs; a
                // runtime-added dataset is not promoted to default here.
                is_default: false,
            });
        }
    }

    /// Enumerate all active tenants and datasets, source-agnostic.
    ///
    /// Returns the union of config-defined tenants (bootstrap seed, with their
    /// explicit slugs/storage overrides preserved) and database-created
    /// tenants (from the attached tenant source, if any). Datasets added to a
    /// config tenant at runtime are merged into its descriptor. Config-disabled
    /// tenants are excluded and never resurrected from the database, and a
    /// database tenant whose id collides with a *different* config tenant's slug
    /// is excluded to preserve tenant isolation. When no tenant source is
    /// attached, this is equivalent to the config-only enumeration.
    pub async fn list_active_tenants(&self) -> Result<Vec<ResolvedTenant>> {
        let mut tenants: Vec<ResolvedTenant> = self
            .get_enabled_tenants()
            .iter()
            .map(|t| self.config_tenant_descriptor(t))
            .collect();

        let Some(source) = &self.tenant_source else {
            return Ok(tenants);
        };

        // Index enabled config descriptors by tenant id so runtime datasets can
        // be merged into the right one.
        let index_by_id: std::collections::HashMap<String, usize> = tenants
            .iter()
            .enumerate()
            .map(|(i, t)| (t.id.clone(), i))
            .collect();
        // Every config tenant id (including disabled ones) — disabled config
        // tenants must NOT be re-added from the database copy that
        // `sync_config_tenants` writes.
        let config_ids: std::collections::HashSet<&str> = self
            .config
            .auth
            .tenants
            .iter()
            .map(|t| t.id.as_str())
            .collect();
        // Config slug -> id, to detect a database tenant id colliding with a
        // different config tenant's slug.
        let config_slug_to_id: std::collections::HashMap<&str, &str> = self
            .config
            .auth
            .tenants
            .iter()
            .map(|t| (t.slug.as_str(), t.id.as_str()))
            .collect();

        let records = source
            .list_tenants()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list database tenants: {e}"))?;
        for record in records {
            if let Some(&idx) = index_by_id.get(&record.id) {
                // Enabled config tenant: merge any datasets added at runtime.
                let db_datasets = source.get_datasets(&record.id).await.map_err(|e| {
                    anyhow::anyhow!("Failed to list datasets for tenant '{}': {e}", record.id)
                })?;
                self.merge_db_datasets(&mut tenants[idx], &record.id, &db_datasets);
                continue;
            }
            if config_ids.contains(record.id.as_str()) {
                // A config tenant that is disabled via schema_config; skip it.
                continue;
            }
            if let Some(&config_id) = config_slug_to_id.get(record.id.as_str()) {
                tracing::error!(
                    db_tenant_id = %record.id,
                    config_tenant_id = %config_id,
                    "Database tenant id collides with a config tenant slug; excluding the database tenant to preserve isolation"
                );
                continue;
            }
            let datasets = source.get_datasets(&record.id).await.map_err(|e| {
                anyhow::anyhow!("Failed to list datasets for tenant '{}': {e}", record.id)
            })?;
            tenants.push(self.db_tenant_descriptor(&record, &datasets));
        }

        Ok(tenants)
    }

    /// Resolve a single active tenant by its slug, source-agnostic.
    ///
    /// Used for lazy on-demand catalog registration: config-defined tenants are
    /// matched first (config slug wins, preserving isolation), then the database
    /// tenant source (where slug == id). Datasets added at runtime are merged
    /// into a config tenant's descriptor. Returns `None` when no active tenant
    /// has that slug, including for a config-disabled tenant.
    pub async fn resolve_tenant_by_slug(&self, slug: &str) -> Result<Option<ResolvedTenant>> {
        if let Some(tenant) = self.get_enabled_tenants().iter().find(|t| t.slug == slug) {
            let mut descriptor = self.config_tenant_descriptor(tenant);
            if let Some(source) = &self.tenant_source {
                let db_datasets = source.get_datasets(&tenant.id).await.map_err(|e| {
                    anyhow::anyhow!("Failed to list datasets for tenant '{}': {e}", tenant.id)
                })?;
                self.merge_db_datasets(&mut descriptor, &tenant.id, &db_datasets);
            }
            return Ok(Some(descriptor));
        }

        if let Some(source) = &self.tenant_source {
            // Database tenants use their id as their slug.
            if let Some(record) = source
                .get_tenant(slug)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load database tenant '{slug}': {e}"))?
            {
                // A record whose id matches a config tenant is either that
                // config tenant (already handled by the slug match above when
                // enabled) or a disabled config tenant — do not resolve it here.
                if self.config.auth.tenants.iter().any(|t| t.id == record.id) {
                    return Ok(None);
                }
                let datasets = source.get_datasets(&record.id).await.map_err(|e| {
                    anyhow::anyhow!("Failed to list datasets for tenant '{}': {e}", record.id)
                })?;
                return Ok(Some(self.db_tenant_descriptor(&record, &datasets)));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, DatasetConfig, StorageConfig, TenantConfig};

    fn create_test_config() -> Configuration {
        Configuration {
            auth: AuthConfig {
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
                        limits: None,
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
                        limits: None,
                    },
                ],
                ..Default::default()
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

    #[tokio::test]
    async fn test_list_active_tenants_falls_back_to_config_only() {
        // No tenant source attached → config-only enumeration.
        let manager = create_test_catalog_manager().await;
        let mut ids: Vec<String> = manager
            .list_active_tenants()
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["acme".to_string(), "beta".to_string()]);
    }

    #[tokio::test]
    async fn test_list_active_tenants_merges_config_and_database() {
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        // A database-only tenant (no config block), created via the admin API.
        source
            .upsert_tenant("gamma", "Gamma", Some("production"), "database")
            .await
            .unwrap();
        source.create_dataset("gamma", "production").await.unwrap();
        let manager = create_test_catalog_manager()
            .await
            .with_tenant_source(source);

        let tenants = manager.list_active_tenants().await.unwrap();
        let gamma = tenants
            .iter()
            .find(|t| t.id == "gamma")
            .expect("database tenant should be present");
        // Slug is derived (identity) for database tenants.
        assert_eq!(gamma.slug, "gamma");
        // Dataset identifier/slug come from the dataset *name*, not the UUID,
        // and storage falls back to the global default.
        assert_eq!(gamma.datasets.len(), 1);
        let ds = &gamma.datasets[0];
        assert_eq!(ds.id, "production");
        assert_eq!(ds.slug, "production");
        assert_eq!(ds.storage_dsn, "memory://");
        assert!(ds.is_default);
        // Config tenants are still present.
        assert!(tenants.iter().any(|t| t.id == "acme"));
        assert!(tenants.iter().any(|t| t.id == "beta"));
    }

    #[tokio::test]
    async fn test_list_active_tenants_config_wins_over_database_dup() {
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        // A row that shadows the config-defined "acme" tenant (as
        // sync_config_tenants would create). Config overrides must win.
        source
            .upsert_tenant("acme", "Acme", Some("production"), "config")
            .await
            .unwrap();
        source.create_dataset("acme", "production").await.unwrap();
        let manager = create_test_catalog_manager()
            .await
            .with_tenant_source(source);

        let tenants = manager.list_active_tenants().await.unwrap();
        let acme: Vec<_> = tenants.iter().filter(|t| t.id == "acme").collect();
        assert_eq!(acme.len(), 1, "acme must not be duplicated");
        // Config's explicit dataset slug override ("prod") is preserved, not
        // the derived database identity ("production").
        assert!(
            acme[0].datasets.iter().any(|d| d.slug == "prod"),
            "config slug override should win"
        );
    }

    #[tokio::test]
    async fn test_resolve_tenant_by_slug_database() {
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        source
            .upsert_tenant("gamma", "Gamma", Some("production"), "database")
            .await
            .unwrap();
        source.create_dataset("gamma", "production").await.unwrap();
        let manager = create_test_catalog_manager()
            .await
            .with_tenant_source(source);

        let resolved = manager.resolve_tenant_by_slug("gamma").await.unwrap();
        let gamma = resolved.expect("database tenant resolvable by slug");
        assert_eq!(gamma.id, "gamma");
        assert_eq!(gamma.datasets[0].id, "production");

        // Config tenant resolvable by its explicit slug.
        let acme = manager.resolve_tenant_by_slug("acme").await.unwrap();
        assert_eq!(acme.expect("config tenant resolvable").id, "acme");

        // Unknown slug resolves to None.
        assert!(
            manager
                .resolve_tenant_by_slug("nope")
                .await
                .unwrap()
                .is_none()
        );
    }

    fn tenant(id: &str, slug: &str, enabled: bool) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: slug.to_string(),
            name: id.to_string(),
            default_dataset: Some("production".to_string()),
            datasets: vec![DatasetConfig {
                id: "production".to_string(),
                slug: "production".to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![],
            schema_config: (!enabled).then_some(crate::config::TenantSchemaConfig {
                schema: None,
                custom_schemas: None,
                enabled: false,
            }),
            limits: None,
        }
    }

    async fn manager_with(tenants: Vec<TenantConfig>, source: Arc<Catalog>) -> CatalogManager {
        let config = Configuration {
            auth: AuthConfig {
                tenants,
                ..Default::default()
            },
            storage: StorageConfig {
                dsn: "memory://".to_string(),
            },
            ..Configuration::default()
        };
        CatalogManager::new(config)
            .await
            .unwrap()
            .with_tenant_source(source)
    }

    #[tokio::test]
    async fn disabled_config_tenant_not_resurrected_from_database() {
        // `beta` is disabled in config; `sync_config_tenants` still wrote it to
        // the database with source="config". It must not come back as active.
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        source
            .upsert_tenant("beta", "Beta", Some("production"), "config")
            .await
            .unwrap();
        source.create_dataset("beta", "production").await.unwrap();
        let manager = manager_with(
            vec![tenant("acme", "acme", true), tenant("beta", "beta", false)],
            source,
        )
        .await;

        let tenants = manager.list_active_tenants().await.unwrap();
        assert!(tenants.iter().any(|t| t.id == "acme"));
        assert!(
            !tenants.iter().any(|t| t.id == "beta"),
            "disabled config tenant must not be resurrected from the database"
        );
        assert!(
            manager
                .resolve_tenant_by_slug("beta")
                .await
                .unwrap()
                .is_none(),
            "disabled config tenant must not resolve by slug"
        );
    }

    #[tokio::test]
    async fn runtime_dataset_merged_into_config_tenant() {
        // `acme` is a config tenant with only `production`; a `staging` dataset
        // is added at runtime via the database.
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        source
            .upsert_tenant("acme", "Acme", Some("production"), "config")
            .await
            .unwrap();
        source.create_dataset("acme", "production").await.unwrap();
        source.create_dataset("acme", "staging").await.unwrap();
        let manager = manager_with(vec![tenant("acme", "acme", true)], source).await;

        let tenants = manager.list_active_tenants().await.unwrap();
        let acme = tenants.iter().find(|t| t.id == "acme").unwrap();
        let ds: std::collections::HashSet<&str> =
            acme.datasets.iter().map(|d| d.id.as_str()).collect();
        assert!(ds.contains("production"), "config dataset preserved");
        assert!(ds.contains("staging"), "runtime dataset merged in");

        // Same via resolve_tenant_by_slug (the lazy path).
        let resolved = manager
            .resolve_tenant_by_slug("acme")
            .await
            .unwrap()
            .unwrap();
        assert!(resolved.datasets.iter().any(|d| d.id == "staging"));
    }

    #[tokio::test]
    async fn db_tenant_id_colliding_with_config_slug_is_excluded() {
        // Config tenant `team-a` has slug `shared`. A database-only tenant with
        // id `shared` would register the same DataFusion catalog slug and break
        // isolation, so it must be excluded.
        let source = Arc::new(Catalog::new_in_memory().await.unwrap());
        source
            .upsert_tenant("team-a", "Team A", Some("production"), "config")
            .await
            .unwrap();
        source.create_dataset("team-a", "production").await.unwrap();
        source
            .upsert_tenant("shared", "Shared", Some("production"), "database")
            .await
            .unwrap();
        source.create_dataset("shared", "production").await.unwrap();
        let manager = manager_with(vec![tenant("team-a", "shared", true)], source).await;

        let tenants = manager.list_active_tenants().await.unwrap();
        let with_shared_slug: Vec<&str> = tenants
            .iter()
            .filter(|t| t.slug == "shared")
            .map(|t| t.id.as_str())
            .collect();
        assert_eq!(
            with_shared_slug,
            vec!["team-a"],
            "only the config tenant may own slug 'shared'; the colliding db tenant is excluded"
        );

        // The slug resolves to the config tenant, never the database one.
        let resolved = manager
            .resolve_tenant_by_slug("shared")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resolved.id, "team-a");
    }
}
