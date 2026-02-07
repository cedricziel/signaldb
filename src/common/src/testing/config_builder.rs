//! Test configuration builder for creating test setups quickly.

use crate::config::{
    Configuration, DatabaseConfig, DatasetConfig, DiscoveryConfig, SchemaConfig, StorageConfig,
    TenantConfig,
};

/// Builder for creating test configurations.
///
/// Provides a fluent API for creating configurations suitable for testing,
/// with sensible defaults that can be customized as needed.
///
/// # Example
///
/// ```rust,ignore
/// use common::testing::TestConfigBuilder;
///
/// // Fully in-memory configuration
/// let config = TestConfigBuilder::new()
///     .in_memory()
///     .with_tenant("acme", "prod")
///     .build();
///
/// // Custom tenant setup
/// let config = TestConfigBuilder::new()
///     .in_memory()
///     .with_tenant_and_slugs("acme", "acme-corp", "prod", "production")
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct TestConfigBuilder {
    config: Configuration,
}

impl Default for TestConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestConfigBuilder {
    /// Create a new test configuration builder with sensible defaults.
    ///
    /// The default configuration uses:
    /// - In-memory object storage (`memory://`)
    /// - In-memory SQLite database (`sqlite::memory:`)
    /// - In-memory discovery catalog
    /// - In-memory schema catalog
    pub fn new() -> Self {
        Self {
            config: Configuration::default(),
        }
    }

    /// Configure for fully in-memory operation (fastest for tests).
    ///
    /// This sets:
    /// - Storage DSN to `memory://`
    /// - Database DSN to `sqlite::memory:`
    /// - Discovery DSN to `sqlite::memory:`
    /// - Schema catalog URI to `sqlite::memory:`
    pub fn in_memory(mut self) -> Self {
        self.config.storage = StorageConfig {
            dsn: "memory://".to_string(),
        };
        self.config.database = DatabaseConfig {
            dsn: "sqlite::memory:".to_string(),
        };
        self.config.discovery = Some(DiscoveryConfig {
            dsn: "sqlite::memory:".to_string(),
            ..DiscoveryConfig::default()
        });
        self.config.schema = SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            ..SchemaConfig::default()
        };
        self
    }

    /// Add a tenant with a default dataset.
    ///
    /// Uses the tenant_id and dataset_id as slugs (URL-friendly identifiers).
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier (also used as slug)
    /// * `dataset_id` - The dataset identifier (also used as slug)
    pub fn with_tenant(self, tenant_id: &str, dataset_id: &str) -> Self {
        self.with_tenant_and_slugs(tenant_id, tenant_id, dataset_id, dataset_id)
    }

    /// Add a tenant with custom slugs.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `tenant_slug` - URL-friendly slug for Iceberg namespace paths
    /// * `dataset_id` - The dataset identifier
    /// * `dataset_slug` - URL-friendly slug for Iceberg namespace paths
    pub fn with_tenant_and_slugs(
        mut self,
        tenant_id: &str,
        tenant_slug: &str,
        dataset_id: &str,
        dataset_slug: &str,
    ) -> Self {
        let tenant_config = TenantConfig {
            id: tenant_id.to_string(),
            slug: tenant_slug.to_string(),
            name: tenant_id.to_string(),
            default_dataset: Some(dataset_id.to_string()),
            datasets: vec![DatasetConfig {
                id: dataset_id.to_string(),
                slug: dataset_slug.to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![],
            schema_config: None,
        };

        self.config.auth.tenants.push(tenant_config);
        self.config.auth.enabled = true;
        self
    }

    /// Set the storage DSN.
    pub fn with_storage_dsn(mut self, dsn: &str) -> Self {
        self.config.storage.dsn = dsn.to_string();
        self
    }

    /// Set the database DSN.
    pub fn with_database_dsn(mut self, dsn: &str) -> Self {
        self.config.database.dsn = dsn.to_string();
        self
    }

    /// Set the discovery DSN.
    pub fn with_discovery_dsn(mut self, dsn: &str) -> Self {
        if let Some(ref mut discovery) = self.config.discovery {
            discovery.dsn = dsn.to_string();
        } else {
            self.config.discovery = Some(DiscoveryConfig {
                dsn: dsn.to_string(),
                ..DiscoveryConfig::default()
            });
        }
        self
    }

    /// Set the schema catalog URI.
    pub fn with_schema_catalog_uri(mut self, uri: &str) -> Self {
        self.config.schema.catalog_uri = uri.to_string();
        self
    }

    /// Set the WAL directory.
    pub fn with_wal_dir(mut self, dir: &str) -> Self {
        self.config.wal.wal_dir = dir.to_string();
        self
    }

    /// Disable discovery.
    pub fn without_discovery(mut self) -> Self {
        self.config.discovery = None;
        self
    }

    /// Enable authentication.
    pub fn with_auth_enabled(mut self) -> Self {
        self.config.auth.enabled = true;
        self
    }

    /// Configure a shared file-backed SQLite catalog for cross-service tests.
    ///
    /// When testing multiple services that need to discover each other, they must
    /// share the same catalog database. In-memory SQLite (`sqlite::memory:`) creates
    /// isolated databases per connection, so services can't see each other.
    ///
    /// This method sets the discovery, database, and schema catalog DSNs to use
    /// file-backed SQLite databases in the given directory.
    ///
    /// # Arguments
    ///
    /// * `dir` - Path to a directory (typically a `TempDir`) where SQLite files will be created
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use common::testing::TestConfigBuilder;
    /// use tempfile::TempDir;
    ///
    /// let temp = TempDir::new().unwrap();
    /// let config = TestConfigBuilder::new()
    ///     .with_shared_catalog_dir(temp.path().to_str().unwrap())
    ///     .with_tenant("acme", "prod")
    ///     .build();
    /// // Now multiple services using this config can discover each other.
    /// ```
    pub fn with_shared_catalog_dir(mut self, dir: &str) -> Self {
        let catalog_dsn = format!("sqlite://{dir}/catalog.db");
        let schema_dsn = format!("sqlite://{dir}/schema.db");

        self.config.database = DatabaseConfig {
            dsn: catalog_dsn.clone(),
        };
        self.config.discovery = Some(DiscoveryConfig {
            dsn: catalog_dsn,
            ..DiscoveryConfig::default()
        });
        self.config.schema = SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: schema_dsn,
            ..SchemaConfig::default()
        };
        self
    }

    /// Build the configuration.
    pub fn build(self) -> Configuration {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_builder() {
        let config = TestConfigBuilder::new().build();
        assert_eq!(config.storage.dsn, "memory://");
    }

    #[test]
    fn test_in_memory_builder() {
        let config = TestConfigBuilder::new().in_memory().build();
        assert_eq!(config.storage.dsn, "memory://");
        assert_eq!(config.database.dsn, "sqlite::memory:");
        assert_eq!(
            config.discovery.as_ref().map(|d| d.dsn.as_str()),
            Some("sqlite::memory:")
        );
        assert_eq!(config.schema.catalog_uri, "sqlite::memory:");
    }

    #[test]
    fn test_with_tenant() {
        let config = TestConfigBuilder::new()
            .in_memory()
            .with_tenant("acme", "prod")
            .build();

        assert!(config.auth.enabled);
        assert_eq!(config.auth.tenants.len(), 1);

        let tenant = &config.auth.tenants[0];
        assert_eq!(tenant.id, "acme");
        assert_eq!(tenant.slug, "acme");
        assert_eq!(tenant.default_dataset, Some("prod".to_string()));
        assert_eq!(tenant.datasets.len(), 1);
        assert_eq!(tenant.datasets[0].id, "prod");
        assert_eq!(tenant.datasets[0].slug, "prod");
    }

    #[test]
    fn test_with_tenant_and_slugs() {
        let config = TestConfigBuilder::new()
            .in_memory()
            .with_tenant_and_slugs("acme", "acme-corp", "prod", "production")
            .build();

        let tenant = &config.auth.tenants[0];
        assert_eq!(tenant.id, "acme");
        assert_eq!(tenant.slug, "acme-corp");
        assert_eq!(tenant.datasets[0].id, "prod");
        assert_eq!(tenant.datasets[0].slug, "production");
    }

    #[test]
    fn test_multiple_tenants() {
        let config = TestConfigBuilder::new()
            .in_memory()
            .with_tenant("acme", "prod")
            .with_tenant("beta", "staging")
            .build();

        assert_eq!(config.auth.tenants.len(), 2);
        assert_eq!(config.auth.tenants[0].id, "acme");
        assert_eq!(config.auth.tenants[1].id, "beta");
    }

    #[test]
    fn test_with_shared_catalog_dir() {
        let config = TestConfigBuilder::new()
            .with_shared_catalog_dir("/tmp/test-signaldb")
            .with_tenant("acme", "prod")
            .build();

        assert_eq!(
            config.database.dsn,
            "sqlite:///tmp/test-signaldb/catalog.db"
        );
        assert_eq!(
            config.discovery.as_ref().map(|d| d.dsn.as_str()),
            Some("sqlite:///tmp/test-signaldb/catalog.db")
        );
        assert_eq!(
            config.schema.catalog_uri,
            "sqlite:///tmp/test-signaldb/schema.db"
        );
    }

    #[test]
    fn test_custom_dsns() {
        let config = TestConfigBuilder::new()
            .with_storage_dsn("file:///tmp/test")
            .with_database_dsn("sqlite:///tmp/test.db")
            .with_discovery_dsn("sqlite:///tmp/discovery.db")
            .with_schema_catalog_uri("sqlite:///tmp/catalog.db")
            .build();

        assert_eq!(config.storage.dsn, "file:///tmp/test");
        assert_eq!(config.database.dsn, "sqlite:///tmp/test.db");
        assert_eq!(
            config.discovery.as_ref().map(|d| d.dsn.as_str()),
            Some("sqlite:///tmp/discovery.db")
        );
        assert_eq!(config.schema.catalog_uri, "sqlite:///tmp/catalog.db");
    }
}
