use std::collections::HashMap;
use std::time::Duration;
use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};

use once_cell::sync::OnceCell;

pub static CONFIG: OnceCell<Configuration> = OnceCell::new();

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// DSN for the storage backend (e.g., file:///path/to/storage, memory://, s3://bucket/prefix)
    pub dsn: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            dsn: "memory://".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub dsn: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            dsn: String::from("sqlite://.data/signaldb.db"),
        }
    }
}

impl DatabaseConfig {
    /// Create an in-memory database configuration for monolithic mode
    pub fn in_memory() -> Self {
        Self {
            dsn: String::from("sqlite::memory:"),
        }
    }
}

/// Configuration for service discovery (Catalog)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Data source name for the Catalog (PostgreSQL or SQLite DSN)
    pub dsn: String,
    /// Interval at which to send heartbeats to the Catalog
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// Interval at which to poll the Catalog for updates
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,
    /// Time-to-live for service entries before they are considered stale
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            dsn: String::from("sqlite::memory:"),
            heartbeat_interval: Duration::from_secs(30),
            poll_interval: Duration::from_secs(60),
            ttl: Duration::from_secs(300),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalConfig {
    /// Directory where WAL files are stored
    pub wal_dir: String,
    /// Maximum size of a single WAL segment in bytes
    pub max_segment_size: u64,
    /// Maximum number of entries in memory buffer before forcing flush
    pub max_buffer_entries: usize,
    /// Maximum time to wait before forcing flush
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    /// Maximum size in bytes to buffer before flushing to object store
    pub max_buffer_size_bytes: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_dir: ".data/wal".to_string(),
            max_segment_size: 64 * 1024 * 1024, // 64MB
            max_buffer_entries: 1000,
            flush_interval: Duration::from_secs(30),
            max_buffer_size_bytes: 128 * 1024 * 1024, // 128MB
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactorConfig {
    /// Enable compactor service
    /// Env: SIGNALDB__COMPACTOR__ENABLED
    #[serde(default)]
    pub enabled: bool,

    /// Interval between compaction planning cycles
    /// Env: SIGNALDB__COMPACTOR__TICK_INTERVAL
    #[serde(with = "humantime_serde")]
    pub tick_interval: Duration,

    /// Target file size in MB after compaction
    /// Env: SIGNALDB__COMPACTOR__TARGET_FILE_SIZE_MB
    pub target_file_size_mb: u64,

    /// Minimum number of files to trigger compaction for a partition
    /// Env: SIGNALDB__COMPACTOR__FILE_COUNT_THRESHOLD
    pub file_count_threshold: usize,

    /// Minimum input file size in KB to consider for compaction
    /// Env: SIGNALDB__COMPACTOR__MIN_INPUT_FILE_SIZE_KB
    pub min_input_file_size_kb: u64,

    /// Maximum files to include in a single compaction job
    /// Env: SIGNALDB__COMPACTOR__MAX_FILES_PER_JOB
    pub max_files_per_job: usize,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick_interval: Duration::from_secs(300), // 5 minutes
            target_file_size_mb: 128,
            file_count_threshold: 10,
            min_input_file_size_kb: 1024, // 1MB
            max_files_per_job: 50,
        }
    }
}

/// Default schemas configuration for OpenTelemetry signal types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DefaultSchemas {
    /// Enable creation of traces table
    #[serde(default = "default_true")]
    pub traces_enabled: bool,
    /// Enable creation of logs table
    #[serde(default = "default_true")]
    pub logs_enabled: bool,
    /// Enable creation of metrics tables
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,
    /// Custom schema definitions (table_name -> schema_json)
    #[serde(default)]
    pub custom_schemas: HashMap<String, serde_json::Value>,
}

impl Default for DefaultSchemas {
    fn default() -> Self {
        Self {
            traces_enabled: true,
            logs_enabled: true,
            metrics_enabled: true,
            custom_schemas: HashMap::new(),
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaConfig {
    /// Type of catalog backend (sql, memory)
    pub catalog_type: String,
    /// URI for the catalog backend (e.g., sqlite://.data/catalog.db)
    pub catalog_uri: String,
    /// Default schemas to create for new tenants
    #[serde(default)]
    pub default_schemas: DefaultSchemas,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            default_schemas: DefaultSchemas::default(),
        }
    }
}

/// Configuration for tenant-specific schema overrides
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantSchemaConfig {
    /// Schema configuration that overrides global settings
    pub schema: Option<SchemaConfig>,
    /// Custom schema definitions for this tenant
    pub custom_schemas: Option<HashMap<String, String>>,
    /// Whether this tenant is enabled
    pub enabled: bool,
}

impl Default for TenantSchemaConfig {
    fn default() -> Self {
        Self {
            schema: None,
            custom_schemas: None,
            enabled: true,
        }
    }
}

/// Configuration for multi-tenant schema management
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantsConfig {
    /// Default tenant ID when none is specified
    pub default_tenant: String,
    /// Per-tenant configuration overrides
    pub tenants: HashMap<String, TenantSchemaConfig>,
}

impl Default for TenantsConfig {
    fn default() -> Self {
        Self {
            default_tenant: "default".to_string(),
            tenants: HashMap::new(),
        }
    }
}

/// API Key configuration for a tenant
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiKeyConfig {
    /// The actual API key (e.g., "sk_acme_prod_1234567890abcdef")
    pub key: String,
    /// Optional name for this API key (e.g., "production-key")
    pub name: Option<String>,
}

/// Dataset configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetConfig {
    /// Dataset ID/name (e.g., "production", "staging")
    pub id: String,
    /// URL-friendly slug for Iceberg namespace paths
    pub slug: String,
    /// Whether this dataset is the default for the tenant
    #[serde(default)]
    pub is_default: bool,
    /// Optional storage configuration for this dataset.
    /// If not specified, uses tenant or global storage config.
    #[serde(default)]
    pub storage: Option<StorageConfig>,
}

/// Tenant configuration for multi-tenancy authentication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantConfig {
    /// Unique tenant identifier
    pub id: String,
    /// URL-friendly slug for Iceberg namespace paths
    pub slug: String,
    /// Human-readable tenant name
    pub name: String,
    /// Default dataset ID when X-Dataset-ID header is missing
    pub default_dataset: Option<String>,
    /// List of datasets for this tenant
    #[serde(default)]
    pub datasets: Vec<DatasetConfig>,
    /// API keys for this tenant
    #[serde(default)]
    pub api_keys: Vec<ApiKeyConfig>,
    /// Optional schema configuration override
    #[serde(default)]
    pub schema_config: Option<TenantSchemaConfig>,
}

/// Authentication configuration
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Whether multi-tenancy authentication is enabled
    #[serde(default)]
    pub enabled: bool,
    /// List of configured tenants
    #[serde(default)]
    pub tenants: Vec<TenantConfig>,
    /// Admin API key for management endpoints
    #[serde(default)]
    pub admin_api_key: Option<String>,
}

fn default_self_monitoring_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_self_monitoring_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_self_monitoring_tenant() -> String {
    "_system".to_string()
}

fn default_self_monitoring_dataset() -> String {
    "_monitoring".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SelfMonitoringConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_self_monitoring_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_self_monitoring_interval", with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default = "default_self_monitoring_tenant")]
    pub tenant_id: String,
    #[serde(default = "default_self_monitoring_dataset")]
    pub dataset_id: String,
}

impl Default for SelfMonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_self_monitoring_endpoint(),
            interval: default_self_monitoring_interval(),
            tenant_id: default_self_monitoring_tenant(),
            dataset_id: default_self_monitoring_dataset(),
        }
    }
}

// Keep IcebergConfig for backward compatibility
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Type of catalog backend (postgresql, sqlite)
    pub catalog_type: String,
    /// URI for the catalog backend (e.g., postgres://..., sqlite://...)
    pub catalog_uri: String,
    /// Path to the warehouse directory for storing table data
    pub warehouse_path: String,
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            catalog_type: "memory".to_string(),
            catalog_uri: "memory://".to_string(),
            warehouse_path: ".data/warehouse".to_string(),
        }
    }
}

impl From<IcebergConfig> for SchemaConfig {
    fn from(iceberg_config: IcebergConfig) -> Self {
        Self {
            catalog_type: iceberg_config.catalog_type, // Preserve original catalog_type
            catalog_uri: iceberg_config.catalog_uri,
            default_schemas: DefaultSchemas::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Configuration {
    /// Database configuration (used for internal storage)
    pub database: DatabaseConfig,
    /// Object storage configuration
    pub storage: StorageConfig,
    /// Service discovery configuration (enabled by default with SQLite)
    pub discovery: Option<DiscoveryConfig>,
    /// WAL configuration (includes buffering policies)
    pub wal: WalConfig,
    /// Schema configuration (defaults to in-memory provider)
    pub schema: SchemaConfig,
    /// Multi-tenant configuration
    pub tenants: TenantsConfig,
    /// Iceberg configuration (deprecated, use schema instead)
    pub iceberg: Option<IcebergConfig>,
    /// Authentication and multi-tenancy configuration
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub self_monitoring: SelfMonitoringConfig,
    /// Compactor configuration
    #[serde(default)]
    pub compactor: CompactorConfig,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            storage: StorageConfig::default(),
            // Enable discovery by default for configless operation
            discovery: Some(DiscoveryConfig::default()),
            wal: WalConfig::default(),
            // Schema defaults to in-memory provider
            schema: SchemaConfig::default(),
            // Tenants disabled by default
            tenants: TenantsConfig::default(),
            // Iceberg is optional and not enabled by default
            iceberg: None,
            // Auth disabled by default
            auth: AuthConfig::default(),
            self_monitoring: SelfMonitoringConfig::default(),
            compactor: CompactorConfig::default(),
        }
    }
}

impl fmt::Display for Configuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Configuration error")
    }
}

impl Error for Configuration {}

impl Configuration {
    pub fn load() -> Result<Self, Box<figment::Error>> {
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Toml::file("signaldb.toml"))
            // Support both single-underscore (legacy) and double-underscore (new) env vars
            // Single underscore for simple configs: SIGNALDB_DATABASE_DSN
            .merge(Env::prefixed("SIGNALDB_").split("_"))
            // Double underscore for fields with underscores: SIGNALDB__COMPACTOR__TICK_INTERVAL
            .merge(Env::prefixed("SIGNALDB__").split("__"))
            .extract()
            .map_err(Box::new)?;

        Ok(config)
    }

    pub fn load_from_path(path: &std::path::Path) -> Result<Self, Box<figment::Error>> {
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Toml::file(path))
            // Support both single-underscore (legacy) and double-underscore (new) env vars
            // Single underscore for simple configs: SIGNALDB_DATABASE_DSN
            .merge(Env::prefixed("SIGNALDB_").split("_"))
            // Double underscore for fields with underscores: SIGNALDB__COMPACTOR__TICK_INTERVAL
            .merge(Env::prefixed("SIGNALDB__").split("__"))
            .extract()
            .map_err(Box::new)?;

        Ok(config)
    }

    /// Get the effective schema configuration for a given tenant
    pub fn get_tenant_schema_config(&self, tenant_id: &str) -> SchemaConfig {
        if let Some(tenant_config) = self.tenants.tenants.get(tenant_id)
            && let Some(ref tenant_schema) = tenant_config.schema
        {
            return tenant_schema.clone();
        }

        // Fall back to global schema config
        self.schema.clone()
    }

    /// Check if a tenant is enabled
    pub fn is_tenant_enabled(&self, tenant_id: &str) -> bool {
        self.tenants
            .tenants
            .get(tenant_id)
            .map(|config| config.enabled)
            .unwrap_or_else(|| tenant_id == self.tenants.default_tenant)
    }

    /// Get the default tenant ID
    pub fn get_default_tenant(&self) -> &str {
        &self.tenants.default_tenant
    }

    /// Get custom schemas for a tenant
    pub fn get_tenant_custom_schemas(&self, tenant_id: &str) -> Option<&HashMap<String, String>> {
        self.tenants
            .tenants
            .get(tenant_id)
            .and_then(|config| config.custom_schemas.as_ref())
    }

    /// Get effective storage config for a dataset (dataset -> tenant -> global fallback).
    ///
    /// This allows each dataset to optionally specify its own storage backend,
    /// falling back to the global storage configuration if not specified.
    pub fn get_dataset_storage_config(&self, tenant_id: &str, dataset_id: &str) -> &StorageConfig {
        // Check dataset-level storage
        if let Some(tenant) = self.auth.tenants.iter().find(|t| t.id == tenant_id)
            && let Some(dataset) = tenant.datasets.iter().find(|d| d.id == dataset_id)
            && let Some(ref storage) = dataset.storage
        {
            return storage;
        }
        // Future: Check tenant-level storage here if we add it
        // Fall back to global storage
        &self.storage
    }

    /// Get the tenant slug for a given tenant ID.
    ///
    /// Returns the tenant's slug if found, otherwise returns the tenant_id as-is.
    pub fn get_tenant_slug(&self, tenant_id: &str) -> String {
        self.auth
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
        self.auth
            .tenants
            .iter()
            .find(|t| t.id == tenant_id)
            .and_then(|t| t.datasets.iter().find(|d| d.id == dataset_id))
            .map(|d| d.slug.clone())
            .unwrap_or_else(|| dataset_id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Jail;
    use std::time::Duration;

    #[test]
    fn test_default_configuration_enables_sqlite() {
        let config = Configuration::default();

        // Database should default to SQLite in .data directory
        assert_eq!(config.database.dsn, "sqlite://.data/signaldb.db");

        // Discovery should be enabled by default with in-memory SQLite
        assert!(config.discovery.is_some());
        let discovery = config.discovery.unwrap();
        assert_eq!(discovery.dsn, "sqlite::memory:");
        assert_eq!(discovery.heartbeat_interval, Duration::from_secs(30));
        assert_eq!(discovery.poll_interval, Duration::from_secs(60));
        assert_eq!(discovery.ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_configless_operation() {
        // Test that we can load defaults without any config file
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .extract::<Configuration>()
            .unwrap();

        // Should work completely configless with SQLite defaults
        assert_eq!(config.database.dsn, "sqlite://.data/signaldb.db");
        assert!(config.discovery.is_some());
        assert_eq!(config.discovery.unwrap().dsn, "sqlite::memory:");
    }

    #[test]
    fn test_env_var_override() {
        // Test environment variable parsing with single underscore separator
        Jail::expect_with(|jail| {
            jail.set_env("SIGNALDB_DATABASE_DSN", "sqlite://./test.db");
            jail.set_env("SIGNALDB_DISCOVERY_DSN", "sqlite://./discovery.db");

            let config = Figment::from(Serialized::defaults(Configuration::default()))
                .merge(Env::prefixed("SIGNALDB_").split("_"))
                .extract::<Configuration>()
                .unwrap();

            assert_eq!(config.database.dsn, "sqlite://./test.db");

            if let Some(discovery) = config.discovery {
                assert_eq!(discovery.dsn, "sqlite://./discovery.db");
            }

            Ok(())
        });
    }

    #[test]
    fn test_env_var_single_underscore_format() {
        // Test that single underscore format works for nested configuration
        // This is now the standard format for environment variables
        Jail::expect_with(|jail| {
            jail.set_env("SIGNALDB_DATABASE_DSN", "sqlite://./test.db");

            let config = Figment::from(Serialized::defaults(Configuration::default()))
                .merge(Env::prefixed("SIGNALDB_").split("_"))
                .extract::<Configuration>()
                .unwrap();

            // Should work with single underscore format
            assert_eq!(config.database.dsn, "sqlite://./test.db");

            Ok(())
        });
    }

    #[test]
    fn test_storage_config_env_vars() {
        // Test storage DSN configuration
        Jail::expect_with(|jail| {
            jail.set_env("SIGNALDB_STORAGE_DSN", "file:///tmp/test");

            let config = Figment::from(Serialized::defaults(Configuration::default()))
                .merge(Env::prefixed("SIGNALDB_").split("_"))
                .extract::<Configuration>()
                .unwrap();

            assert_eq!(config.storage.dsn, "file:///tmp/test");

            Ok(())
        });
    }

    #[test]
    fn test_compactor_config_env_vars() {
        // Test compactor configuration via environment variables
        // Uses double-underscore separator to support field names with underscores
        Jail::expect_with(|jail| {
            jail.set_env("SIGNALDB__COMPACTOR__ENABLED", "true");
            jail.set_env("SIGNALDB__COMPACTOR__TICK_INTERVAL", "10m");
            jail.set_env("SIGNALDB__COMPACTOR__TARGET_FILE_SIZE_MB", "256");
            jail.set_env("SIGNALDB__COMPACTOR__FILE_COUNT_THRESHOLD", "20");
            jail.set_env("SIGNALDB__COMPACTOR__MIN_INPUT_FILE_SIZE_KB", "2048");
            jail.set_env("SIGNALDB__COMPACTOR__MAX_FILES_PER_JOB", "100");

            let config = Figment::from(Serialized::defaults(Configuration::default()))
                .merge(Env::prefixed("SIGNALDB_").split("_"))
                .merge(Env::prefixed("SIGNALDB__").split("__"))
                .extract::<Configuration>()
                .unwrap();

            assert!(config.compactor.enabled);
            assert_eq!(config.compactor.tick_interval, Duration::from_secs(600)); // 10 minutes
            assert_eq!(config.compactor.target_file_size_mb, 256);
            assert_eq!(config.compactor.file_count_threshold, 20);
            assert_eq!(config.compactor.min_input_file_size_kb, 2048);
            assert_eq!(config.compactor.max_files_per_job, 100);

            Ok(())
        });
    }

    #[test]
    fn test_iceberg_config_to_schema_config_conversion() {
        // Test that catalog_type is preserved in From conversion
        let iceberg_config = IcebergConfig {
            catalog_type: "memory".to_string(),
            catalog_uri: "memory://".to_string(),
            warehouse_path: "/tmp/warehouse".to_string(),
        };

        let schema_config: SchemaConfig = iceberg_config.into();

        assert_eq!(schema_config.catalog_type, "memory");
        assert_eq!(schema_config.catalog_uri, "memory://");

        // Test with different catalog_type
        let iceberg_config_sql = IcebergConfig {
            catalog_type: "postgresql".to_string(),
            catalog_uri: "postgres://localhost:5432/catalog".to_string(),
            warehouse_path: "/data/warehouse".to_string(),
        };

        let schema_config_sql: SchemaConfig = iceberg_config_sql.into();

        assert_eq!(schema_config_sql.catalog_type, "postgresql");
        assert_eq!(
            schema_config_sql.catalog_uri,
            "postgres://localhost:5432/catalog"
        );
    }

    #[test]
    fn test_tenant_configuration_default() {
        let config = Configuration::default();

        // Default tenant should be "default"
        assert_eq!(config.get_default_tenant(), "default");

        // Should return global schema config for unknown tenants
        let tenant_schema = config.get_tenant_schema_config("unknown-tenant");
        assert_eq!(tenant_schema.catalog_type, config.schema.catalog_type);
        assert_eq!(tenant_schema.catalog_uri, config.schema.catalog_uri);

        // Default tenant should be enabled
        assert!(config.is_tenant_enabled("default"));
        // Unknown tenants should be disabled
        assert!(!config.is_tenant_enabled("unknown-tenant"));

        // No custom schemas for unknown tenants
        assert!(config.get_tenant_custom_schemas("unknown-tenant").is_none());
    }

    #[test]
    fn test_tenant_configuration_with_custom_tenant() {
        let tenant_config = TenantSchemaConfig {
            schema: Some(SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://tenant".to_string(),
                default_schemas: DefaultSchemas::default(),
            }),
            custom_schemas: Some({
                let mut schemas = HashMap::new();
                schemas.insert("traces".to_string(), "custom_traces_schema".to_string());
                schemas
            }),
            ..TenantSchemaConfig::default()
        };

        let mut tenants = HashMap::new();
        tenants.insert("tenant1".to_string(), tenant_config);

        let config = Configuration {
            tenants: TenantsConfig {
                default_tenant: "tenant1".to_string(),
                tenants,
            },
            ..Configuration::default()
        };

        assert_eq!(config.get_default_tenant(), "tenant1");

        // Should return tenant-specific schema config
        let tenant_schema = config.get_tenant_schema_config("tenant1");
        assert_eq!(tenant_schema.catalog_type, "memory");
        assert_eq!(tenant_schema.catalog_uri, "memory://tenant");

        // Should fall back to global config for unknown tenant
        let unknown_schema = config.get_tenant_schema_config("unknown");
        assert_eq!(unknown_schema.catalog_type, config.schema.catalog_type);

        // Configured tenant should be enabled
        assert!(config.is_tenant_enabled("tenant1"));
        // Unknown tenant should be disabled
        assert!(!config.is_tenant_enabled("unknown"));

        // Should return custom schemas for tenant
        let custom_schemas = config.get_tenant_custom_schemas("tenant1");
        assert!(custom_schemas.is_some());
        assert_eq!(
            custom_schemas.unwrap().get("traces"),
            Some(&"custom_traces_schema".to_string())
        );
    }

    #[test]
    fn test_tenant_disabled_individual() {
        let tenant_config = TenantSchemaConfig {
            enabled: false,
            ..TenantSchemaConfig::default()
        };

        let mut tenants = HashMap::new();
        tenants.insert("disabled-tenant".to_string(), tenant_config);

        let config = Configuration {
            tenants: TenantsConfig {
                default_tenant: "default".to_string(),
                tenants,
            },
            ..Configuration::default()
        };

        // Tenant should be disabled
        assert!(!config.is_tenant_enabled("disabled-tenant"));
        // Default tenant should still be enabled
        assert!(config.is_tenant_enabled("default"));
    }
}
