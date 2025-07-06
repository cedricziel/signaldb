use std::collections::HashMap;
use std::time::Duration;
use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};

use once_cell::sync::OnceCell;

pub static CONFIG: OnceCell<Configuration> = OnceCell::new();

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    default: String,
    adapters: HashMap<String, ObjectStorageConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ObjectStorageConfig {
    pub url: String,
    pub prefix: String,
    #[serde(rename = "type")]
    pub storage_type: String,
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
pub struct SchemaConfig {
    /// Type of catalog backend (sql, memory)
    pub catalog_type: String,
    /// URI for the catalog backend (e.g., sqlite://.data/catalog.db)
    pub catalog_uri: String,
    /// Path to the warehouse directory for storing table data
    pub warehouse_path: String,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            warehouse_path: ".data/warehouse".to_string(),
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
            catalog_type: "sql".to_string(), // Map iceberg config to sql catalog type
            catalog_uri: iceberg_config.catalog_uri,
            warehouse_path: iceberg_config.warehouse_path,
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
    /// Iceberg configuration (deprecated, use schema instead)
    pub iceberg: Option<IcebergConfig>,
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
            // Iceberg is optional and not enabled by default
            iceberg: None,
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
    pub fn default_storage_url(&self) -> String {
        let default_storage = self.storage.default.clone();
        self.storage
            .adapters
            .get(&default_storage)
            .map(|config| config.url.clone())
            .unwrap_or_else(|| String::from("file://.data/ds"))
    }

    pub fn default_storage_prefix(&self) -> String {
        let default_storage = self.storage.default.clone();
        self.storage
            .adapters
            .get(&default_storage)
            .map(|config| config.prefix.clone())
            .unwrap_or_else(|| String::from(".data"))
    }

    pub fn load() -> Result<Self, Box<figment::Error>> {
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Toml::file("signaldb.toml"))
            .merge(Env::prefixed("SIGNALDB__").split("__"))
            .extract()
            .map_err(Box::new)?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        // Test environment variable parsing with double underscore separator
        std::env::set_var("SIGNALDB__DATABASE__DSN", "sqlite://./test.db");
        std::env::set_var("SIGNALDB__DISCOVERY__DSN", "sqlite://./discovery.db");

        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Env::prefixed("SIGNALDB__").split("__"))
            .extract::<Configuration>()
            .unwrap();

        assert_eq!(config.database.dsn, "sqlite://./test.db");

        if let Some(discovery) = config.discovery {
            assert_eq!(discovery.dsn, "sqlite://./discovery.db");
        }

        // Clean up
        std::env::remove_var("SIGNALDB__DATABASE__DSN");
        std::env::remove_var("SIGNALDB__DISCOVERY__DSN");
    }

    #[test]
    fn test_env_var_single_underscore_format() {
        // Test that single underscore format works when used as a flat key
        // This is actually valid since "DATABASE_DSN" could be interpreted as a top-level key
        std::env::set_var("SIGNALDB__DATABASE_DSN", "sqlite://./test.db");

        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Env::prefixed("SIGNALDB__").split("__"))
            .extract::<Configuration>()
            .unwrap();

        // This should work because "DATABASE_DSN" is treated as a single key
        // and we're looking for nested "database.dsn" but figment is flexible
        assert_eq!(config.database.dsn, "sqlite://./test.db");

        // Clean up
        std::env::remove_var("SIGNALDB__DATABASE_DSN");
    }

    #[test]
    fn test_nested_storage_config_env_vars() {
        // Test deeply nested storage configuration
        std::env::set_var("SIGNALDB__STORAGE__DEFAULT", "local");
        std::env::set_var(
            "SIGNALDB__STORAGE__ADAPTERS__LOCAL__URL",
            "file:///tmp/test",
        );
        std::env::set_var("SIGNALDB__STORAGE__ADAPTERS__LOCAL__PREFIX", "test-prefix");
        std::env::set_var("SIGNALDB__STORAGE__ADAPTERS__LOCAL__TYPE", "filesystem");

        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Env::prefixed("SIGNALDB__").split("__"))
            .extract::<Configuration>()
            .unwrap();

        assert_eq!(config.storage.default, "local");
        let local_adapter = config.storage.adapters.get("local").unwrap();
        assert_eq!(local_adapter.url, "file:///tmp/test");
        assert_eq!(local_adapter.prefix, "test-prefix");
        assert_eq!(local_adapter.storage_type, "filesystem");

        // Clean up
        std::env::remove_var("SIGNALDB__STORAGE__DEFAULT");
        std::env::remove_var("SIGNALDB__STORAGE__ADAPTERS__LOCAL__URL");
        std::env::remove_var("SIGNALDB__STORAGE__ADAPTERS__LOCAL__PREFIX");
        std::env::remove_var("SIGNALDB__STORAGE__ADAPTERS__LOCAL__TYPE");
    }
}
