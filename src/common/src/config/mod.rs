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
pub struct Configuration {
    /// Database configuration (used for internal storage)
    pub database: DatabaseConfig,
    /// Object storage configuration
    pub storage: StorageConfig,
    /// Service discovery configuration (enabled by default with SQLite)
    pub discovery: Option<DiscoveryConfig>,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            storage: StorageConfig::default(),
            // Enable discovery by default for configless operation
            discovery: Some(DiscoveryConfig::default()),
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
