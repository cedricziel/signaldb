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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueConfig {
    pub max_batch_size: usize,
    #[serde(with = "humantime_serde")]
    pub max_batch_wait: Duration,
    pub dsn: String,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            max_batch_wait: Duration::from_secs(10),
            dsn: String::from("memory://"),
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
            dsn: String::from("sqlite://.data/signaldb.db"),
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
    /// Message queue configuration
    pub queue: QueueConfig,
    /// Service discovery configuration (enabled by default with SQLite)
    pub discovery: Option<DiscoveryConfig>,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            storage: StorageConfig::default(),
            queue: QueueConfig::default(),
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

    pub fn load() -> Result<Self, figment::Error> {
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Toml::file("signaldb.toml"))
            .merge(Env::prefixed("SIGNALDB_"))
            .extract()?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_queue_config_defaults() {
        let config = QueueConfig::default();
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.max_batch_wait, Duration::from_secs(10));
        assert_eq!(config.dsn, "memory://");
    }

    #[test]
    fn test_queue_config_from_toml() {
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Toml::string(
                r#"
                [queue]
                max_batch_size = 2000
                max_batch_wait = "20s"
                dsn = "memory://custom"
            "#,
            ))
            .extract::<Configuration>()
            .unwrap();

        assert_eq!(config.queue.max_batch_size, 2000);
        assert_eq!(config.queue.max_batch_wait, Duration::from_secs(20));
        assert_eq!(config.queue.dsn, "memory://custom");
    }

    #[test]
    fn test_default_configuration_enables_sqlite() {
        let config = Configuration::default();

        // Database should default to SQLite in .data directory
        assert_eq!(config.database.dsn, "sqlite://.data/signaldb.db");

        // Discovery should be enabled by default with SQLite
        assert!(config.discovery.is_some());
        let discovery = config.discovery.unwrap();
        assert_eq!(discovery.dsn, "sqlite://.data/signaldb.db");
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
    }
}
