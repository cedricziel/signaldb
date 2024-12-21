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

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    default: String,
    adapters: HashMap<String, ObjectStorageConfig>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ObjectStorageConfig {
    pub url: String,
    pub prefix: String,
    #[serde(rename = "type")]
    pub storage_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueConfig {
    pub max_batch_size: usize,
    #[serde(with = "humantime_serde")]
    pub max_batch_wait: Duration,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            max_batch_wait: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Configuration {
    pub database: DatabaseConfig,
    pub storage: StorageConfig,
    pub queue: QueueConfig,
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
