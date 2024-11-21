use serde::{Deserialize, Serialize};

use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};

use once_cell::sync::OnceCell;

pub static CONFIG: OnceCell<Configuration> = OnceCell::new();

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
pub struct Configuration {
    pub database: DatabaseConfig,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            database: Default::default(),
        }
    }
}

impl Configuration {
    pub fn load() -> Result<Self, figment::Error> {
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Toml::file("signaldb.toml"))
            .merge(Env::prefixed("SIGNALDB_"))
            .extract()?;

        Ok(config)
    }
}
