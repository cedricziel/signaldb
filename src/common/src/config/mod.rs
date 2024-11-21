use serde::Deserialize;

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};

use once_cell::sync::OnceCell;

pub static CONFIG: OnceCell<Configuration> = OnceCell::new();

#[derive(Debug, Deserialize)]
pub struct Configuration {}

impl Configuration {
    pub fn load() -> Result<Self, figment::Error> {
        let config: Configuration = Figment::new()
            .merge(Toml::file("signaldb.toml"))
            .merge(Env::prefixed("SIGNALDB_"))
            .extract()?;

        Ok(config)
    }
}
