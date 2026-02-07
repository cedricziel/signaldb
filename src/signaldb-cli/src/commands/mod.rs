pub mod api_key;
pub mod dataset;
pub mod tenant;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use signaldb_sdk::Client;

/// SignalDB CLI — manage tenants, API keys, and datasets
#[derive(Parser)]
#[command(name = "signaldb-cli", version, about)]
pub struct Cli {
    /// Path to SignalDB configuration file (reads admin_api_key from [auth])
    #[arg(long)]
    config: Option<PathBuf>,

    /// SignalDB router base URL
    #[arg(long, env = "SIGNALDB_URL", default_value = "http://localhost:3000")]
    url: String,

    /// Admin API key (overrides value from config file)
    #[arg(long, env = "SIGNALDB_ADMIN_KEY")]
    admin_key: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage tenants
    Tenant {
        #[command(subcommand)]
        action: tenant::TenantAction,
    },
    /// Manage API keys
    ApiKey {
        #[command(subcommand)]
        action: api_key::ApiKeyAction,
    },
    /// Manage datasets
    Dataset {
        #[command(subcommand)]
        action: dataset::DatasetAction,
    },
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        let admin_key = self.resolve_admin_key()?;
        let base_url = format!("{}/api/v1/admin", self.url.trim_end_matches('/'));

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {admin_key}"))?,
        );
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        let client = Client::new_with_client(&base_url, http);

        match self.command {
            Commands::Tenant { action } => action.run(&client).await,
            Commands::ApiKey { action } => action.run(&client).await,
            Commands::Dataset { action } => action.run(&client).await,
        }
    }

    fn resolve_admin_key(&self) -> anyhow::Result<String> {
        if let Some(key) = &self.admin_key {
            return Ok(key.clone());
        }

        if let Some(config_path) = &self.config {
            let config = common::config::Configuration::load_from_path(config_path)
                .map_err(|e| anyhow::anyhow!("Failed to load config: {e}"))?;
            if let Some(key) = config.auth.admin_api_key {
                return Ok(key);
            }
            anyhow::bail!("Config file has no admin_api_key under [auth]");
        }

        anyhow::bail!(
            "No admin key provided. Use --admin-key, SIGNALDB_ADMIN_KEY, or --config with [auth] admin_api_key"
        )
    }
}
