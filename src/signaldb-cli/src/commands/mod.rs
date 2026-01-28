pub mod api_key;
pub mod dataset;
pub mod tenant;

use clap::{Parser, Subcommand};
use signaldb_sdk::Client;

/// SignalDB CLI — manage tenants, API keys, and datasets
#[derive(Parser)]
#[command(name = "signaldb-cli", version, about)]
pub struct Cli {
    /// SignalDB router base URL
    #[arg(long, env = "SIGNALDB_URL", default_value = "http://localhost:3000")]
    url: String,

    /// Admin API key
    #[arg(long, env = "SIGNALDB_ADMIN_KEY")]
    admin_key: String,

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
        let base_url = format!("{}/api/v1/admin", self.url.trim_end_matches('/'));

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", self.admin_key))?,
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
}
