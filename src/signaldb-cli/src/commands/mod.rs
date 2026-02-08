pub mod api_key;
pub mod dataset;
pub mod query;
pub mod tenant;

use std::path::PathBuf;
use std::time::Duration;

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
    /// Query SignalDB via SQL
    Query {
        #[command(subcommand)]
        action: query::QueryAction,
    },
    /// Terminal User Interface for SignalDB
    Tui {
        /// Path to SignalDB configuration file (reads admin_api_key from [auth])
        #[arg(long)]
        config: Option<PathBuf>,

        /// SignalDB router base URL
        #[arg(long, env = "SIGNALDB_URL", default_value = "http://localhost:3000")]
        url: String,

        /// SignalDB Flight endpoint URL
        #[arg(
            long,
            env = "SIGNALDB_FLIGHT_URL",
            default_value = "http://localhost:50053"
        )]
        flight_url: String,

        /// API key for authentication
        #[arg(long, env = "SIGNALDB_API_KEY")]
        api_key: Option<String>,

        /// Admin API key for authentication
        #[arg(long, env = "SIGNALDB_ADMIN_KEY")]
        admin_key: Option<String>,

        /// Refresh rate for data updates
        #[arg(long, env = "SIGNALDB_TUI_REFRESH_RATE", default_value = "5s")]
        refresh_rate: String,

        /// Tenant ID
        #[arg(long, env = "SIGNALDB_TENANT_ID")]
        tenant_id: Option<String>,

        /// Dataset ID
        #[arg(long, env = "SIGNALDB_DATASET_ID")]
        dataset_id: Option<String>,
    },
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        if let Commands::Query { action } = self.command {
            return action.run().await;
        }

        let config_admin_key = self.try_resolve_admin_key();

        if let Commands::Tui {
            config: tui_config,
            url,
            flight_url,
            api_key,
            admin_key,
            refresh_rate,
            tenant_id,
            dataset_id,
        } = self.command
        {
            let tui_config_admin_key = tui_config.and_then(|path| {
                let cfg = common::config::Configuration::load_from_path(&path).ok()?;
                cfg.auth.admin_api_key
            });
            let effective_admin_key = admin_key.or(tui_config_admin_key).or(config_admin_key);
            let refresh = parse_duration(&refresh_rate)?;
            let mut app = crate::tui::app::App::new(
                url,
                flight_url,
                api_key,
                effective_admin_key,
                refresh,
                tenant_id,
                dataset_id,
            );
            return app.run().await;
        }

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
            Commands::Query { .. } => unreachable!(),
            Commands::Tui { .. } => unreachable!(),
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

    fn try_resolve_admin_key(&self) -> Option<String> {
        if let Some(key) = &self.admin_key {
            return Some(key.clone());
        }
        let config_path = self.config.as_ref()?;
        let config = common::config::Configuration::load_from_path(config_path).ok()?;
        config.auth.admin_api_key
    }
}

/// Parse a human-readable duration string like "5s", "100ms", "2m".
fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    if let Some(ms) = s.strip_suffix("ms") {
        let val: u64 = ms
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid duration: {s}"))?;
        return Ok(Duration::from_millis(val));
    }
    if let Some(secs) = s.strip_suffix('s') {
        let val: u64 = secs
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid duration: {s}"))?;
        return Ok(Duration::from_secs(val));
    }
    if let Some(mins) = s.strip_suffix('m') {
        let val: u64 = mins
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid duration: {s}"))?;
        return Ok(Duration::from_secs(val * 60));
    }
    anyhow::bail!("unsupported duration format: {s} (expected e.g. '5s', '100ms', '2m')")
}
