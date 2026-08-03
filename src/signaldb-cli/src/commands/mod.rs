pub mod api_key;
pub mod completions;
pub mod dataset;
pub mod query;
pub mod tenant;
pub mod user;

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use clap_complete::engine::ArgValueCompleter;
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
    /// Query SignalDB in one language (exactly one of
    /// --sql/--promql/--logql/--traceql/--ir)
    Query(query::QueryArgs),
    /// Administrative operations (tenants, API keys, datasets)
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
    /// Bootstrap human users directly in the service catalog
    User {
        #[command(subcommand)]
        action: user::UserAction,
    },
    /// Generate a shell completion script on stdout
    ///
    /// Install it with your shell's completion mechanism, e.g.:
    ///
    ///   signaldb-cli completions bash > /etc/bash_completion.d/signaldb-cli
    ///   signaldb-cli completions zsh > ~/.zfunc/_signaldb-cli
    ///   signaldb-cli completions fish > ~/.config/fish/completions/signaldb-cli.fish
    ///
    /// For dynamic completions that also suggest live tenant IDs (fetched
    /// from the admin API using SIGNALDB_URL / SIGNALDB_ADMIN_KEY), register
    /// the COMPLETE hook instead, e.g. for zsh:
    ///
    ///   echo 'source <(COMPLETE=zsh signaldb-cli)' >> ~/.zshrc
    #[command(verbatim_doc_comment)]
    Completions {
        /// Shell to generate a completion script for
        shell: clap_complete::Shell,
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
        #[arg(
            long,
            env = "SIGNALDB_TENANT_ID",
            add = ArgValueCompleter::new(completions::tenant_id_completer)
        )]
        tenant_id: Option<String>,

        /// Dataset ID
        #[arg(long, env = "SIGNALDB_DATASET_ID")]
        dataset_id: Option<String>,
    },
}

/// Administrative subcommands, all reached through the admin API via the SDK.
#[derive(Subcommand)]
enum AdminAction {
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
        if let Commands::Query(args) = self.command {
            return args.run().await;
        }

        if let Commands::Completions { shell } = self.command {
            return completions::generate(shell);
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
            let tui_config_admin_key = tui_config.and_then(|path| admin_key_from_config(&path));
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
            Commands::Admin { action } => match action {
                AdminAction::Tenant { action } => action.run(&client).await,
                AdminAction::ApiKey { action } => action.run(&client).await,
                AdminAction::Dataset { action } => action.run(&client).await,
            },
            Commands::User { action } => action.run(&client).await,
            Commands::Query(_) => unreachable!(),
            Commands::Completions { .. } => unreachable!(),
            Commands::Tui { .. } => unreachable!(),
        }
    }

    fn resolve_admin_key(&self) -> anyhow::Result<String> {
        if let Some(key) = &self.admin_key {
            return Ok(key.clone());
        }

        if let Some(config_path) = &self.config {
            if let Some(key) = admin_key_from_config(config_path) {
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
        admin_key_from_config(config_path)
    }
}

/// Read only `[auth].admin_api_key` from a SignalDB config file, without
/// depending on the server's full `Configuration` type — the CLI is a
/// first-class SDK consumer and does not link SignalDB internals.
fn admin_key_from_config(path: &std::path::Path) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct MinimalConfig {
        auth: Option<MinimalAuth>,
    }
    #[derive(serde::Deserialize)]
    struct MinimalAuth {
        admin_api_key: Option<String>,
    }
    let text = std::fs::read_to_string(path).ok()?;
    let cfg: MinimalConfig = toml::from_str(&text).ok()?;
    cfg.auth?.admin_api_key
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

#[cfg(test)]
mod parse_tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(args)
    }

    #[test]
    fn query_requires_a_language_flag() {
        // No flag: clap rejects (exit code 2 at runtime).
        assert!(parse(&["signaldb-cli", "query", "SELECT 1"]).is_err());
    }

    #[test]
    fn query_accepts_exactly_one_language() {
        assert!(parse(&["signaldb-cli", "query", "--sql", "SELECT 1"]).is_ok());
        assert!(parse(&["signaldb-cli", "query", "--promql", "up"]).is_ok());
        assert!(parse(&["signaldb-cli", "query", "--logql", "{x=\"y\"}"]).is_ok());
        assert!(parse(&["signaldb-cli", "query", "--traceql", "{}"]).is_ok());
    }

    #[test]
    fn query_rejects_multiple_languages() {
        assert!(parse(&["signaldb-cli", "query", "--sql", "--promql", "x"]).is_err());
        assert!(parse(&["signaldb-cli", "query", "--sql", "--ir", "x"]).is_err());
    }

    #[test]
    fn ir_needs_no_positional() {
        // --ir reads from --file or stdin, so the positional is optional.
        assert!(parse(&["signaldb-cli", "query", "--ir", "--file", "q.json"]).is_ok());
        assert!(parse(&["signaldb-cli", "query", "--ir"]).is_ok());
    }

    #[test]
    fn management_lives_under_admin() {
        assert!(parse(&["signaldb-cli", "admin", "tenant", "list"]).is_ok());
        assert!(parse(&["signaldb-cli", "admin", "api-key", "list", "acme"]).is_ok());
        assert!(parse(&["signaldb-cli", "admin", "dataset", "list", "acme"]).is_ok());
    }

    #[test]
    fn old_top_level_management_commands_are_gone() {
        // BREAKING (post-1.0): tenant/api-key/dataset moved under `admin`.
        assert!(parse(&["signaldb-cli", "tenant", "list"]).is_err());
        assert!(parse(&["signaldb-cli", "api-key", "list", "acme"]).is_err());
        assert!(parse(&["signaldb-cli", "dataset", "list", "acme"]).is_err());
    }
}
