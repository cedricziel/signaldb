pub mod api_key;
pub mod completions;
pub mod dataset;
pub mod discover;
pub mod ops;
pub mod profiles;
pub mod query;
pub mod schema;
pub mod tenant;
pub mod tenant_self;
pub mod user;

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use clap_complete::engine::ArgValueCompleter;
use signaldb_sdk::Client;

/// Pretty-print a JSON-serializable value to stdout.
pub(crate) fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// Render an API key's dataset restriction for human display: the
/// comma-joined set, or `unrestricted` when there is none.
pub(crate) fn format_dataset_restriction(ids: Option<&[String]>) -> String {
    match ids {
        Some(ids) if !ids.is_empty() => ids.join(", "),
        _ => "unrestricted".to_string(),
    }
}

/// Render `ID  NAME  SCOPES  DATASETS` rows, column-aligned, shared by the
/// admin and tenant `api-key list` human-readable output.
pub(crate) fn format_api_key_table(rows: &[(String, String, String, String)]) -> String {
    if rows.is_empty() {
        return "No API keys.".to_string();
    }

    let widths = [
        rows.iter()
            .map(|(v, ..)| v.len())
            .chain(std::iter::once("ID".len()))
            .max()
            .unwrap_or(0),
        rows.iter()
            .map(|(_, v, ..)| v.len())
            .chain(std::iter::once("NAME".len()))
            .max()
            .unwrap_or(0),
        rows.iter()
            .map(|(_, _, v, _)| v.len())
            .chain(std::iter::once("SCOPES".len()))
            .max()
            .unwrap_or(0),
    ];

    let mut out = format!(
        "{:w0$}  {:w1$}  {:w2$}  DATASETS\n",
        "ID",
        "NAME",
        "SCOPES",
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2]
    );
    for (id, name, scopes, datasets) in rows {
        out.push_str(&format!(
            "{id:w0$}  {name:w1$}  {scopes:w2$}  {datasets}\n",
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2]
        ));
    }
    out.trim_end().to_string()
}

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

    /// Fail fast instead of retrying throttled (429) or transient requests
    /// (also SIGNALDB_NO_RETRY=1)
    #[arg(long, global = true)]
    no_retry: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Query SignalDB in one language (exactly one of
    /// --sql/--promql/--logql/--traceql/--ir)
    Query(query::QueryArgs),
    /// Discover attribute/label names and metric names
    Discover {
        #[command(subcommand)]
        action: discover::DiscoverAction,
    },
    /// Schema registry lookup (registries, attributes, entities, metrics)
    Schema {
        #[command(subcommand)]
        action: schema::SchemaAction,
    },
    /// Pyroscope-compatible profile query surface (types, labels,
    /// label-values, render, diff, by-trace)
    Profiles {
        #[command(subcommand)]
        action: profiles::ProfilesAction,
    },
    /// Administrative operations (tenants, API keys, datasets, schema registries)
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
    /// The caller's own tenant, through the management API (datasets, API
    /// keys, memberships, schema, signal tables)
    Tenant {
        #[command(subcommand)]
        action: tenant_self::TenantSelfAction,
    },
    /// Operational control (compaction)
    Ops {
        #[command(subcommand)]
        action: ops::OpsAction,
    },
    /// Bootstrap human users directly in the service catalog
    User {
        #[command(subcommand)]
        action: user::UserAction,
    },
    /// Report the authenticated identity (tenant, dataset, user) for the
    /// given credential
    Whoami(discover::ConnectArgs),
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
    /// Manage custom schema registries (tenant API key with `schema:write`)
    Schema {
        #[command(subcommand)]
        action: schema::AdminSchemaAction,
    },
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        // Retry policy for every client this invocation builds: `--no-retry`
        // or SIGNALDB_NO_RETRY=1 means fail-fast (see `crate::retry`). `main`
        // already applied the env var alone before dynamic completion ran;
        // this re-applies it ORed with the now-parsed `--no-retry` flag.
        crate::retry::init_no_retry_from_env();
        if self.no_retry {
            crate::retry::set_no_retry(true);
        }

        if let Commands::Query(args) = self.command {
            return args.run().await;
        }

        if let Commands::Discover { action } = self.command {
            return action.run().await;
        }

        if let Commands::Schema { action } = self.command {
            return action.run().await;
        }

        if let Commands::Profiles { action } = self.command {
            return action.run().await;
        }

        // The `tenant` group and `whoami` authenticate with a tenant API key
        // (management API / `/api/v1/whoami`), like `discover` and `schema`
        // above — never the instance admin key `admin` uses.
        if let Commands::Tenant { action } = self.command {
            return action.run().await;
        }

        if let Commands::Whoami(connect) = self.command {
            let v = connect.build_client()?.whoami().send().await;
            return query::print_json_response(v.map(|r| r.into_inner()), "whoami");
        }

        // Custom-registry management authenticates with a tenant API key
        // carrying `schema:write` (the schema API is tenant-scoped), not the
        // instance admin key the other `admin` nouns use.
        if let Commands::Admin {
            action: AdminAction::Schema { action },
        } = self.command
        {
            return action.run().await;
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

        // Both admin-authenticated dispatches (Ops and Admin/User below) carry
        // absolute paths (e.g. `/api/v1/ops/...`, `/api/v1/admin/tenants`), so
        // the SDK client base is the router root in both cases, not
        // `{url}/api/v1/...`, which would double-prefix.
        if matches!(self.command, Commands::Ops { .. }) {
            let admin_key = self.resolve_admin_key()?;
            let client = self.bearer_client(&admin_key)?;
            let Commands::Ops { action } = self.command else {
                unreachable!()
            };
            return action.run(&client).await;
        }

        let admin_key = self.resolve_admin_key()?;
        let client = self.bearer_client(&admin_key)?;

        match self.command {
            Commands::Admin { action } => match action {
                AdminAction::Tenant { action } => action.run(&client).await,
                AdminAction::ApiKey { action } => action.run(&client).await,
                AdminAction::Dataset { action } => action.run(&client).await,
                AdminAction::Schema { .. } => unreachable!(),
            },
            Commands::User { action } => action.run(&client).await,
            Commands::Ops { .. } => unreachable!(),
            Commands::Query(_) => unreachable!(),
            Commands::Discover { .. } => unreachable!(),
            Commands::Schema { .. } => unreachable!(),
            Commands::Profiles { .. } => unreachable!(),
            Commands::Completions { .. } => unreachable!(),
            Commands::Tui { .. } => unreachable!(),
            Commands::Tenant { .. } => unreachable!(),
            Commands::Whoami(_) => unreachable!(),
        }
    }

    /// A SignalDB SDK client authenticated with `admin_key`, rooted at the
    /// router base URL (the generated admin/ops methods carry absolute
    /// paths, so the client base must not add an `/api/v1/...` prefix).
    fn bearer_client(&self, admin_key: &str) -> anyhow::Result<Client> {
        Ok(crate::retry::client_builder(&self.url)
            .bearer(admin_key)
            .build()?)
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
    fn schema_lookup_lives_under_schema() {
        assert!(parse(&["signaldb-cli", "schema", "registry", "list"]).is_ok());
        assert!(
            parse(&[
                "signaldb-cli",
                "schema",
                "registry",
                "get",
                "otel",
                "1.43.0"
            ])
            .is_ok()
        );
        assert!(parse(&["signaldb-cli", "schema", "attribute", "get", "k8s.pod.uid"]).is_ok());
        assert!(parse(&["signaldb-cli", "schema", "attribute", "search", "k8s."]).is_ok());
        assert!(parse(&["signaldb-cli", "schema", "entity", "get", "k8s.pod"]).is_ok());
        assert!(parse(&["signaldb-cli", "schema", "entity", "search", "k8s."]).is_ok());
        assert!(
            parse(&[
                "signaldb-cli",
                "schema",
                "metric",
                "get",
                "k8s.pod.cpu.time"
            ])
            .is_ok()
        );
        assert!(parse(&["signaldb-cli", "schema", "metric", "search", "k8s."]).is_ok());
        // `schema` requires a noun and a verb.
        assert!(parse(&["signaldb-cli", "schema"]).is_err());
        assert!(parse(&["signaldb-cli", "schema", "attribute"]).is_err());
    }

    #[test]
    fn custom_registry_management_lives_under_admin() {
        assert!(
            parse(&[
                "signaldb-cli",
                "admin",
                "schema",
                "create",
                "--file",
                "conventions.yaml"
            ])
            .is_ok()
        );
        assert!(
            parse(&[
                "signaldb-cli",
                "admin",
                "schema",
                "replace",
                "acme",
                "1.0.0",
                "--file",
                "conventions.json"
            ])
            .is_ok()
        );
        assert!(parse(&["signaldb-cli", "admin", "schema", "delete", "acme", "1.0.0"]).is_ok());
        assert!(
            parse(&[
                "signaldb-cli",
                "admin",
                "schema",
                "validate",
                "--file",
                "conventions.yaml"
            ])
            .is_ok()
        );
        // Mutations are not reachable from the read-only `schema` group.
        assert!(parse(&["signaldb-cli", "schema", "create", "--file", "x.yaml"]).is_err());
    }

    #[test]
    fn ops_compact_subcommands_parse() {
        assert!(parse(&["signaldb-cli", "ops", "compact", "run"]).is_ok());
        assert!(parse(&["signaldb-cli", "ops", "compact", "status"]).is_ok());
        assert!(parse(&["signaldb-cli", "ops", "compact", "dry-run"]).is_ok());
        // `ops` requires a subcommand.
        assert!(parse(&["signaldb-cli", "ops"]).is_err());
    }

    #[test]
    fn old_top_level_management_commands_are_gone() {
        // BREAKING (post-1.0): tenant/api-key/dataset moved under `admin`.
        assert!(parse(&["signaldb-cli", "tenant", "list"]).is_err());
        assert!(parse(&["signaldb-cli", "api-key", "list", "acme"]).is_err());
        assert!(parse(&["signaldb-cli", "dataset", "list", "acme"]).is_err());
    }

    // Regression: the admin client base URL is the router root, so the generated
    // methods' absolute paths hit `/api/v1/admin/...` — not a double-prefixed
    // `/api/v1/admin/api/v1/admin/...`.
    #[tokio::test]
    async fn admin_client_uses_root_base_and_absolute_paths() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/admin/tenants")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"tenants":[]}"#)
            .create_async()
            .await;

        // The admin dispatch configures the client with the router root.
        let client = signaldb_sdk::ClientBuilder::new(server.url())
            .build()
            .unwrap();
        // The request must reach `/api/v1/admin/tenants`; a double-prefixed URL
        // would miss the mock and fail the assertion below.
        let _ = client.list_tenants().send().await;
        mock.assert_async().await;
    }
}
