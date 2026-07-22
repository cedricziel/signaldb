use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Common CLI functionality shared across all SignalDB binaries
#[allow(async_fn_in_trait)]
pub trait SignalDbCli {
    /// Get the service name for this binary
    fn service_name() -> &'static str;

    /// Get the service description
    fn service_description() -> &'static str;

    /// Execute the CLI command
    async fn execute(self) -> anyhow::Result<()>;
}

/// Common CLI arguments shared across all binaries
#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
    #[arg(long, help = "Configuration file path")]
    pub config: Option<PathBuf>,

    #[arg(short, long, help = "Enable verbose logging")]
    pub verbose: bool,

    #[arg(short, long, help = "Enable quiet mode (minimal output)")]
    pub quiet: bool,
}

/// Common subcommands available for all services
#[derive(Subcommand, Debug, Clone, Default)]
pub enum CommonCommands {
    /// Start the service (default behavior)
    #[default]
    Start,
    /// Show current configuration and exit
    Config {
        #[arg(long, help = "Show configuration in JSON format")]
        json: bool,
    },
    /// Validate configuration and exit
    Validate,
    /// Show version information and exit
    Version,
}

/// Utility functions for CLI operations
pub mod utils {
    use super::*;
    use crate::config::Configuration;
    use crate::self_monitoring::{OtelExportFilter, Telemetry as SelfTelemetry};
    use anyhow::{Context, Result};
    use tracing_subscriber::Layer as _;

    /// Initialize logging based on CLI arguments.
    ///
    /// Builds a layered `tracing` subscriber:
    /// - `EnvFilter` from `RUST_LOG` (falling back to the level implied by
    ///   `--verbose`/`--quiet`)
    /// - an fmt (console) layer, always present
    /// - when self-monitoring `telemetry` is provided: an OpenTelemetry span
    ///   bridge (`#[tracing::instrument]` spans become OTel spans) and an
    ///   OpenTelemetry log bridge (`tracing`/`log` events become OTel log
    ///   records), both guarded by [`OtelExportFilter`] so `_system`-tenant
    ///   processing and the exporter's own transport stack are never exported.
    ///
    /// `log::` macro calls flow into all layers via tracing-subscriber's
    /// built-in `tracing-log` compatibility (enabled by `.init()`).
    pub fn init_logging(args: &CommonArgs, telemetry: Option<&SelfTelemetry>) {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let default_level = if args.quiet {
            "warn"
        } else if args.verbose {
            "debug"
        } else {
            "info"
        };
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));

        let registry = tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer());

        if let Some(telemetry) = telemetry {
            let tracer = telemetry.tracer_provider().tracer("signaldb");
            let otel_span_layer = tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(OtelExportFilter);
            let otel_log_layer =
                opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
                    telemetry.logger_provider(),
                )
                .with_filter(OtelExportFilter);
            registry.with(otel_span_layer).with(otel_log_layer).init();
        } else {
            registry.init();
        }
    }

    /// Load configuration with optional override from CLI
    pub fn load_config(config_path: Option<&PathBuf>) -> Result<Configuration> {
        match config_path {
            Some(path) => {
                tracing::info!(path = %path.display(), "Loading configuration");
                Configuration::load_from_path(path).context("Failed to load configuration")
            }
            None => Configuration::load().context("Failed to load configuration"),
        }
    }

    /// Display configuration in human-readable or JSON format
    pub fn display_config(config: &Configuration, json: bool) -> Result<()> {
        if json {
            let json = serde_json::to_string_pretty(config)
                .context("Failed to serialize configuration to JSON")?;
            println!("{json}");
        } else {
            println!("SignalDB Configuration:");
            println!("======================");
            println!("Database DSN: {}", config.database.dsn);
            println!("Storage DSN: {}", config.storage.dsn);

            if let Some(discovery) = &config.discovery {
                println!("Discovery DSN: {}", discovery.dsn);
                println!(
                    "Discovery heartbeat interval: {:?}",
                    discovery.heartbeat_interval
                );
                println!("Discovery poll interval: {:?}", discovery.poll_interval);
            } else {
                println!("Discovery: disabled");
            }

            println!("Schema catalog type: {}", config.schema.catalog_type);
            println!("Schema catalog URI: {}", config.schema.catalog_uri);

            println!("WAL directory: {:?}", config.wal.wal_dir);
            println!("Default tenant: {}", config.tenants.default_tenant);
        }
        Ok(())
    }

    /// Validate configuration and report any issues
    pub fn validate_config(config: &Configuration) -> Result<()> {
        tracing::info!("Validating configuration...");

        // Basic validation checks
        if config.database.dsn.is_empty() {
            anyhow::bail!("Database DSN cannot be empty");
        }

        if config.storage.dsn.is_empty() {
            anyhow::bail!("Storage DSN cannot be empty");
        }

        if config.schema.catalog_uri.is_empty() {
            anyhow::bail!("Schema catalog URI cannot be empty");
        }

        // Validate discovery configuration if present
        if let Some(discovery) = &config.discovery
            && discovery.dsn.is_empty()
        {
            anyhow::bail!("Discovery DSN cannot be empty when discovery is enabled");
        }

        tracing::info!("Configuration validation passed");
        Ok(())
    }

    /// Handle common CLI commands that don't require starting services
    pub async fn handle_common_command(
        command: &CommonCommands,
        config: &Configuration,
    ) -> Result<bool> {
        match command {
            CommonCommands::Config { json } => {
                display_config(config, *json)?;
                Ok(true) // Command handled, don't start service
            }
            CommonCommands::Validate => {
                validate_config(config)?;
                Ok(true) // Command handled, don't start service  
            }
            CommonCommands::Version => {
                println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
                println!("Rust version: {}", env!("CARGO_PKG_RUST_VERSION"));
                Ok(true) // Command handled, don't start service
            }
            CommonCommands::Start => {
                Ok(false) // Don't handle, let service start
            }
        }
    }

    /// Standard version information
    pub fn version_info() -> String {
        format!(
            "{} {} ({})",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            env!("CARGO_PKG_RUST_VERSION")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_commands_default() {
        let default_cmd = CommonCommands::default();
        matches!(default_cmd, CommonCommands::Start);
    }

    #[test]
    fn test_version_info() {
        let version = utils::version_info();
        assert!(version.contains(env!("CARGO_PKG_VERSION")));
    }
}
