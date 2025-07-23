use anyhow::Result;
use clap::{Parser, Subcommand};
use common::cli::{CommonArgs, CommonCommands, utils};
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "heraclitus")]
#[command(about = "Heraclitus - Stateless Kafka-compatible agent for SignalDB")]
#[command(version)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<HeraclitusCommands>,

    #[arg(long, help = "Kafka protocol port", default_value = "9092")]
    kafka_port: u16,
}

#[derive(Subcommand)]
enum HeraclitusCommands {
    #[command(flatten)]
    Common(CommonCommands),
}

impl Default for HeraclitusCommands {
    fn default() -> Self {
        Self::Common(CommonCommands::Start)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging based on CLI arguments
    utils::init_logging(&cli.common);

    // Load application configuration
    let config = utils::load_config(cli.common.config.as_ref())?;

    // Handle common commands that don't require starting the service
    let command = cli.command.unwrap_or_default();
    let HeraclitusCommands::Common(ref common_cmd) = command;
    if utils::handle_common_command(common_cmd, &config).await? {
        return Ok(()); // Command handled, exit early
    }

    info!("Starting Heraclitus Kafka-compatible agent");

    // Create Heraclitus config from common configuration
    let mut heraclitus_config = HeraclitusConfig::from_common_config(config);
    // Override with CLI args if provided
    heraclitus_config.kafka_port = cli.kafka_port;

    // Create and run the agent
    let agent = HeraclitusAgent::new(heraclitus_config).await?;

    match agent.run().await {
        Ok(_) => {
            info!("Heraclitus agent stopped");
            Ok(())
        }
        Err(e) => {
            error!("Heraclitus agent failed: {}", e);
            Err(e.into())
        }
    }
}
