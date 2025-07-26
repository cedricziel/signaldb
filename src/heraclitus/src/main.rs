// Standalone main entry point without SignalDB dependencies

use clap::Parser;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about = "Heraclitus - Kafka-compatible server")]
struct Args {
    /// Kafka protocol port
    #[arg(long, default_value = "9092")]
    kafka_port: u16,

    /// HTTP metrics port
    #[arg(long, default_value = "9093")]
    http_port: u16,

    /// Storage path (use "memory://" for in-memory storage)
    #[arg(long, default_value = "/var/lib/heraclitus")]
    storage_path: String,

    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    let filter = if args.debug {
        "heraclitus=debug,info"
    } else {
        "heraclitus=info,warn"
    };

    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("Starting Heraclitus Kafka-compatible server");

    // Load configuration
    let config = if let Some(config_path) = args.config {
        let config_str = std::fs::read_to_string(&config_path)?;
        toml::from_str(&config_str)?
    } else {
        // Create default config with CLI overrides
        HeraclitusConfig {
            kafka_port: args.kafka_port,
            http_port: args.http_port,
            storage: heraclitus::config::StorageConfig {
                path: args.storage_path,
            },
            ..Default::default()
        }
    };

    // Create storage directory if needed
    if !config.storage.path.starts_with("memory://") {
        std::fs::create_dir_all(&config.storage.path)?;
    }

    // Create and run server
    let server = HeraclitusAgent::new(config).await?;

    if let Err(e) = server.run().await {
        error!("Server error: {}", e);
        return Err(e.into());
    }

    Ok(())
}
