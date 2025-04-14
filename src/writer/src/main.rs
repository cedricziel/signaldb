use std::sync::Arc;

use anyhow::Result;
use common::config::QueueConfig;
use object_store::local::LocalFileSystem;
use tracing_subscriber;
use writer::{BatchWriter, QueueBatchWriter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create object store
    let object_store = Arc::new(LocalFileSystem::new_with_prefix("./data")?);

    // Create writer with default queue config
    let writer = QueueBatchWriter::new(QueueConfig::default(), object_store);

    // Start the writer
    log::info!("Starting writer service...");
    writer.start().await?;

    Ok(())
}
