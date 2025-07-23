pub mod config;
pub mod error;
pub mod protocol;
pub mod state;
pub mod storage;

pub use config::HeraclitusConfig;
pub use error::{HeraclitusError, Result};

// Re-export commonly used types for integration tests
pub use protocol::{CompressionType, RecordBatch, RecordBatchBuilder};
pub use storage::KafkaMessage;

use common::service_bootstrap::ServiceBootstrap;
use std::sync::Arc;
use storage::BatchWriter;
use tokio::net::TcpListener;
use tracing::{error, info};

pub struct HeraclitusAgent {
    config: HeraclitusConfig,
    #[allow(dead_code)] // Will be used for direct state access when needed
    state_manager: Arc<state::StateManager>,
    batch_writer: Arc<BatchWriter>,
    protocol_handler: protocol::ProtocolHandler,
    _service_bootstrap: ServiceBootstrap,
}

impl HeraclitusAgent {
    pub async fn new(config: HeraclitusConfig) -> Result<Self> {
        info!("Initializing Heraclitus Kafka-compatible agent");

        // Initialize service discovery
        let service_address = format!("0.0.0.0:{}", config.kafka_port);
        let service_bootstrap = ServiceBootstrap::new(
            config.common_config.clone(),
            common::service_bootstrap::ServiceType::Heraclitus,
            service_address,
        )
        .await
        .map_err(|e| HeraclitusError::Initialization(e.to_string()))?;

        // Create object store from config
        let object_store = common::storage::create_object_store(&config.common_config.storage)
            .map_err(|e| HeraclitusError::Storage(e.to_string()))?;

        // Initialize state manager (all state in object storage)
        let state_manager =
            Arc::new(state::StateManager::new(object_store.clone(), config.state.clone()).await?);

        // Create batch writer for message storage
        let batch_writer = Arc::new(BatchWriter::new(
            object_store,
            state_manager.layout().clone(),
            config.batching.clone(),
        ));

        // Create protocol handler
        let protocol_handler = protocol::ProtocolHandler::new(
            state_manager.clone(),
            batch_writer.clone(),
            config.kafka_port,
        );

        Ok(Self {
            config,
            state_manager,
            batch_writer,
            protocol_handler,
            _service_bootstrap: service_bootstrap,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let port = self.config.kafka_port;
        info!("Starting Heraclitus agent on port {port}");

        // Start the batch writer flush timer
        self.batch_writer.start_flush_timer().await;
        info!("Started batch writer flush timer");

        let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .map_err(|e| HeraclitusError::Network(e.to_string()))?;

        info!("Heraclitus agent listening on port {port}");

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("New Kafka client connection from {addr}");
                    let handler = self.protocol_handler.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handler.handle_connection(socket).await {
                            error!("Error handling connection from {addr}: {e}");
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting connection: {e}");
                }
            }
        }
    }
}
