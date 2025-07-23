pub mod config;
pub mod error;
pub mod protocol;
pub mod state;
pub mod storage;

pub use config::HeraclitusConfig;
pub use error::{HeraclitusError, Result};

use common::service_bootstrap::ServiceBootstrap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

pub struct HeraclitusAgent {
    config: HeraclitusConfig,
    #[allow(dead_code)] // Will be used when protocol is fully implemented
    state_manager: Arc<state::StateManager>,
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
            Arc::new(state::StateManager::new(object_store, config.state.clone()).await?);

        // Create protocol handler
        let protocol_handler =
            protocol::ProtocolHandler::new(state_manager.clone(), config.kafka_port);

        Ok(Self {
            config,
            state_manager,
            protocol_handler,
            _service_bootstrap: service_bootstrap,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let port = self.config.kafka_port;
        info!("Starting Heraclitus agent on port {port}");

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
