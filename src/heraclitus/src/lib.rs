pub mod config;
pub mod error;
pub mod http;
pub mod metrics;
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
    metrics_registry: Arc<prometheus::Registry>,
    #[allow(dead_code)] // Used indirectly through components
    metrics: Arc<metrics::Metrics>,
}

impl HeraclitusAgent {
    pub async fn new(config: HeraclitusConfig) -> Result<Self> {
        info!("Initializing Heraclitus Kafka-compatible agent");

        // Initialize metrics
        let (metrics_registry, metrics) = if config.metrics.enabled {
            metrics::create_metrics_registry(&config.metrics.prefix)?
        } else {
            // Create empty registry if metrics are disabled
            let registry = Arc::new(prometheus::Registry::new());
            let metrics = Arc::new(metrics::Metrics::new(&registry, &config.metrics.prefix)?);
            (registry, metrics)
        };

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
            metrics.clone(),
        ));

        // Create protocol handler with metrics
        let protocol_handler = protocol::ProtocolHandler::new(
            state_manager.clone(),
            batch_writer.clone(),
            config.kafka_port,
            Arc::new(config.auth.clone()),
            metrics.clone(),
        );

        Ok(Self {
            config,
            state_manager,
            batch_writer,
            protocol_handler,
            _service_bootstrap: service_bootstrap,
            metrics_registry,
            metrics,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let kafka_port = self.config.kafka_port;
        let http_port = self.config.http_port;

        info!("Starting Heraclitus agent - Kafka: {kafka_port}, HTTP: {http_port}");

        // Start the batch writer flush timer
        self.batch_writer.start_flush_timer().await;
        info!("Started batch writer flush timer");

        // Start HTTP server for metrics and health
        let metrics_registry = self.metrics_registry.clone();
        let http_handle = tokio::spawn(async move {
            if let Err(e) = http::run_http_server(http_port, metrics_registry).await {
                error!("HTTP server error: {}", e);
            }
        });

        // Start Kafka protocol server
        let listener = TcpListener::bind(format!("0.0.0.0:{kafka_port}"))
            .await
            .map_err(|e| HeraclitusError::Network(e.to_string()))?;

        info!("Heraclitus Kafka server listening on port {kafka_port}");

        // Handle graceful shutdown
        let shutdown_signal = tokio::signal::ctrl_c();
        tokio::pin!(shutdown_signal);

        loop {
            tokio::select! {
                Ok((socket, addr)) = listener.accept() => {
                    info!("New Kafka client connection from {addr}");
                    let handler = self.protocol_handler.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handler.handle_connection(socket).await {
                            error!("Error handling connection from {addr}: {e}");
                        }
                    });
                }
                _ = &mut shutdown_signal => {
                    info!("Received shutdown signal, starting graceful shutdown");
                    break;
                }
            }
        }

        // Graceful shutdown
        info!("Shutting down Heraclitus agent...");

        // TODO: Close active connections gracefully
        // TODO: Flush pending batches
        // TODO: Deregister from service discovery

        // Wait for HTTP server to finish
        http_handle.abort();

        info!("Heraclitus agent shutdown complete");
        Ok(())
    }
}
