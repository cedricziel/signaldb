// Simplified standalone Heraclitus without SignalDB dependencies

pub mod config;
pub mod error;
pub mod http;
pub mod metrics;
pub mod protocol_v2;
pub mod state;
pub mod storage;

pub use config::HeraclitusConfig;
pub use error::{HeraclitusError, Result};
pub use storage::KafkaMessage;

use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use std::sync::Arc;
use storage::{BatchWriter, MessageReader};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{error, info};

pub struct HeraclitusAgent {
    config: HeraclitusConfig,
    _state_manager: Arc<state::StateManager>,
    batch_writer: Arc<BatchWriter>,
    protocol_handler: protocol_v2::ProtocolHandler,
    metrics_registry: Arc<prometheus::Registry>,
    _metrics: Arc<metrics::Metrics>,
    shutdown_tx: broadcast::Sender<()>,
}

impl HeraclitusAgent {
    pub async fn new(config: HeraclitusConfig) -> Result<Self> {
        info!("Initializing Heraclitus Kafka-compatible server");

        // Initialize metrics
        let (metrics_registry, metrics) = if config.metrics.enabled {
            metrics::create_metrics_registry(&config.metrics.prefix)?
        } else {
            let registry = Arc::new(prometheus::Registry::new());
            let metrics = Arc::new(metrics::Metrics::new(&registry, &config.metrics.prefix)?);
            (registry, metrics)
        };

        // Create object store directly based on storage path
        let object_store: Arc<dyn ObjectStore> = if config.storage.path.starts_with("memory://") {
            Arc::new(InMemory::new())
        } else {
            Arc::new(LocalFileSystem::new_with_prefix(&config.storage.path)?)
        };

        // Initialize state manager
        let state_manager =
            Arc::new(state::StateManager::new(object_store.clone(), config.state.clone()).await?);

        // Create batch writer
        let batch_writer = Arc::new(BatchWriter::new(
            object_store.clone(),
            state_manager.layout().clone(),
            config.batching.clone(),
            metrics.clone(),
        ));

        // Create message reader
        let message_reader = Arc::new(MessageReader::new(
            object_store,
            state_manager.layout().clone(),
        ));

        // Create protocol handler using kafka-protocol implementation
        let protocol_config = protocol_v2::ProtocolConfig {
            auth_config: Arc::new(config.auth.clone()),
            topic_config: Arc::new(config.topics.clone()),
            compression_config: Arc::new(config.compression.clone()),
        };

        let protocol_handler = protocol_v2::ProtocolHandler::new(
            state_manager.clone(),
            batch_writer.clone(),
            message_reader,
            config.kafka_port,
            metrics.clone(),
            protocol_config,
        );

        // Create shutdown channel
        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            config,
            _state_manager: state_manager,
            batch_writer,
            protocol_handler,
            metrics_registry,
            _metrics: metrics,
            shutdown_tx,
        })
    }

    pub async fn run(self) -> Result<()> {
        info!(
            "Starting Heraclitus - Kafka: {}, HTTP: {}",
            self.config.kafka_port, self.config.http_port
        );

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Start batch writer flush timer
        self.batch_writer.start_flush_timer().await;
        info!("Started batch writer flush timer");

        // Start Kafka protocol server
        let kafka_listener =
            TcpListener::bind(format!("0.0.0.0:{}", self.config.kafka_port)).await?;
        info!("Kafka server listening on port {}", self.config.kafka_port);

        let protocol_handler = self.protocol_handler;
        let kafka_task = tokio::spawn(async move {
            let mut connection_count = 0;
            loop {
                match kafka_listener.accept().await {
                    Ok((socket, addr)) => {
                        connection_count += 1;
                        info!(
                            "New Kafka client connection #{} from {}",
                            connection_count, addr
                        );
                        let handler = protocol_handler.clone();
                        let conn_id = connection_count;
                        tokio::spawn(async move {
                            info!("Spawning handler #{} for connection from {}", conn_id, addr);
                            if let Err(e) = handler.handle_connection(socket).await {
                                error!(
                                    "Connection handler #{} error from {}: {}",
                                    conn_id, addr, e
                                );
                            }
                            info!("Connection handler #{} finished for {}", conn_id, addr);
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
        });

        // Start HTTP server
        let http_task = if self.config.metrics.enabled {
            let metrics_registry = self.metrics_registry.clone();
            let http_port = self.config.http_port;
            Some(tokio::spawn(async move {
                if let Err(e) = http::run_http_server(http_port, metrics_registry).await {
                    error!("HTTP server error: {}", e);
                }
            }))
        } else {
            None
        };

        // Wait for shutdown signal
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Received shutdown signal");
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, initiating shutdown");
            }
        }

        // Cleanup
        kafka_task.abort();
        if let Some(task) = http_task {
            task.abort();
        }

        Ok(())
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heraclitus_config_creation() {
        let config = HeraclitusConfig::default();
        assert_eq!(config.kafka_port, 9092);
        assert_eq!(config.http_port, 9093);
    }
}

// TODO: Fix test module imports - tests are currently run as separate binaries
// #[cfg(test)]
// #[path = "../tests/mod.rs"]
// mod test_suite;
