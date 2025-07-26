// Simplified standalone Heraclitus without SignalDB dependencies

pub mod config;
pub mod error;
pub mod http;
pub mod metrics;
pub mod protocol;
pub mod state;
pub mod storage;

pub use config::HeraclitusConfig;
pub use error::{HeraclitusError, Result};
pub use protocol::{CompressionType, RecordBatch, RecordBatchBuilder};
pub use storage::KafkaMessage;

use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use std::sync::Arc;
use storage::BatchWriter;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{error, info};

pub struct HeraclitusAgent {
    config: HeraclitusConfig,
    _state_manager: Arc<state::StateManager>,
    batch_writer: Arc<BatchWriter>,
    protocol_handler: protocol::ProtocolHandler,
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
            object_store,
            state_manager.layout().clone(),
            config.batching.clone(),
            metrics.clone(),
        ));

        // Create protocol handler
        let protocol_handler = protocol::ProtocolHandler::new(
            state_manager.clone(),
            batch_writer.clone(),
            config.kafka_port,
            Arc::new(config.auth.clone()),
            metrics.clone(),
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
            loop {
                match kafka_listener.accept().await {
                    Ok((socket, addr)) => {
                        info!("New Kafka client connection from {}", addr);
                        let handler = protocol_handler.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handler.handle_connection(socket).await {
                                error!("Connection handler error: {}", e);
                            }
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
