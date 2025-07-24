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
use tokio::sync::broadcast;
use tokio::time::{Duration, timeout};
use tracing::{error, info, warn};

pub struct HeraclitusAgent {
    config: HeraclitusConfig,
    #[allow(dead_code)] // Will be used for direct state access when needed
    state_manager: Arc<state::StateManager>,
    batch_writer: Arc<BatchWriter>,
    protocol_handler: protocol::ProtocolHandler,
    service_bootstrap: Option<ServiceBootstrap>,
    metrics_registry: Arc<prometheus::Registry>,
    #[allow(dead_code)] // Used indirectly through components
    metrics: Arc<metrics::Metrics>,
    shutdown_tx: broadcast::Sender<()>,
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

        // Create shutdown broadcast channel
        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            config,
            state_manager,
            batch_writer,
            protocol_handler,
            service_bootstrap: Some(service_bootstrap),
            metrics_registry,
            metrics,
            shutdown_tx,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let kafka_port = self.config.kafka_port;
        let http_port = self.config.http_port;

        info!("Starting Heraclitus agent - Kafka: {kafka_port}, HTTP: {http_port}");

        // Start the batch writer flush timer
        self.batch_writer.start_flush_timer().await;
        info!("Started batch writer flush timer");

        // Start HTTP server for metrics and health
        let metrics_registry = self.metrics_registry.clone();
        let mut http_shutdown_rx = self.shutdown_tx.subscribe();
        let http_handle = tokio::spawn(async move {
            tokio::select! {
                result = http::run_http_server(http_port, metrics_registry) => {
                    if let Err(e) = result {
                        error!("HTTP server error: {}", e);
                    }
                }
                _ = http_shutdown_rx.recv() => {
                    info!("HTTP server received shutdown signal");
                }
            }
        });

        // Start Kafka protocol server
        let listener = TcpListener::bind(format!("0.0.0.0:{kafka_port}"))
            .await
            .map_err(|e| HeraclitusError::Network(e.to_string()))?;

        info!("Heraclitus Kafka server listening on port {kafka_port}");

        // Track active connections
        let (conn_tx, mut conn_rx) =
            tokio::sync::mpsc::unbounded_channel::<tokio::task::JoinHandle<()>>();
        let mut active_connections = Vec::new();

        // Handle graceful shutdown
        let shutdown_signal = tokio::signal::ctrl_c();
        tokio::pin!(shutdown_signal);

        // Accept connections until shutdown
        loop {
            tokio::select! {
                Ok((socket, addr)) = listener.accept() => {
                    info!("New Kafka client connection from {addr}");
                    let handler = self.protocol_handler.clone();
                    let mut shutdown_rx = self.shutdown_tx.subscribe();

                    let handle = tokio::spawn(async move {
                        tokio::select! {
                            result = handler.handle_connection(socket) => {
                                if let Err(e) = result {
                                    error!("Error handling connection from {addr}: {e}");
                                }
                            }
                            _ = shutdown_rx.recv() => {
                                info!("Connection from {addr} received shutdown signal");
                            }
                        }
                    });

                    // Track the connection
                    if conn_tx.send(handle).is_err() {
                        warn!("Failed to track connection handle");
                    }
                }
                Some(handle) = conn_rx.recv() => {
                    // Collect completed connections
                    active_connections.push(handle);
                }
                _ = &mut shutdown_signal => {
                    info!("Received shutdown signal, starting graceful shutdown");
                    break;
                }
            }
        }

        // Graceful shutdown sequence
        info!("Shutting down Heraclitus agent...");
        let shutdown_timeout = Duration::from_secs(self.config.shutdown_timeout_sec);

        // 1. Stop accepting new connections (already done by breaking the loop)

        // 2. Notify all components about shutdown
        if let Err(e) = self.shutdown_tx.send(()) {
            warn!("Failed to send shutdown signal: {}", e);
        }

        // 3. Wait for active connections to complete (with timeout)
        info!(
            "Waiting for {} active connections to close...",
            active_connections.len()
        );
        let connections_future = async {
            for handle in active_connections {
                let _ = handle.await;
            }
        };

        if timeout(shutdown_timeout, connections_future).await.is_err() {
            warn!("Some connections did not close within timeout");
        }

        // 4. Flush all pending batches
        info!("Flushing pending batches...");
        if let Err(e) = self.batch_writer.flush_all_pending().await {
            error!("Error flushing pending batches: {}", e);
        }

        // 5. Deregister from service discovery
        info!("Deregistering from service discovery...");
        if let Some(service_bootstrap) = self.service_bootstrap.take() {
            if let Err(e) = service_bootstrap.shutdown().await {
                error!("Error during service deregistration: {}", e);
            }
        }

        // 6. Wait for HTTP server to finish
        info!("Waiting for HTTP server to shutdown...");
        if timeout(Duration::from_secs(5), http_handle).await.is_err() {
            warn!("HTTP server did not shutdown cleanly within timeout");
        }

        // 7. Final metrics update
        self.metrics.connection.active_connections.set(0);

        info!("Heraclitus agent shutdown complete");
        Ok(())
    }
}
