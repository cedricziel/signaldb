pub mod connection;
pub mod protocol;
pub mod storage;

use crate::error::Result;
use prometheus::Registry;
use std::sync::Arc;

/// All metrics for Heraclitus
pub struct Metrics {
    pub connection: connection::ConnectionMetrics,
    pub protocol: protocol::ProtocolMetrics,
    pub storage: storage::StorageMetrics,
}

impl Metrics {
    /// Create and register all metrics
    pub fn new(registry: &Registry, prefix: &str) -> Result<Self> {
        Ok(Self {
            connection: connection::ConnectionMetrics::new(registry, prefix)?,
            protocol: protocol::ProtocolMetrics::new(registry, prefix)?,
            storage: storage::StorageMetrics::new(registry, prefix)?,
        })
    }
}

/// Create a new metrics registry with default metrics
pub fn create_metrics_registry(prefix: &str) -> Result<(Arc<Registry>, Arc<Metrics>)> {
    let registry = Registry::new();

    // Register default process and runtime metrics (only on Linux)
    #[cfg(target_os = "linux")]
    {
        let process_collector = prometheus::process_collector::ProcessCollector::for_self();
        registry
            .register(Box::new(process_collector))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register process metrics: {e}"
                ))
            })?;
    }

    // Create our custom metrics
    let metrics = Arc::new(Metrics::new(&registry, prefix)?);

    Ok((Arc::new(registry), metrics))
}
