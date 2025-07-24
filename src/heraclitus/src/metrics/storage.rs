use crate::error::Result;
use prometheus::{IntCounter, IntGauge, Opts, Registry};

pub struct StorageMetrics {
    pub messages_written: IntCounter,
    pub bytes_written: IntCounter,
    pub batches_flushed: IntCounter,
    pub flush_errors: IntCounter,
    pub pending_messages: IntGauge,
}

impl StorageMetrics {
    pub fn new(registry: &Registry, prefix: &str) -> Result<Self> {
        let messages_written = IntCounter::with_opts(
            Opts::new(
                "messages_written_total",
                "Total number of messages written to storage",
            )
            .namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create messages_written metric: {e}"
            ))
        })?;

        let bytes_written = IntCounter::with_opts(
            Opts::new("bytes_written_total", "Total bytes written to storage").namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create bytes_written metric: {e}"
            ))
        })?;

        let batches_flushed = IntCounter::with_opts(
            Opts::new(
                "batches_flushed_total",
                "Total number of batches flushed to storage",
            )
            .namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create batches_flushed metric: {e}"
            ))
        })?;

        let flush_errors = IntCounter::with_opts(
            Opts::new("flush_errors_total", "Total number of flush errors").namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create flush_errors metric: {e}"
            ))
        })?;

        let pending_messages = IntGauge::with_opts(
            Opts::new("pending_messages", "Number of messages pending flush").namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create pending_messages metric: {e}"
            ))
        })?;

        // Register all metrics
        registry
            .register(Box::new(messages_written.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register messages_written: {e}"
                ))
            })?;
        registry
            .register(Box::new(bytes_written.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register bytes_written: {e}"
                ))
            })?;
        registry
            .register(Box::new(batches_flushed.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register batches_flushed: {e}"
                ))
            })?;
        registry
            .register(Box::new(flush_errors.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register flush_errors: {e}"
                ))
            })?;
        registry
            .register(Box::new(pending_messages.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register pending_messages: {e}"
                ))
            })?;

        Ok(Self {
            messages_written,
            bytes_written,
            batches_flushed,
            flush_errors,
            pending_messages,
        })
    }
}
