use crate::error::Result;
use prometheus::{IntCounter, IntGauge, Opts, Registry};

pub struct ConnectionMetrics {
    pub active_connections: IntGauge,
    pub total_connections: IntCounter,
    pub connection_errors: IntCounter,
    pub authentication_attempts: IntCounter,
    pub authentication_successes: IntCounter,
    pub authentication_failures: IntCounter,
}

impl ConnectionMetrics {
    pub fn new(registry: &Registry, prefix: &str) -> Result<Self> {
        let active_connections = IntGauge::with_opts(
            Opts::new("active_connections", "Number of active client connections")
                .namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create active_connections metric: {e}"
            ))
        })?;

        let total_connections = IntCounter::with_opts(
            Opts::new("total_connections", "Total number of client connections").namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create total_connections metric: {e}"
            ))
        })?;

        let connection_errors = IntCounter::with_opts(
            Opts::new("connection_errors", "Number of connection errors").namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create connection_errors metric: {e}"
            ))
        })?;

        let authentication_attempts = IntCounter::with_opts(
            Opts::new("authentication_attempts", "Total authentication attempts").namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create authentication_attempts metric: {e}"
            ))
        })?;

        let authentication_successes = IntCounter::with_opts(
            Opts::new("authentication_successes", "Successful authentications").namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create authentication_successes metric: {e}"
            ))
        })?;

        let authentication_failures = IntCounter::with_opts(
            Opts::new("authentication_failures", "Failed authentications").namespace(prefix),
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create authentication_failures metric: {e}"
            ))
        })?;

        // Register all metrics
        registry
            .register(Box::new(active_connections.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register active_connections: {e}"
                ))
            })?;
        registry
            .register(Box::new(total_connections.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register total_connections: {e}"
                ))
            })?;
        registry
            .register(Box::new(connection_errors.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register connection_errors: {e}"
                ))
            })?;
        registry
            .register(Box::new(authentication_attempts.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register authentication_attempts: {e}"
                ))
            })?;
        registry
            .register(Box::new(authentication_successes.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register authentication_successes: {e}"
                ))
            })?;
        registry
            .register(Box::new(authentication_failures.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register authentication_failures: {e}"
                ))
            })?;

        Ok(Self {
            active_connections,
            total_connections,
            connection_errors,
            authentication_attempts,
            authentication_successes,
            authentication_failures,
        })
    }
}
