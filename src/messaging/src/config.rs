use crate::{
    backend::{
        memory::InMemoryStreamingBackend, nats::NatsBackend, nats_jetstream::JetStreamBackend,
    },
    MessagingBackend,
};
use std::sync::Arc;

/// Configuration for the messaging system.
#[derive(Debug, Clone)]
pub enum BackendConfig {
    /// In-memory backend configuration.
    Memory {
        /// The capacity of the broadcast channel.
        capacity: usize,
    },
    /// NATS backend configuration.
    Nats {
        /// The URL of the NATS server.
        server_url: String,
    },
    /// NATS JetStream backend configuration.
    JetStream {
        /// The URL of the NATS server.
        server_url: String,
    },
}

impl BackendConfig {
    /// Create a new in-memory backend configuration.
    pub fn memory(capacity: usize) -> Self {
        Self::Memory { capacity }
    }

    /// Create a new NATS backend configuration.
    pub fn nats(server_url: impl Into<String>) -> Self {
        Self::Nats {
            server_url: server_url.into(),
        }
    }

    /// Create a new NATS JetStream backend configuration.
    pub fn jetstream(server_url: impl Into<String>) -> Self {
        Self::JetStream {
            server_url: server_url.into(),
        }
    }

    /// Create a backend from the configuration.
    pub async fn create_backend(&self) -> Result<Arc<dyn MessagingBackend>, String> {
        match self {
            BackendConfig::Memory { capacity } => {
                Ok(Arc::new(InMemoryStreamingBackend::new(*capacity)))
            }
            BackendConfig::Nats { server_url } => {
                let backend = NatsBackend::new(server_url).await?;
                Ok(Arc::new(backend))
            }
            BackendConfig::JetStream { server_url } => {
                let backend = JetStreamBackend::new(server_url).await?;
                Ok(Arc::new(backend))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_config() {
        let config = BackendConfig::memory(10);
        match config {
            BackendConfig::Memory { capacity } => {
                assert_eq!(capacity, 10);
            }
            _ => panic!("Expected Memory config"),
        }
    }

    #[test]
    fn test_nats_config() {
        let config = BackendConfig::nats("nats://localhost:4222");
        match config {
            BackendConfig::Nats { server_url } => {
                assert_eq!(server_url, "nats://localhost:4222");
            }
            _ => panic!("Expected Nats config"),
        }
    }

    #[test]
    fn test_jetstream_config() {
        let config = BackendConfig::jetstream("nats://localhost:4222");
        match config {
            BackendConfig::JetStream { server_url } => {
                assert_eq!(server_url, "nats://localhost:4222");
            }
            _ => panic!("Expected JetStream config"),
        }
    }

    #[tokio::test]
    async fn test_create_memory_backend() {
        let config = BackendConfig::memory(10);
        let backend = config.create_backend().await;
        // Just check that we can create a backend, we don't need to test its functionality here
        assert!(backend.is_ok());
    }
}
