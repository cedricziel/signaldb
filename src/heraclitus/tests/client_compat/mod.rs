// Client compatibility test framework for Heraclitus
// Tests real Kafka clients (rdkafka, Java, Python) against our implementation

use anyhow::Result;
use std::time::Duration;
use tokio::time::timeout;

// Include the testcontainers module for containerized testing
include!("../testcontainers_heraclitus.rs");

#[cfg(test)]
pub mod rdkafka_tests;

/// Test context for running Heraclitus with real clients using testcontainers
pub struct HeraclitusTestContext {
    pub kafka_port: u16,
    pub http_port: u16,
    container: testcontainers::ContainerAsync<Heraclitus>,
}

impl HeraclitusTestContext {
    /// Start a test instance of Heraclitus using testcontainers
    pub async fn new() -> Result<Self> {
        // Start Heraclitus container (automatically builds image if needed)
        let container = Heraclitus::default()
            .build_and_start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start Heraclitus container: {}", e))?;

        // Get mapped ports
        let kafka_port = container
            .get_host_port_ipv4(Heraclitus::kafka_port())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get Kafka port: {}", e))?;

        let http_port = container
            .get_host_port_ipv4(Heraclitus::http_port())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get HTTP port: {}", e))?;

        // Wait a bit for the container to be fully ready
        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(Self {
            kafka_port,
            http_port,
            container,
        })
    }
    
    /// Get the Kafka address for clients to connect to
    pub fn kafka_addr(&self) -> String {
        format!("127.0.0.1:{}", self.kafka_port)
    }
    
    /// Get stored messages for verification (via HTTP API)
    pub async fn get_stored_messages(&self, topic: &str) -> Result<Vec<StoredMessage>> {
        // TODO: Implement HTTP API call to retrieve messages
        // For now, return empty vec
        Ok(vec![])
    }
}

/// Helper to find an available port
fn find_available_port(start: u16) -> Result<u16> {
    for port in start..start + 100 {
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port);
        }
    }
    Err(anyhow::anyhow!("No available ports found"))
}

/// Represents a stored message for verification
#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: i64,
}

/// Compare two protocol responses structurally (not byte-for-byte)
pub fn assert_protocol_equivalent(actual: &[u8], expected: &[u8]) {
    // TODO: Implement structural comparison
    // For now, just compare lengths
    assert_eq!(
        actual.len(),
        expected.len(),
        "Response lengths differ: {} vs {}",
        actual.len(),
        expected.len()
    );
}

/// Load a captured Kafka response from test fixtures
pub fn load_kafka_response(filename: &str) -> Result<Vec<u8>> {
    let path = format!("tests/client_compat/fixtures/{}", filename);
    Ok(std::fs::read(path)?)
}