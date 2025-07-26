// Client compatibility test framework for Heraclitus
// Tests real Kafka clients (rdkafka, Java, Python) against our implementation

use anyhow::Result;
use std::time::Duration;
use tokio::time::timeout;

#[cfg(test)]
pub mod rdkafka_tests;

/// Test context for running Heraclitus with real clients
pub struct HeraclitusTestContext {
    pub kafka_port: u16,
    pub http_port: u16,
    pub data_dir: tempfile::TempDir,
    process: tokio::process::Child,
}

impl HeraclitusTestContext {
    /// Start a test instance of Heraclitus
    pub async fn new() -> Result<Self> {
        // Create temp directory for data
        let data_dir = tempfile::tempdir()?;
        
        // Find available ports
        let kafka_port = find_available_port(9092)?;
        let http_port = find_available_port(9093)?;
        
        // Create config file
        let config_content = format!(
            r#"
[database]
dsn = "sqlite://memory"

[storage]
dsn = "file://{}"

[wal]
enabled = false

[kafka]
port = {}

[http]
port = {}

[auth]
enabled = false
"#,
            data_dir.path().display(),
            kafka_port,
            http_port
        );
        
        let config_path = data_dir.path().join("heraclitus.toml");
        std::fs::write(&config_path, config_content)?;
        
        // Build the heraclitus binary path
        let exe_path = std::env::current_exe()?;
        let target_dir = exe_path
            .parent()
            .and_then(|p| p.parent())
            .ok_or_else(|| anyhow::anyhow!("Failed to find target directory"))?;
        
        let heraclitus_path = target_dir.join("heraclitus");
        
        // Start Heraclitus
        let mut cmd = tokio::process::Command::new(&heraclitus_path);
        cmd.arg("--kafka-port")
            .arg(kafka_port.to_string())
            .arg("--http-port")
            .arg(http_port.to_string())
            .arg("--data-dir")
            .arg(data_dir.path())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
            
        let mut process = cmd.spawn()?;
        
        // Wait for startup - check if port is listening
        let mut retries = 0;
        let max_retries = 20;
        loop {
            match tokio::net::TcpStream::connect(("127.0.0.1", kafka_port)).await {
                Ok(_) => break,
                Err(_) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(anyhow::anyhow!("Heraclitus failed to start listening on port {}", kafka_port));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            
            // Also check if process is still running
            if let Some(status) = process.try_wait()? {
                return Err(anyhow::anyhow!("Heraclitus exited with status: {:?}", status));
            }
        }
        
        Ok(Self {
            kafka_port,
            http_port,
            data_dir,
            process,
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

impl Drop for HeraclitusTestContext {
    fn drop(&mut self) {
        // Kill the process when test context is dropped
        let _ = self.process.start_kill();
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