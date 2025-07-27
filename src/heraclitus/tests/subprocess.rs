// Helper module for running HeraclitusAgent directly for integration tests

use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use std::net::TcpListener;
use tokio::task::JoinHandle;

/// Test context for Heraclitus server running as HeraclitusAgent
pub struct HeraclitusTestContext {
    pub kafka_port: u16,
    #[allow(dead_code)]
    pub http_port: u16,
    agent_handle: JoinHandle<()>,
}

impl HeraclitusTestContext {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Find available ports
        let kafka_port = find_available_port().await?;
        let http_port = find_available_port().await?;

        // Create test configuration with memory storage
        let config = HeraclitusConfig {
            kafka_port,
            http_port,
            storage: heraclitus::config::StorageConfig {
                path: "memory://".to_string(),
            },
            metrics: heraclitus::config::MetricsConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };

        println!("Starting HeraclitusAgent on kafka_port: {kafka_port}");

        // Spawn the agent in a background task
        let agent_handle = tokio::spawn(async move {
            // Initialize tracing for the agent
            let _ = tracing_subscriber::fmt()
                .with_env_filter("heraclitus=trace,info")
                .with_test_writer()
                .try_init();

            // Create the agent inside the spawned task
            match HeraclitusAgent::new(config).await {
                Ok(agent) => {
                    println!("HeraclitusAgent created successfully, starting...");

                    // Run the agent - it will handle its own lifecycle
                    if let Err(e) = agent.run().await {
                        eprintln!("HeraclitusAgent error: {e}");
                    }
                    println!("HeraclitusAgent task completed");
                }
                Err(e) => {
                    eprintln!("Failed to create HeraclitusAgent: {e}");
                }
            }
        });

        // Wait for the server to be ready by polling the port
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(10);

        loop {
            if start.elapsed() > timeout {
                return Err("Timeout waiting for HeraclitusAgent to start".into());
            }

            match tokio::net::TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await {
                Ok(stream) => {
                    println!("Successfully connected to HeraclitusAgent on port {kafka_port}");
                    // Explicitly drop the connection to avoid interfering with the test
                    drop(stream);
                    break;
                }
                Err(_) => {
                    // Server not ready yet, wait a bit
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }

        // Give it a bit more time to fully initialize
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        Ok(Self {
            kafka_port,
            http_port,
            agent_handle,
        })
    }

    pub fn kafka_addr(&self) -> String {
        format!("127.0.0.1:{}", self.kafka_port)
    }

    #[allow(dead_code)]
    pub fn http_addr(&self) -> String {
        format!("127.0.0.1:{}", self.http_port)
    }
}

impl Drop for HeraclitusTestContext {
    fn drop(&mut self) {
        // Abort the agent task when the test context is dropped
        self.agent_handle.abort();
    }
}

pub async fn find_available_port() -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}
