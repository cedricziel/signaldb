// Helper module for running HeraclitusAgent directly for integration tests

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

/// Test context for Heraclitus server running as HeraclitusAgent in dedicated thread
pub struct HeraclitusTestContext {
    pub kafka_port: u16,
    #[allow(dead_code)]
    pub http_port: u16,
    server_thread: Option<JoinHandle<()>>,
    shutdown_flag: Arc<AtomicBool>,
}

impl HeraclitusTestContext {
    pub async fn new() -> Result<Self> {
        // Find available ports - use synchronous version to avoid async/await in thread
        let kafka_port = find_available_port_sync()?;
        let http_port = find_available_port_sync()?;

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

        // Create shutdown flag for graceful termination
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        // Spawn the agent in a dedicated OS thread with its own Tokio runtime
        // This ensures complete isolation from the test runtime, which is critical
        // for librdkafka (C library with blocking I/O) to work properly
        let server_thread = std::thread::spawn(move || {
            // Initialize tracing for the agent
            let _ = tracing_subscriber::fmt()
                .with_env_filter("heraclitus=trace,info")
                .with_test_writer()
                .try_init();

            // Create a dedicated Tokio runtime for this thread
            let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

            rt.block_on(async move {
                // Create the agent
                match HeraclitusAgent::new(config).await {
                    Ok(agent) => {
                        println!("HeraclitusAgent created successfully, starting...");

                        // Run the agent - it will handle its own lifecycle
                        // We could use shutdown_flag_clone here to gracefully shutdown if needed
                        let _ = shutdown_flag_clone; // Acknowledge we have it for future use
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
        });

        // Wait for the server to be ready by polling the port
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(10);

        loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for HeraclitusAgent to start"
                ));
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
            server_thread: Some(server_thread),
            shutdown_flag,
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
        // Signal shutdown
        self.shutdown_flag.store(true, Ordering::SeqCst);

        // The server thread will terminate when the agent finishes
        // We don't wait for it to prevent blocking test cleanup
        // The thread will be cleaned up when the process exits
        if let Some(thread) = self.server_thread.take() {
            // Note: We could add graceful shutdown with agent.shutdown() via a channel
            // For now, the thread will be terminated when the process exits
            drop(thread);
        }
    }
}

/// Synchronous version of find_available_port for use in thread context
pub fn find_available_port_sync() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Async version for backward compatibility (if needed elsewhere)
#[allow(dead_code)]
pub async fn find_available_port() -> Result<u16> {
    find_available_port_sync()
}
