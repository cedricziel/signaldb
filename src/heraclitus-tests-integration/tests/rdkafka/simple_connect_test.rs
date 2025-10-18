// Simple connection test

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_simple_connections() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=info")
        .with_test_writer()
        .try_init();

    println!("Testing simple connections");

    // Create HeraclitusConfig
    let mut config = HeraclitusConfig {
        kafka_port: 59092,
        ..Default::default()
    };
    config.storage.path = "memory://".to_string();
    config.auth.enabled = false;
    config.metrics.enabled = false;

    // Create and start HeraclitusAgent
    let agent = HeraclitusAgent::new(config.clone()).await?;

    // Create a connection counter
    let connection_count = Arc::new(AtomicU32::new(0));
    let _counter = connection_count.clone();

    // Run the agent in a background task
    let agent_handle = tokio::spawn(async move {
        if let Err(e) = agent.run().await {
            eprintln!("HeraclitusAgent error: {e}");
        }
    });

    // Give HeraclitusAgent time to start
    sleep(Duration::from_millis(500)).await;

    // Make multiple connections
    println!("Making 5 test connections...");
    for i in 1..=5 {
        match TcpStream::connect(format!("127.0.0.1:{}", config.kafka_port)).await {
            Ok(socket) => {
                println!("Connection {i} established");
                // Keep the connection open for a moment
                sleep(Duration::from_millis(100)).await;
                drop(socket);
                println!("Connection {i} closed");
            }
            Err(e) => {
                eprintln!("Failed to connect #{i}: {e}");
            }
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Wait a bit to see if connections were logged
    sleep(Duration::from_millis(500)).await;

    // Cleanup
    agent_handle.abort();

    Ok(())
}
