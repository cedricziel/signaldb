// Test basic connection to HeraclitusAgent

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_basic_connection() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=trace")
        .with_test_writer()
        .try_init();

    println!("Testing basic connection to HeraclitusAgent");

    // Create HeraclitusConfig with memory storage and specific settings
    let mut config = HeraclitusConfig {
        kafka_port: 29092,
        ..Default::default()
    };
    config.http_port = 29093;
    config.storage.path = "memory://".to_string();
    config.auth.enabled = false;
    config.metrics.enabled = false;

    // Create and start HeraclitusAgent
    let agent = HeraclitusAgent::new(config.clone()).await?;

    // Run the agent in a background task
    let agent_handle = tokio::spawn(async move {
        if let Err(e) = agent.run().await {
            eprintln!("HeraclitusAgent error: {e}");
        }
    });

    // Give HeraclitusAgent time to start
    sleep(Duration::from_millis(500)).await;

    // Try to connect
    println!("Attempting to connect to port {}", config.kafka_port);
    let mut socket = TcpStream::connect(format!("127.0.0.1:{}", config.kafka_port)).await?;
    println!("Connected successfully!");

    // Send a simple ApiVersionsRequest (API key 18, version 0)
    // Frame format: [4 bytes length][payload]
    // Payload: [2 bytes api_key][2 bytes api_version][4 bytes correlation_id][2 bytes client_id length][client_id]

    let mut request = Vec::new();
    request.extend_from_slice(&18i16.to_be_bytes()); // API key 18 (ApiVersions)
    request.extend_from_slice(&0i16.to_be_bytes()); // API version 0
    request.extend_from_slice(&1i32.to_be_bytes()); // Correlation ID 1
    request.extend_from_slice(&0i16.to_be_bytes()); // Empty client ID

    let frame_size = request.len() as i32;
    let mut frame = Vec::new();
    frame.extend_from_slice(&frame_size.to_be_bytes());
    frame.extend_from_slice(&request);

    println!("Sending ApiVersionsRequest frame of {} bytes", frame.len());
    socket.write_all(&frame).await?;
    socket.flush().await?;

    // Try to read response
    let mut response_buf = vec![0u8; 1024];
    match tokio::time::timeout(Duration::from_secs(2), socket.read(&mut response_buf)).await {
        Ok(Ok(n)) => {
            println!("Received {n} bytes in response");
            println!("Response data: {:?}", &response_buf[..n]);
        }
        Ok(Err(e)) => {
            eprintln!("Error reading response: {e}");
        }
        Err(_) => {
            eprintln!("Timeout waiting for response");
        }
    }

    // Cleanup
    agent_handle.abort();

    Ok(())
}
