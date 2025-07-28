// Debug connection test to understand rdkafka behavior

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

#[tokio::test]
async fn test_raw_connection() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=trace,info")
        .with_test_writer()
        .try_init();

    println!("Testing raw connection to HeraclitusAgent");

    // Create HeraclitusConfig with memory storage
    let mut config = HeraclitusConfig {
        kafka_port: 19093,
        ..Default::default()
    };
    config.http_port = 19094;
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

    // Try raw TCP connection
    println!(
        "Attempting raw TCP connection to port {}",
        config.kafka_port
    );
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", config.kafka_port)).await?;
    println!("Successfully connected!");

    // Send ApiVersions request (API key 18, version 0)
    // Frame format: [4 bytes size][request header][request body]

    // Build request header (v2)
    // request_api_key: 18 (ApiVersions)
    // request_api_version: 0
    // correlation_id: 1
    // client_id: "test-client"

    let mut request = Vec::new();

    // Request header
    request.extend_from_slice(&18i16.to_be_bytes()); // api_key
    request.extend_from_slice(&0i16.to_be_bytes()); // api_version
    request.extend_from_slice(&1i32.to_be_bytes()); // correlation_id

    // client_id (compact string: length+1 as varint, then string)
    let client_id = b"test-client";
    request.push((client_id.len() + 1) as u8); // varint encoding for short strings
    request.extend_from_slice(client_id);

    // tagged fields (empty)
    request.push(0);

    // Request body (empty for ApiVersions v0)
    // No body needed for ApiVersions v0

    // Frame with size prefix
    let mut frame = Vec::new();
    frame.extend_from_slice(&(request.len() as i32).to_be_bytes());
    frame.extend_from_slice(&request);

    println!("Sending ApiVersions request ({} bytes)", frame.len());
    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Try to read response
    println!("Waiting for response...");
    let mut response_size_buf = [0u8; 4];

    // Set read timeout
    let read_result = tokio::time::timeout(
        Duration::from_secs(5),
        stream.read_exact(&mut response_size_buf),
    )
    .await;

    match read_result {
        Ok(Ok(_)) => {
            let response_size = i32::from_be_bytes(response_size_buf);
            println!("Response size: {response_size} bytes");

            let mut response_buf = vec![0u8; response_size as usize];
            stream.read_exact(&mut response_buf).await?;

            println!(
                "Received response: {:?}",
                &response_buf[..20.min(response_buf.len())]
            );
        }
        Ok(Err(e)) => {
            println!("Failed to read response size: {e}");
        }
        Err(_) => {
            println!("Timeout waiting for response");
        }
    }

    // Cleanup
    agent_handle.abort();

    Ok(())
}
