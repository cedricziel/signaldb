// Test ApiVersions v3 compatibility

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_api_versions_v3() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=trace")
        .with_test_writer()
        .try_init();

    println!("Testing ApiVersions v3 request");

    // Create HeraclitusConfig
    let mut config = HeraclitusConfig {
        kafka_port: 49092,
        ..Default::default()
    };
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

    // Connect
    let mut socket = TcpStream::connect(format!("127.0.0.1:{}", config.kafka_port)).await?;
    println!("Connected successfully!");

    // Send the exact request that rdkafka sent (captured from tcp_capture test)
    let request_data = vec![
        0, 0, 0, 37, // Frame size: 37
        0, 18, // API key: 18 (ApiVersions)
        0, 3, // API version: 3
        0, 0, 0, 1, // Correlation ID: 1
        0, 7, // Client ID length: 7
        114, 100, 107, 97, 102, 107, 97, // Client ID: "rdkafka"
        0,  // Tagged fields (empty)
        11, // Compact string length: 11
        108, 105, 98, 114, 100, 107, 97, 102, 107, 97, // "librdkafka"
        7,  // Compact string length: 7
        50, 46, 49, 48, 46, 48, // "2.10.0"
        0,  // Tagged fields (empty)
    ];

    println!(
        "Sending ApiVersions v3 request ({} bytes)",
        request_data.len()
    );
    socket.write_all(&request_data).await?;
    socket.flush().await?;

    // Try to read response
    let mut response_buf = vec![0u8; 1024];
    match tokio::time::timeout(Duration::from_secs(2), socket.read(&mut response_buf)).await {
        Ok(Ok(n)) => {
            println!("Received {n} bytes in response");
            if n >= 4 {
                let frame_size = i32::from_be_bytes([
                    response_buf[0],
                    response_buf[1],
                    response_buf[2],
                    response_buf[3],
                ]);
                println!("Response frame size: {frame_size}");
            }
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
