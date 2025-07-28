// Minimal rdkafka test to debug connection issues

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_minimal_rdkafka() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=debug,rdkafka=debug,info")
        .with_test_writer()
        .try_init();

    println!("Starting minimal rdkafka test");

    // Create HeraclitusConfig with memory storage
    let mut config = HeraclitusConfig {
        kafka_port: 19095,
        ..Default::default()
    };
    config.http_port = 19096;
    config.storage.path = "memory://".to_string();

    // Create and start HeraclitusAgent
    let agent = HeraclitusAgent::new(config.clone()).await?;

    // Run the agent in a background task
    let agent_handle = tokio::spawn(async move {
        if let Err(e) = agent.run().await {
            eprintln!("HeraclitusAgent error: {e}");
        }
    });

    // Give HeraclitusAgent time to start
    sleep(Duration::from_millis(1000)).await;

    // Verify connection
    match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", config.kafka_port)).await {
        Ok(_) => println!("✓ TCP connection successful"),
        Err(e) => {
            eprintln!("✗ TCP connection failed: {e}");
            agent_handle.abort();
            return Err(anyhow::anyhow!("Failed to connect"));
        }
    }

    let bootstrap_servers = format!("127.0.0.1:{}", config.kafka_port);
    println!("Bootstrap servers: {bootstrap_servers}");

    // Try to create producer with minimal config
    println!("Creating producer...");
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("debug", "all")
        .create()
        .expect("Failed to create producer");

    println!("✓ Producer created");

    // Try to send a simple message
    println!("Sending test message...");
    let record = BaseRecord::to("test-topic")
        .key("test-key")
        .payload("test-value");

    match producer.send(record) {
        Ok(_) => println!("✓ Message queued"),
        Err((e, _)) => println!("✗ Failed to queue message: {e:?}"),
    }

    // Wait a bit to see debug output
    sleep(Duration::from_secs(3)).await;

    // Cleanup
    agent_handle.abort();

    Ok(())
}
