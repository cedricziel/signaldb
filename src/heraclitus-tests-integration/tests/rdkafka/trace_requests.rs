// Trace all requests from rdkafka

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_trace_requests() -> Result<()> {
    // Initialize test logging with TRACE level for heraclitus
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=trace,rdkafka=debug")
        .with_test_writer()
        .try_init();

    println!("=== Starting trace requests test ===");

    // Create HeraclitusConfig with auto-topic creation enabled
    let mut config = HeraclitusConfig {
        kafka_port: 49092,
        ..Default::default()
    };
    config.storage.path = "memory://".to_string();
    config.auth.enabled = false;
    config.metrics.enabled = false;
    config.topics.auto_create_topics_enable = true;

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

    let bootstrap_servers = format!("127.0.0.1:{}", config.kafka_port);

    // Create producer with minimal config
    println!("\n=== Creating producer ===");
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("debug", "broker,metadata,protocol")
        .set("api.version.request", "false") // Disable API version request to simplify
        .set("broker.version.fallback", "2.0.0") // Set a known version
        .create()
        .expect("Failed to create producer");

    println!("\n=== Waiting for initial metadata ===");
    sleep(Duration::from_secs(1)).await;

    let topic = "trace-test-topic";

    // Send a single message
    println!("\n=== Sending message to '{topic}' ===");
    let record = BaseRecord::to(topic).key("key1").payload("value1");

    match producer.send(record) {
        Ok(_) => println!("Message queued"),
        Err((e, _)) => eprintln!("Failed to queue: {e:?}"),
    }

    println!("\n=== Flushing producer ===");
    producer.flush(Duration::from_secs(5))?;

    println!("\n=== Waiting to observe behavior ===");
    sleep(Duration::from_secs(2)).await;

    // Cleanup
    agent_handle.abort();

    Ok(())
}
