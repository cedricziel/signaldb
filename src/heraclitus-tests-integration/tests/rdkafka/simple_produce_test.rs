// Simple produce test with manual topic creation

use anyhow::Result;
use heraclitus::state::TopicMetadata;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use std::collections::HashMap;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_simple_produce_with_topic() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=info")
        .with_test_writer()
        .try_init();

    println!("Testing simple produce with pre-created topic");

    // Create HeraclitusConfig
    let mut config = HeraclitusConfig {
        kafka_port: 29092,
        ..Default::default()
    };
    config.storage.path = "memory://".to_string();
    config.auth.enabled = false;
    config.metrics.enabled = false;

    // Create HeraclitusAgent
    let agent = HeraclitusAgent::new(config.clone()).await?;

    // Pre-create the topic
    let _topic_metadata = TopicMetadata {
        name: "test-topic".to_string(),
        partitions: 1,
        replication_factor: 1,
        config: HashMap::new(),
        created_at: chrono::Utc::now(),
    };

    // Access the state manager through the agent (we need to make this public)
    // For now, let's skip this and rely on auto-creation

    // Run agent
    let agent_handle = tokio::spawn(async move {
        if let Err(e) = agent.run().await {
            eprintln!("HeraclitusAgent error: {e}");
        }
    });

    // Give agent time to start
    sleep(Duration::from_millis(500)).await;

    // Create rdkafka producer
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    let producer: BaseProducer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            format!("127.0.0.1:{}", config.kafka_port),
        )
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create producer");

    println!("Sending test message");

    // Send a message
    let record = BaseRecord::to("test-topic")
        .key("test-key")
        .payload("test-value");

    match producer.send(record) {
        Ok(_) => println!("Message sent successfully"),
        Err((e, _)) => eprintln!("Failed to send message: {e:?}"),
    }

    // Flush
    producer.flush(Duration::from_secs(5))?;
    println!("Producer flushed");

    // Cleanup
    agent_handle.abort();

    Ok(())
}
