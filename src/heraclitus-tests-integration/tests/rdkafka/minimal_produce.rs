// Minimal produce test to isolate the issue

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_minimal_produce() -> Result<()> {
    // Minimal logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=info,rdkafka=error")
        .with_test_writer()
        .try_init();

    println!("Starting minimal produce test");

    // Create HeraclitusConfig
    let mut config = HeraclitusConfig {
        kafka_port: 29092,
        ..Default::default()
    };
    config.storage.path = "memory://".to_string();
    config.topics.auto_create_topics_enable = true;

    // Create and start HeraclitusAgent
    let agent = HeraclitusAgent::new(config.clone()).await?;
    let agent_handle = tokio::spawn(async move {
        agent.run().await.ok();
    });

    // Wait for startup
    sleep(Duration::from_millis(500)).await;

    // Create producer with minimal config
    let producer: BaseProducer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            format!("127.0.0.1:{}", config.kafka_port),
        )
        .set("message.timeout.ms", "5000")
        // Enable API version request like in the main test
        .set("api.version.request", "true")
        .create()?;

    println!("Producer created");

    // Send one message
    let record = BaseRecord::to("test-topic").key("key").payload("value");

    println!("Sending message...");
    producer.send(record).map_err(|(e, _)| e)?;

    println!("Flushing...");
    producer.flush(Duration::from_secs(10))?;

    println!("Success!");

    // Now try to consume
    use rdkafka::consumer::{BaseConsumer, Consumer};

    let consumer: BaseConsumer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            format!("127.0.0.1:{}", config.kafka_port),
        )
        .set("group.id", "test-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("api.version.request", "true")
        .create()?;

    consumer.subscribe(&["test-topic"])?;
    println!("Consumer created and subscribed");

    // Try to consume
    use rdkafka::Message;
    match consumer.poll(Duration::from_secs(5)) {
        Some(Ok(message)) => {
            if let Some(payload) = message.payload() {
                println!("Consumed: {}", std::str::from_utf8(payload)?);
            }
        }
        Some(Err(e)) => println!("Consume error: {e:?}"),
        None => println!("No message consumed"),
    }

    agent_handle.abort();
    Ok(())
}
