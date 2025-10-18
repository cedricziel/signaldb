// Test with pre-created topic

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use heraclitus::state::{StateManager, TopicMetadata};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::Message;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_precreate_topic() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=info,rdkafka=warn")
        .with_test_writer()
        .try_init();

    println!("Testing with pre-created topic");

    // Create HeraclitusConfig
    let mut config = HeraclitusConfig {
        kafka_port: 59092,
    ..Default::default()
};
    config.storage.path = "memory://".to_string();
    config.auth.enabled = false;
    config.metrics.enabled = false;

    let topic_name = "precreated-topic";
    
    // Create and start HeraclitusAgent
    let agent = HeraclitusAgent::new(config.clone()).await?;
    
    // Pre-create topic through the agent's state manager
    // For now, we'll rely on auto-topic creation instead
    // TODO: Add API to pre-create topics
    
    // Run the agent in a background task
    let agent_handle = tokio::spawn(async move {
        if let Err(e) = agent.run().await {
            eprintln!("HeraclitusAgent error: {}", e);
        }
    });

    // Give HeraclitusAgent time to start
    sleep(Duration::from_millis(500)).await;

    let bootstrap_servers = format!("127.0.0.1:{}", config.kafka_port);

    // Create producer
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create producer");

    println!("Created producer");

    // Produce messages
    let messages = vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
    ];

    println!("Producing {} messages to topic '{}'", messages.len(), topic_name);
    for (i, (key, value)) in messages.iter().enumerate() {
        let record = BaseRecord::to(topic_name)
            .key(*key)
            .payload(*value);
        match producer.send(record) {
            Ok(_) => println!("Sent message {}: {} -> {}", i, key, value),
            Err((e, _)) => eprintln!("Failed to send message {}: {:?}", i, e),
        }
    }

    // Flush producer
    producer.flush(Duration::from_secs(5))?;
    println!("Flushed producer");

    // Create consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create consumer");

    consumer.subscribe(&[topic_name])?;
    println!("Created consumer and subscribed to topic");

    // Consume messages
    let start = std::time::Instant::now();
    let mut consumed = 0;

    while consumed < messages.len() && start.elapsed() < Duration::from_secs(10) {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    let payload_str = std::str::from_utf8(payload)?;
                    println!("Consumed message: {}", payload_str);
                    consumed += 1;
                }
            }
            Some(Err(e)) => {
                eprintln!("Consume error: {:?}", e);
            }
            None => {
                // No message yet
            }
        }
    }

    println!("Consumed {} messages in {:?}", consumed, start.elapsed());
    assert_eq!(consumed, messages.len(), "Should consume all produced messages");

    // Cleanup
    agent_handle.abort();
    
    Ok(())
}