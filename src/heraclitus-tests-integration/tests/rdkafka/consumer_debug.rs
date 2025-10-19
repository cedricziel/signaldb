// Debug consumer connection issues

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_consumer_connection_debug() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=trace,rdkafka=debug")
        .with_test_writer()
        .try_init();

    println!("=== Starting consumer connection debug test ===");

    // Create HeraclitusConfig
    let mut config = HeraclitusConfig {
        kafka_port: 39092,
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

    // First, produce a message so there's something to consume
    println!("\n=== Creating producer ===");
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create producer");

    println!("Producing test message");
    let record = BaseRecord::to("test-topic")
        .key("test-key")
        .payload("test-value");

    match producer.send(record) {
        Ok(_) => println!("Message queued"),
        Err((e, _)) => eprintln!("Failed to queue: {e:?}"),
    }

    producer.flush(Duration::from_secs(5))?;
    println!("Producer flushed");

    // Now create consumer
    println!("\n=== Creating consumer ===");
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("debug", "broker,metadata,protocol,cgrp")
        .create()
        .expect("Failed to create consumer");

    println!("Subscribing to test-topic");
    consumer.subscribe(&["test-topic"])?;

    println!("\n=== Waiting for metadata ===");
    sleep(Duration::from_secs(2)).await;

    println!("\n=== Polling for messages ===");
    for i in 0..5 {
        println!("Poll attempt {}", i + 1);
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    let payload_str = std::str::from_utf8(payload)?;
                    println!("SUCCESS: Consumed message: {payload_str}");
                    break;
                }
            }
            Some(Err(e)) => {
                eprintln!("Consume error: {e:?}");
            }
            None => {
                println!("No message yet");
            }
        }
    }

    // Cleanup
    agent_handle.abort();

    Ok(())
}
