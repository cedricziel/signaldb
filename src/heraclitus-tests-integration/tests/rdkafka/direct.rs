// Direct HeraclitusAgent tests with rdkafka
// These tests run HeraclitusAgent directly in-process instead of as a subprocess

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_heraclitus_rdkafka_basic() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=debug,rdkafka=debug,info")
        .with_test_writer()
        .try_init();

    println!("Testing HeraclitusAgent with rdkafka directly");

    // Create HeraclitusConfig with memory storage and specific settings
    let mut config = HeraclitusConfig {
        kafka_port: 19092,
        http_port: 19093,
        ..Default::default()
    };
    config.storage.path = "memory://".to_string();
    config.auth.enabled = false;
    config.metrics.enabled = false;
    config.state.prefix = "heraclitus/".to_string();
    config.batching.max_batch_messages = 1000;
    config.batching.flush_interval_ms = 100;

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

    // Verify the server is listening
    match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", config.kafka_port)).await {
        Ok(_) => println!(
            "Successfully connected to Heraclitus on port {}",
            config.kafka_port
        ),
        Err(e) => {
            eprintln!(
                "Failed to connect to Heraclitus on port {}: {}",
                config.kafka_port, e
            );
            agent_handle.abort();
            return Err(anyhow::anyhow!("Heraclitus not listening on expected port"));
        }
    }

    let bootstrap_servers = format!("127.0.0.1:{}", config.kafka_port);

    // Create producer with debug logging
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("debug", "all")
        .set("socket.timeout.ms", "10000")
        .set("request.timeout.ms", "10000")
        .create()
        .expect("Failed to create producer");

    println!("Created producer");

    // Give rdkafka time to connect and send ApiVersionsRequest
    println!("Waiting for rdkafka to connect...");
    sleep(Duration::from_secs(2)).await;

    let topic = "test-basic";

    // Produce messages
    let messages = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];

    println!("Producing {} messages to topic '{}'", messages.len(), topic);
    for (i, (key, value)) in messages.iter().enumerate() {
        let record = BaseRecord::to(topic).key(*key).payload(*value);
        match producer.send(record) {
            Ok(_) => println!("Sent message {i}: {key} -> {value}"),
            Err((e, _)) => eprintln!("Failed to send message {i}: {e:?}"),
        }
    }

    // Flush producer
    producer.flush(Duration::from_secs(5))?;
    println!("Flushed producer successfully!");

    // Create consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create consumer");

    consumer.subscribe(&[topic])?;
    println!("Created consumer and subscribed to topic");

    // Consume messages
    let start = std::time::Instant::now();
    let mut consumed = 0;

    while consumed < messages.len() && start.elapsed() < Duration::from_secs(10) {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    let payload_str = std::str::from_utf8(payload)?;
                    println!("Consumed message: {payload_str}");
                    consumed += 1;
                }
            }
            Some(Err(e)) => {
                eprintln!("Consume error: {e:?}");
            }
            None => {
                // No message yet
            }
        }
    }

    println!("Consumed {} messages in {:?}", consumed, start.elapsed());
    assert_eq!(
        consumed,
        messages.len(),
        "Should consume all produced messages"
    );

    // Cleanup: abort the agent task
    agent_handle.abort();

    Ok(())
}
