// Debug metadata test

use anyhow::Result;
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_debug_metadata() -> Result<()> {
    // Initialize test logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=debug,rdkafka=debug,info")
        .with_test_writer()
        .try_init();

    println!("Testing metadata interactions");

    // Create HeraclitusConfig with auto-topic creation enabled
    let mut config = HeraclitusConfig {
        kafka_port: 39092,
        ..Default::default()
    };
    config.http_port = 39093;
    config.storage.path = "memory://".to_string();
    config.auth.enabled = false;
    config.metrics.enabled = false;
    config.topics.auto_create_topics_enable = true; // Enable auto-topic creation

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

    // Create producer with specific topic metadata
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("debug", "broker,metadata,topic,protocol")
        .set("socket.timeout.ms", "10000")
        .set("request.timeout.ms", "10000")
        .set("api.version.request", "true")
        .set("topic.metadata.refresh.interval.ms", "1000") // Refresh metadata every second
        .create()
        .expect("Failed to create producer");

    println!("Created producer, waiting for metadata refresh...");

    // Wait for metadata to be fetched
    sleep(Duration::from_secs(2)).await;

    let topic = "test-metadata-topic";

    // Force topic metadata refresh by getting metadata
    println!("Requesting metadata for topic '{topic}'");
    match producer
        .client()
        .fetch_metadata(Some(topic), Duration::from_secs(5))
    {
        Ok(metadata) => {
            println!(
                "Got metadata: {} topics, {} brokers",
                metadata.topics().len(),
                metadata.brokers().len()
            );
            for t in metadata.topics() {
                println!(
                    "  Topic: {} with {} partitions",
                    t.name(),
                    t.partitions().len()
                );
            }
        }
        Err(e) => eprintln!("Failed to fetch metadata: {e:?}"),
    }

    // Try to produce a single message
    println!("Attempting to produce to topic '{topic}'");
    let record = BaseRecord::to(topic).key("test-key").payload("test-value");

    match producer.send(record) {
        Ok(_) => println!("Message queued successfully"),
        Err((e, _)) => eprintln!("Failed to queue message: {e:?}"),
    }

    // Flush and wait
    println!("Flushing producer...");
    match producer.flush(Duration::from_secs(10)) {
        Ok(_) => println!("Producer flushed successfully"),
        Err(e) => eprintln!("Flush error: {e:?}"),
    }

    // Cleanup
    agent_handle.abort();

    Ok(())
}
