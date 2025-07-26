// rdkafka client compatibility tests
// These tests verify that real rdkafka clients can interact with Heraclitus

use super::{HeraclitusTestContext, StoredMessage};
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;
use futures::StreamExt;

#[tokio::test]
async fn test_rdkafka_basic_connectivity() -> Result<()> {
    println!("Testing rdkafka {} compatibility", get_rdkafka_version().1);
    
    let context = HeraclitusTestContext::new().await?;
    
    // Test 1: Can create a consumer and fetch metadata
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .set("group.id", "test-group")
        .set("client.id", "test-client")
        .create()?;
    
    // Fetch metadata should work
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;
    
    assert!(metadata.brokers().len() > 0, "Should have at least one broker");
    println!("✓ Successfully fetched metadata with {} brokers", metadata.brokers().len());
    
    Ok(())
}

#[tokio::test]
async fn test_rdkafka_producer_basic() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    
    // Create producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .set("message.timeout.ms", "5000")
        .create()?;
    
    println!("✓ Created rdkafka producer");
    
    // Send a test message
    let delivery_result = producer.send(
        FutureRecord::to("test-topic")
            .payload("Hello, Heraclitus!")
            .key("test-key"),
        Duration::from_secs(5),
    ).await;
    
    match delivery_result {
        Ok((partition, offset)) => {
            println!("✓ Message sent successfully: partition={}, offset={}", partition, offset);
        }
        Err((e, _)) => {
            return Err(anyhow::anyhow!("Failed to send message: {:?}", e));
        }
    }
    
    // Verify message was stored
    tokio::time::sleep(Duration::from_millis(500)).await;
    let messages = context.get_stored_messages("test-topic").await?;
    assert!(messages.len() >= 1, "Should have stored at least one message");
    
    Ok(())
}

#[tokio::test]
async fn test_rdkafka_consumer_basic() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    
    // First, produce a message
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .set("message.timeout.ms", "5000")
        .create()?;
    
    producer.send(
        FutureRecord::to("test-consume")
            .payload("Test message for consumption")
            .key("consume-key"),
        Duration::from_secs(5),
    ).await?;
    
    println!("✓ Produced test message");
    
    // Now consume it
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .set("group.id", "test-consumer-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    
    consumer.subscribe(&["test-consume"])?;
    println!("✓ Created consumer and subscribed to topic");
    
    // Try to consume the message
    let mut message_stream = consumer.stream();
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);
    
    tokio::select! {
        Some(message_result) = message_stream.next() => {
            match message_result {
                Ok(message) => {
                    let payload = message.payload()
                        .ok_or_else(|| anyhow::anyhow!("No payload"))?;
                    let payload_str = std::str::from_utf8(payload)?;
                    
                    assert_eq!(payload_str, "Test message for consumption");
                    println!("✓ Successfully consumed message: {}", payload_str);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Consume error: {:?}", e));
                }
            }
        }
        _ = &mut timeout => {
            return Err(anyhow::anyhow!("Timeout waiting for message"));
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_rdkafka_batch_produce() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .set("message.timeout.ms", "5000")
        .set("batch.size", "16384")
        .set("linger.ms", "100")
        .create()?;
    
    // Send multiple messages
    let mut futures = Vec::new();
    for i in 0..10 {
        let future = producer.send(
            FutureRecord::to("batch-topic")
                .payload(&format!("Batch message {}", i))
                .key(&format!("key-{}", i)),
            Duration::from_secs(5),
        );
        futures.push(future);
    }
    
    // Wait for all messages
    let results = futures::future::join_all(futures).await;
    
    let mut success_count = 0;
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok((partition, offset)) => {
                println!("✓ Message {} sent: partition={}, offset={}", i, partition, offset);
                success_count += 1;
            }
            Err((e, _)) => {
                println!("✗ Message {} failed: {:?}", i, e);
            }
        }
    }
    
    assert_eq!(success_count, 10, "All messages should be sent successfully");
    
    Ok(())
}

#[tokio::test]
async fn test_rdkafka_consumer_group() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    
    // Create a producer to send test messages
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .create()?;
    
    // Send messages to multiple partitions
    for i in 0..6 {
        producer.send(
            FutureRecord::to("group-topic")
                .partition(i % 3) // Distribute across 3 partitions
                .payload(&format!("Group message {}", i))
                .key(&format!("key-{}", i)),
            Duration::from_secs(5),
        ).await?;
    }
    
    println!("✓ Produced 6 messages across 3 partitions");
    
    // Create two consumers in the same group
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .set("group.id", "test-group")
        .set("client.id", "consumer-1")
        .set("auto.offset.reset", "earliest")
        .create()?;
    
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .set("group.id", "test-group")
        .set("client.id", "consumer-2")
        .set("auto.offset.reset", "earliest")
        .create()?;
    
    consumer1.subscribe(&["group-topic"])?;
    consumer2.subscribe(&["group-topic"])?;
    
    println!("✓ Created two consumers in the same group");
    
    // Both consumers should receive messages (partitions should be balanced)
    let mut messages1 = 0;
    let mut messages2 = 0;
    
    // Collect messages from both consumers
    let timeout = Duration::from_secs(10);
    let start = tokio::time::Instant::now();
    
    while start.elapsed() < timeout && (messages1 + messages2) < 6 {
        // Try consumer 1
        if let Ok(msg) = consumer1.recv().now_or_never().unwrap_or(Err(rdkafka::error::KafkaError::NoMessageReceived)) {
            messages1 += 1;
            println!("Consumer 1 received message from partition {}", msg.partition());
        }
        
        // Try consumer 2
        if let Ok(msg) = consumer2.recv().now_or_never().unwrap_or(Err(rdkafka::error::KafkaError::NoMessageReceived)) {
            messages2 += 1;
            println!("Consumer 2 received message from partition {}", msg.partition());
        }
        
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    println!("✓ Consumer 1 received {} messages", messages1);
    println!("✓ Consumer 2 received {} messages", messages2);
    
    assert!(messages1 > 0, "Consumer 1 should receive some messages");
    assert!(messages2 > 0, "Consumer 2 should receive some messages");
    assert_eq!(messages1 + messages2, 6, "All messages should be consumed");
    
    Ok(())
}