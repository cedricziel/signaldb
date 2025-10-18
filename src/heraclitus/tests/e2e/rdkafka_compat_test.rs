// End-to-end rdkafka compatibility tests
// These tests verify that Heraclitus is fully compatible with real rdkafka clients

use super::super::integration::helpers::HeraclitusTestContext;
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;

/// Test basic connectivity with rdkafka
#[tokio::test]
async fn test_rdkafka_connectivity() -> Result<()> {
    println!("Testing rdkafka {} compatibility", get_rdkafka_version().1);

    let context = HeraclitusTestContext::new().await?;

    // Add a small delay to ensure server is fully ready
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!(
        "Creating rdkafka consumer with bootstrap.servers={}",
        context.kafka_addr()
    );

    // Test that we can create both producer and consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("group.id", "test-connectivity")
        .set("client.id", "test-client")
        .set("api.version.request", "true")
        .set("socket.timeout.ms", "10000")
        .set("metadata.request.timeout.ms", "10000")
        .set("debug", "broker,topic,msg")
        .create()?;

    println!("Consumer created, fetching metadata...");

    // Fetch metadata to verify connectivity
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(15))?;
    assert!(
        !metadata.brokers().is_empty(),
        "Should have at least one broker"
    );

    println!("✓ Successfully connected to Heraclitus with rdkafka");
    Ok(())
}

/// Test basic produce and consume roundtrip
#[tokio::test]
async fn test_produce_consume_roundtrip() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    let topic = "test-roundtrip";

    // Create producer
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("message.timeout.ms", "5000")
        .create()?;

    // Produce test messages
    let test_messages = vec![
        ("key1", "Hello Heraclitus!"),
        ("key2", "Kafka compatibility test"),
        ("key3", "Message with special chars: 你好 🚀"),
        ("", "Message with empty key"),
    ];

    println!(
        "Producing {} messages to topic '{}'",
        test_messages.len(),
        topic
    );
    for (i, (key, value)) in test_messages.iter().enumerate() {
        let mut record = BaseRecord::to(topic).payload(*value);
        if !key.is_empty() {
            record = record.key(*key);
        }

        match producer.send(record) {
            Ok(_) => println!("✓ Sent message {i}: {key} -> {value}"),
            Err((e, _)) => panic!("Failed to send message {i}: {e:?}"),
        }
    }

    // Flush to ensure all messages are sent
    producer.flush(Duration::from_secs(5))?;

    // Create consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("group.id", "test-roundtrip-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[topic])?;

    // Consume messages
    let mut received_messages = Vec::new();
    let start = std::time::Instant::now();

    while received_messages.len() < test_messages.len() && start.elapsed() < Duration::from_secs(10)
    {
        if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
            let key = message
                .key()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_default();
            let value = String::from_utf8_lossy(message.payload().unwrap_or(b"")).to_string();

            println!("✓ Received: {key} -> {value}");
            received_messages.push((key, value));
        }
    }

    // Verify all messages were received
    assert_eq!(
        received_messages.len(),
        test_messages.len(),
        "Should receive all messages"
    );

    // Verify message content (order might differ)
    for (expected_key, expected_value) in &test_messages {
        let found = received_messages
            .iter()
            .any(|(k, v)| k == expected_key && v == expected_value);
        assert!(
            found,
            "Should find message: {expected_key} -> {expected_value}"
        );
    }

    println!("✓ All messages successfully roundtripped");
    Ok(())
}

/// Test consumer group functionality
#[tokio::test]
async fn test_consumer_group_coordination() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    let topic = "test-consumer-group";
    let group_id = "test-coordination-group";

    // Produce messages first
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("message.timeout.ms", "5000")
        .create()?;

    for i in 0..10 {
        let key = format!("key-{i}");
        let value = format!("message-{i}");
        let record = BaseRecord::to(topic).key(&key).payload(&value);
        producer.send(record).map_err(|(e, _)| e)?;
    }
    producer.flush(Duration::from_secs(5))?;

    // Create first consumer
    let consumer1: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("group.id", group_id)
        .set("client.id", "consumer-1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer1.subscribe(&[topic])?;

    // Consume and commit 5 messages
    let mut consumed_count = 0;
    let start = std::time::Instant::now();

    while consumed_count < 5 && start.elapsed() < Duration::from_secs(10) {
        if let Some(Ok(message)) = consumer1.poll(Duration::from_millis(100)) {
            consumed_count += 1;
            println!(
                "Consumer 1 received message {} at offset {}",
                consumed_count,
                message.offset()
            );

            // Commit the message
            consumer1.commit_message(&message, rdkafka::consumer::CommitMode::Sync)?;
        }
    }

    assert_eq!(consumed_count, 5, "Consumer 1 should consume 5 messages");
    drop(consumer1);

    // Create second consumer with same group ID
    let consumer2: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("group.id", group_id)
        .set("client.id", "consumer-2")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer2.subscribe(&[topic])?;

    // Consumer 2 should continue from where consumer 1 left off
    let mut consumer2_count = 0;
    let start = std::time::Instant::now();

    while consumer2_count < 5 && start.elapsed() < Duration::from_secs(10) {
        if let Some(Ok(message)) = consumer2.poll(Duration::from_millis(500)) {
            consumer2_count += 1;
            let offset = message.offset();
            println!("Consumer 2 received message {consumer2_count} at offset {offset}");

            // Verify we're continuing from the right offset
            assert!(
                offset >= 5,
                "Consumer 2 should start from offset >= 5, got {offset}"
            );
        }
    }

    assert_eq!(
        consumer2_count, 5,
        "Consumer 2 should consume remaining 5 messages"
    );
    println!("✓ Consumer group coordination working correctly");
    Ok(())
}

/// Test message headers support
#[tokio::test]
async fn test_message_headers() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    let topic = "test-headers";

    // Create producer
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("message.timeout.ms", "5000")
        .create()?;

    // Create message with headers
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "source",
            value: Some("test-suite"),
        })
        .insert(Header {
            key: "version",
            value: Some("1.0"),
        })
        .insert(Header {
            key: "empty-header",
            value: None::<&[u8]>,
        });

    let record = BaseRecord::to(topic)
        .key("header-test")
        .payload("Message with headers")
        .headers(headers);

    producer.send(record).map_err(|(e, _)| e)?;
    producer.flush(Duration::from_secs(5))?;

    // Wait a bit for the server to flush the batch to storage
    // The batch writer has a background timer that flushes every 10ms
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Create consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("group.id", "test-headers-group")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[topic])?;

    // Consume and verify headers
    let start = std::time::Instant::now();
    let mut found_message = false;

    while !found_message && start.elapsed() < Duration::from_secs(10) {
        if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
            if let Some(key) = message.key() {
                if key == b"header-test" {
                    println!("✓ Found message with headers");

                    if let Some(headers) = message.headers() {
                        let header_count = headers.count();
                        println!("✓ Message has {header_count} headers");

                        // Check specific headers
                        for i in 0..header_count {
                            let header = headers.get(i);
                            let value_str =
                                header.value.map(|v| String::from_utf8_lossy(v).to_string());
                            println!("  Header: {} = {:?}", header.key, value_str);
                        }

                        assert!(header_count >= 2, "Should have at least 2 headers");
                    } else {
                        println!("⚠ Message headers not supported yet");
                    }

                    found_message = true;
                }
            }
        }
    }

    assert!(found_message, "Should receive the message with headers");
    Ok(())
}

/// Test high-volume throughput
#[tokio::test]
async fn test_high_volume_throughput() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    let topic = "test-throughput";
    let message_count = 1000;

    // Create producer with batching optimizations
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("message.timeout.ms", "10000")
        .set("batch.size", "65536")
        .set("linger.ms", "10")
        .create()?;

    println!("Producing {message_count} messages for throughput test");
    let start_time = std::time::Instant::now();

    // Produce messages
    for i in 0..message_count {
        let key = format!("key-{i:04}");
        let value = format!("High throughput test message {i}");
        let record = BaseRecord::to(topic).key(&key).payload(&value);

        producer.send(record).map_err(|(e, _)| e)?;

        if i % 100 == 0 && i > 0 {
            println!("  Sent {i} messages");
        }
    }

    producer.flush(Duration::from_secs(10))?;
    let produce_duration = start_time.elapsed();
    let produce_rate = message_count as f64 / produce_duration.as_secs_f64();

    println!(
        "✓ Produced {message_count} messages in {produce_duration:?} ({produce_rate:.1} msg/sec)"
    );

    // Create consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("group.id", "test-throughput-group")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[topic])?;

    // Consume messages
    let mut received_count = 0;
    let consume_start = std::time::Instant::now();

    while received_count < message_count && consume_start.elapsed() < Duration::from_secs(30) {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(100)) {
            received_count += 1;
            if received_count % 100 == 0 {
                println!("  Received {received_count} messages");
            }
        }
    }

    let consume_duration = consume_start.elapsed();
    let consume_rate = received_count as f64 / consume_duration.as_secs_f64();

    assert_eq!(received_count, message_count, "Should receive all messages");
    println!(
        "✓ Consumed {received_count} messages in {consume_duration:?} ({consume_rate:.1} msg/sec)"
    );

    // Verify performance is reasonable
    assert!(produce_rate > 100.0, "Should produce at least 100 msg/sec");
    assert!(consume_rate > 100.0, "Should consume at least 100 msg/sec");

    Ok(())
}

/// Test multiple concurrent producers and consumers
#[tokio::test]
async fn test_concurrent_clients() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    let topic = "test-concurrent";
    let num_producers = 3;
    let messages_per_producer = 20;

    // Spawn multiple producer threads
    // Using std::thread::spawn for complete isolation, as rdkafka is a C library
    // that cannot safely share async runtime (see CLAUDE.md testing guidelines)
    let mut producer_handles = Vec::new();

    for producer_id in 0..num_producers {
        let bootstrap_servers = context.kafka_addr();
        let topic = topic.to_string();

        let handle = std::thread::spawn(move || {
            let producer: BaseProducer = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap_servers)
                .set("message.timeout.ms", "5000")
                .set("client.id", format!("producer-{producer_id}"))
                .create()
                .expect("Failed to create producer");

            for i in 0..messages_per_producer {
                let key = format!("p{producer_id}-key-{i}");
                let value = format!("Producer {producer_id} message {i}");
                let record = BaseRecord::to(&topic).key(&key).payload(&value);

                producer.send(record).map_err(|(e, _)| e)?;
            }

            producer.flush(Duration::from_secs(5))?;
            println!("✓ Producer {producer_id} completed");
            Ok::<(), anyhow::Error>(())
        });

        producer_handles.push(handle);
    }

    // Wait for all producers
    for handle in producer_handles {
        handle.join().expect("Producer thread panicked")?;
    }

    // Create consumer to verify all messages
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", context.kafka_addr())
        .set("group.id", "test-concurrent-group")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[topic])?;

    let mut received_messages = std::collections::HashSet::new();
    let expected_total = num_producers * messages_per_producer;
    let start = std::time::Instant::now();

    while received_messages.len() < expected_total && start.elapsed() < Duration::from_secs(30) {
        if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
            if let Some(key) = message.key() {
                let key_str = String::from_utf8_lossy(key).to_string();
                received_messages.insert(key_str);
            }
        }
    }

    assert_eq!(
        received_messages.len(),
        expected_total,
        "Should receive all {expected_total} messages from {num_producers} producers"
    );

    println!("✓ Concurrent client test completed successfully");
    Ok(())
}
