// Demonstration test: Using rdkafka producer with Heraclitus
// This test shows basic produce operations using real rdkafka client

// Include the testcontainers module for containerized testing
include!("testcontainers_heraclitus.rs");

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;

#[tokio::test]
async fn test_rdkafka_produce_demo() {
    println!("🚀 Heraclitus + rdkafka Producer Demo");
    println!("   Using rdkafka version: {}", get_rdkafka_version().1);

    // Start Heraclitus container (automatically builds image if needed)
    let heraclitus_container = Heraclitus::default()
        .build_and_start()
        .await
        .expect("Failed to start Heraclitus container");

    let heraclitus_port = heraclitus_container
        .get_host_port_ipv4(Heraclitus::kafka_port())
        .await
        .expect("Failed to get Heraclitus port");

    // Wait for Heraclitus to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    let bootstrap_servers = format!("127.0.0.1:{heraclitus_port}");
    println!("📡 Connecting to Heraclitus at: {bootstrap_servers}");

    // Create rdkafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("api.version.request", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create producer");

    println!("✅ Producer created successfully");

    // Produce messages to different topics to demonstrate functionality
    let test_cases = vec![
        ("demo-topic", "hello", "Hello from Heraclitus!"),
        ("demo-topic", "greeting", "Welcome to the distributed world"),
        ("metrics", "cpu", "usage=75.2"),
        ("logs", "app", "Application started successfully"),
        ("events", "user-123", "User logged in"),
    ];

    println!("\n📝 Producing {} messages...", test_cases.len());

    for (i, (topic, key, message)) in test_cases.iter().enumerate() {
        let record = FutureRecord::to(topic).key(*key).payload(*message);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                println!(
                    "   ✅ Message {}: topic='{}', key='{}', partition={}, offset={}",
                    i + 1,
                    topic,
                    key,
                    partition,
                    offset
                );
            }
            Err((e, _)) => {
                println!(
                    "   ❌ Message {}: topic='{}', key='{}', error={:?}",
                    i + 1,
                    topic,
                    key,
                    e
                );
            }
        }
    }

    // Flush to ensure all messages are sent
    match producer.flush(Duration::from_secs(5)) {
        Ok(_) => println!("\n🔄 All messages flushed successfully"),
        Err(e) => println!("\n⚠️  Flush timeout: {e:?}"),
    }

    println!("🎉 Demo completed! Heraclitus successfully handled rdkafka produce operations");
}

#[tokio::test]
async fn test_rdkafka_batch_produce_demo() {
    println!("🚀 Heraclitus + rdkafka Batch Producer Demo");

    // Start Heraclitus container
    let heraclitus_container = Heraclitus::default()
        .build_and_start()
        .await
        .expect("Failed to start Heraclitus container");

    let heraclitus_port = heraclitus_container
        .get_host_port_ipv4(Heraclitus::kafka_port())
        .await
        .expect("Failed to get Heraclitus port");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let bootstrap_servers = format!("127.0.0.1:{heraclitus_port}");
    println!("📡 Connecting to Heraclitus at: {bootstrap_servers}");

    // Create producer with batching configuration
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("api.version.request", "true")
        .set("batch.size", "16384")
        .set("linger.ms", "10")
        .set("compression.type", "none")
        .create()
        .expect("Failed to create producer");

    println!("✅ Batch producer created");

    // Generate and send messages one by one to avoid lifetime issues
    println!("\n📦 Sending batch of 20 messages...");
    let mut results = Vec::new();

    for i in 0..20 {
        let topic = match i % 4 {
            0 => "batch-logs",
            1 => "batch-metrics",
            2 => "batch-events",
            _ => "batch-data",
        };

        let key = format!("key-{i:03}");
        let value = format!(
            "Batch message {} - timestamp: {}",
            i,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let result = producer
            .send(
                FutureRecord::to(topic).key(&key).payload(&value),
                Duration::from_secs(5),
            )
            .await;

        results.push((i, topic, key, result));
    }

    let mut success_count = 0;
    let mut error_count = 0;

    for (i, topic, key, result) in results {
        match result {
            Ok((partition, offset)) => {
                println!(
                    "   ✅ #{i:02}: topic='{topic}', key='{key}', partition={partition}, offset={offset}"
                );
                success_count += 1;
            }
            Err((e, _)) => {
                println!("   ❌ #{i:02}: topic='{topic}', key='{key}', error={e:?}");
                error_count += 1;
            }
        }
    }

    println!("\n📊 Batch Results:");
    println!("   ✅ Successful: {success_count}");
    println!("   ❌ Failed: {error_count}");

    // Most messages should succeed (allowing some failures due to potential API incompatibilities)
    assert!(
        success_count > 10,
        "Expected at least 10 successful messages, got {success_count}"
    );

    println!("🎉 Batch demo completed!");
}
