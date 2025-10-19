use anyhow::Result;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use tokio::time::Duration;

/// Minimal test with just 2 compression types to isolate the issue
#[tokio::test]
async fn test_two_compression_types() -> Result<()> {
    init_test_tracing();

    println!("Starting minimal 2-compression test");

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-compression-debug").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    println!("Using Kafka port: {kafka_port}");
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;
    println!("Heraclitus started successfully");

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "debug-compression-test";

    // Create consumer first
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-consumer-debug")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[topic])?;

    // Test with all 5 compression types
    for compression_type in ["none", "gzip", "snappy", "lz4", "zstd"] {
        println!("\n=== Testing {compression_type} compression ===");

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("compression.type", compression_type)
            .set("message.timeout.ms", "5000")
            .create()?;

        let message = format!("Message with {compression_type} compression");
        println!("Sending message...");

        let result = producer
            .send(
                FutureRecord::to(topic)
                    .key(compression_type)
                    .payload(&message),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Failed to produce: {:?}", e))?;

        println!("✓ Sent to partition {} at offset {}", result.0, result.1);

        println!("Flushing producer...");
        producer.flush(Duration::from_secs(5))?;
        println!("✓ Producer flushed");

        // Wait a bit between producers
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    println!("\n=== All messages sent, waiting for persistence ===");
    // Give time for messages to be persisted
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("=== Starting to consume ===");
    // Consume all messages
    let mut consumed_count = 0;
    let start_time = tokio::time::Instant::now();

    while consumed_count < 5 && start_time.elapsed() < Duration::from_secs(10) {
        if let Ok(msg) = consumer.recv().await {
            if let Some(key) = msg.key() {
                let compression_type = std::str::from_utf8(key)?;
                let value = std::str::from_utf8(msg.payload().unwrap_or(&[]))?;
                let expected = format!("Message with {compression_type} compression");
                println!(
                    "Consumed message {}: {} ({})",
                    consumed_count + 1,
                    compression_type,
                    value
                );
                assert_eq!(value, expected);
                consumed_count += 1;
            }
        } else {
            println!("No message received, waiting...");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    println!("\n=== Test result: consumed {consumed_count}/5 messages ===");
    assert_eq!(consumed_count, 5, "Should have consumed all 5 messages");
    Ok(())
}
