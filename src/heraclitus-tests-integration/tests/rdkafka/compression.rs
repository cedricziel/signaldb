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

#[tokio::test]
async fn test_rdkafka_gzip_compression() -> Result<()> {
    test_compression("gzip").await
}

#[tokio::test]
async fn test_rdkafka_snappy_compression() -> Result<()> {
    test_compression("snappy").await
}

#[tokio::test]
async fn test_rdkafka_lz4_compression() -> Result<()> {
    test_compression("lz4").await
}

#[tokio::test]
async fn test_rdkafka_zstd_compression() -> Result<()> {
    test_compression("zstd").await
}

async fn test_compression(compression_type: &str) -> Result<()> {
    init_test_tracing();

    println!("Starting compression test for {compression_type}");

    // Start MinIO
    let minio =
        MinioTestContext::new(&format!("heraclitus-compression-{compression_type}")).await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    println!("Using Kafka port: {kafka_port}");
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;
    println!("Heraclitus started successfully");

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = format!("compression-test-{compression_type}");

    // Create producer with compression
    println!("Creating producer with {compression_type} compression");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("compression.type", compression_type)
        .set("message.timeout.ms", "5000")
        //.set("debug", "all") // Enable debug logging
        .create()?;

    // Create consumer
    println!("Creating consumer");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", format!("test-consumer-{compression_type}"))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        //.set("debug", "consumer,fetch") // Enable debug logging
        .create()?;

    println!("Subscribing to topic: {topic}");
    consumer.subscribe(&[&topic])?;

    // Produce a message with some compressible content
    let test_data = "This is test data that should compress well. ".repeat(10);

    println!("Producing message with {compression_type} compression...");
    let delivery_result = producer
        .send(
            FutureRecord::to(&topic).key("test-key").payload(&test_data),
            Timeout::After(Duration::from_secs(5)),
        )
        .await;

    match delivery_result {
        Ok((partition, offset)) => {
            println!("Message delivered to partition {partition} at offset {offset}");
        }
        Err((e, _)) => {
            return Err(anyhow::anyhow!("Failed to deliver message: {:?}", e));
        }
    }

    // Give some time for the message to be persisted
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Consume the message
    println!("Consuming message...");
    let mut message_consumed = false;
    let start_time = tokio::time::Instant::now();

    while !message_consumed && start_time.elapsed() < Duration::from_secs(10) {
        match consumer.recv().await {
            Ok(msg) => {
                let key = msg.key().map(|k| std::str::from_utf8(k).unwrap_or(""));
                let value = msg.payload().map(|v| std::str::from_utf8(v).unwrap_or(""));

                if key == Some("test-key") && value == Some(&test_data) {
                    message_consumed = true;
                    println!("Successfully consumed compressed message with {compression_type}");
                }
            }
            Err(e) => {
                eprintln!("Consumer error: {e}");
            }
        }
    }

    assert!(message_consumed, "Failed to consume the compressed message");
    Ok(())
}

#[tokio::test]
async fn test_mixed_compression() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-mixed-compression").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "mixed-compression-test";

    // Create consumer first
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-consumer-mixed")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[topic])?;

    // Produce messages with different compression types
    for compression_type in ["none", "gzip", "snappy", "lz4", "zstd"] {
        println!("Creating producer for {compression_type} compression");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("compression.type", compression_type)
            .set("message.timeout.ms", "5000")
            .create()?;

        let message = format!("Message with {compression_type} compression");
        println!("Sending message with {compression_type} compression...");
        let result = producer
            .send(
                FutureRecord::to(topic)
                    .key(compression_type)
                    .payload(&message),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Failed to produce: {:?}", e))?;

        println!(
            "✓ Message sent with {compression_type} compression to partition {} at offset {}",
            result.0, result.1
        );

        // Flush producer to ensure message is persisted before it's dropped
        println!("Flushing producer for {compression_type}...");
        producer.flush(Duration::from_secs(5))?;
        println!("✓ Producer flushed for {compression_type}");

        // Allow time for producer connection to stabilize before dropping
        // This prevents race conditions when creating/dropping producers rapidly
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Give additional time for messages to be persisted to storage
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Consume all messages
    let mut consumed_count = 0;
    let start_time = tokio::time::Instant::now();

    while consumed_count < 5 && start_time.elapsed() < Duration::from_secs(10) {
        if let Ok(msg) = consumer.recv().await {
            if let Some(key) = msg.key() {
                let compression_type = std::str::from_utf8(key)?;
                let value = std::str::from_utf8(msg.payload().unwrap_or(&[]))?;
                let expected = format!("Message with {compression_type} compression");
                assert_eq!(value, expected);
                consumed_count += 1;
                println!("Consumed message {consumed_count}/5: {compression_type}");
            }
        }
    }

    assert_eq!(consumed_count, 5, "Should have consumed all 5 messages");
    Ok(())
}
