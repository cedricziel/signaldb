use anyhow::Result;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;

#[tokio::test]
async fn test_rdkafka_basic_connectivity() -> Result<()> {
    init_test_tracing();

    println!("Testing rdkafka {} compatibility", get_rdkafka_version().1);

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-rdkafka-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Give Heraclitus a moment to fully start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test 1: Can create a consumer and fetch metadata
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{kafka_port}"))
        .set("group.id", "test-group")
        .set("client.id", "test-client")
        .set("debug", "all")
        .create()?;

    println!("Created rdkafka consumer");

    // Fetch metadata should work
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;

    assert!(
        !metadata.brokers().is_empty(),
        "Should have at least one broker"
    );
    println!(
        "✓ Successfully fetched metadata with {} brokers",
        metadata.brokers().len()
    );

    // Print broker info
    for broker in metadata.brokers() {
        println!(
            "  Broker: id={}, host={}, port={}",
            broker.id(),
            broker.host(),
            broker.port()
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_rdkafka_producer_basic() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-rdkafka-producer").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Give Heraclitus a moment to fully start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{kafka_port}"))
        .set("message.timeout.ms", "5000")
        .create()?;

    println!("✓ Created rdkafka producer");

    // Send a test message
    let delivery_result = producer
        .send(
            FutureRecord::to("test-topic")
                .payload("Hello, Heraclitus!")
                .key("test-key"),
            Duration::from_secs(5),
        )
        .await;

    match delivery_result {
        Ok((partition, offset)) => {
            println!("✓ Message sent successfully: partition={partition}, offset={offset}");
        }
        Err((e, _)) => {
            return Err(anyhow::anyhow!("Failed to send message: {:?}", e));
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_rdkafka_consumer_basic() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-rdkafka-consumer").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Give Heraclitus a moment to fully start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // First, produce a message
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{kafka_port}"))
        .set("message.timeout.ms", "5000")
        .create()?;

    match producer
        .send(
            FutureRecord::to("test-consume")
                .payload("Test message for consumption")
                .key("consume-key"),
            Duration::from_secs(5),
        )
        .await
    {
        Ok((partition, offset)) => {
            println!("✓ Produced message to partition {partition} offset {offset}");
        }
        Err((e, _)) => {
            return Err(anyhow::anyhow!("Failed to produce message: {:?}", e));
        }
    }

    println!("✓ Produced test message");

    // Now create a consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{kafka_port}"))
        .set("group.id", "test-consumer-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&["test-consume"])?;
    println!("✓ Created consumer and subscribed to topic");

    // Try to consume the message with a timeout
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(10);

    while start.elapsed() < timeout {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    let payload_str = std::str::from_utf8(payload)?;
                    assert_eq!(payload_str, "Test message for consumption");
                    println!("✓ Successfully consumed message: {payload_str}");
                    return Ok(());
                }
            }
            Some(Err(e)) => {
                return Err(anyhow::anyhow!("Consume error: {:?}", e));
            }
            None => {
                // No message yet, continue polling
            }
        }
    }

    Err(anyhow::anyhow!("Timeout waiting for message"))
}
