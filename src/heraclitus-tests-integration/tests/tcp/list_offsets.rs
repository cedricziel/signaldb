use anyhow::Result;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::time::Duration;

#[tokio::test]
async fn test_list_offsets_basic() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-list-offsets").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "list-offsets-test";

    // Create producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;

    // Produce 10 messages
    for i in 0..10 {
        let message = format!("Message {i}");
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&format!("key-{i}"))
                    .payload(&message),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Failed to produce: {:?}", e))?;
    }

    // Give some time for messages to be persisted
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create consumer with auto.offset.reset=earliest
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-list-offsets")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Subscribe to topic
    consumer.subscribe(&[topic])?;

    // The consumer should internally use ListOffsets to determine where to start reading
    // Since we set auto.offset.reset=earliest, it should start from offset 0
    let mut messages_consumed = 0;
    let start_time = tokio::time::Instant::now();

    while messages_consumed < 10 && start_time.elapsed() < Duration::from_secs(10) {
        if let Ok(msg) = consumer.recv().await {
            let offset = msg.offset();
            assert_eq!(
                offset, messages_consumed,
                "Messages should be consumed in order"
            );
            messages_consumed += 1;
        }
    }

    assert_eq!(
        messages_consumed, 10,
        "Should have consumed all 10 messages"
    );
    Ok(())
}

#[tokio::test]
async fn test_list_offsets_latest() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-list-offsets-latest").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "list-offsets-latest-test";

    // Create producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;

    // Produce 5 messages first
    for i in 0..5 {
        let message = format!("First batch message {i}");
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&format!("key-{i}"))
                    .payload(&message),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Failed to produce: {:?}", e))?;
    }

    // Give some time for messages to be persisted
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create consumer with auto.offset.reset=latest
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-list-offsets-latest")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .create()?;

    // Subscribe to topic
    consumer.subscribe(&[topic])?;

    // Give consumer time to establish position
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now produce 5 more messages
    for i in 5..10 {
        let message = format!("Second batch message {i}");
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&format!("key-{i}"))
                    .payload(&message),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Failed to produce: {:?}", e))?;
    }

    // Consumer should only see messages from the second batch (offsets 5-9)
    let mut messages_consumed = 0;
    let mut min_offset = i64::MAX;
    let start_time = tokio::time::Instant::now();

    while messages_consumed < 5 && start_time.elapsed() < Duration::from_secs(10) {
        if let Ok(msg) = consumer.recv().await {
            let offset = msg.offset();
            min_offset = min_offset.min(offset);
            messages_consumed += 1;
        }
    }

    assert_eq!(
        messages_consumed, 5,
        "Should have consumed 5 messages from second batch"
    );
    assert_eq!(
        min_offset, 5,
        "Should start consuming from offset 5 (latest position when subscribed)"
    );
    Ok(())
}

#[tokio::test]
async fn test_list_offsets_empty_topic() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-list-offsets-empty").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "list-offsets-empty-test";

    // Create consumer with auto.offset.reset=earliest on a topic with no messages
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-list-offsets-empty")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Subscribe to topic (will auto-create it)
    consumer.subscribe(&[topic])?;

    // Give consumer time to establish position
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now produce a message
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;

    producer
        .send(
            FutureRecord::to(topic)
                .key("test-key")
                .payload("test message"),
            Timeout::After(Duration::from_secs(5)),
        )
        .await
        .map_err(|(e, _)| anyhow::anyhow!("Failed to produce: {:?}", e))?;

    // Consumer should see the message at offset 0
    let start_time = tokio::time::Instant::now();
    let mut received = false;

    while !received && start_time.elapsed() < Duration::from_secs(5) {
        if let Ok(msg) = consumer.recv().await {
            assert_eq!(msg.offset(), 0, "First message should be at offset 0");
            received = true;
        }
    }

    assert!(received, "Should have received the message");
    Ok(())
}
