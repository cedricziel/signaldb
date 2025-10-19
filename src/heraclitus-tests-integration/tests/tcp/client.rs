use anyhow::Result;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_rdkafka_produce_consume() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-kafka-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "rdkafka-test-topic";

    // Create producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()?;

    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-consumer-group")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Subscribe to topic
    consumer.subscribe(&[topic])?;

    // Test 1: Produce a single message
    let key = "test-key-1";
    let value = "test-value-1";

    println!("Attempting to produce message to topic: {topic}");

    let delivery_result = producer
        .send(
            FutureRecord::to(topic).key(key).payload(value),
            Timeout::After(Duration::from_secs(5)),
        )
        .await;

    match delivery_result {
        Ok((partition, offset)) => {
            println!("Message delivered to partition {partition} at offset {offset}");
            assert_eq!(partition, 0);
            assert!(offset >= 0);
        }
        Err((e, _)) => {
            return Err(anyhow::anyhow!("Failed to deliver message: {:?}", e));
        }
    }

    // Give some time for the message to be persisted
    sleep(Duration::from_millis(500)).await;

    // Test 2: Consume the message
    let mut message_consumed = false;
    let start_time = tokio::time::Instant::now();

    while !message_consumed && start_time.elapsed() < Duration::from_secs(10) {
        match consumer.recv().await {
            Ok(msg) => {
                let msg_key = msg.key().map(|k| std::str::from_utf8(k).unwrap_or(""));
                let msg_value = msg.payload().map(|v| std::str::from_utf8(v).unwrap_or(""));

                println!(
                    "Consumed message: key={:?}, value={:?}, offset={}",
                    msg_key,
                    msg_value,
                    msg.offset()
                );

                if msg_key == Some(key) && msg_value == Some(value) {
                    message_consumed = true;
                    assert_eq!(msg.partition(), 0);
                }
            }
            Err(e) => {
                eprintln!("Consumer error: {e}");
            }
        }
    }

    assert!(message_consumed, "Failed to consume the produced message");

    // Test 3: Produce multiple messages
    let messages = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];

    for (k, v) in &messages {
        producer
            .send(
                FutureRecord::to(topic).key(*k).payload(*v),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Failed to produce: {:?}", e))?;
    }

    // Test 4: Verify all messages can be consumed
    let mut consumed_count = 0;
    let start_time = tokio::time::Instant::now();

    while consumed_count < messages.len() && start_time.elapsed() < Duration::from_secs(10) {
        if let Ok(msg) = consumer.recv().await {
            let msg_key = msg.key().map(|k| std::str::from_utf8(k).unwrap_or(""));
            if messages.iter().any(|(k, _)| msg_key == Some(*k)) {
                consumed_count += 1;
            }
        }
    }

    assert_eq!(
        consumed_count,
        messages.len(),
        "Not all messages were consumed"
    );

    Ok(())
}

#[tokio::test]
async fn test_rdkafka_metadata() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-metadata-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");

    // Create a client
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "metadata-test")
        .create()?;

    // Get metadata
    let metadata = consumer.fetch_metadata(None, Timeout::After(Duration::from_secs(5)))?;

    // Verify broker information
    assert_eq!(metadata.brokers().len(), 1);
    let broker = &metadata.brokers()[0];
    assert_eq!(broker.id(), 0);
    assert_eq!(broker.host(), "127.0.0.1");
    assert_eq!(broker.port() as u16, kafka_port);

    // Initially there should be no topics
    assert_eq!(metadata.topics().len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_rdkafka_error_handling() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-error-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");

    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "error-test")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Try to consume from non-existent topic
    consumer.subscribe(&["non-existent-topic"])?;

    // This should not crash but handle gracefully
    let start_time = tokio::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(2) {
        match consumer.recv().await {
            Ok(_) => {
                // Shouldn't receive any messages
            }
            Err(e) => {
                println!("Expected error for non-existent topic: {e}");
            }
        }
    }

    Ok(())
}
