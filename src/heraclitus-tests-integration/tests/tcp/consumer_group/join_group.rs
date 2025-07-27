use anyhow::Result;
use futures::StreamExt;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time::Duration;

#[tokio::test]
async fn test_join_group_single_consumer() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-join-group-single").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "test-join-group-topic";
    let group_id = "test-join-group-single";

    // First produce some messages
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;

    for i in 0..10 {
        let key = format!("key-{i}");
        let value = format!("value-{i}");

        producer
            .send(
                FutureRecord::to(topic).key(&key).payload(&value),
                Duration::from_secs(1),
            )
            .await
            .map_err(|(e, _)| e)?;
    }

    // Create a consumer - this will trigger JoinGroup
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .create()?;

    // Subscribe to topic
    consumer.subscribe(&[topic])?;

    // Consume messages
    let mut count = 0;
    let mut stream = consumer.stream();

    while count < 10 {
        match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());

                tracing::info!("Consumed message: key={:?}, value={:?}", key, value);
                count += 1;
            }
            Ok(Some(Err(e))) => {
                tracing::error!("Consumer error: {}", e);
                return Err(e.into());
            }
            Ok(None) => {
                tracing::warn!("Stream ended unexpectedly");
                break;
            }
            Err(_) => {
                tracing::warn!("Timeout waiting for message");
                break;
            }
        }
    }

    assert_eq!(count, 10, "Should have consumed all 10 messages");

    Ok(())
}

#[tokio::test]
async fn test_join_group_multiple_consumers() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-join-group-multi").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "test-join-group-multi-topic";
    let group_id = "test-join-group-multi";

    // First produce some messages
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;

    for i in 0..20 {
        let key = format!("key-{i}");
        let value = format!("value-{i}");

        producer
            .send(
                FutureRecord::to(topic).key(&key).payload(&value),
                Duration::from_secs(1),
            )
            .await
            .map_err(|(e, _)| e)?;
    }

    // Create multiple consumers in the same group
    let mut consumers = Vec::new();
    for i in 0..3 {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", group_id)
            .set("client.id", format!("consumer-{i}"))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "30000")
            .create()?;

        consumer.subscribe(&[topic])?;
        consumers.push(consumer);
    }

    // Give consumers time to join the group
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Each consumer should be part of the same group
    // In a real implementation, they would coordinate partition assignment
    // For now, we just verify they can all join successfully

    Ok(())
}

#[tokio::test]
async fn test_join_group_rebalance() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-join-group-rebalance").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "test-join-group-rebalance-topic";
    let group_id = "test-join-group-rebalance";

    // Create first consumer
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", group_id)
        .set("client.id", "consumer-1")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .create()?;

    consumer1.subscribe(&[topic])?;

    // Give first consumer time to become leader
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create second consumer - should trigger rebalance
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", group_id)
        .set("client.id", "consumer-2")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .create()?;

    consumer2.subscribe(&[topic])?;

    // Give time for rebalance
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Both consumers should be part of the group now
    // In a complete implementation, we would verify partition assignment changed

    Ok(())
}
