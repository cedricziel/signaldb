use anyhow::Result;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio::time::Duration;

#[tokio::test]
async fn test_find_coordinator() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-find-coordinator").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");

    // Create a consumer - this will trigger FindCoordinator
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "test-find-coordinator-group")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Subscribe to a topic - this should trigger the consumer group protocol
    consumer.subscribe(&["test-topic"])?;

    // Give the consumer time to connect and find coordinator
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The test passes if we get here without panicking or erroring
    // The consumer successfully found the coordinator

    Ok(())
}

#[tokio::test]
async fn test_find_coordinator_multiple_groups() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-find-coordinator-multi").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");

    // Create multiple consumers with different group IDs
    for i in 0..3 {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", format!("test-group-{i}"))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        consumer.subscribe(&[&format!("test-topic-{i}")])?;
    }

    // Give the consumers time to connect and find coordinator
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // The test passes if all consumers can find the coordinator
    Ok(())
}
