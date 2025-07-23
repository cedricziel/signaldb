use anyhow::Result;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::util::Timeout;
use std::time::Duration;

#[tokio::test]
async fn test_rdkafka_simple_produce() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-rdkafka-simple").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");
    let topic = "simple-test-topic";

    // Create producer with debug logging
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("debug", "all")
        .set("message.timeout.ms", "30000")
        .set("socket.timeout.ms", "10000")
        .set("request.timeout.ms", "10000")
        .create()?;

    // Wait for metadata to be available
    println!("Waiting for broker metadata...");
    let mut retries = 0;
    loop {
        let metadata = producer
            .client()
            .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(5)));
        match metadata {
            Ok(m) => {
                println!(
                    "Got metadata: {} brokers, {} topics",
                    m.brokers().len(),
                    m.topics().len()
                );

                // Print broker details
                for broker in m.brokers() {
                    println!(
                        "  Broker: id={}, host={}, port={}",
                        broker.id(),
                        broker.host(),
                        broker.port()
                    );
                }

                // Print topic details
                for topic_meta in m.topics() {
                    println!(
                        "  Topic: name={}, partitions={}",
                        topic_meta.name(),
                        topic_meta.partitions().len()
                    );
                    for partition in topic_meta.partitions() {
                        println!(
                            "    Partition: id={}, leader={}, error={:?}",
                            partition.id(),
                            partition.leader(),
                            partition.error()
                        );
                    }
                }

                if !m.brokers().is_empty() {
                    break;
                }
            }
            Err(e) => {
                println!("Metadata fetch error: {e:?}");
            }
        }

        retries += 1;
        if retries > 10 {
            return Err(anyhow::anyhow!(
                "Failed to get broker metadata after 10 retries"
            ));
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Now try to produce
    println!("Producing message to topic: {topic}");

    let record = BaseRecord::to(topic).key("test-key").payload("test-value");

    match producer.send(record) {
        Ok(_) => {
            println!("Message queued successfully");
        }
        Err((e, _)) => {
            return Err(anyhow::anyhow!("Failed to queue message: {:?}", e));
        }
    }

    // Flush to ensure message is sent
    producer.flush(Timeout::After(Duration::from_secs(10)))?;
    println!("Message flushed successfully");

    Ok(())
}
