// Debug test to investigate rdkafka crash
use anyhow::Result;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::time::Duration;

#[tokio::test]
async fn test_rdkafka_debug_simple() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-rdkafka-debug").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Give Heraclitus a moment to fully start
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Heraclitus started on port {kafka_port}");

    // Try to connect with minimal rdkafka config
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};

    // Enable all debug output
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }

    println!("Creating rdkafka consumer with debug enabled...");

    let result = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{kafka_port}"))
        .set("group.id", "debug-group")
        .set("debug", "all")
        .set("log_level", "7")
        .create::<BaseConsumer>();

    match result {
        Ok(consumer) => {
            println!("✓ Successfully created consumer");

            // Try to fetch metadata
            println!("Attempting to fetch metadata...");
            match consumer.fetch_metadata(None, Duration::from_secs(5)) {
                Ok(metadata) => {
                    println!("✓ Successfully fetched metadata");
                    println!("  Brokers: {}", metadata.brokers().len());
                    for broker in metadata.brokers() {
                        println!(
                            "    - Broker {}: {}:{}",
                            broker.id(),
                            broker.host(),
                            broker.port()
                        );
                    }
                    println!("  Topics: {}", metadata.topics().len());
                }
                Err(e) => {
                    println!("✗ Failed to fetch metadata: {e:?}");
                    return Err(anyhow::anyhow!("Metadata fetch failed: {:?}", e));
                }
            }
        }
        Err(e) => {
            println!("✗ Failed to create consumer: {e:?}");
            return Err(anyhow::anyhow!("Consumer creation failed: {:?}", e));
        }
    }

    println!("Test completed successfully");
    Ok(())
}
