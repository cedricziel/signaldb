use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;

fn main() {
    println!("Testing minimal rdkafka connection...");

    // Create a consumer just to test basic connectivity
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("client.id", "minimal-test")
        .set("group.id", "test-group")
        .set("api.version.request", "true")
        .set("debug", "all") // Enable all debug logging
        .create()
        .expect("Consumer creation failed");

    println!("Consumer created, fetching metadata...");

    // Try to fetch metadata
    match consumer.fetch_metadata(None, Duration::from_secs(5)) {
        Ok(metadata) => {
            println!("✓ Metadata fetched successfully!");
            println!("  Brokers: {}", metadata.brokers().len());
            println!("  Topics: {}", metadata.topics().len());

            for broker in metadata.brokers() {
                println!(
                    "  Broker {}: {}:{}",
                    broker.id(),
                    broker.host(),
                    broker.port()
                );
            }
        }
        Err(e) => {
            println!("✗ Failed to fetch metadata: {e:?}");
        }
    }

    println!("Test completed.");
}
