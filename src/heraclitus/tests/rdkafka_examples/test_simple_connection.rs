// Simple test to check if rdkafka can connect to Heraclitus
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;

fn main() {
    println!("rdkafka version: {}", get_rdkafka_version().1);
    
    // Enable debug logging
    std::env::set_var("RUST_LOG", "debug");
    
    println!("Creating consumer...");
    
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("group.id", "simple-test")
        .set("client.id", "simple-test-client")
        .set("api.version.request", "true")
        .set("debug", "broker,protocol")
        .create()
        .expect("Consumer creation failed");
    
    println!("Consumer created successfully");
    
    // Try to fetch metadata
    println!("Fetching metadata...");
    match consumer.fetch_metadata(None, Duration::from_secs(5)) {
        Ok(metadata) => {
            println!("✓ Metadata fetched successfully!");
            println!("  Brokers: {}", metadata.brokers().len());
            for broker in metadata.brokers() {
                println!("    - Broker {}: {}:{}", broker.id(), broker.host(), broker.port());
            }
        }
        Err(e) => {
            println!("✗ Failed to fetch metadata: {:?}", e);
        }
    }
}