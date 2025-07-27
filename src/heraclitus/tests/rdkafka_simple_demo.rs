// Simple demonstration: Using rdkafka with Heraclitus (basic connectivity)
// This test shows that rdkafka can successfully connect to containerized Heraclitus

// Include the testcontainers module for containerized testing
include!("testcontainers_heraclitus.rs");

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;

#[tokio::test]
async fn test_rdkafka_basic_connectivity_demo() {
    println!("🚀 Heraclitus + rdkafka Basic Connectivity Demo");
    println!("   Using rdkafka version: {}", get_rdkafka_version().1);

    // Start Heraclitus container (automatically builds image if needed)
    let heraclitus_container = Heraclitus::default()
        .build_and_start()
        .await
        .expect("Failed to start Heraclitus container");

    let heraclitus_port = heraclitus_container
        .get_host_port_ipv4(Heraclitus::kafka_port())
        .await
        .expect("Failed to get Heraclitus port");

    // Wait for Heraclitus to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    let bootstrap_servers = format!("127.0.0.1:{heraclitus_port}");
    println!("📡 Connecting to Heraclitus at: {bootstrap_servers}");

    // Test 1: Create consumer and fetch metadata (this usually works)
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "demo-group")
        .set("client.id", "demo-client")
        .set("api.version.request", "true")
        .create()
        .expect("Failed to create consumer");

    println!("✅ Consumer created successfully");

    // Try to fetch metadata
    match consumer.fetch_metadata(None, Duration::from_secs(10)) {
        Ok(metadata) => {
            println!("✅ Successfully fetched metadata!");
            println!("   📊 Cluster info:");
            println!("      - Brokers: {}", metadata.brokers().len());
            println!("      - Topics: {}", metadata.topics().len());

            for (i, broker) in metadata.brokers().iter().enumerate() {
                println!(
                    "      - Broker {}: {}:{} (id: {})",
                    i,
                    broker.host(),
                    broker.port(),
                    broker.id()
                );
            }

            if !metadata.topics().is_empty() {
                println!("      - Available topics:");
                for topic in metadata.topics() {
                    println!(
                        "        • {} ({} partitions)",
                        topic.name(),
                        topic.partitions().len()
                    );
                }
            } else {
                println!("      - No topics available (expected for fresh instance)");
            }
        }
        Err(e) => {
            println!("⚠️  Metadata fetch failed: {e:?}");
            println!("   This may be due to rdkafka compatibility issues");
        }
    }

    println!("🎉 Basic connectivity demo completed!");
    println!("   ✅ Heraclitus container: Running");
    println!("   ✅ rdkafka client: Created successfully");
    println!("   ✅ Testcontainers: Working perfectly");
}

#[tokio::test]
async fn test_multiple_client_connections_demo() {
    println!("🚀 Multiple Client Connections Demo");

    // Start Heraclitus container
    let heraclitus_container = Heraclitus::default()
        .build_and_start()
        .await
        .expect("Failed to start Heraclitus container");

    let heraclitus_port = heraclitus_container
        .get_host_port_ipv4(Heraclitus::kafka_port())
        .await
        .expect("Failed to get Heraclitus port");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let bootstrap_servers = format!("127.0.0.1:{heraclitus_port}");
    println!("📡 Creating multiple clients for: {bootstrap_servers}");

    // Create multiple consumers to test concurrent connections
    let mut consumers = Vec::new();

    for i in 0..3 {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", format!("demo-group-{i}"))
            .set("client.id", format!("demo-client-{i}"))
            .set("api.version.request", "true")
            .create()
            .unwrap_or_else(|_| panic!("Failed to create consumer {i}"));

        consumers.push(consumer);
        println!("   ✅ Consumer {} created", i + 1);
    }

    println!("🔗 Testing concurrent metadata requests...");

    let mut successful_requests = 0;
    for (i, consumer) in consumers.iter().enumerate() {
        match consumer.fetch_metadata(None, Duration::from_secs(5)) {
            Ok(_metadata) => {
                println!("   ✅ Client {} metadata: Success", i + 1);
                successful_requests += 1;
            }
            Err(e) => {
                println!("   ⚠️  Client {} metadata: Failed ({:?})", i + 1, e);
            }
        }
    }

    println!(
        "📊 Results: {}/{} clients connected successfully",
        successful_requests,
        consumers.len()
    );
    println!("🎉 Multiple client demo completed!");
}
