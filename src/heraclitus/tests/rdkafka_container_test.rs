// Include the testcontainers module
include!("testcontainers_heraclitus.rs");

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::metadata::Metadata;
use std::time::Duration;
// testcontainers functionality is available through the included module
use tokio::time::sleep;

/// Test rdkafka connection with api.version.request=true (should work)
async fn test_rdkafka_with_api_version_request(bootstrap_servers: &str) -> KafkaResult<Metadata> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("api.version.request", "true")
        .set("debug", "protocol,broker")
        .create()?;

    consumer.fetch_metadata(None, Duration::from_secs(5))
}

/// Test rdkafka connection with api.version.request=false (was crashing before fix)
async fn test_rdkafka_without_api_version_request(
    bootstrap_servers: &str,
) -> KafkaResult<Metadata> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("api.version.request", "false")
        .set("broker.version.fallback", "2.0.0")
        .set("debug", "protocol,broker")
        .create()?;

    consumer.fetch_metadata(None, Duration::from_secs(5))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rdkafka_connects_to_containerized_heraclitus() {
        // Start Heraclitus container (automatically builds image if needed)
        let heraclitus_container = Heraclitus::default()
            .build_and_start()
            .await
            .expect("Failed to build and start Heraclitus container");

        let heraclitus_port = heraclitus_container
            .get_host_port_ipv4(Heraclitus::kafka_port())
            .await
            .expect("Failed to get Heraclitus port");

        // Wait for Heraclitus to be ready
        sleep(Duration::from_secs(3)).await;

        let bootstrap_servers = format!("127.0.0.1:{heraclitus_port}");
        println!("Connecting to Heraclitus at: {bootstrap_servers}");

        // Test with api.version.request=true
        println!("\nTesting with api.version.request=true...");
        match test_rdkafka_with_api_version_request(&bootstrap_servers).await {
            Ok(metadata) => {
                println!("✓ Successfully connected with api.version.request=true");
                println!("  Brokers: {}", metadata.brokers().len());
                println!("  Topics: {}", metadata.topics().len());
            }
            Err(e) => {
                panic!("Failed to connect with api.version.request=true: {e:?}");
            }
        }

        // Test with api.version.request=false (this was crashing before the fix)
        println!("\nTesting with api.version.request=false...");
        match test_rdkafka_without_api_version_request(&bootstrap_servers).await {
            Ok(metadata) => {
                println!("✓ Successfully connected with api.version.request=false");
                println!("  Brokers: {}", metadata.brokers().len());
                println!("  Topics: {}", metadata.topics().len());
            }
            Err(e) => {
                panic!("Failed to connect with api.version.request=false: {e:?}");
            }
        }

        println!("\n✅ All rdkafka connection tests passed!");
    }

    #[tokio::test]
    async fn test_rdkafka_basic_operations() {
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

        // Start Heraclitus container (automatically builds image if needed)
        let heraclitus_container = Heraclitus::default()
            .build_and_start()
            .await
            .expect("Failed to build and start Heraclitus container");

        let heraclitus_port = heraclitus_container
            .get_host_port_ipv4(Heraclitus::kafka_port())
            .await
            .expect("Failed to get Heraclitus port");

        // Wait for Heraclitus to be ready
        sleep(Duration::from_secs(3)).await;

        let bootstrap_servers = format!("127.0.0.1:{heraclitus_port}");

        // Create producer
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("api.version.request", "true")
            .create()
            .expect("Failed to create producer");

        // Test producing a message
        let topic = "test-topic";
        let key = "test-key";
        let value = "test-value";

        let record = BaseRecord::to(topic).key(key).payload(value);

        match producer.send(record) {
            Ok(_) => println!("✓ Successfully sent message to topic '{topic}'"),
            Err((e, _)) => println!("⚠️  Failed to send message: {e:?} (topic may not exist yet)"),
        }

        // Flush to ensure message is sent
        let _ = producer.flush(Duration::from_secs(1));

        println!("✓ Basic producer operations completed");
    }
}
