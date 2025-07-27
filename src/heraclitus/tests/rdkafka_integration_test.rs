// Integration tests for rdkafka producer and consumer functionality
// These tests verify that Heraclitus is fully compatible with rdkafka clients

mod subprocess;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, FutureProducer, FutureRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use std::time::Duration;
use tokio::time::sleep;

use subprocess::HeraclitusTestContext;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test basic produce and consume operations
    #[tokio::test]
    async fn test_basic_produce_consume() {
        // Start Heraclitus subprocess
        let heraclitus = HeraclitusTestContext::new()
            .await
            .expect("Failed to start Heraclitus");

        let bootstrap_servers = heraclitus.kafka_addr();

        // Create producer
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("api.version.request", "true") // Enable API version requests
            .create()
            .expect("Failed to create producer");

        let topic = "test-basic";

        // Produce messages
        let messages = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];

        println!("Producing {} messages to topic '{}'", messages.len(), topic);
        for (i, (key, value)) in messages.iter().enumerate() {
            let record = BaseRecord::to(topic).key(*key).payload(*value);
            match producer.send(record) {
                Ok(_) => println!("Sent message {i}: {key} -> {value}"),
                Err((e, _)) => eprintln!("Failed to send message {i}: {e:?}"),
            }
        }

        // Flush producer
        println!("Flushing producer...");
        match producer.flush(Duration::from_secs(5)) {
            Ok(_) => println!("Producer flushed successfully"),
            Err(e) => eprintln!("Producer flush error: {e:?}"),
        }

        // Create consumer after producing messages
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", "test-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("api.version.request", "true") // Enable API version requests
            .create()
            .expect("Failed to create consumer");

        // Subscribe to topic
        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        // Give the consumer time to connect and subscribe
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Consume messages
        let mut received = Vec::new();
        let start = std::time::Instant::now();

        println!("Starting to consume messages...");
        while received.len() < messages.len() && start.elapsed() < Duration::from_secs(10) {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(msg)) => {
                    let key = msg.key().map(|k| std::str::from_utf8(k).unwrap());
                    let value = msg.payload().map(|v| std::str::from_utf8(v).unwrap());

                    if let (Some(k), Some(v)) = (key, value) {
                        println!("Received message: {k} -> {v}");
                        received.push((k.to_string(), v.to_string()));
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Consumer error: {e:?}");
                }
                None => {
                    // No message available
                }
            }
        }
        println!(
            "Consumed {} messages in {:?}",
            received.len(),
            start.elapsed()
        );

        // Verify all messages received
        assert_eq!(received.len(), messages.len());
        for (key, value) in messages {
            assert!(received.contains(&(key.to_string(), value.to_string())));
        }
    }

    /// Test batch producing with future producer
    #[tokio::test]
    async fn test_batch_produce_with_future_producer() {
        // Start Heraclitus subprocess
        let heraclitus = HeraclitusTestContext::new()
            .await
            .expect("Failed to start Heraclitus");

        let bootstrap_servers = heraclitus.kafka_addr();

        // Create future producer
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("linger.ms", "100") // Enable batching
            .set("batch.size", "16384")
            .create()
            .expect("Failed to create future producer");

        // Create consumer for verification
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", "test-batch-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Failed to create consumer");

        let topic = "test-batch";
        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        // Produce batch of messages
        let batch_size = 100;
        let mut success_count = 0;

        for i in 0..batch_size {
            let key = format!("key-{i:04}");
            let value = format!("batch-value-{i:04}");
            let record = FutureRecord::to(topic).key(&key).payload(&value);

            match producer.send(record, Duration::from_secs(0)).await {
                Ok((partition, offset)) => {
                    success_count += 1;
                    println!("Message {i} sent to partition {partition} at offset {offset}");
                }
                Err((e, _)) => {
                    eprintln!("Failed to send message {i}: {e:?}");
                }
            }
        }

        assert_eq!(success_count, batch_size);

        // Verify consumption
        let mut received_count = 0;
        let start = std::time::Instant::now();

        while received_count < batch_size && start.elapsed() < Duration::from_secs(10) {
            match consumer.recv().await {
                Ok(message) => {
                    received_count += 1;
                    let key = message.key().map(|k| std::str::from_utf8(k).unwrap());
                    assert!(key.unwrap().starts_with("key-"));
                }
                Err(e) => {
                    eprintln!("Error receiving message: {e:?}");
                    break;
                }
            }
        }

        assert_eq!(received_count, batch_size);
    }

    /// Test consumer group functionality
    #[tokio::test]
    async fn test_consumer_group_operations() {
        // Start Heraclitus subprocess
        let heraclitus = HeraclitusTestContext::new()
            .await
            .expect("Failed to start Heraclitus");

        let bootstrap_servers = heraclitus.kafka_addr();

        // Create producer
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .expect("Failed to create producer");

        let topic = "test-consumer-group";
        let group_id = "test-cg-group";

        // Produce messages
        for i in 0..10 {
            let key = format!("key-{i}");
            let value = format!("value-{i}");
            let record = BaseRecord::to(topic).key(&key).payload(&value);
            producer.send(record).expect("Failed to send message");
        }
        let _ = producer.flush(Duration::from_secs(5));

        // Create first consumer in group
        let consumer1: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "1000")
            .create()
            .expect("Failed to create consumer 1");

        consumer1.subscribe(&[topic]).expect("Failed to subscribe");

        // Consume some messages
        let mut consumed_by_c1 = 0;
        for _ in 0..5 {
            if let Some(Ok(_)) = consumer1.poll(Duration::from_secs(1)) {
                consumed_by_c1 += 1;
            }
        }

        assert!(
            consumed_by_c1 > 0,
            "Consumer 1 should have consumed messages"
        );

        // Create second consumer in same group
        let consumer2: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .create()
            .expect("Failed to create consumer 2");

        consumer2.subscribe(&[topic]).expect("Failed to subscribe");

        // Wait for rebalance
        sleep(Duration::from_secs(3)).await;

        // Both consumers should now share the load
        // Produce more messages
        for i in 10..20 {
            let key = format!("key-{i}");
            let value = format!("value-{i}");
            let record = BaseRecord::to(topic).key(&key).payload(&value);
            producer.send(record).expect("Failed to send message");
        }
        let _ = producer.flush(Duration::from_secs(5));

        // Both consumers should receive messages
        let mut c1_new = 0;
        let mut c2_new = 0;

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if let Some(Ok(_)) = consumer1.poll(Duration::from_millis(100)) {
                c1_new += 1;
            }
            if let Some(Ok(_)) = consumer2.poll(Duration::from_millis(100)) {
                c2_new += 1;
            }
        }

        // Both consumers should have received some messages
        assert!(
            c1_new > 0 || c2_new > 0,
            "At least one consumer should receive messages"
        );
        println!("Consumer 1 received {c1_new} new messages");
        println!("Consumer 2 received {c2_new} new messages");
    }

    /// Test message headers and metadata
    #[tokio::test]
    async fn test_message_headers_and_metadata() {
        // Start Heraclitus subprocess
        let heraclitus = HeraclitusTestContext::new()
            .await
            .expect("Failed to start Heraclitus");

        let bootstrap_servers = heraclitus.kafka_addr();

        // Create producer
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .expect("Failed to create producer");

        // Create consumer
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", "test-headers-group")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Failed to create consumer");

        let topic = "test-headers";
        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        // Create message with headers
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "header1",
                value: Some("value1"),
            })
            .insert(Header {
                key: "header2",
                value: Some("value2"),
            })
            .insert(Header {
                key: "header3",
                value: None::<&str>, // Null value
            });

        let record = BaseRecord::to(topic)
            .key("test-key")
            .payload("test-payload")
            .headers(headers);

        producer.send(record).expect("Failed to send message");
        let _ = producer.flush(Duration::from_secs(5));

        // Consume and verify
        let timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
                // Verify key and payload
                assert_eq!(message.key(), Some(b"test-key".as_ref()));
                assert_eq!(message.payload(), Some(b"test-payload".as_ref()));

                // Verify headers
                if let Some(headers) = message.headers() {
                    assert_eq!(headers.count(), 3);

                    let mut found_headers = 0;
                    for header in headers.iter() {
                        match header.key {
                            "header1" => {
                                assert_eq!(header.value, Some(b"value1".as_ref()));
                                found_headers += 1;
                            }
                            "header2" => {
                                assert_eq!(header.value, Some(b"value2".as_ref()));
                                found_headers += 1;
                            }
                            "header3" => {
                                assert_eq!(header.value, None);
                                found_headers += 1;
                            }
                            _ => panic!("Unexpected header key: {}", header.key),
                        }
                    }
                    assert_eq!(found_headers, 3);
                } else {
                    panic!("Message should have headers");
                }

                return; // Test passed
            }
        }

        panic!("Failed to receive message within timeout");
    }

    /// Test offset management
    #[tokio::test]
    async fn test_offset_management() {
        // Start Heraclitus subprocess
        let heraclitus = HeraclitusTestContext::new()
            .await
            .expect("Failed to start Heraclitus");

        let bootstrap_servers = heraclitus.kafka_addr();

        // Create producer
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .expect("Failed to create producer");

        let topic = "test-offsets";
        let group_id = "test-offset-group";

        // Produce 10 messages
        for i in 0..10 {
            let key = format!("key-{i}");
            let value = format!("value-{i}");
            let record = BaseRecord::to(topic).partition(0).key(&key).payload(&value);
            producer.send(record).expect("Failed to send message");
        }
        let _ = producer.flush(Duration::from_secs(5));

        // Create consumer with manual offset management
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Failed to create consumer");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        // Consume first 5 messages and commit offset
        let mut consumed = 0;
        let mut last_offset = 0;

        while consumed < 5 {
            if let Some(Ok(message)) = consumer.poll(Duration::from_secs(1)) {
                last_offset = message.offset();
                consumed += 1;
            }
        }

        // Manually commit offset
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, 0, Offset::from_raw(last_offset + 1))
            .expect("Failed to add partition offset");

        consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .expect("Failed to commit offset");

        // Create new consumer in same group
        let consumer2: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Failed to create consumer 2");

        consumer2
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        // Should start from committed offset (message 5)
        let mut first_message_index = None;
        if let Some(Ok(message)) = consumer2.poll(Duration::from_secs(2)) {
            if let Some(key) = message.key() {
                let key_str = std::str::from_utf8(key).unwrap();
                if let Some(index) = key_str.strip_prefix("key-") {
                    first_message_index = Some(index.parse::<i32>().unwrap());
                }
            }
        }

        assert_eq!(
            first_message_index,
            Some(5),
            "Consumer should start from offset 5"
        );
    }

    /// Test error scenarios
    #[tokio::test]
    async fn test_error_scenarios() {
        // Start Heraclitus subprocess
        let heraclitus = HeraclitusTestContext::new()
            .await
            .expect("Failed to start Heraclitus");

        let bootstrap_servers = heraclitus.kafka_addr();

        // Test 1: Producer with invalid configuration
        let result = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("message.max.bytes", "invalid_number")
            .create::<BaseProducer>();

        assert!(result.is_err(), "Should fail with invalid configuration");

        // Test 2: Produce to non-existent partition
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .expect("Failed to create producer");

        let record = BaseRecord::to("test-error-topic")
            .partition(999) // Non-existent partition
            .key("key")
            .payload("value");

        let result = producer.send(record);
        assert!(
            result.is_err(),
            "Should fail sending to non-existent partition"
        );

        // Test 3: Consumer with invalid group ID characters (if enforced)
        // Note: This might succeed depending on Heraclitus's validation
        let consumer_result = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", "test-group") // Valid group ID
            .set("auto.offset.reset", "invalid_reset_value")
            .create::<BaseConsumer>();

        assert!(
            consumer_result.is_err(),
            "Should fail with invalid auto.offset.reset"
        );

        // Test 4: Timeout scenarios
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", "timeout-test-group")
            .set("session.timeout.ms", "6000")
            .set("heartbeat.interval.ms", "2000")
            .create()
            .expect("Failed to create consumer");

        consumer
            .subscribe(&["non-existent-topic"])
            .expect("Failed to subscribe");

        // Poll with timeout - should return None, not error
        let result = consumer.poll(Duration::from_millis(100));
        assert!(result.is_none(), "Poll should timeout without error");
    }
}
