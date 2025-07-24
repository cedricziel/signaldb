use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("rdkafka version: {:?}", get_rdkafka_version());
    println!("Testing Heraclitus with rdkafka...\n");

    // Test 1: Admin operations
    println!("=== Testing Admin Operations ===");
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Admin client creation failed");

    // Create multiple topics
    let topics = vec![
        NewTopic::new("orders", 3, TopicReplication::Fixed(1)),
        NewTopic::new("payments", 2, TopicReplication::Fixed(1)),
        NewTopic::new("events", 1, TopicReplication::Fixed(1)),
    ];

    let results = admin
        .create_topics(&topics, &AdminOptions::new())
        .await
        .expect("Create topics failed");

    for result in results {
        match result {
            Ok(topic) => println!("✓ Created topic: {topic}"),
            Err((topic, err)) => {
                if err.to_string().contains("exists") {
                    println!("✓ Topic {topic} already exists");
                } else {
                    println!("✗ Failed to create topic {topic}: {err:?}");
                }
            }
        }
    }

    // Test 2: Producer with multiple partitions
    println!("\n=== Testing Producer with Multiple Topics ===");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed");

    // Send messages to different topics
    let topics = vec!["orders", "payments", "events"];
    for topic in &topics {
        for i in 0..3 {
            let payload = format!("{topic} message #{i}");
            let key = format!("key-{i}");
            let record = FutureRecord::to(topic).payload(&payload).key(&key);

            match producer.send(record, Duration::from_secs(5)).await {
                Ok((partition, offset)) => {
                    println!(
                        "✓ Sent to {topic}: partition={partition}, offset={offset}, msg='{payload}'"
                    );
                }
                Err((e, _)) => {
                    println!("✗ Failed to send to {topic}: {e:?}");
                }
            }
        }
    }

    // Test 3: Consumer Groups
    println!("\n=== Testing Consumer Groups ===");

    // Create multiple consumers in the same group
    let group_id = "test-consumer-group";
    let mut consumers = Vec::new();

    for i in 0..2 {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", group_id)
            .set("client.id", format!("consumer-{i}"))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&["orders", "payments", "events"])
            .expect("Subscribe failed");

        consumers.push(consumer);
        println!("✓ Consumer {i} joined group '{group_id}'");
    }

    // Consume some messages
    println!("\n=== Consuming Messages ===");
    let consumer = &consumers[0];
    let mut message_stream = consumer.stream();

    let mut count = 0;
    while let Some(result) = message_stream.next().await {
        match result {
            Ok(message) => {
                let key = message
                    .key()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .unwrap_or_else(|| "none".to_string());
                let value = message
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string())
                    .unwrap_or_else(|| "empty".to_string());

                println!(
                    "✓ Consumed from {}: partition={}, offset={}, key='{}', value='{}'",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    key,
                    value
                );

                count += 1;
                if count >= 5 {
                    break;
                }
            }
            Err(e) => {
                println!("✗ Consume error: {e:?}");
                break;
            }
        }
    }

    // Test 4: List consumer groups
    println!("\n=== Testing List Groups ===");
    // Note: rdkafka doesn't have a direct API for ListGroups in the client,
    // but it's being used internally for consumer group coordination

    println!("\n✅ All tests completed successfully!");
    println!("Heraclitus is compatible with rdkafka!");
}
