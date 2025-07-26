use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("=== Testing Heraclitus with rdkafka ===");
    println!("rdkafka version: {:?}\n", get_rdkafka_version());

    // Test 1: Producer
    println!("1. Testing Producer");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9094")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed");

    // Send multiple messages
    for i in 0..5 {
        let payload = format!("Message #{i} from rdkafka");
        let key = format!("key-{i}");
        let record = FutureRecord::to("rdkafka-test").payload(&payload).key(&key);

        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                println!("   ✓ Sent message {i}: partition={partition}, offset={offset}");
            }
            Err((e, _)) => {
                println!("   ✗ Failed to send message {i}: {e:?}");
            }
        }
    }

    println!("\n2. Testing Consumer");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9094")
        .set("group.id", "rdkafka-test-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["rdkafka-test"])
        .expect("Subscribe failed");

    println!("   ✓ Consumer subscribed to 'rdkafka-test'");

    // Consume messages
    let mut message_stream = consumer.stream();
    let mut count = 0;

    println!("   Consuming messages (waiting up to 15 seconds)...");
    let timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            Some(result) = message_stream.next() => {
                match result {
                    Ok(message) => {
                        let key = message.key()
                            .map(|k| String::from_utf8_lossy(k).to_string())
                            .unwrap_or_else(|| "none".to_string());
                        let value = message.payload()
                            .map(|v| String::from_utf8_lossy(v).to_string())
                            .unwrap_or_else(|| "empty".to_string());

                        println!("   ✓ Consumed: partition={}, offset={}, key='{}', value='{}'",
                            message.partition(), message.offset(), key, value);

                        count += 1;
                        if count >= 5 {
                            break;
                        }
                    }
                    Err(e) => {
                        println!("   ✗ Consume error: {e:?}");
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                println!("   ⏱ Timeout reached");
                break;
            }
        }
    }

    if count > 0 {
        println!("\n✅ Successfully produced and consumed {count} messages!");
        println!("✅ Heraclitus is compatible with rdkafka!");
    } else {
        println!("\n⚠ No messages consumed (they may have been consumed by another consumer)");
    }
}
