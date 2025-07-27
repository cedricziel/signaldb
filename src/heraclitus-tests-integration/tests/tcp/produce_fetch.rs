use anyhow::Result;
use aws_config::Region;
use aws_credential_types::Credentials;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::collections::HashMap;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_end_to_end_workflow() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-e2e-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");

    // Test 1: Multiple producers writing to same topic
    let topic = "e2e-test-topic";
    let num_producers = 3;
    let messages_per_producer = 10;

    let mut producer_handles = Vec::new();

    for producer_id in 0..num_producers {
        let servers = bootstrap_servers.clone();
        let topic = topic.to_string();

        let handle = tokio::spawn(async move {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &servers)
                .set("message.timeout.ms", "5000")
                .create()?;

            for msg_id in 0..messages_per_producer {
                let key = format!("producer-{producer_id}-msg-{msg_id}");
                let value = format!("value from producer {producer_id} message {msg_id}");

                producer
                    .send(
                        FutureRecord::to(&topic)
                            .key(&key)
                            .payload(&value)
                            .headers(create_test_headers(producer_id, msg_id)),
                        Timeout::After(Duration::from_secs(5)),
                    )
                    .await
                    .map_err(|(e, _)| anyhow::anyhow!("Failed to produce: {:?}", e))?;
            }

            Ok::<(), anyhow::Error>(())
        });

        producer_handles.push(handle);
    }

    // Wait for all producers to finish
    for handle in producer_handles {
        handle.await??;
    }

    println!("All producers finished sending messages");

    // Give time for messages to be persisted
    sleep(Duration::from_secs(1)).await;

    // Test 2: Consume all messages and verify
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "e2e-consumer-group")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[topic])?;

    let mut consumed_messages = HashMap::new();
    let expected_total = num_producers * messages_per_producer;
    let start_time = tokio::time::Instant::now();

    while consumed_messages.len() < expected_total && start_time.elapsed() < Duration::from_secs(30)
    {
        match consumer.recv().await {
            Ok(msg) => {
                if let (Some(key), Some(payload)) = (msg.key(), msg.payload()) {
                    let key_str = std::str::from_utf8(key)?;
                    let value_str = std::str::from_utf8(payload)?;

                    // Verify headers
                    let headers = msg.headers();
                    if let Some(headers) = headers {
                        for i in 0..headers.count() {
                            let header = headers.get(i);
                            println!("Header: {} = {:?}", header.key, header.value);
                        }
                    }

                    consumed_messages.insert(key_str.to_string(), value_str.to_string());
                    println!(
                        "Consumed: {} = {} (offset: {})",
                        key_str,
                        value_str,
                        msg.offset()
                    );
                }
            }
            Err(e) => {
                eprintln!("Consumer error: {e}");
            }
        }
    }

    assert_eq!(
        consumed_messages.len(),
        expected_total,
        "Expected {} messages but got {}",
        expected_total,
        consumed_messages.len()
    );

    // Test 3: Verify data in MinIO
    let s3_client = create_s3_client(&minio).await?;
    let objects = list_all_objects(&s3_client, &minio.bucket).await?;

    println!("Found {} objects in MinIO:", objects.len());
    for obj in &objects {
        println!("  - {obj}");
    }

    assert!(!objects.is_empty(), "Expected Parquet files in MinIO");

    // Verify Parquet files exist for our topic
    let topic_files: Vec<_> = objects
        .iter()
        .filter(|path| path.contains(&format!("topic={topic}")))
        .collect();

    assert!(
        !topic_files.is_empty(),
        "Expected Parquet files for topic {topic}"
    );

    // Test 4: Consumer group offset tracking (basic test)
    // Create a new consumer in the same group
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "e2e-consumer-group")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer2.subscribe(&[topic])?;

    // Should not receive any messages since the first consumer already consumed them
    // (once offset commit is implemented)
    let mut new_messages = 0;
    let start_time = tokio::time::Instant::now();

    while start_time.elapsed() < Duration::from_secs(2) {
        match consumer2.recv().await {
            Ok(_) => {
                new_messages += 1;
            }
            Err(_) => {
                // Expected - no new messages
            }
        }
    }

    // Currently will receive all messages again since offset commit isn't implemented
    // Once implemented, this should be 0
    println!("New consumer received {new_messages} messages");

    Ok(())
}

fn create_test_headers(producer_id: usize, msg_id: usize) -> rdkafka::message::OwnedHeaders {
    use rdkafka::message::OwnedHeaders;

    OwnedHeaders::new()
        .insert(rdkafka::message::Header {
            key: "producer_id",
            value: Some(producer_id.to_string().as_bytes()),
        })
        .insert(rdkafka::message::Header {
            key: "message_id",
            value: Some(msg_id.to_string().as_bytes()),
        })
        .insert(rdkafka::message::Header {
            key: "timestamp",
            value: Some(chrono::Utc::now().to_rfc3339().as_bytes()),
        })
}

async fn create_s3_client(minio: &MinioTestContext) -> Result<aws_sdk_s3::Client> {
    let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(&minio.endpoint)
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .load()
        .await;

    Ok(aws_sdk_s3::Client::new(&config))
}

async fn list_all_objects(client: &aws_sdk_s3::Client, bucket: &str) -> Result<Vec<String>> {
    let mut objects = Vec::new();
    let mut continuation_token = None;

    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("heraclitus-test/");

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request.send().await?;

        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(key) = object.key {
                    objects.push(key);
                }
            }
        }

        if response.is_truncated == Some(true) {
            continuation_token = response.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(objects)
}
