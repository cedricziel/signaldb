use anyhow::Result;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::Duration;

#[tokio::test]
async fn test_produce_and_fetch_with_minio() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-test").await?;

    // Find available port and start Heraclitus with MinIO storage
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    let bootstrap_servers = format!("127.0.0.1:{kafka_port}");

    // Create producer
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create producer");

    // Produce test message
    let test_key = "test-key";
    let test_value = "test-value-stored-in-minio";
    let topic = "minio-test-topic";

    let record = BaseRecord::to(topic).key(test_key).payload(test_value);

    producer
        .send(record)
        .map_err(|(e, _)| anyhow::anyhow!("Failed to queue message: {:?}", e))?;
    producer.flush(Duration::from_secs(5))?;

    // Give time for data to be written to MinIO
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "minio-test-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create consumer");

    consumer.subscribe(&[topic])?;

    // Wait for assignment
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Poll for the message
    let mut found_message = false;
    for _ in 0..10 {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(message)) => {
                let key = message.key().and_then(|k| std::str::from_utf8(k).ok());
                let payload = message.payload().and_then(|p| std::str::from_utf8(p).ok());

                if key == Some(test_key) && payload == Some(test_value) {
                    found_message = true;
                    break;
                }
            }
            Some(Err(e)) => {
                return Err(anyhow::anyhow!("Consumer error: {:?}", e));
            }
            None => {
                // No message yet, continue polling
            }
        }
    }

    assert!(
        found_message,
        "Failed to consume the message that was produced"
    );

    // Verify data is actually stored in MinIO by listing objects
    let objects = list_minio_objects(&minio).await?;
    assert!(!objects.is_empty(), "No objects found in MinIO bucket");

    println!(
        "MinIO integration test passed! Found {} objects in bucket",
        objects.len()
    );

    Ok(())
}

// Helper function to list objects in MinIO bucket
async fn list_minio_objects(minio: &MinioTestContext) -> Result<Vec<String>> {
    use aws_config::{BehaviorVersion, Region};
    use aws_credential_types::Credentials;
    use aws_sdk_s3::{Client, config::Builder};

    // Create AWS config for MinIO
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "static");

    let config = Builder::new()
        .endpoint_url(&minio.endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .force_path_style(true)
        .behavior_version(BehaviorVersion::latest())
        .build();

    let client = Client::from_conf(config);
    let mut objects = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(&minio.bucket)
            .max_keys(1000);

        if let Some(token) = continuation_token.as_ref() {
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
