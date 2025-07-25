use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing simple rdkafka operations...");

    // Create client configuration - explicitly use IPv4
    let config = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")  // Use IPv4 explicitly
        .set("client.id", "simple-test")
        .set("api.version.request", "true")
        .set("api.version.request.timeout.ms", "5000")
        .set("socket.timeout.ms", "5000")
        .set("metadata.request.timeout.ms", "5000");

    println!("1. Testing API Versions (via producer creation)...");
    let producer: FutureProducer = config.create()?;
    println!("✓ Producer created successfully - ApiVersions works!");

    println!("2. Testing admin client...");
    let admin: AdminClient<DefaultClientContext> = config.create()?;
    println!("✓ Admin client created successfully!");

    // First test metadata (list topics)
    println!("3. Testing metadata request...");
    let metadata = timeout(Duration::from_secs(10), admin.inner().fetch_metadata(None, Duration::from_secs(5))).await??;
    println!("✓ Metadata request successful! Found {} topics", metadata.topics().len());

    println!("4. Testing CreateTopics...");
    let new_topic = NewTopic::new("simple-test-topic", 1, TopicReplication::Fixed(1));
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    
    match timeout(Duration::from_secs(15), admin.create_topics(&[new_topic], &options)).await {
        Ok(Ok(results)) => {
            for result in results {
                match result {
                    Ok(topic_name) => println!("✓ Created topic: {}", topic_name),
                    Err(e) => println!("✗ Failed to create topic: {:?}", e),
                }
            }
        }
        Ok(Err(e)) => println!("✗ CreateTopics failed: {:?}", e),
        Err(_) => println!("✗ CreateTopics timed out"),
    }

    println!("5. Testing simple produce...");
    let record = FutureRecord::to("simple-test-topic")
        .key("test-key")
        .payload("test-message");
    
    match timeout(Duration::from_secs(10), producer.send(record, Duration::from_secs(5))).await {
        Ok(Ok((partition, offset))) => {
            println!("✓ Message sent successfully to partition {} at offset {}", partition, offset);
        }
        Ok(Err(e)) => println!("✗ Failed to send message: {:?}", e),
        Err(_) => println!("✗ Send timed out"),
    }

    println!("Test completed!");
    Ok(())
}