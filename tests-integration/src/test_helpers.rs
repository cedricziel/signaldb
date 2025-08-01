use anyhow::Result;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::testcontainers::{ContainerAsync, runners::AsyncRunner};
use url::Url;

pub struct MinioTestContext {
    pub container: ContainerAsync<MinIO>,
    pub dsn: Url,
}

impl MinioTestContext {
    pub async fn new() -> Result<Self> {
        // Start MinIO container
        let minio = MinIO::default();
        let container = minio.start().await?;

        // Get connection details
        let host_port = container.get_host_port_ipv4(9000).await?;

        // Create DSN without embedded credentials
        // Store the endpoint information separately from the bucket name
        let dsn = Url::parse(&format!("s3://127.0.0.1:{host_port}/signaldb-test"))?;

        // Set MinIO credentials as environment variables for JanKaul
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
            std::env::set_var("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{host_port}"));
            std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        }

        // Create the test bucket
        create_test_bucket(&dsn).await?;

        Ok(Self { container, dsn })
    }
}

async fn create_test_bucket(dsn: &Url) -> Result<()> {
    // Extract connection details from DSN
    let endpoint = format!(
        "http://{}:{}",
        dsn.host_str().unwrap(),
        dsn.port().unwrap_or(9000)
    );
    let bucket = dsn.path().trim_start_matches('/');

    // Create S3 client configuration using environment variables
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .credentials_provider(Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "minio",
        ))
        .region(Region::new("us-east-1"))
        .load()
        .await;

    let client = Client::new(&shared_config);

    // Create bucket
    match client.create_bucket().bucket(bucket).send().await {
        Ok(_) => {
            log::info!("Created test bucket: {bucket}");
            Ok(())
        }
        Err(e) => {
            // If bucket already exists, that's fine
            if e.to_string().contains("BucketAlreadyOwnedByYou") {
                log::info!("Test bucket already exists: {bucket}");
                Ok(())
            } else {
                Err(anyhow::anyhow!("Failed to create bucket: {}", e))
            }
        }
    }
}
