use anyhow::Result;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use tempfile::TempDir;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::testcontainers::{ContainerAsync, runners::AsyncRunner};
use tokio::time::sleep;
use url::Url;

/// Test context for MinIO container
pub struct MinioTestContext {
    pub container: ContainerAsync<MinIO>,
    pub dsn: Url,
    pub endpoint: String,
    pub bucket: String,
}

impl MinioTestContext {
    pub async fn new(bucket_name: &str) -> Result<Self> {
        tracing::info!("Starting MinIO container for bucket '{bucket_name}'");

        // Start MinIO container
        let minio = MinIO::default();
        tracing::debug!("MinIO image configured, starting container...");

        let container = minio.start().await?;
        tracing::info!("✓ MinIO container started successfully");

        // Get connection details
        let host_port = container.get_host_port_ipv4(9000).await?;
        let endpoint = format!("http://127.0.0.1:{host_port}");
        tracing::info!("MinIO accessible at: {endpoint}");

        // Create DSN for object storage - use proper S3 URL format with bucket name as host
        // The actual endpoint URL will be passed via AWS_ENDPOINT_URL environment variable
        let dsn = Url::parse(&format!("s3://{bucket_name}/"))?;
        tracing::debug!("Storage DSN: {dsn} (endpoint will be set via AWS_ENDPOINT_URL)");

        // Create the test bucket
        tracing::info!("Creating test bucket '{bucket_name}'...");
        create_test_bucket(&endpoint, bucket_name).await?;

        tracing::info!("✓ MinIO test context ready with bucket '{bucket_name}'");

        Ok(Self {
            container,
            dsn,
            endpoint,
            bucket: bucket_name.to_string(),
        })
    }

    pub fn storage_dsn(&self) -> String {
        self.dsn.to_string()
    }
}

async fn create_test_bucket(endpoint: &str, bucket: &str) -> Result<()> {
    tracing::info!("Creating S3 client for endpoint: {endpoint}, bucket: {bucket}");

    // Create S3 client
    let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .load()
        .await;

    let client = Client::new(&config);
    tracing::info!("S3 client created, attempting to create bucket '{bucket}'");

    // Wait for MinIO to be ready with retry logic
    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 30;

    loop {
        tracing::debug!(
            "Attempt {}/{MAX_ATTEMPTS} to create bucket '{bucket}'",
            attempts + 1
        );

        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => {
                tracing::info!(
                    "✓ Successfully created bucket '{bucket}' after {attempts} attempts"
                );
                return Ok(());
            }
            Err(e) => {
                attempts += 1;
                tracing::warn!("Attempt {}/{MAX_ATTEMPTS} failed: {e:?}", attempts);

                if attempts >= MAX_ATTEMPTS {
                    tracing::error!(
                        "✗ Failed to create bucket '{bucket}' after {MAX_ATTEMPTS} attempts"
                    );
                    return Err(anyhow::anyhow!(
                        "Failed to create bucket '{bucket}' after {MAX_ATTEMPTS} attempts: {e:?}"
                    ));
                }

                sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Test context for Heraclitus server
pub struct HeraclitusTestContext {
    pub process: Child,
    pub kafka_port: u16,
    pub http_port: u16,
    pub config_dir: TempDir,
    pub data_dir: TempDir,
}

impl HeraclitusTestContext {
    pub async fn new(minio: &MinioTestContext, kafka_port: u16) -> Result<Self> {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY_MS: u64 = 500;

        let mut last_error = None;

        for attempt in 1..=MAX_RETRIES {
            match Self::try_new(minio, kafka_port, attempt).await {
                Ok(context) => return Ok(context),
                Err(e) => {
                    tracing::warn!(
                        "Failed to start Heraclitus (attempt {}/{}): {}",
                        attempt,
                        MAX_RETRIES,
                        e
                    );
                    last_error = Some(e);

                    if attempt < MAX_RETRIES {
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("Failed to start Heraclitus after {} attempts", MAX_RETRIES)
        }))
    }

    async fn try_new(minio: &MinioTestContext, kafka_port: u16, attempt: u32) -> Result<Self> {
        tracing::info!("Attempting to start Heraclitus (attempt {})", attempt);

        // Create temporary directories
        let config_dir = TempDir::new()?;
        let data_dir = TempDir::new()?;

        // Get a unique HTTP port
        let http_port = find_available_port().await?;

        // Create configuration file
        let config_path = config_dir.path().join("heraclitus.toml");
        let config_content = format!(
            r#"
kafka_port = {kafka_port}
http_port = {http_port}

[storage]
path = "{}"

[state]
prefix = "heraclitus-test/"

[batching]
batch_size = 100
flush_interval_ms = 100

[metrics]
enabled = false
"#,
            minio.storage_dsn()
        );

        std::fs::write(&config_path, config_content)?;

        // Build the heraclitus binary if not already built
        let cargo_output = Command::new("cargo")
            .args(["build", "-p", "heraclitus", "--bin", "heraclitus"])
            .output()?;

        if !cargo_output.status.success() {
            return Err(anyhow::anyhow!(
                "Failed to build heraclitus: {}",
                String::from_utf8_lossy(&cargo_output.stderr)
            ));
        }

        // Find the heraclitus binary
        let mut heraclitus_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        heraclitus_path.pop(); // Remove heraclitus-tests-integration
        heraclitus_path.pop(); // Remove src
        heraclitus_path.push("target");
        heraclitus_path.push("debug");
        heraclitus_path.push("heraclitus");

        // Create log file for heraclitus output
        let log_path = config_dir.path().join("heraclitus.log");
        let log_file = std::fs::File::create(&log_path)?;

        // Start Heraclitus process with stdout/stderr captured to log file
        let mut cmd = Command::new(&heraclitus_path);
        cmd.arg("--config")
            .arg(&config_path)
            .env("RUST_LOG", "heraclitus=info")
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .env("AWS_ENDPOINT_URL", &minio.endpoint)
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .stdout(log_file.try_clone()?)
            .stderr(log_file);

        tracing::info!("Starting heraclitus on kafka_port={kafka_port}, http_port={http_port}");
        tracing::debug!(
            "Spawning heraclitus binary at: {}",
            heraclitus_path.display()
        );
        tracing::debug!("Heraclitus logs will be written to: {}", log_path.display());

        let mut process = cmd
            .spawn()
            .map_err(|e| anyhow::anyhow!("Failed to spawn heraclitus process: {e}"))?;

        // Give the process a moment to start or fail
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check if the process is still running
        if let Some(exit_status) = process.try_wait()? {
            // Read the log file to see what went wrong
            let log_content = std::fs::read_to_string(&log_path).unwrap_or_default();
            // Kill the process to ensure cleanup
            let _ = process.kill();
            return Err(anyhow::anyhow!(
                "Heraclitus process exited immediately with status: {exit_status}\nLogs:\n{log_content}"
            ));
        }

        // Wait for server to be ready
        if let Err(e) = wait_for_heraclitus(kafka_port).await {
            // If the server didn't start, try to read the log file
            let log_content = std::fs::read_to_string(&log_path).unwrap_or_default();
            // Kill the process if it's still running
            let _ = process.kill();
            return Err(anyhow::anyhow!(
                "Failed to start Heraclitus: {e}\nLogs:\n{log_content}"
            ));
        }

        Ok(Self {
            process,
            kafka_port,
            http_port,
            config_dir,
            data_dir,
        })
    }

    pub fn kafka_addr(&self) -> String {
        format!("127.0.0.1:{}", self.kafka_port)
    }
}

impl Drop for HeraclitusTestContext {
    fn drop(&mut self) {
        // Kill the process on drop
        let _ = self.process.kill();
    }
}

/// Wait for Heraclitus to be ready to accept connections
async fn wait_for_heraclitus(port: u16) -> Result<()> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 30;

    loop {
        match tokio::net::TcpStream::connect(&addr).await {
            Ok(_) => {
                tracing::info!("Heraclitus is ready on port {}", port);
                return Ok(());
            }
            Err(e) => {
                attempts += 1;
                if attempts >= MAX_ATTEMPTS {
                    return Err(anyhow::anyhow!(
                        "Failed to connect to Heraclitus after {} attempts: {}",
                        MAX_ATTEMPTS,
                        e
                    ));
                }
                tracing::debug!(
                    "Waiting for Heraclitus to start (attempt {}/{})",
                    attempts,
                    MAX_ATTEMPTS
                );
                sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Find an available port for testing
pub async fn find_available_port() -> Result<u16> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Initialize tracing for tests
pub fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("heraclitus=debug,heraclitus_tests_integration=debug,info")
        .with_test_writer()
        .try_init();

    tracing::info!("✓ Test tracing initialized");
}
