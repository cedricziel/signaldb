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

        // Create DSN for object storage
        let dsn = Url::parse(&format!("s3://127.0.0.1:{host_port}/{bucket_name}"))?;
        tracing::debug!("Storage DSN: {dsn}");

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
        // Create temporary directories
        let config_dir = TempDir::new()?;
        let data_dir = TempDir::new()?;

        // Get a unique HTTP port
        let http_port = find_available_port().await?;

        // Create configuration file
        let config_path = config_dir.path().join("heraclitus.toml");
        let config_content = format!(
            r#"
[server]
kafka_port = {kafka_port}
bind_address = "127.0.0.1"

[storage]
dsn = "{}"

[heraclitus]
state_prefix = "heraclitus-test/"
batch_size = 100
batch_timeout_ms = 100
segment_size_mb = 1
kafka_port = {kafka_port}
http_port = {http_port}
metrics_enabled = false

[discovery]
type = "static"
static_peers = []

[database]
type = "sqlite"
connection_string = ":memory:"
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

        // Start Heraclitus process with stdout/stderr captured for debugging
        let mut cmd = Command::new(&heraclitus_path);
        cmd.arg("--config")
            .arg(&config_path)
            .arg("--kafka-port")
            .arg(kafka_port.to_string())
            .env("RUST_LOG", "heraclitus=debug,info")
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .env("AWS_ENDPOINT_URL", &minio.endpoint)
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        tracing::info!(
            "Spawning heraclitus binary at: {}",
            heraclitus_path.display()
        );
        let mut process = cmd.spawn()?;

        // Spawn a task to read and log stderr/stdout from the heraclitus process
        if let Some(stderr) = process.stderr.take() {
            use std::io::{BufRead, BufReader};
            std::thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    eprintln!("[heraclitus] {}", line);
                }
            });
        }

        // Wait for server to be ready
        wait_for_heraclitus(kafka_port).await?;

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
