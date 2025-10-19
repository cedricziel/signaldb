use anyhow::Result;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tempfile::TempDir;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::testcontainers::{ContainerAsync, runners::AsyncRunner};
use tokio::time::sleep;
use url::Url;

/// Captures log output from a process in a background thread
pub struct LogCapture {
    lines: Arc<Mutex<Vec<String>>>,
    _reader_thread: Option<JoinHandle<()>>,
}

impl LogCapture {
    /// Create a new log capture from a reader
    pub fn new<R: Read + Send + 'static>(reader: R, name: &str) -> Self {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let lines_clone = Arc::clone(&lines);
        let name = name.to_string();

        let reader_thread = thread::spawn(move || {
            let buf_reader = BufReader::new(reader);
            for line in buf_reader.lines() {
                match line {
                    Ok(line) => {
                        // Optionally print to stdout if debug env var is set
                        if std::env::var("HERACLITUS_TEST_DEBUG_LOGS").is_ok() {
                            println!("[{}] {}", name, line);
                        }

                        if let Ok(mut lines) = lines_clone.lock() {
                            lines.push(line);
                        }
                    }
                    Err(_) => break, // EOF or error
                }
            }
        });

        Self {
            lines,
            _reader_thread: Some(reader_thread),
        }
    }

    /// Get all captured log lines
    pub fn get_logs(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    /// Get the last n log lines
    pub fn tail(&self, n: usize) -> Vec<String> {
        let logs = self.lines.lock().unwrap();
        let start = logs.len().saturating_sub(n);
        logs[start..].to_vec()
    }

    /// Search logs for lines matching a pattern
    pub fn grep(&self, pattern: &str) -> Vec<String> {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.contains(pattern))
            .cloned()
            .collect()
    }

    /// Get the number of captured lines
    pub fn len(&self) -> usize {
        self.lines.lock().unwrap().len()
    }

    /// Check if no logs have been captured
    pub fn is_empty(&self) -> bool {
        self.lines.lock().unwrap().is_empty()
    }

    /// Clear all captured logs
    pub fn clear(&self) {
        self.lines.lock().unwrap().clear();
    }
}

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
    pub stdout_capture: Arc<LogCapture>,
    pub stderr_capture: Arc<LogCapture>,
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

        // Start Heraclitus process with stdout/stderr captured to pipes
        let mut cmd = Command::new(&heraclitus_path);
        cmd.arg("--config")
            .arg(&config_path)
            .env("RUST_LOG", "heraclitus=info")
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .env("AWS_ENDPOINT_URL", &minio.endpoint)
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        tracing::info!("Starting heraclitus on kafka_port={kafka_port}, http_port={http_port}");
        tracing::debug!(
            "Spawning heraclitus binary at: {}",
            heraclitus_path.display()
        );
        tracing::debug!("Heraclitus logs will be captured in real-time");

        let mut process = cmd
            .spawn()
            .map_err(|e| anyhow::anyhow!("Failed to spawn heraclitus process: {e}"))?;

        // Capture stdout and stderr in background threads
        let stdout = process
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;
        let stderr = process
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stderr"))?;

        let stdout_capture = Arc::new(LogCapture::new(stdout, "stdout"));
        let stderr_capture = Arc::new(LogCapture::new(stderr, "stderr"));

        // Give the process a moment to start or fail
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check if the process is still running
        if let Some(exit_status) = process.try_wait()? {
            // Give log capture threads a moment to finish
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Get captured logs
            let stdout_logs = stdout_capture.get_logs().join("\n");
            let stderr_logs = stderr_capture.get_logs().join("\n");

            // Kill the process to ensure cleanup
            let _ = process.kill();
            return Err(anyhow::anyhow!(
                "Heraclitus process exited immediately with status: {exit_status}\n\nStdout:\n{stdout_logs}\n\nStderr:\n{stderr_logs}"
            ));
        }

        // Wait for server to be ready
        if let Err(e) = wait_for_heraclitus(kafka_port).await {
            // Give log capture threads a moment to finish
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Get captured logs
            let stdout_logs = stdout_capture.get_logs().join("\n");
            let stderr_logs = stderr_capture.get_logs().join("\n");

            // Kill the process if it's still running
            let _ = process.kill();
            return Err(anyhow::anyhow!(
                "Failed to start Heraclitus: {e}\n\nStdout:\n{stdout_logs}\n\nStderr:\n{stderr_logs}"
            ));
        }

        Ok(Self {
            process,
            kafka_port,
            http_port,
            config_dir,
            data_dir,
            stdout_capture,
            stderr_capture,
        })
    }

    pub fn kafka_addr(&self) -> String {
        format!("127.0.0.1:{}", self.kafka_port)
    }

    /// Get all stdout logs
    pub fn stdout_logs(&self) -> Vec<String> {
        self.stdout_capture.get_logs()
    }

    /// Get all stderr logs
    pub fn stderr_logs(&self) -> Vec<String> {
        self.stderr_capture.get_logs()
    }

    /// Get all logs (combined stdout and stderr)
    pub fn all_logs(&self) -> Vec<String> {
        let mut logs = Vec::new();
        logs.extend(self.stdout_logs());
        logs.extend(self.stderr_logs());
        logs
    }

    /// Get the last n lines from stdout
    pub fn tail_stdout(&self, n: usize) -> Vec<String> {
        self.stdout_capture.tail(n)
    }

    /// Get the last n lines from stderr
    pub fn tail_stderr(&self, n: usize) -> Vec<String> {
        self.stderr_capture.tail(n)
    }

    /// Search stdout logs for a pattern
    pub fn grep_stdout(&self, pattern: &str) -> Vec<String> {
        self.stdout_capture.grep(pattern)
    }

    /// Search stderr logs for a pattern
    pub fn grep_stderr(&self, pattern: &str) -> Vec<String> {
        self.stderr_capture.grep(pattern)
    }

    /// Search all logs for a pattern
    pub fn grep_all(&self, pattern: &str) -> Vec<String> {
        let mut results = self.grep_stdout(pattern);
        results.extend(self.grep_stderr(pattern));
        results
    }

    /// Print all logs to stdout (useful for debugging)
    pub fn print_logs(&self) {
        println!("\n=== Heraclitus Stdout ===");
        for line in self.stdout_logs() {
            println!("{}", line);
        }
        println!("\n=== Heraclitus Stderr ===");
        for line in self.stderr_logs() {
            println!("{}", line);
        }
    }

    /// Print the last n lines to stdout
    pub fn print_tail(&self, n: usize) {
        println!("\n=== Last {} lines (Stdout) ===", n);
        for line in self.tail_stdout(n) {
            println!("{}", line);
        }
        println!("\n=== Last {} lines (Stderr) ===", n);
        for line in self.tail_stderr(n) {
            println!("{}", line);
        }
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
