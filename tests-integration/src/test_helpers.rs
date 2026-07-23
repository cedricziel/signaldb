use anyhow::Result;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::testcontainers::core::ExecCommand;
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

        // Set MinIO credentials as environment variables for iceberg-rust
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
            std::env::set_var("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{host_port}"));
            std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        }

        // Create the test bucket
        create_test_bucket(&container, "signaldb-test").await?;

        Ok(Self { container, dsn })
    }
}

/// Creates a bucket using the `mc` client that ships inside the official
/// minio/minio image — avoids pulling the AWS SDK (and its legacy hyper 0.14
/// stack) into the dependency graph just to issue one CreateBucket call.
async fn create_test_bucket(container: &ContainerAsync<MinIO>, bucket: &str) -> Result<()> {
    let cmd = format!(
        "MC_HOST_local=http://minioadmin:minioadmin@127.0.0.1:9000 \
         mc mb --ignore-existing local/{bucket}"
    );
    let mut result = container.exec(ExecCommand::new(["sh", "-c", &cmd])).await?;

    let exit_code = result.exit_code().await?;
    if exit_code != Some(0) {
        let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await?).into_owned();
        anyhow::bail!("Failed to create bucket {bucket} (exit {exit_code:?}): {stderr}");
    }

    log::info!("Created test bucket: {bucket}");
    Ok(())
}
