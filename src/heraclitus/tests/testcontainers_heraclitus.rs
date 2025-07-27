use std::borrow::Cow;
use std::collections::HashMap;
use testcontainers::{
    Image,
    core::{ContainerPort, WaitFor},
};

const DEFAULT_KAFKA_PORT: u16 = 9092;
const DEFAULT_HTTP_PORT: u16 = 9093;

/// Heraclitus testcontainer image
#[derive(Debug, Clone)]
pub struct Heraclitus {
    env_vars: HashMap<String, String>,
    tag: String,
}

impl Default for Heraclitus {
    fn default() -> Self {
        Self {
            env_vars: HashMap::new(),
            tag: "latest".to_string(),
        }
    }
}

impl Heraclitus {
    /// Create a new Heraclitus container with the specified tag
    pub fn with_tag(tag: impl Into<String>) -> Self {
        Self {
            tag: tag.into(),
            ..Default::default()
        }
    }

    /// Set an environment variable
    pub fn with_env_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env_vars.insert(key.into(), value.into());
        self
    }

    /// Use file-based storage instead of in-memory
    pub fn with_file_storage(self, path: impl Into<String>) -> Self {
        self.with_env_var("HERACLITUS_STORAGE_PATH", path.into())
    }

    /// Get the Kafka protocol port
    pub fn kafka_port() -> u16 {
        DEFAULT_KAFKA_PORT
    }

    /// Get the HTTP metrics port
    pub fn http_port() -> u16 {
        DEFAULT_HTTP_PORT
    }

    /// Check if the Heraclitus Docker image exists, and build it if needed
    pub async fn ensure_image_built() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::process::Command;

        // First check if the image already exists
        let check_output = Command::new("docker")
            .args(["images", "-q", "heraclitus:latest"])
            .output()?;

        if check_output.status.success() && !check_output.stdout.is_empty() {
            println!("heraclitus:latest image already exists, skipping build");
            return Ok(());
        }

        // Get the workspace root (go up from heraclitus directory)
        let current_dir = std::env::current_dir()?;
        let workspace_root = if current_dir.ends_with("heraclitus") {
            // We're in the heraclitus directory
            current_dir
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf()
        } else {
            // We're probably in the workspace root already
            current_dir
        };

        println!(
            "Building Heraclitus image from workspace root: {}",
            workspace_root.display()
        );

        // Use docker build command directly (allow cache for faster builds)
        let output = Command::new("docker")
            .args([
                "build",
                "-f",
                "src/heraclitus/Dockerfile",
                "-t",
                "heraclitus:latest",
                ".",
            ])
            .current_dir(&workspace_root)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Docker build failed: {stderr}").into());
        }

        println!("Successfully built heraclitus:latest image");
        Ok(())
    }

    /// Create a builder that automatically ensures the image is built
    pub async fn build_and_start(
        self,
    ) -> Result<testcontainers::ContainerAsync<Self>, Box<dyn std::error::Error + Send + Sync>>
    {
        // Ensure the image is built first
        Self::ensure_image_built().await?;

        // Now start the container using the regular Image trait
        use testcontainers::runners::AsyncRunner;
        Ok(self.start().await?)
    }
}

impl Image for Heraclitus {
    fn name(&self) -> &str {
        "heraclitus"
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![
            WaitFor::message_on_stdout("Kafka server listening on port"),
            WaitFor::message_on_stdout("HTTP server on"),
        ]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        self.env_vars.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        static PORTS: &[ContainerPort] = &[
            ContainerPort::Tcp(DEFAULT_KAFKA_PORT),
            ContainerPort::Tcp(DEFAULT_HTTP_PORT),
        ];
        PORTS
    }
}

#[cfg(test)]
mod heraclitus_container_tests {
    use super::*;

    #[tokio::test]
    async fn test_heraclitus_container_starts() {
        let heraclitus = Heraclitus::default();

        let container = heraclitus
            .build_and_start()
            .await
            .expect("Failed to build and start Heraclitus container");

        // Get mapped ports
        let kafka_port = container
            .get_host_port_ipv4(DEFAULT_KAFKA_PORT)
            .await
            .expect("Failed to get Kafka port");
        let http_port = container
            .get_host_port_ipv4(DEFAULT_HTTP_PORT)
            .await
            .expect("Failed to get HTTP port");

        println!("Heraclitus Kafka port mapped to: {kafka_port}");
        println!("Heraclitus HTTP port mapped to: {http_port}");

        // Container should be running
        assert!(kafka_port > 0);
        assert!(http_port > 0);
    }

    #[tokio::test]
    async fn test_heraclitus_image_building() {
        // Test that we can build the image successfully
        Heraclitus::ensure_image_built()
            .await
            .expect("Failed to build Heraclitus image");

        println!("✓ Heraclitus image built successfully");
    }

    #[tokio::test]
    async fn test_heraclitus_container_starts_quickly() {
        // Quick test that just verifies the container can start
        let heraclitus = Heraclitus::default();

        let container = heraclitus
            .build_and_start()
            .await
            .expect("Failed to build and start Heraclitus container");

        // Get mapped ports to verify container is running
        let kafka_port = container
            .get_host_port_ipv4(DEFAULT_KAFKA_PORT)
            .await
            .expect("Failed to get Kafka port");

        println!("✓ Heraclitus container started on port {kafka_port}");
        assert!(kafka_port > 0);

        // Container will be automatically cleaned up when dropped
    }
}
