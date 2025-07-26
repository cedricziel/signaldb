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
    use testcontainers::{ContainerAsync, runners::AsyncRunner};

    #[tokio::test]
    #[ignore] // Ignore by default since it requires Docker image to be built
    async fn test_heraclitus_container_starts() {
        let heraclitus = Heraclitus::default();

        let container: ContainerAsync<_> = heraclitus
            .start()
            .await
            .expect("Failed to start Heraclitus container");

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
}
