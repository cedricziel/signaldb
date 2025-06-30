use anyhow::Result;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::catalog::{Catalog, Ingester};
use crate::config::Configuration;

/// Service types that can be bootstrapped
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceType {
    Acceptor,
    Writer,
    Router,
    Querier,
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ServiceType::Acceptor => "acceptor",
            ServiceType::Writer => "writer",
            ServiceType::Router => "router",
            ServiceType::Querier => "querier",
        };
        write!(f, "{s}")
    }
}

/// Shared service bootstrap that handles catalog registration and discovery
pub struct ServiceBootstrap {
    catalog: Catalog,
    service_id: Uuid,
    service_type: ServiceType,
    address: String,
    config: Configuration,
    heartbeat_handle: Option<JoinHandle<()>>,
}

impl ServiceBootstrap {
    /// Create a new service bootstrap instance and register with catalog
    pub async fn new(
        config: Configuration,
        service_type: ServiceType,
        address: String,
    ) -> Result<Self> {
        // Use discovery config DSN if available, fallback to database DSN
        let dsn = if let Some(discovery_config) = &config.discovery {
            &discovery_config.dsn
        } else {
            &config.database.dsn
        };

        log::info!("Using DSN for service bootstrap: {dsn}");
        log::info!("Database config DSN: {}", config.database.dsn);
        if let Some(discovery_config) = &config.discovery {
            log::info!("Discovery config DSN: {}", discovery_config.dsn);
        } else {
            log::info!("No discovery config found");
        }

        // Ensure data directory exists for SQLite databases
        Self::ensure_data_directory(dsn)?;

        let catalog = Catalog::new(dsn).await?;
        let service_id = Uuid::new_v4();

        log::info!("Registering {service_type} service {service_id} at {address} with catalog");

        // Register as ingester for backward compatibility with existing catalog schema
        catalog.register_ingester(service_id, &address).await?;

        // Start heartbeat if discovery config is available
        let heartbeat_handle = if let Some(discovery_config) = &config.discovery {
            Some(catalog.spawn_ingester_heartbeat(service_id, discovery_config.heartbeat_interval))
        } else {
            // Use default heartbeat interval
            Some(catalog.spawn_ingester_heartbeat(service_id, Duration::from_secs(30)))
        };

        Ok(ServiceBootstrap {
            catalog,
            service_id,
            service_type,
            address,
            config,
            heartbeat_handle,
        })
    }

    /// Ensure the data directory exists for SQLite databases
    fn ensure_data_directory(dsn: &str) -> Result<()> {
        log::info!("Ensuring data directory exists for DSN: {dsn}");

        // Only handle SQLite databases
        if !dsn.starts_with("sqlite:") {
            log::info!("Not a SQLite DSN, skipping directory creation");
            return Ok(());
        }

        // Extract the file path from the SQLite DSN
        // Handle different SQLite DSN formats:
        // - sqlite::memory: (in-memory)
        // - sqlite:relative/path/file.db (relative path)
        // - sqlite://path/file.db (relative path with //)
        // - sqlite:///absolute/path/file.db (absolute path with ///)
        let file_path = if let Some(path) = dsn.strip_prefix("sqlite:") {
            if path == ":memory:" {
                log::info!("In-memory database, skipping directory creation");
                return Ok(());
            }

            // Handle different slash patterns
            if path.starts_with("///") {
                // Absolute path: sqlite:///absolute/path/file.db -> /absolute/path/file.db
                &path[2..] // Remove only 2 slashes to keep the leading /
            } else if let Some(stripped) = path.strip_prefix("//") {
                // Relative path with //: sqlite://relative/path/file.db -> relative/path/file.db
                stripped // Remove both slashes
            } else if path.starts_with('/') {
                // Single slash: sqlite:/path/file.db -> /path/file.db
                path // Keep as is
            } else {
                // No slashes: sqlite:relative/path/file.db -> relative/path/file.db
                path // Keep as is
            }
        } else {
            log::info!("Failed to extract file path from DSN: {dsn}");
            return Ok(());
        };

        log::info!("Extracted file path: {file_path}");

        // Get the directory part of the path
        if let Some(parent) = Path::new(file_path).parent() {
            log::info!(
                "Parent directory: {}, exists: {}",
                parent.display(),
                parent.exists()
            );
            if !parent.exists() {
                log::info!("Creating directory: {}", parent.display());
                fs::create_dir_all(parent).map_err(|e| {
                    log::error!("Failed to create directory '{}': {}", parent.display(), e);
                    e
                })?;
                log::info!("Created data directory: {}", parent.display());
            } else {
                log::info!("Directory already exists: {}", parent.display());
            }
        } else {
            log::info!("No parent directory found for path: {file_path}");
        }

        log::info!("Directory setup completed successfully");
        Ok(())
    }

    /// Get the service ID
    pub fn service_id(&self) -> Uuid {
        self.service_id
    }

    /// Get the service type
    pub fn service_type(&self) -> &ServiceType {
        &self.service_type
    }

    /// Get the service address
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get access to the catalog for service-specific operations
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Get access to the configuration
    pub fn config(&self) -> &Configuration {
        &self.config
    }

    /// Discover other services of the specified type
    pub async fn discover_ingesters(&self) -> Result<Vec<Ingester>> {
        let ingesters = self.catalog.list_ingesters().await?;
        Ok(ingesters)
    }

    /// Discover all registered services (returns ingesters for now, extensible for other types)
    pub async fn discover_services(&self, _service_type: ServiceType) -> Result<Vec<Ingester>> {
        // For now, return ingesters since that's what the catalog supports
        // This can be extended when we add a services table to the catalog
        self.discover_ingesters().await
    }

    /// Manually send a heartbeat (useful for testing or manual health checks)
    pub async fn heartbeat(&self) -> Result<()> {
        self.catalog.heartbeat(self.service_id).await?;
        Ok(())
    }

    /// Create and register a Flight transport integrated with this bootstrap
    pub fn create_flight_transport(self) -> crate::flight::transport::InMemoryFlightTransport {
        crate::flight::transport::InMemoryFlightTransport::new(self)
    }

    /// Create and register a Flight transport with custom pool configuration
    pub fn create_flight_transport_with_config(
        self,
        max_pool_size: usize,
        connection_timeout: u64,
    ) -> crate::flight::transport::InMemoryFlightTransport {
        crate::flight::transport::InMemoryFlightTransport::with_pool_config(
            self,
            max_pool_size,
            connection_timeout,
        )
    }

    /// Helper method to derive Flight capabilities from service type
    pub fn default_flight_capabilities(&self) -> Vec<crate::flight::transport::ServiceCapability> {
        use crate::flight::transport::ServiceCapability;

        match self.service_type {
            ServiceType::Acceptor => vec![ServiceCapability::TraceIngestion],
            ServiceType::Writer => vec![
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
            ServiceType::Router => vec![ServiceCapability::Routing],
            ServiceType::Querier => vec![ServiceCapability::QueryExecution],
        }
    }

    /// Get Flight endpoint from service address
    pub fn flight_endpoint(&self) -> String {
        // If address contains a port, use it directly, otherwise assume it's a Flight endpoint
        if self.address.contains(':') {
            format!("http://{}", self.address)
        } else {
            // Default Flight port if no port specified
            format!("http://{}:50051", self.address)
        }
    }

    /// Extract port from service address
    pub fn extract_port(&self) -> Result<u16> {
        let parts: Vec<&str> = self.address.split(':').collect();
        if parts.len() == 2 {
            parts[1]
                .parse::<u16>()
                .map_err(|e| anyhow::anyhow!("Invalid port in address '{}': {}", self.address, e))
        } else {
            // Default to Flight port if no port specified
            Ok(50051)
        }
    }

    /// Extract hostname from service address
    pub fn extract_hostname(&self) -> String {
        let parts: Vec<&str> = self.address.split(':').collect();
        parts[0].to_string()
    }

    /// Gracefully shutdown the service and deregister from catalog
    pub async fn shutdown(mut self) -> Result<()> {
        log::info!(
            "Shutting down {} service {} and deregistering from catalog",
            self.service_type,
            self.service_id
        );

        // Stop heartbeat task
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }

        // Deregister from catalog
        self.catalog.deregister_ingester(self.service_id).await?;

        log::info!("Service {} deregistered successfully", self.service_id);
        Ok(())
    }
}

impl Drop for ServiceBootstrap {
    fn drop(&mut self) {
        // Stop heartbeat task on drop
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DatabaseConfig, DiscoveryConfig};
    use std::time::Duration;

    #[test]
    fn test_ensure_data_directory() {
        // Test with in-memory SQLite (should be no-op)
        let result = ServiceBootstrap::ensure_data_directory("sqlite::memory:");
        assert!(result.is_ok());

        // Test with PostgreSQL DSN (should be no-op)
        let result = ServiceBootstrap::ensure_data_directory("postgresql://user:pass@localhost/db");
        assert!(result.is_ok());

        // Test with file-based SQLite DSN
        let test_dir = ".test_data/test_subdir";
        let test_dsn = format!("sqlite:{}/test.db", test_dir);

        // Clean up any existing test directory
        let _ = std::fs::remove_dir_all(".test_data");

        // Test directory creation
        let result = ServiceBootstrap::ensure_data_directory(&test_dsn);
        assert!(result.is_ok());

        // Verify directory was created
        assert!(Path::new(test_dir).exists());

        // Clean up
        let _ = std::fs::remove_dir_all(".test_data");
    }

    #[test]
    fn test_ensure_data_directory_temp_locations() {
        use std::env;

        // Test with system temp directory (should always work)
        let temp_dir = env::temp_dir();
        let test_subdir = temp_dir.join("signaldb_test_temp");
        let test_dsn = format!("sqlite:{}/test.db", test_subdir.display());

        // Clean up any existing test directory
        let _ = std::fs::remove_dir_all(&test_subdir);

        // Test directory creation in temp
        let result = ServiceBootstrap::ensure_data_directory(&test_dsn);
        assert!(
            result.is_ok(),
            "Should be able to create directory in system temp"
        );

        // Verify directory was created
        assert!(test_subdir.exists());

        // Clean up
        let _ = std::fs::remove_dir_all(&test_subdir);
    }

    #[test]
    fn test_ensure_data_directory_edge_cases() {
        // Test with root path (no parent directory)
        let result = ServiceBootstrap::ensure_data_directory("sqlite:test.db");
        assert!(result.is_ok(), "Should handle files in current directory");

        // Test with empty string after sqlite: prefix
        let result = ServiceBootstrap::ensure_data_directory("sqlite:");
        assert!(result.is_ok(), "Should handle empty path gracefully");

        // Test with relative paths
        let result = ServiceBootstrap::ensure_data_directory("sqlite:./relative/path/test.db");
        assert!(result.is_ok(), "Should handle relative paths");

        // Clean up any created directories
        let _ = std::fs::remove_dir_all("./relative");
    }

    #[test]
    #[cfg(unix)] // Unix-specific permission test
    fn test_ensure_data_directory_permissions() {
        use std::env;
        use std::os::unix::fs::PermissionsExt;

        // Create a test directory with known permissions
        let temp_dir = env::temp_dir();
        let test_parent = temp_dir.join("signaldb_perm_test");
        let test_subdir = test_parent.join("subdir");
        let test_dsn = format!("sqlite:{}/test.db", test_subdir.display());

        // Clean up any existing test directory
        let _ = std::fs::remove_dir_all(&test_parent);

        // Create parent with write permissions
        std::fs::create_dir_all(&test_parent).unwrap();
        let mut perms = std::fs::metadata(&test_parent).unwrap().permissions();
        perms.set_mode(0o755); // rwxr-xr-x
        std::fs::set_permissions(&test_parent, perms).unwrap();

        // Test directory creation
        let result = ServiceBootstrap::ensure_data_directory(&test_dsn);
        assert!(
            result.is_ok(),
            "Should be able to create subdirectory with proper permissions"
        );

        // Verify subdirectory was created
        assert!(test_subdir.exists());

        // Clean up
        let _ = std::fs::remove_dir_all(&test_parent);
    }

    #[test]
    fn test_ensure_data_directory_ci_simulation() {
        use std::env;

        // Simulate GitHub Actions runner.temp scenario
        let temp_base = env::temp_dir();
        let runner_temp = temp_base.join("github_runner_temp");
        let signaldb_data = runner_temp.join("signaldb-data");

        // Clean up any existing directories
        let _ = std::fs::remove_dir_all(&runner_temp);

        // Create the runner temp directory (simulating GitHub Actions)
        std::fs::create_dir_all(&runner_temp).unwrap();

        // Test the DSN pattern used in CI
        let test_dsn = format!("sqlite:{}/test_deployment.db", signaldb_data.display());

        // This should create the signaldb-data directory
        let result = ServiceBootstrap::ensure_data_directory(&test_dsn);
        assert!(
            result.is_ok(),
            "Should handle GitHub Actions temp directory pattern"
        );

        // Verify the directory was created
        assert!(
            signaldb_data.exists(),
            "signaldb-data directory should be created"
        );
        assert!(
            signaldb_data.is_dir(),
            "signaldb-data should be a directory"
        );

        // Test that we can actually write to it
        let test_file = signaldb_data.join("test_write.txt");
        std::fs::write(&test_file, "test content")
            .expect("Should be able to write to created directory");

        // Verify the file was written
        assert!(test_file.exists());
        let content = std::fs::read_to_string(&test_file).unwrap();
        assert_eq!(content, "test content");

        // Clean up
        let _ = std::fs::remove_dir_all(&runner_temp);
    }

    #[test]
    fn test_bootstrap_filesystem_operations() {
        use std::env;

        // Test the filesystem operations that bootstrap depends on
        let temp_dir = env::temp_dir().join("signaldb_bootstrap_fs_test");
        let _ = std::fs::remove_dir_all(&temp_dir);

        // Test various DSN patterns that might be used in CI or production
        let temp_dsn = format!("sqlite:{}/test.db", temp_dir.display());
        let test_cases = vec![
            ("sqlite::memory:", true),            // In-memory should always work
            (temp_dsn.as_str(), true),            // Temp dir should work
            ("sqlite:./test_bootstrap.db", true), // Current dir should work
        ];

        for (dsn, should_succeed) in test_cases {
            let result = ServiceBootstrap::ensure_data_directory(&dsn);
            if should_succeed {
                assert!(
                    result.is_ok(),
                    "DSN '{}' should work: {:?}",
                    dsn,
                    result.err()
                );
            } else {
                assert!(result.is_err(), "DSN '{}' should fail", dsn);
            }
        }

        // Test that we can write to the created directory
        if temp_dir.exists() {
            let test_file = temp_dir.join("write_test.txt");
            std::fs::write(&test_file, "test")
                .expect("Should be able to write to created directory");
            assert!(test_file.exists());
        }

        // Clean up
        let _ = std::fs::remove_dir_all(&temp_dir);
        let _ = std::fs::remove_file("./test_bootstrap.db");
    }

    #[tokio::test]
    async fn test_service_bootstrap_creation() {
        let config = Configuration {
            database: DatabaseConfig {
                dsn: "sqlite::memory:".to_string(),
            },
            discovery: Some(DiscoveryConfig {
                dsn: "sqlite::memory:".to_string(),
                heartbeat_interval: Duration::from_secs(5),
                poll_interval: Duration::from_secs(10),
                ttl: Duration::from_secs(60),
            }),
            ..Default::default()
        };

        // Create a service bootstrap with in-memory SQLite
        let result =
            ServiceBootstrap::new(config, ServiceType::Writer, "localhost:50051".to_string()).await;

        // The operation should succeed with in-memory SQLite
        assert!(result.is_ok());

        // Verify the service was created with correct properties
        let bootstrap = result.unwrap();
        assert_eq!(bootstrap.service_type, ServiceType::Writer);
        assert_eq!(bootstrap.address, "localhost:50051");
    }
}
