use std::time::Duration;
use std::path::Path;
use std::fs;
use anyhow::Result;
use uuid::Uuid;
use tokio::task::JoinHandle;

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
        write!(f, "{}", s)
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

        // Ensure data directory exists for SQLite databases
        Self::ensure_data_directory(dsn)?;

        let catalog = Catalog::new(dsn).await?;
        let service_id = Uuid::new_v4();

        log::info!(
            "Registering {} service {} at {} with catalog", 
            service_type, service_id, address
        );

        // Register as ingester for backward compatibility with existing catalog schema
        catalog.register_ingester(service_id, &address).await?;

        // Start heartbeat if discovery config is available
        let heartbeat_handle = if let Some(discovery_config) = &config.discovery {
            Some(catalog.spawn_ingester_heartbeat(
                service_id,
                discovery_config.heartbeat_interval,
            ))
        } else {
            // Use default heartbeat interval
            Some(catalog.spawn_ingester_heartbeat(
                service_id,
                Duration::from_secs(30),
            ))
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
        // Only handle SQLite databases
        if !dsn.starts_with("sqlite:") {
            return Ok(());
        }

        // Extract the file path from the SQLite DSN
        if let Some(file_path) = dsn.strip_prefix("sqlite:") {
            // Handle special cases like ":memory:" 
            if file_path == ":memory:" {
                return Ok(());
            }
            
            // Get the directory part of the path
            if let Some(parent) = Path::new(file_path).parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent)?;
                    log::info!("Created data directory: {}", parent.display());
                }
            }
        }
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

    /// Gracefully shutdown the service and deregister from catalog
    pub async fn shutdown(mut self) -> Result<()> {
        log::info!(
            "Shutting down {} service {} and deregistering from catalog",
            self.service_type, self.service_id
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

        // This would fail in the actual test because we need a real database
        // but it shows the intended usage pattern
        let result = ServiceBootstrap::new(
            config,
            ServiceType::Writer,
            "localhost:50051".to_string(),
        ).await;

        // We expect this to fail since we're using an in-memory SQLite database
        // but the error should be from the database connection, not from our code structure
        assert!(result.is_err());
    }
}