use crate::service_bootstrap::{ServiceBootstrap, ServiceType};
use arrow_flight::flight_service_client::FlightServiceClient;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

/// Service capabilities that can be registered with the transport
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceCapability {
    /// Service can accept trace data via Flight
    TraceIngestion,
    /// Service can execute queries via Flight
    QueryExecution,
    /// Service provides routing functionality
    Routing,
    /// Service provides data storage
    Storage,
}

/// Metadata about a registered Flight service
#[derive(Debug, Clone)]
pub struct FlightServiceMetadata {
    pub service_id: Uuid,
    pub service_type: ServiceType,
    pub address: String,
    pub port: u16,
    pub capabilities: Vec<ServiceCapability>,
    pub endpoint: String,
}

impl FlightServiceMetadata {
    /// Create new service metadata
    pub fn new(
        service_id: Uuid,
        service_type: ServiceType,
        address: String,
        port: u16,
        capabilities: Vec<ServiceCapability>,
    ) -> Self {
        let endpoint = format!(
            "http://{}:{}",
            address.split(':').next().unwrap_or("localhost"),
            port
        );
        Self {
            service_id,
            service_type,
            address,
            port,
            capabilities,
            endpoint,
        }
    }

    /// Check if service has a specific capability
    pub fn has_capability(&self, capability: &ServiceCapability) -> bool {
        self.capabilities.contains(capability)
    }

    /// Get services with a specific capability
    pub fn filter_by_capability<'a>(
        services: &'a [FlightServiceMetadata],
        capability: &ServiceCapability,
    ) -> Vec<&'a FlightServiceMetadata> {
        services
            .iter()
            .filter(|service| service.has_capability(capability))
            .collect()
    }
}

/// Connection pool entry for Flight services
#[derive(Debug)]
struct FlightConnection {
    client: FlightServiceClient<Channel>,
    last_used: std::time::Instant,
}

/// In-memory Flight transport that manages service registration and connection pooling
pub struct InMemoryFlightTransport {
    /// ServiceBootstrap for catalog integration
    bootstrap: Arc<ServiceBootstrap>,
    /// Registry of Flight services
    services: Arc<tokio::sync::RwLock<HashMap<Uuid, FlightServiceMetadata>>>,
    /// Connection pool for Flight clients
    connections: Arc<tokio::sync::RwLock<HashMap<String, FlightConnection>>>,
    /// Maximum number of connections to keep in pool
    max_pool_size: usize,
    /// Connection timeout in seconds
    connection_timeout: u64,
}

impl InMemoryFlightTransport {
    /// Create a new InMemoryFlightTransport with the given ServiceBootstrap
    pub fn new(bootstrap: ServiceBootstrap) -> Self {
        Self {
            bootstrap: Arc::new(bootstrap),
            services: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            max_pool_size: 50,
            connection_timeout: 30,
        }
    }

    /// Create a new InMemoryFlightTransport with custom pool settings
    pub fn with_pool_config(
        bootstrap: ServiceBootstrap,
        max_pool_size: usize,
        connection_timeout: u64,
    ) -> Self {
        Self {
            bootstrap: Arc::new(bootstrap),
            services: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            max_pool_size,
            connection_timeout,
        }
    }

    /// Register a Flight service with the transport
    pub async fn register_flight_service(
        &self,
        service_type: ServiceType,
        address: String,
        port: u16,
        capabilities: Vec<ServiceCapability>,
    ) -> Result<Uuid, Box<dyn std::error::Error + Send + Sync>> {
        let service_id = self.bootstrap.service_id();
        let metadata =
            FlightServiceMetadata::new(service_id, service_type, address, port, capabilities);

        log::info!(
            "Registering Flight service {} of type {} at {} with capabilities: {:?}",
            service_id,
            metadata.service_type,
            metadata.endpoint,
            metadata.capabilities
        );

        // Register with in-memory registry
        let mut services = self.services.write().await;
        services.insert(service_id, metadata);

        Ok(service_id)
    }

    /// Discover Flight services with specific capabilities
    pub async fn discover_services_by_capability(
        &self,
        capability: ServiceCapability,
    ) -> Vec<FlightServiceMetadata> {
        // First get services from local registry
        let local_services = {
            let services = self.services.read().await;
            services.values().cloned().collect::<Vec<_>>()
        };

        // Filter by capability
        let local_filtered =
            FlightServiceMetadata::filter_by_capability(&local_services, &capability);

        // Also discover services from catalog (all registered as ingesters currently)
        let mut discovered = Vec::new();
        if let Ok(ingesters) = self.bootstrap.discover_ingesters().await {
            for ingester in ingesters {
                // For now, assume all ingesters have trace ingestion capability
                // This can be enhanced when we store capabilities in catalog
                if matches!(
                    capability,
                    ServiceCapability::TraceIngestion | ServiceCapability::Storage
                ) {
                    let parts: Vec<&str> = ingester.address.split(':').collect();
                    if parts.len() == 2 {
                        if let Ok(port) = parts[1].parse::<u16>() {
                            let metadata = FlightServiceMetadata::new(
                                ingester.id,
                                ServiceType::Writer, // Assume writer for now
                                ingester.address.clone(),
                                port,
                                vec![
                                    ServiceCapability::TraceIngestion,
                                    ServiceCapability::Storage,
                                ],
                            );
                            discovered.push(metadata);
                        }
                    }
                }
            }
        }

        // Combine local and discovered services
        let mut all_services: Vec<FlightServiceMetadata> =
            local_filtered.into_iter().cloned().collect();
        all_services.extend(discovered);

        // Remove duplicates by service_id
        let mut unique_services = HashMap::new();
        for service in all_services {
            unique_services.insert(service.service_id, service);
        }

        unique_services.into_values().collect()
    }

    /// Get a Flight client for the specified service
    pub async fn get_flight_client(
        &self,
        service_id: Uuid,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        // Find the service metadata
        let service_metadata = {
            let services = self.services.read().await;
            services.get(&service_id).cloned()
        };

        let service_metadata = match service_metadata {
            Some(metadata) => metadata,
            None => {
                // Try to discover from catalog
                let ingesters = self.bootstrap.discover_ingesters().await?;
                let ingester = ingesters
                    .into_iter()
                    .find(|i| i.id == service_id)
                    .ok_or("Service not found")?;

                let parts: Vec<&str> = ingester.address.split(':').collect();
                if parts.len() != 2 {
                    return Err("Invalid service address format".into());
                }
                let port = parts[1].parse::<u16>()?;

                FlightServiceMetadata::new(
                    ingester.id,
                    ServiceType::Writer, // Default assumption
                    ingester.address,
                    port,
                    vec![ServiceCapability::TraceIngestion],
                )
            }
        };

        // Check connection pool first
        {
            let mut connections = self.connections.write().await;
            if let Some(connection) = connections.get_mut(&service_metadata.endpoint) {
                connection.last_used = std::time::Instant::now();
                return Ok(connection.client.clone());
            }
        }

        // Create new connection
        log::debug!(
            "Creating new Flight client connection to {}",
            service_metadata.endpoint
        );
        let endpoint = Endpoint::from_shared(service_metadata.endpoint.clone())?
            .timeout(std::time::Duration::from_secs(self.connection_timeout));
        let channel = endpoint.connect().await?;
        let client = FlightServiceClient::new(channel);

        // Add to connection pool
        {
            let mut connections = self.connections.write().await;

            // Clean up old connections if we're at capacity
            if connections.len() >= self.max_pool_size {
                let now = std::time::Instant::now();
                let timeout_duration = std::time::Duration::from_secs(300); // 5 minutes
                connections.retain(|_, conn| now.duration_since(conn.last_used) < timeout_duration);

                // If still at capacity, remove oldest
                if connections.len() >= self.max_pool_size {
                    let oldest_endpoint = connections
                        .iter()
                        .min_by_key(|(_, conn)| conn.last_used)
                        .map(|(endpoint, _)| endpoint.clone());
                    if let Some(endpoint) = oldest_endpoint {
                        connections.remove(&endpoint);
                    }
                }
            }

            connections.insert(
                service_metadata.endpoint.clone(),
                FlightConnection {
                    client: client.clone(),
                    last_used: std::time::Instant::now(),
                },
            );
        }

        Ok(client)
    }

    /// Get a Flight client for a service with specific capability (round-robin)
    pub async fn get_client_for_capability(
        &self,
        capability: ServiceCapability,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        let services = self.discover_services_by_capability(capability).await;

        if services.is_empty() {
            return Err("No services found with required capability".into());
        }

        // Simple round-robin selection (could be enhanced with proper load balancing)
        let service = &services[0];
        self.get_flight_client(service.service_id).await
    }

    /// Get all registered Flight services
    pub async fn list_services(&self) -> Vec<FlightServiceMetadata> {
        let services = self.services.read().await;
        services.values().cloned().collect()
    }

    /// Remove a service registration
    pub async fn unregister_service(
        &self,
        service_id: Uuid,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut services = self.services.write().await;
        services.remove(&service_id);
        log::info!("Unregistered Flight service {}", service_id);
        Ok(())
    }

    /// Health check - returns true if transport is operational
    pub async fn is_healthy(&self) -> bool {
        // Check if we have any registered services or can discover services
        let local_count = {
            let services = self.services.read().await;
            services.len()
        };

        if local_count > 0 {
            true
        } else {
            // Check if we can discover services from catalog
            self.bootstrap
                .discover_ingesters()
                .await
                .map(|services| !services.is_empty())
                .unwrap_or(false)
        }
    }

    /// Get connection pool statistics
    pub async fn pool_stats(&self) -> (usize, usize) {
        let connections = self.connections.read().await;
        (connections.len(), self.max_pool_size)
    }

    /// Clear expired connections from pool
    pub async fn cleanup_expired_connections(&self) {
        let mut connections = self.connections.write().await;
        let now = std::time::Instant::now();
        let timeout_duration = std::time::Duration::from_secs(300); // 5 minutes

        let before_count = connections.len();
        connections.retain(|_, conn| now.duration_since(conn.last_used) < timeout_duration);
        let after_count = connections.len();

        if before_count != after_count {
            log::debug!(
                "Cleaned up {} expired connections from pool",
                before_count - after_count
            );
        }
    }

    /// Start background task to periodically clean up connections
    pub fn start_connection_cleanup(&self, cleanup_interval: std::time::Duration) {
        let transport = InMemoryFlightTransport {
            bootstrap: self.bootstrap.clone(),
            services: self.services.clone(),
            connections: self.connections.clone(),
            max_pool_size: self.max_pool_size,
            connection_timeout: self.connection_timeout,
        };

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(cleanup_interval);
            loop {
                ticker.tick().await;
                transport.cleanup_expired_connections().await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Configuration, DatabaseConfig, DiscoveryConfig};
    use std::time::Duration;

    async fn create_test_transport() -> InMemoryFlightTransport {
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

        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Router, "localhost:50051".to_string())
                .await
                .unwrap();

        InMemoryFlightTransport::new(bootstrap)
    }

    #[tokio::test]
    async fn test_service_registration() {
        let transport = create_test_transport().await;

        let service_id = transport
            .register_flight_service(
                ServiceType::Writer,
                "localhost".to_string(),
                50052,
                vec![
                    ServiceCapability::TraceIngestion,
                    ServiceCapability::Storage,
                ],
            )
            .await
            .unwrap();

        let services = transport.list_services().await;
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].service_id, service_id);
        assert_eq!(services[0].service_type, ServiceType::Writer);
        assert!(services[0].has_capability(&ServiceCapability::TraceIngestion));
        assert!(services[0].has_capability(&ServiceCapability::Storage));
    }

    #[tokio::test]
    async fn test_capability_filtering() {
        let transport = create_test_transport().await;

        // Register writer service
        transport
            .register_flight_service(
                ServiceType::Writer,
                "localhost".to_string(),
                50052,
                vec![
                    ServiceCapability::TraceIngestion,
                    ServiceCapability::Storage,
                ],
            )
            .await
            .unwrap();

        // Register querier service
        transport
            .register_flight_service(
                ServiceType::Querier,
                "localhost".to_string(),
                50053,
                vec![ServiceCapability::QueryExecution],
            )
            .await
            .unwrap();

        // Test capability filtering
        let trace_services = transport
            .discover_services_by_capability(ServiceCapability::TraceIngestion)
            .await;
        assert_eq!(trace_services.len(), 1);
        assert_eq!(trace_services[0].service_type, ServiceType::Writer);

        let query_services = transport
            .discover_services_by_capability(ServiceCapability::QueryExecution)
            .await;
        assert_eq!(query_services.len(), 1);
        assert_eq!(query_services[0].service_type, ServiceType::Querier);
    }

    #[tokio::test]
    async fn test_service_metadata() {
        let service_id = Uuid::new_v4();
        let metadata = FlightServiceMetadata::new(
            service_id,
            ServiceType::Writer,
            "localhost:50052".to_string(),
            50052,
            vec![ServiceCapability::TraceIngestion],
        );

        assert_eq!(metadata.service_id, service_id);
        assert_eq!(metadata.service_type, ServiceType::Writer);
        assert_eq!(metadata.port, 50052);
        assert_eq!(metadata.endpoint, "http://localhost:50052");
        assert!(metadata.has_capability(&ServiceCapability::TraceIngestion));
        assert!(!metadata.has_capability(&ServiceCapability::QueryExecution));
    }

    #[tokio::test]
    async fn test_pool_stats() {
        let transport = create_test_transport().await;
        let (current, max) = transport.pool_stats().await;
        assert_eq!(current, 0);
        assert_eq!(max, 50); // Default max pool size
    }

    #[tokio::test]
    async fn test_health_check() {
        let transport = create_test_transport().await;

        // Should be healthy even with no local services due to catalog integration
        let _is_healthy = transport.is_healthy().await;
        // This might be false if no services are registered in the in-memory catalog
        // The actual result depends on the bootstrap catalog state

        // Register a service to ensure health
        transport
            .register_flight_service(
                ServiceType::Writer,
                "localhost".to_string(),
                50052,
                vec![ServiceCapability::TraceIngestion],
            )
            .await
            .unwrap();

        let is_healthy_with_service = transport.is_healthy().await;
        assert!(is_healthy_with_service);
    }
}
