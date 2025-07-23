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
    /// Service provides Kafka-compatible ingestion
    KafkaIngestion,
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
        // Extract just the host part for endpoint construction
        let host = address.split(':').next().unwrap_or("localhost");
        let endpoint = format!("http://{host}:{port}");
        Self {
            service_id,
            service_type,
            address, // Store the full address
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

/// Catalog-based Flight transport that manages service discovery and connection pooling
#[derive(Clone)]
pub struct InMemoryFlightTransport {
    /// ServiceBootstrap for catalog integration
    bootstrap: Arc<ServiceBootstrap>,
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
            connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            max_pool_size,
            connection_timeout,
        }
    }

    /// Register a Flight service with the catalog-based transport
    /// Note: Services are now registered automatically through ServiceBootstrap
    /// This method is kept for backward compatibility but now just returns the bootstrap service ID
    pub async fn register_flight_service(
        &self,
        _service_type: ServiceType,
        _address: String,
        _port: u16,
        _capabilities: Vec<ServiceCapability>,
    ) -> Result<Uuid, Box<dyn std::error::Error + Send + Sync>> {
        // Services are now registered automatically via ServiceBootstrap catalog integration
        // Return the current service's ID for compatibility
        let service_id = self.bootstrap.service_id();

        log::info!(
            "Flight service registration requested - using catalog service ID: {service_id}"
        );

        Ok(service_id)
    }

    /// Discover Flight services with specific capabilities using catalog
    pub async fn discover_services_by_capability(
        &self,
        capability: ServiceCapability,
    ) -> Vec<FlightServiceMetadata> {
        // Use catalog-based discovery only
        if let Ok(ingesters) = self
            .bootstrap
            .discover_services_by_capability(capability.clone())
            .await
        {
            let mut services = Vec::new();

            for ingester in ingesters {
                let parts: Vec<&str> = ingester.address.split(':').collect();
                if parts.len() == 2 {
                    if let Ok(port) = parts[1].parse::<u16>() {
                        let metadata = FlightServiceMetadata::new(
                            ingester.id,
                            ingester.service_type.clone(),
                            ingester.address.clone(),
                            port,
                            ingester.capabilities.clone(),
                        );
                        services.push(metadata);
                    }
                }
            }

            log::debug!(
                "Discovered {} services with capability {:?} from catalog",
                services.len(),
                capability
            );

            services
        } else {
            log::warn!("Failed to discover services from catalog");
            Vec::new()
        }
    }

    /// Get a Flight client for the specified service
    pub async fn get_flight_client(
        &self,
        service_id: Uuid,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        // Discover service from catalog
        let ingesters = self.bootstrap.discover_ingesters().await?;
        let ingester = ingesters
            .into_iter()
            .find(|i| i.id == service_id)
            .ok_or("Service not found in catalog")?;

        let parts: Vec<&str> = ingester.address.split(':').collect();
        if parts.len() != 2 {
            return Err("Invalid service address format".into());
        }
        let port = parts[1].parse::<u16>()?;

        let service_metadata = FlightServiceMetadata::new(
            ingester.id,
            ingester.service_type,
            ingester.address,
            port,
            ingester.capabilities,
        );

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

    /// Get all registered Flight services from catalog
    pub async fn list_services(&self) -> Vec<FlightServiceMetadata> {
        if let Ok(ingesters) = self.bootstrap.discover_ingesters().await {
            let mut services = Vec::new();

            for ingester in ingesters {
                let parts: Vec<&str> = ingester.address.split(':').collect();
                if parts.len() == 2 {
                    if let Ok(port) = parts[1].parse::<u16>() {
                        let metadata = FlightServiceMetadata::new(
                            ingester.id,
                            ingester.service_type,
                            ingester.address,
                            port,
                            ingester.capabilities,
                        );
                        services.push(metadata);
                    }
                }
            }

            services
        } else {
            Vec::new()
        }
    }

    /// Remove a service registration from catalog
    pub async fn unregister_service(
        &self,
        service_id: Uuid,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.bootstrap
            .catalog()
            .deregister_ingester(service_id)
            .await?;
        log::info!("Unregistered Flight service {service_id} from catalog");
        Ok(())
    }

    /// Health check - returns true if transport is operational
    pub async fn is_healthy(&self) -> bool {
        // Check if we can discover services from catalog
        self.bootstrap
            .discover_ingesters()
            .await
            .map(|services| !services.is_empty())
            .unwrap_or(false)
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

        // Service registration now uses catalog, so we should find services from catalog
        let services = transport.list_services().await;
        // Should include the bootstrap service itself which was registered with catalog
        assert!(!services.is_empty());
        // The returned service ID should be the bootstrap service ID
        assert_eq!(service_id, transport.bootstrap.service_id());
    }

    #[tokio::test]
    async fn test_capability_filtering() {
        let transport = create_test_transport().await;

        // The transport bootstrap registered a Router service with Routing capability
        // Test capability filtering for Router service
        let routing_services = transport
            .discover_services_by_capability(ServiceCapability::Routing)
            .await;

        // Should find the router service that was registered during bootstrap
        assert!(!routing_services.is_empty());
        assert!(
            routing_services
                .iter()
                .any(|s| s.service_type == ServiceType::Router)
        );

        // Test for capabilities that shouldn't exist
        let trace_services = transport
            .discover_services_by_capability(ServiceCapability::TraceIngestion)
            .await;
        // Router doesn't have trace ingestion capability, so this should be empty
        assert!(trace_services.is_empty());
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

        // Should be healthy because the bootstrap service was registered with catalog
        let is_healthy = transport.is_healthy().await;
        assert!(
            is_healthy,
            "Transport should be healthy with bootstrap service in catalog"
        );
    }
}
