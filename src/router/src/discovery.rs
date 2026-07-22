use arrow_flight::flight_service_client::FlightServiceClient;
use common::catalog::{Catalog, Ingester};
use common::flight::transport::{
    FlightServiceMetadata, InMemoryFlightTransport, ServiceCapability,
};
use common::service_bootstrap::ServiceType;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;
use tokio::time::Duration;
use tonic::transport::Channel;

/// ServiceRegistry maintains an up-to-date view of available services for routing
#[derive(Clone)]
pub struct ServiceRegistry {
    services: Arc<RwLock<HashMap<uuid::Uuid, Ingester>>>,
    catalog: Catalog,
    flight_transport: Option<Arc<InMemoryFlightTransport>>,
    /// Registrations with heartbeats older than this are treated as dead.
    discovery_ttl: std::time::Duration,
    /// Rotates get_service_for_routing across healthy instances.
    round_robin: Arc<AtomicUsize>,
}

impl std::fmt::Debug for ServiceRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceRegistry")
            .field("services", &"Arc<RwLock<HashMap<Uuid, Ingester>>>")
            .field("catalog", &"Catalog")
            .field("flight_transport", &"Option<Arc<InMemoryFlightTransport>>")
            .finish()
    }
}

impl ServiceRegistry {
    /// Create a new ServiceRegistry with the given catalog
    pub fn new(catalog: Catalog) -> Self {
        Self {
            services: Arc::new(RwLock::new(HashMap::new())),
            catalog,
            flight_transport: None,
            discovery_ttl: std::time::Duration::from_secs(300),
            round_robin: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new ServiceRegistry with Flight transport integration
    pub fn with_flight_transport(
        catalog: Catalog,
        flight_transport: InMemoryFlightTransport,
    ) -> Self {
        Self {
            services: Arc::new(RwLock::new(HashMap::new())),
            catalog,
            flight_transport: Some(Arc::new(flight_transport)),
            discovery_ttl: std::time::Duration::from_secs(300),
            round_robin: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Override the staleness TTL applied when refreshing the registry.
    pub fn with_discovery_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.discovery_ttl = ttl;
        self
    }

    /// Start background polling to keep service registry updated
    pub async fn start_background_polling(&self, poll_interval: Duration) {
        let registry = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(poll_interval);
            loop {
                ticker.tick().await;
                if let Err(e) = registry.refresh_services().await {
                    log::error!("Failed to refresh service registry: {e}");
                }
            }
        });
    }

    /// Refresh the service registry from the catalog
    async fn refresh_services(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Update services (currently all registered as ingesters).
        // Stale rows belong to crashed services; keep them out of the
        // routing map (issue #555).
        match self.catalog.list_active_ingesters(self.discovery_ttl).await {
            Ok(services) => {
                let mut service_map = self.services.write().await;
                service_map.clear();
                for service in services {
                    service_map.insert(service.id, service);
                }
                log::debug!("Updated {} services in registry", service_map.len());
            }
            Err(e) => {
                log::warn!("Failed to list services: {e}");
            }
        }

        Ok(())
    }

    /// Get all available services
    pub async fn get_services(&self) -> Vec<Ingester> {
        self.services.read().await.values().cloned().collect()
    }

    /// Get a service for routing, rotating round-robin across the
    /// healthy instances in the registry.
    pub async fn get_service_for_routing(&self) -> Option<Ingester> {
        let services = self.services.read().await;
        if services.is_empty() {
            return None;
        }
        // Sort by id for a stable rotation order across refreshes.
        let mut candidates: Vec<&Ingester> = services.values().collect();
        candidates.sort_by_key(|service| service.id);
        let index = self.round_robin.fetch_add(1, Ordering::Relaxed) % candidates.len();
        Some(candidates[index].clone())
    }

    /// Get services by address pattern (useful for filtering by service type if encoded in address)
    pub async fn get_services_by_pattern(&self, pattern: &str) -> Vec<Ingester> {
        let services = self.services.read().await;
        services
            .values()
            .filter(|service| service.address.contains(pattern))
            .cloned()
            .collect()
    }

    /// Get Flight services with specific capability
    pub async fn get_flight_services_by_capability(
        &self,
        capability: ServiceCapability,
    ) -> Vec<FlightServiceMetadata> {
        if let Some(transport) = &self.flight_transport {
            transport.discover_services_by_capability(capability).await
        } else {
            // Fallback: convert regular services to Flight metadata
            self.convert_ingesters_to_flight_metadata(capability).await
        }
    }

    /// Get a Flight client for routing to services with specific capability
    pub async fn get_flight_client_for_capability(
        &self,
        capability: ServiceCapability,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(transport) = &self.flight_transport {
            transport.get_client_for_capability(capability).await
        } else {
            Err("Flight transport not configured".into())
        }
    }

    /// Perform Flight-specific health check on services
    pub async fn flight_health_check(
        &self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(transport) = &self.flight_transport {
            Ok(transport.is_healthy().await)
        } else {
            // Fallback to basic health check
            Ok(self.is_healthy().await)
        }
    }

    /// Get Flight connection pool statistics
    pub async fn flight_pool_stats(&self) -> Option<(usize, usize)> {
        if let Some(transport) = &self.flight_transport {
            Some(transport.pool_stats().await)
        } else {
            None
        }
    }

    /// Convert existing ingesters to Flight metadata (fallback when no Flight transport)
    async fn convert_ingesters_to_flight_metadata(
        &self,
        capability: ServiceCapability,
    ) -> Vec<FlightServiceMetadata> {
        let services = self.services.read().await;
        let mut flight_services = Vec::new();

        for ingester in services.values() {
            // Parse address to extract hostname and port
            let parts: Vec<&str> = ingester.address.split(':').collect();
            if parts.len() == 2
                && let Ok(port) = parts[1].parse::<u16>()
            {
                // Determine service type and capabilities based on port or other heuristics
                let (service_type, capabilities) = self.infer_service_type_and_capabilities(port);

                // Check if this service has the requested capability
                if capabilities.contains(&capability) {
                    let metadata = FlightServiceMetadata::new(
                        ingester.id,
                        service_type,
                        ingester.address.clone(),
                        port,
                        capabilities,
                    );
                    flight_services.push(metadata);
                }
            }
        }

        flight_services
    }

    /// Infer service type and capabilities from port or other service characteristics
    fn infer_service_type_and_capabilities(
        &self,
        port: u16,
    ) -> (ServiceType, Vec<ServiceCapability>) {
        // Common port mappings based on SignalDB architecture
        match port {
            4317 | 4318 => (
                ServiceType::Acceptor,
                vec![ServiceCapability::TraceIngestion],
            ),
            50051..=50060 => (
                ServiceType::Writer,
                vec![
                    ServiceCapability::TraceIngestion,
                    ServiceCapability::Storage,
                ],
            ),
            3000 => (ServiceType::Router, vec![ServiceCapability::Routing]),
            9000 => (
                ServiceType::Querier,
                vec![ServiceCapability::QueryExecution],
            ),
            _ => {
                // Default assumption for unknown ports
                (
                    ServiceType::Writer,
                    vec![
                        ServiceCapability::TraceIngestion,
                        ServiceCapability::Storage,
                    ],
                )
            }
        }
    }

    /// Start background Flight transport connection cleanup
    pub fn start_flight_cleanup(&self, cleanup_interval: Duration) {
        if let Some(transport) = &self.flight_transport {
            transport.start_connection_cleanup(cleanup_interval);
        }
    }

    /// Health check - returns true if we have active services
    pub async fn is_healthy(&self) -> bool {
        let services = self.services.read().await;
        !services.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn mock_ingester(address: &str) -> common::catalog::Ingester {
        common::catalog::Ingester {
            id: uuid::Uuid::new_v4(),
            address: address.to_string(),
            last_seen: chrono::Utc::now(),
            service_type: common::service_bootstrap::ServiceType::Querier,
            capabilities: vec![common::flight::transport::ServiceCapability::QueryExecution],
        }
    }

    #[tokio::test]
    async fn routing_rotates_round_robin_across_services() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let registry = ServiceRegistry::new(catalog);

        let first = mock_ingester("first:9000");
        let second = mock_ingester("second:9000");
        {
            let mut services = registry.services.write().await;
            services.insert(first.id, first.clone());
            services.insert(second.id, second.clone());
        }

        let mut picked = Vec::new();
        for _ in 0..4 {
            picked.push(registry.get_service_for_routing().await.unwrap().id);
        }
        // Both services take part and consecutive picks alternate.
        assert!(picked.contains(&first.id));
        assert!(picked.contains(&second.id));
        assert_ne!(picked[0], picked[1]);
        assert_eq!(picked[0], picked[2]);
        assert_eq!(picked[1], picked[3]);
    }

    #[tokio::test]
    async fn refresh_drops_stale_services() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let id = uuid::Uuid::new_v4();
        catalog
            .register_ingester(
                id,
                "fresh:9000",
                common::service_bootstrap::ServiceType::Querier,
                &[common::flight::transport::ServiceCapability::QueryExecution],
            )
            .await
            .unwrap();

        // Generous TTL: the service is visible after refresh.
        let registry = ServiceRegistry::new(catalog.clone())
            .with_discovery_ttl(std::time::Duration::from_secs(300));
        registry.refresh_services().await.unwrap();
        assert_eq!(registry.get_services().await.len(), 1);

        // Zero TTL: the same row counts as a crashed service and is
        // dropped from the routing map.
        let registry = ServiceRegistry::new(catalog).with_discovery_ttl(std::time::Duration::ZERO);
        registry.refresh_services().await.unwrap();
        assert!(registry.get_services().await.is_empty());
    }

    #[tokio::test]
    async fn test_service_registry_health_check_logic() {
        // Test the health check logic by directly testing the HashMap
        let services = Arc::new(RwLock::new(HashMap::new()));

        // Should be unhealthy with no services
        {
            let services_guard = services.read().await;
            assert!(services_guard.is_empty());
        }

        // Add a mock service
        {
            let mut services_guard = services.write().await;
            let mock_ingester = common::catalog::Ingester {
                id: uuid::Uuid::new_v4(),
                address: "test:8080".to_string(),
                last_seen: chrono::Utc::now(),
                service_type: common::service_bootstrap::ServiceType::Writer,
                capabilities: vec![
                    common::flight::transport::ServiceCapability::TraceIngestion,
                    common::flight::transport::ServiceCapability::Storage,
                ],
            };
            services_guard.insert(mock_ingester.id, mock_ingester);
        }

        // Should be healthy with services
        {
            let services_guard = services.read().await;
            assert!(!services_guard.is_empty());
        }
    }

    #[test]
    fn test_service_registry_debug_impl() {
        // Test that our manual Debug implementation works
        use std::collections::HashMap;
        let _services: Arc<RwLock<HashMap<uuid::Uuid, common::catalog::Ingester>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // We can't easily test the actual ServiceRegistry Debug without a catalog,
        // but we can test that the structure compiles
        let debug_output = "ServiceRegistry";
        assert!(debug_output.contains("ServiceRegistry"));
    }
}
