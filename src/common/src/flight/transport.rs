use crate::service_bootstrap::{ServiceBootstrap, ServiceType};
use arrow_flight::flight_service_client::FlightServiceClient;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

/// Service capabilities that can be registered with the transport
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    /// Service provides storage maintenance (compaction, cleanup)
    StorageMaintenance,
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

/// Cached result of a capability discovery query against the catalog.
///
/// Uses `tokio::time::Instant` so tests can drive expiry deterministically
/// with `tokio::time::pause`/`advance`.
#[derive(Debug, Clone)]
struct DiscoveryCacheEntry {
    discovered_at: tokio::time::Instant,
    services: Vec<FlightServiceMetadata>,
}

/// Time allowed to *dial* a peer before giving up. Distinct from the
/// per-request deadline: a slow query is not a slow connection.
pub const DEFAULT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Headroom added on top of the querier's `query_timeout` when deriving the
/// per-request deadline. The server-side timeout must always win the race so
/// callers receive its `DeadlineExceeded` (→ 504) rather than a client-side
/// `Cancelled` that carries no diagnostic detail.
pub const REQUEST_TIMEOUT_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// Catalog-based Flight transport that manages service discovery and connection pooling
#[derive(Clone)]
pub struct InMemoryFlightTransport {
    /// ServiceBootstrap for catalog integration
    bootstrap: Arc<ServiceBootstrap>,
    /// Connection pool for Flight clients
    connections: Arc<tokio::sync::RwLock<HashMap<String, FlightConnection>>>,
    /// Maximum number of connections to keep in pool
    max_pool_size: usize,
    /// Time allowed to establish a channel to a peer.
    connect_timeout: std::time::Duration,
    /// Deadline applied to every request issued on a channel. Kept above the
    /// querier's own `query_timeout` so the server, not the client, decides
    /// when a query has run too long.
    request_timeout: std::time::Duration,
    /// Rotates capability-based selection across healthy instances.
    round_robin: Arc<std::sync::atomic::AtomicUsize>,
    /// Memoized capability-discovery results, valid for `discovery_cache_ttl`.
    /// Entries are dropped early when connecting to a discovered address
    /// fails, so a stale address cannot wedge clients until expiry.
    discovery_cache: Arc<tokio::sync::RwLock<HashMap<ServiceCapability, DiscoveryCacheEntry>>>,
    /// How long a discovery result may be served from cache. Mirrors the
    /// discovery `poll_interval`: the catalog is not expected to change
    /// faster than services poll it.
    discovery_cache_ttl: std::time::Duration,
}

impl InMemoryFlightTransport {
    /// Create a new InMemoryFlightTransport with the given ServiceBootstrap
    pub fn new(bootstrap: ServiceBootstrap) -> Self {
        let request_timeout = Self::default_request_timeout(bootstrap.config());
        let discovery_cache_ttl = Self::default_discovery_cache_ttl(bootstrap.config());
        Self {
            bootstrap: Arc::new(bootstrap),
            connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            max_pool_size: 50,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout,
            round_robin: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            discovery_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            discovery_cache_ttl,
        }
    }

    /// Create a new InMemoryFlightTransport with custom pool settings
    pub fn with_pool_config(
        bootstrap: ServiceBootstrap,
        max_pool_size: usize,
        connect_timeout: std::time::Duration,
    ) -> Self {
        let request_timeout = Self::default_request_timeout(bootstrap.config());
        let discovery_cache_ttl = Self::default_discovery_cache_ttl(bootstrap.config());
        Self {
            bootstrap: Arc::new(bootstrap),
            connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            max_pool_size,
            connect_timeout,
            request_timeout,
            round_robin: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            discovery_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            discovery_cache_ttl,
        }
    }

    /// Derive the per-request deadline from the configured querier
    /// `query_timeout` plus [`REQUEST_TIMEOUT_GRACE`], so raising the query
    /// budget automatically raises the client's patience with it.
    fn default_request_timeout(config: &crate::config::Configuration) -> std::time::Duration {
        config
            .querier
            .query_timeout
            .saturating_add(REQUEST_TIMEOUT_GRACE)
    }

    /// Discovery results are memoized for the configured discovery
    /// `poll_interval` — the same cadence at which the rest of the system
    /// expects catalog contents to change.
    fn default_discovery_cache_ttl(config: &crate::config::Configuration) -> std::time::Duration {
        config
            .discovery
            .as_ref()
            .map(|d| d.poll_interval)
            .unwrap_or_else(|| std::time::Duration::from_secs(60))
    }

    /// The configured internal service key for authenticating outbound
    /// Flight calls to other SignalDB services, if any.
    pub fn internal_service_key(&self) -> Option<&str> {
        self.bootstrap.config().auth.internal_service_key.as_deref()
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

    /// Discover Flight services with specific capabilities.
    ///
    /// Results are memoized for the discovery `poll_interval` so hot paths
    /// (one call per OTLP export / query) do not pay a catalog round-trip
    /// each time. Empty results are never cached: a service registering a
    /// moment later must become visible immediately, not after the TTL.
    #[tracing::instrument(level = "debug", skip_all, fields(capability = ?capability))]
    pub async fn discover_services_by_capability(
        &self,
        capability: ServiceCapability,
    ) -> Vec<FlightServiceMetadata> {
        {
            let cache = self.discovery_cache.read().await;
            if let Some(entry) = cache.get(&capability)
                && entry.discovered_at.elapsed() < self.discovery_cache_ttl
            {
                return entry.services.clone();
            }
        }

        let services = self
            .discover_services_by_capability_uncached(&capability)
            .await;

        if !services.is_empty() {
            let mut cache = self.discovery_cache.write().await;
            cache.insert(
                capability,
                DiscoveryCacheEntry {
                    discovered_at: tokio::time::Instant::now(),
                    services: services.clone(),
                },
            );
        }

        services
    }

    /// Drop the cached discovery result for a capability so the next lookup
    /// re-consults the catalog.
    async fn invalidate_discovery_cache(&self, capability: &ServiceCapability) {
        self.discovery_cache.write().await.remove(capability);
    }

    /// Catalog-backed capability discovery, bypassing the memoization cache.
    async fn discover_services_by_capability_uncached(
        &self,
        capability: &ServiceCapability,
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
                if parts.len() == 2
                    && let Ok(port) = parts[1].parse::<u16>()
                {
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
    #[tracing::instrument(level = "debug", skip_all, fields(service_id = %service_id))]
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

        self.client_for_endpoint(&service_metadata.endpoint).await
    }

    /// Get a Flight client for an already-discovered service.
    ///
    /// Goes straight to the connection pool (or dials the metadata's
    /// endpoint) without re-querying the catalog — the caller already holds
    /// the resolved address.
    #[tracing::instrument(level = "debug", skip_all, fields(service_id = %service.service_id))]
    pub async fn get_flight_client_for(
        &self,
        service: &FlightServiceMetadata,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        self.client_for_endpoint(&service.endpoint).await
    }

    /// Pool-or-connect for a Flight endpoint URL.
    async fn client_for_endpoint(
        &self,
        endpoint_url: &str,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        // Check connection pool first
        {
            let mut connections = self.connections.write().await;
            if let Some(connection) = connections.get_mut(endpoint_url) {
                connection.last_used = std::time::Instant::now();
                return Ok(connection.client.clone());
            }
        }

        // Create new connection
        log::debug!("Creating new Flight client connection to {endpoint_url}");
        // `connect_timeout` bounds dialing only; `timeout` is tonic's
        // per-request deadline and must outlast the querier's query_timeout
        // so a slow-but-progressing query is not aborted by the client.
        let endpoint = Endpoint::from_shared(endpoint_url.to_string())?
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout);
        let channel = endpoint.connect().await?;
        // Internal service-to-service hop: compress requests with zstd and
        // advertise zstd for responses. Every in-tree Flight server accepts
        // zstd (see `crate::flight::flight_service_server`); mixed-version
        // deployments must upgrade servers before clients.
        //
        // Also raise tonic's 4 MiB receive default on both directions: one
        // OTLP export becomes one FlightData message, and Arrow encoding
        // inflates OTLP, so the default wedges oversized batches in the WAL
        // retry loop (#944). Every Flight server applies the same limit.
        let client = FlightServiceClient::new(channel)
            .send_compressed(tonic::codec::CompressionEncoding::Zstd)
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
            .max_decoding_message_size(crate::flight::MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(crate::flight::MAX_GRPC_MESSAGE_SIZE);

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
                endpoint_url.to_string(),
                FlightConnection {
                    client: client.clone(),
                    last_used: std::time::Instant::now(),
                },
            );
        }

        Ok(client)
    }

    /// Round-robin across healthy instances; sort by id for a stable
    /// rotation order regardless of catalog listing order.
    fn select_round_robin(
        &self,
        mut services: Vec<FlightServiceMetadata>,
    ) -> FlightServiceMetadata {
        services.sort_by_key(|s| s.service_id);
        let index = self
            .round_robin
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % services.len();
        services.swap_remove(index)
    }

    /// Get a Flight client for a service with specific capability (round-robin)
    ///
    /// Uses the memoized discovery result and connects via the discovered
    /// metadata directly, so the happy path costs at most one catalog query
    /// (zero within the cache TTL). A connect failure invalidates the cached
    /// discovery for this capability and retries once against a fresh
    /// catalog lookup before failing, so a stale address cannot wedge
    /// clients until the TTL expires.
    #[tracing::instrument(level = "debug", skip_all, fields(capability = ?capability))]
    pub async fn get_client_for_capability(
        &self,
        capability: ServiceCapability,
    ) -> Result<FlightServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        let services = self
            .discover_services_by_capability(capability.clone())
            .await;

        if services.is_empty() {
            return Err("No services found with required capability".into());
        }

        let service = self.select_round_robin(services);
        let first_err = match self.get_flight_client_for(&service).await {
            Ok(client) => return Ok(client),
            Err(e) => e,
        };

        // The address may have come from a stale cache entry: drop it,
        // re-discover once, and retry before giving up.
        log::warn!(
            "Failed to connect to {} for capability {capability:?}, re-discovering: {first_err}",
            service.endpoint
        );
        self.invalidate_discovery_cache(&capability).await;

        let services = self
            .discover_services_by_capability(capability.clone())
            .await;
        if services.is_empty() {
            return Err(first_err);
        }

        let service = self.select_round_robin(services);
        match self.get_flight_client_for(&service).await {
            Ok(client) => Ok(client),
            Err(e) => {
                // Do not keep a discovery result we could not connect to.
                self.invalidate_discovery_cache(&capability).await;
                Err(e)
            }
        }
    }

    /// Get all registered Flight services from catalog
    pub async fn list_services(&self) -> Vec<FlightServiceMetadata> {
        if let Ok(ingesters) = self.bootstrap.discover_ingesters().await {
            let mut services = Vec::new();

            for ingester in ingesters {
                let parts: Vec<&str> = ingester.address.split(':').collect();
                if parts.len() == 2
                    && let Ok(port) = parts[1].parse::<u16>()
                {
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
        let transport = self.clone();

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

    fn test_configuration() -> Configuration {
        Configuration {
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
        }
    }

    async fn create_test_transport() -> InMemoryFlightTransport {
        let config = test_configuration();

        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Router, "localhost:50051".to_string())
                .await
                .unwrap();

        InMemoryFlightTransport::new(bootstrap)
    }

    /// The per-request deadline the transport applies to outbound Flight
    /// calls must outlast the querier's own `query_timeout`. Otherwise the
    /// client aborts first with `Cancelled` and the querier's honest
    /// `DeadlineExceeded` (which routers map to 504) can never be observed.
    #[tokio::test]
    async fn request_timeout_outlasts_querier_query_timeout() {
        let transport = create_test_transport().await;
        let query_timeout = transport.bootstrap.config().querier.query_timeout;

        assert!(
            transport.request_timeout > query_timeout,
            "request timeout {:?} must exceed querier query_timeout {query_timeout:?}",
            transport.request_timeout,
        );
    }

    /// Dialing a peer and waiting on a long-running query are different
    /// concerns: a connect timeout must not bound query wall-clock time.
    #[tokio::test]
    async fn connect_timeout_is_independent_of_request_timeout() {
        let transport = create_test_transport().await;

        assert_eq!(transport.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        assert!(transport.request_timeout > transport.connect_timeout);
    }

    /// `with_pool_config` is where a caller can override the connect timeout,
    /// so it must keep that value without letting it bound query wall-clock
    /// time — the request deadline still comes from `query_timeout`.
    #[tokio::test]
    async fn with_pool_config_keeps_connect_timeout_and_derives_request_timeout() {
        let config = test_configuration();
        let query_timeout = config.querier.query_timeout;
        let bootstrap =
            ServiceBootstrap::new(config, ServiceType::Router, "localhost:50051".to_string())
                .await
                .unwrap();

        let connect_timeout = Duration::from_secs(3);
        let transport = InMemoryFlightTransport::with_pool_config(bootstrap, 2, connect_timeout);

        assert_eq!(transport.connect_timeout, connect_timeout);
        assert_eq!(
            transport.request_timeout,
            query_timeout.saturating_add(REQUEST_TIMEOUT_GRACE)
        );
        assert!(transport.request_timeout > connect_timeout);
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

    /// Register a fake storage ingester directly in the catalog, bypassing
    /// ServiceBootstrap, so tests can add/remove peers at will.
    async fn register_fake_storage_service(
        transport: &InMemoryFlightTransport,
        address: &str,
    ) -> Uuid {
        let id = Uuid::new_v4();
        transport
            .bootstrap
            .catalog()
            .register_ingester(
                id,
                address,
                ServiceType::Writer,
                &[ServiceCapability::Storage],
            )
            .await
            .unwrap();
        id
    }

    /// Within the discovery poll interval, repeated capability lookups must
    /// be served from the cache instead of re-querying the catalog. Removing
    /// the service from the catalog proves the second lookup never hit it.
    #[tokio::test]
    async fn capability_discovery_is_served_from_cache_within_ttl() {
        let transport = create_test_transport().await;
        let id = register_fake_storage_service(&transport, "localhost:50061").await;

        let first = transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;
        assert_eq!(first.len(), 1, "catalog-backed discovery must find service");

        // Remove from catalog: a cached lookup still sees it, a catalog
        // round-trip would not.
        transport
            .bootstrap
            .catalog()
            .deregister_ingester(id)
            .await
            .unwrap();

        let second = transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;
        assert_eq!(
            second.len(),
            1,
            "lookup inside the TTL must be served from cache, not the catalog"
        );
        assert_eq!(second[0].service_id, id);
    }

    /// Once the discovery poll interval has elapsed, the cache entry is stale
    /// and the next lookup must go back to the catalog.
    #[tokio::test]
    async fn capability_discovery_cache_expires_after_poll_interval() {
        let transport = create_test_transport().await;
        let id = register_fake_storage_service(&transport, "localhost:50061").await;

        let first = transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;
        assert_eq!(first.len(), 1);

        transport
            .bootstrap
            .catalog()
            .deregister_ingester(id)
            .await
            .unwrap();

        // Deterministically advance past the 10s poll_interval configured in
        // test_configuration() without sleeping.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::time::resume();

        let second = transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;
        assert!(
            second.is_empty(),
            "expired cache entry must trigger a fresh catalog lookup"
        );
    }

    /// Empty discovery results must not be cached: a service coming up
    /// moments later has to become visible immediately, not after the TTL.
    #[tokio::test]
    async fn empty_discovery_results_are_not_cached() {
        let transport = create_test_transport().await;

        let before = transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;
        assert!(before.is_empty());

        register_fake_storage_service(&transport, "localhost:50061").await;

        let after = transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;
        assert_eq!(
            after.len(),
            1,
            "an empty result must not shadow a newly registered service"
        );
    }

    /// A connect failure must drop the cached discovery entry so a stale
    /// address cannot wedge clients until the TTL expires. After the failed
    /// call, a lookup goes back to the catalog and observes the removal.
    #[tokio::test]
    async fn connect_failure_invalidates_discovery_cache() {
        let transport = create_test_transport().await;
        // Port 1 is unassigned; dialing it fails fast with connection refused.
        let id = register_fake_storage_service(&transport, "127.0.0.1:1").await;

        let discovered = transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;
        assert_eq!(discovered.len(), 1, "cache must be populated first");

        let result = transport
            .get_client_for_capability(ServiceCapability::Storage)
            .await;
        assert!(result.is_err(), "connecting to a dead address must fail");

        transport
            .bootstrap
            .catalog()
            .deregister_ingester(id)
            .await
            .unwrap();

        let after_failure = transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;
        assert!(
            after_failure.is_empty(),
            "connect failure must invalidate the cache so lookups re-consult the catalog"
        );
    }

    /// `get_flight_client_for` must not consult the catalog at all: the
    /// caller already holds the resolved metadata. With the service absent
    /// from the catalog it still attempts the dial (and fails on the dead
    /// address rather than on a catalog miss).
    #[tokio::test]
    async fn get_flight_client_for_skips_catalog_lookup() {
        let transport = create_test_transport().await;
        let metadata = FlightServiceMetadata::new(
            Uuid::new_v4(),
            ServiceType::Writer,
            "127.0.0.1:1".to_string(),
            1,
            vec![ServiceCapability::Storage],
        );

        // Never registered in the catalog: a catalog-dependent path would
        // fail with "Service not found in catalog"; the direct path must get
        // as far as dialing the endpoint.
        let err = transport
            .get_flight_client_for(&metadata)
            .await
            .expect_err("dialing a dead endpoint must fail");
        assert!(
            !err.to_string().contains("not found in catalog"),
            "direct client acquisition must not depend on a catalog lookup: {err}"
        );
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
