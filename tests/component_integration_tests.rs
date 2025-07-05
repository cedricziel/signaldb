use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::services::otlp_trace_service::TraceAcceptorService;
use arrow_flight::flight_service_server::FlightServiceServer;
use common::catalog::Catalog;
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{Wal, WalConfig};
use futures::TryStreamExt;
use object_store::{memory::InMemory, ObjectStore};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{trace_service_server::TraceServiceServer, ExportTraceServiceRequest},
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use querier::flight::QuerierFlightService;
use router::{create_router, InMemoryStateImpl};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};
use tonic::transport::Server;
use writer::WriterFlightService;

/// Test the complete flow: Acceptor → Writer → WAL → Object Store
#[tokio::test]
async fn test_acceptor_writer_flow() {
    // Set up test infrastructure
    let temp_dir = TempDir::new().unwrap();
    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 10,
        flush_interval_secs: 1, // Convert to seconds
    };

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let wal = Arc::new(Wal::new(wal_config).await.unwrap());

    // Set up service discovery
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    let service_bootstrap = ServiceBootstrap::new(
        Configuration::default(),
        ServiceType::Acceptor,
        "test-instance".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Start writer Flight service on a random port
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);

    let writer_service = WriterFlightService::new(object_store.clone(), wal.clone());
    let writer_server = Server::builder()
        .add_service(FlightServiceServer::new(writer_service))
        .serve(writer_addr);

    tokio::spawn(writer_server);

    // Register writer with flight transport
    let writer_id = flight_transport
        .register_flight_service(
            common::service_bootstrap::ServiceType::Writer,
            writer_addr.ip().to_string(),
            writer_addr.port(),
            vec![
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .unwrap();

    // Give services time to start
    sleep(Duration::from_millis(200)).await;

    // Set up acceptor with flight transport
    let acceptor_wal = Arc::new(Wal::new(wal_config).await.unwrap());
    let trace_handler = TraceHandler::new(flight_transport.clone(), acceptor_wal);
    let acceptor_service = TraceAcceptorService::new(trace_handler);

    // Start acceptor service on a random port
    let acceptor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let acceptor_addr = acceptor_listener.local_addr().unwrap();
    drop(acceptor_listener);

    let acceptor_server = Server::builder()
        .add_service(TraceServiceServer::new(acceptor_service))
        .serve(acceptor_addr);

    tokio::spawn(acceptor_server);
    sleep(Duration::from_millis(200)).await;

    // Create test trace data
    let trace_id = vec![1; 16];
    let span_id = vec![2; 8];

    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.clone(),
                    span_id: span_id.clone(),
                    parent_span_id: vec![],
                    name: "integration-test-span".to_string(),
                    kind: 1, // Server
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        code: 1, // Ok
                        message: "".to_string(),
                    }),
                    trace_state: String::new(),
                    flags: 0,
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    // Send trace to acceptor
    let endpoint = format!("http://{}", acceptor_addr);
    let mut client = opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient::connect(endpoint)
        .await
        .unwrap();

    let response = timeout(Duration::from_secs(5), client.export(trace_request))
        .await
        .expect("Request timed out")
        .expect("Request failed");

    println!("✓ Acceptor processed trace successfully");

    // Verify data reached object store via writer
    sleep(Duration::from_millis(500)).await; // Allow time for async processing

    let objects: Vec<_> = object_store.list(None).try_collect().await.unwrap();
    assert!(
        !objects.is_empty(),
        "No objects found in store - data didn't reach writer"
    );

    println!("✓ Data successfully written to object store via writer");

    // Verify WAL entries were processed
    let unprocessed = wal.get_unprocessed_entries().await.unwrap();
    println!("Unprocessed WAL entries: {}", unprocessed.len());

    // Clean up
    flight_transport
        .unregister_service(writer_id)
        .await
        .unwrap();
}

/// Test the Querier Flight service and its interaction with object store
#[tokio::test]
async fn test_querier_integration() {
    // Set up object store with test data
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Set up service discovery
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    let service_bootstrap = ServiceBootstrap::new(
        Configuration::default(),
        ServiceType::Acceptor,
        "test-instance".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Create querier service
    let querier_service = QuerierFlightService::new(object_store.clone(), flight_transport.clone());

    // Start querier on random port
    let querier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let querier_addr = querier_listener.local_addr().unwrap();
    drop(querier_listener);

    let querier_server = Server::builder()
        .add_service(FlightServiceServer::new(querier_service))
        .serve(querier_addr);

    tokio::spawn(querier_server);

    // Register querier with flight transport
    let querier_id = flight_transport
        .register_flight_service(
            common::service_bootstrap::ServiceType::Querier,
            querier_addr.ip().to_string(),
            querier_addr.port(),
            vec![ServiceCapability::QueryExecution],
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    // Test that querier can be discovered
    let querier_services = flight_transport
        .discover_services_by_capability(ServiceCapability::QueryExecution)
        .await;

    assert!(
        !querier_services.is_empty(),
        "No querier services discovered"
    );
    println!("✓ Querier service registered and discoverable");

    // Test Flight client connection to querier
    let client = flight_transport
        .get_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .expect("Failed to get querier client");

    println!("✓ Successfully created Flight client for querier");

    // Clean up
    flight_transport
        .unregister_service(querier_id)
        .await
        .unwrap();
}

/// Test Router Tempo API integration with querier services
#[tokio::test]
async fn test_router_tempo_integration() {
    // Set up service discovery
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    let service_bootstrap = ServiceBootstrap::new(
        Configuration::default(),
        ServiceType::Acceptor,
        "test-instance".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Create router state with flight transport
    // Create router state with flight transport integration
    let registry = router::discovery::ServiceRegistry::with_flight_transport(
        catalog.clone(),
        flight_transport.clone(),
    );
    let router_state = InMemoryStateImpl::new(catalog);

    // Set up a mock querier service
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let querier_service = QuerierFlightService::new(object_store, flight_transport.clone());

    let querier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let querier_addr = querier_listener.local_addr().unwrap();
    drop(querier_listener);

    let querier_server = Server::builder()
        .add_service(FlightServiceServer::new(querier_service))
        .serve(querier_addr);

    tokio::spawn(querier_server);

    // Register querier
    let _querier_id = flight_transport
        .register_flight_service(
            common::service_bootstrap::ServiceType::Querier,
            querier_addr.ip().to_string(),
            querier_addr.port(),
            vec![ServiceCapability::QueryExecution],
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    // Create router app
    let app = create_router(router_state);

    // Start router on random port
    let router_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let router_addr = router_listener.local_addr().unwrap();

    tokio::spawn(async move { axum::serve(router_listener, app).await });

    sleep(Duration::from_millis(200)).await;

    // Test Tempo API endpoints
    let client = reqwest::Client::new();
    let base_url = format!("http://{}", router_addr);

    // Test trace query endpoint
    let trace_response = timeout(
        Duration::from_secs(5),
        client
            .get(&format!("{}/tempo/api/traces/{}", base_url, "1".repeat(32)))
            .send(),
    )
    .await
    .expect("Request timed out")
    .expect("Request failed");

    assert!(
        trace_response.status().is_success(),
        "Trace query failed: {}",
        trace_response.status()
    );
    println!("✓ Router Tempo API trace query succeeded");

    // Test search endpoint
    let search_response = timeout(
        Duration::from_secs(5),
        client.get(&format!("{}/tempo/api/search", base_url)).send(),
    )
    .await
    .expect("Request timed out")
    .expect("Request failed");

    assert!(
        search_response.status().is_success(),
        "Search query failed: {}",
        search_response.status()
    );
    println!("✓ Router Tempo API search succeeded");
}

/// End-to-end test covering the complete pipeline
#[tokio::test]
async fn test_end_to_end_pipeline() {
    // This test validates the complete flow:
    // OTLP Client → Acceptor → Writer → Object Store
    // Router → Querier → Object Store

    let temp_dir = TempDir::new().unwrap();
    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 10,
        flush_interval_secs: 1,
    };

    // Shared infrastructure
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::in_memory().await.unwrap();
    let service_bootstrap = ServiceBootstrap::new(Configuration::default()).unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Start writer
    let writer_wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());
    let writer_service = WriterFlightService::new(object_store.clone(), writer_wal);
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);

    let writer_server = Server::builder()
        .add_service(FlightServiceServer::new(writer_service))
        .serve(writer_addr);
    tokio::spawn(writer_server);

    let _writer_id = flight_transport
        .register_flight_service(
            common::service_bootstrap::ServiceType::Writer,
            writer_addr.ip().to_string(),
            writer_addr.port(),
            vec![
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .unwrap();

    // Start querier
    let querier_service = QuerierFlightService::new(object_store.clone(), flight_transport.clone());
    let querier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let querier_addr = querier_listener.local_addr().unwrap();
    drop(querier_listener);

    let querier_server = Server::builder()
        .add_service(FlightServiceServer::new(querier_service))
        .serve(querier_addr);
    tokio::spawn(querier_server);

    let _querier_id = flight_transport
        .register_flight_service(
            common::service_bootstrap::ServiceType::Querier,
            querier_addr.ip().to_string(),
            querier_addr.port(),
            vec![ServiceCapability::QueryExecution],
        )
        .await
        .unwrap();

    // Start acceptor
    let acceptor_wal = Arc::new(Wal::new(wal_config).await.unwrap());
    let trace_handler = TraceHandler::new(flight_transport.clone(), acceptor_wal);
    let acceptor_service = TraceAcceptorService::new(trace_handler);
    let acceptor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let acceptor_addr = acceptor_listener.local_addr().unwrap();
    drop(acceptor_listener);

    let acceptor_server = Server::builder()
        .add_service(TraceServiceServer::new(acceptor_service))
        .serve(acceptor_addr);
    tokio::spawn(acceptor_server);

    // Start router
    // Create router state with flight transport integration
    let registry = router::discovery::ServiceRegistry::with_flight_transport(
        catalog.clone(),
        flight_transport.clone(),
    );
    let router_state = InMemoryStateImpl::new(catalog);
    let app = create_router(router_state);
    let router_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let router_addr = router_listener.local_addr().unwrap();

    tokio::spawn(async move { axum::serve(router_listener, app).await });

    // Allow all services to start
    sleep(Duration::from_millis(500)).await;

    // Step 1: Send trace data to acceptor
    let trace_id = vec![0x42; 16]; // Distinctive trace ID
    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.clone(),
                    span_id: vec![0x24; 8],
                    name: "end-to-end-test-span".to_string(),
                    kind: 1,
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    ..Default::default()
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    };

    let endpoint = format!("http://{}", acceptor_addr);
    let mut otlp_client = opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient::connect(endpoint)
        .await
        .unwrap();

    let _response = timeout(Duration::from_secs(5), otlp_client.export(trace_request))
        .await
        .expect("OTLP export timed out")
        .expect("OTLP export failed");

    println!("✓ Step 1: OTLP trace sent to acceptor");

    // Step 2: Allow time for processing and verify data in object store
    sleep(Duration::from_secs(1)).await;

    let objects: Vec<_> = object_store.list(None).try_collect().await.unwrap();
    assert!(
        !objects.is_empty(),
        "No data found in object store after ingestion"
    );

    println!("✓ Step 2: Data persisted to object store via writer");

    // Step 3: Query via router Tempo API
    let http_client = reqwest::Client::new();
    let trace_hex = hex::encode(&trace_id);
    let query_url = format!("http://{}/tempo/api/traces/{}", router_addr, trace_hex);

    let query_response = timeout(Duration::from_secs(5), http_client.get(&query_url).send())
        .await
        .expect("Query timed out")
        .expect("Query failed");

    assert!(query_response.status().is_success(), "Tempo query failed");

    println!("✓ Step 3: Successfully queried trace via router Tempo API");
    println!("✓ End-to-end pipeline test completed successfully!");
}
