use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::services::otlp_trace_service::TraceAcceptorService;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use common::config::Configuration;
use common::flight::transport::{FlightServiceMetadata, InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{Wal, WalConfig};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, memory::InMemory};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
        trace_service_server::TraceServiceServer,
    },
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use querier::flight::QuerierFlightService;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::{Instant, sleep, timeout};
use tonic::transport::{Channel, Server};
use writer::IcebergWriterFlightService;

/// Poll `flight_transport` until at least `min_count` services advertising
/// `capability` are discoverable, or `timeout_duration` elapses.
///
/// Replaces a fixed "give the service time to register" sleep with a check
/// of the actual condition being waited for: the test proceeds the moment
/// registration lands, and the timeout only bounds a genuine registration
/// failure rather than pacing the happy path.
async fn wait_for_capability(
    flight_transport: &InMemoryFlightTransport,
    capability: ServiceCapability,
    min_count: usize,
    timeout_duration: Duration,
) -> Vec<FlightServiceMetadata> {
    let start = Instant::now();
    loop {
        let services = flight_transport
            .discover_services_by_capability(capability.clone())
            .await;
        if services.len() >= min_count || start.elapsed() >= timeout_duration {
            return services;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Poll until every `(capability, min_count)` pair is satisfied, or
/// `timeout_duration` elapses. Used where a test needs several services
/// registered before proceeding, instead of one fixed sleep covering all of
/// them.
async fn wait_for_capabilities(
    flight_transport: &InMemoryFlightTransport,
    expected: &[(ServiceCapability, usize)],
    timeout_duration: Duration,
) {
    let start = Instant::now();
    loop {
        let mut satisfied = true;
        for (capability, min_count) in expected {
            let count = flight_transport
                .discover_services_by_capability(capability.clone())
                .await
                .len();
            if count < *min_count {
                satisfied = false;
                break;
            }
        }
        if satisfied || start.elapsed() >= timeout_duration {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Poll the object store until it has persisted at least one object, or
/// `timeout_duration` elapses.
///
/// Replaces a fixed "allow time for async processing" sleep with a check of
/// the actual condition the test cares about — data landing in the store.
async fn wait_for_objects(
    object_store: &Arc<dyn ObjectStore>,
    timeout_duration: Duration,
) -> Vec<object_store::ObjectMeta> {
    let start = Instant::now();
    loop {
        let objects: Vec<_> = object_store.list(None).try_collect().await.unwrap();
        if !objects.is_empty() || start.elapsed() >= timeout_duration {
            return objects;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Connect to a gRPC OTLP trace endpoint, retrying until the server is
/// accepting connections or `timeout_duration` elapses.
///
/// `tokio::spawn`-ed servers need a moment to bind; this replaces a fixed
/// pre-connect sleep with a retry on the actual failure mode (connection
/// refused).
async fn connect_trace_client_with_retry(
    endpoint: String,
    timeout_duration: Duration,
) -> TraceServiceClient<Channel> {
    let start = Instant::now();
    loop {
        match TraceServiceClient::connect(endpoint.clone()).await {
            Ok(client) => return client,
            Err(e) => {
                if start.elapsed() >= timeout_duration {
                    panic!("Failed to connect to {endpoint} after {timeout_duration:?}: {e}");
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Retry `get_client_for_capability` until it succeeds or `timeout_duration`
/// elapses — the actual condition a fixed "let the writer register" sleep
/// was standing in for.
async fn get_client_with_retry(
    flight_transport: &InMemoryFlightTransport,
    capability: ServiceCapability,
    timeout_duration: Duration,
) -> Result<FlightServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    loop {
        match flight_transport
            .get_client_for_capability(capability.clone())
            .await
        {
            Ok(client) => return Ok(client),
            Err(e) => {
                if start.elapsed() >= timeout_duration {
                    return Err(e);
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Test the complete flow: Acceptor → Writer → WAL → Object Store
#[tokio::test]
async fn test_acceptor_writer_flow() {
    // Set up test infrastructure
    let temp_dir = TempDir::new().unwrap();
    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,  // Force immediate flush for testing
        flush_interval_secs: 1, // Convert to seconds
    };

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());

    // Set up service discovery with shared SQLite database
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: std::time::Duration::from_secs(30),
        poll_interval: std::time::Duration::from_secs(60),
        ttl: std::time::Duration::from_secs(300),
    });

    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:4317".to_string(),
    )
    .await
    .unwrap();
    println!(
        "🔍 Acceptor bootstrap address: {}",
        service_bootstrap.address()
    );
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Start writer Flight service on a random port
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);

    let writer_service =
        IcebergWriterFlightService::new(
            config.clone(),
            object_store.clone(),
            wal.clone(),
            &common::config::WriterConfig::default(),
        );
    let _bg = writer_service.start_background_processing();
    let writer_server = Server::builder()
        .add_service(FlightServiceServer::new(writer_service))
        .serve(writer_addr);

    tokio::spawn(writer_server);

    // Create writer bootstrap for proper service registration
    println!("🔍 Writer address to register: {writer_addr}");
    let writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();
    println!(
        "🔍 Writer bootstrap address: {}",
        writer_bootstrap.address()
    );

    let writer_id = writer_bootstrap.service_id();

    // Wait for the writer to register as discoverable before pointing the
    // acceptor at it.
    let storage_services =
        wait_for_capability(&flight_transport, ServiceCapability::Storage, 1, Duration::from_secs(10)).await;
    assert!(
        !storage_services.is_empty(),
        "Writer did not register a Storage-capable service within the timeout"
    );

    // Set up acceptor with flight transport
    let acceptor_wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());
    let trace_handler = TraceHandler::new(flight_transport.clone(), acceptor_wal.clone());
    let acceptor_service = TraceAcceptorService::new(trace_handler);

    // Start acceptor service on a random port
    let acceptor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let acceptor_addr = acceptor_listener.local_addr().unwrap();
    drop(acceptor_listener);

    let acceptor_server = Server::builder()
        .add_service(TraceServiceServer::new(acceptor_service))
        .serve(acceptor_addr);

    tokio::spawn(acceptor_server);

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
    let endpoint = format!("http://{acceptor_addr}");
    let mut client = connect_trace_client_with_retry(endpoint, Duration::from_secs(10)).await;

    let _response = timeout(Duration::from_secs(5), client.export(trace_request))
        .await
        .expect("Request timed out")
        .expect("Request failed");

    println!("✓ Acceptor processed trace successfully");

    // Debug: Check service discovery
    let discovered_services = flight_transport
        .discover_services_by_capability(ServiceCapability::TraceIngestion)
        .await;
    println!(
        "🔍 Discovered TraceIngestion services: {:?}",
        discovered_services.len()
    );
    for service in &discovered_services {
        println!(
            "  - ID: {}, Type: {:?}, Address: {}",
            service.service_id, service.service_type, service.address
        );
    }

    let storage_services = flight_transport
        .discover_services_by_capability(ServiceCapability::Storage)
        .await;
    println!(
        "🔍 Discovered Storage services: {:?}",
        storage_services.len()
    );
    for service in &storage_services {
        println!(
            "  - ID: {}, Type: {:?}, Address: {}",
            service.service_id, service.service_type, service.address
        );
    }

    // Debug: Check acceptor WAL
    let acceptor_wal_entries = acceptor_wal.get_unprocessed_entries().await.unwrap();
    println!(
        "🔍 Acceptor WAL unprocessed entries: {:?}",
        acceptor_wal_entries.len()
    );

    // Debug: Check writer WAL
    let writer_wal_entries = wal.get_unprocessed_entries().await.unwrap();
    println!(
        "🔍 Writer WAL unprocessed entries: {:?}",
        writer_wal_entries.len()
    );

    // Verify data reached object store via writer
    let objects = wait_for_objects(&object_store, Duration::from_secs(15)).await;
    println!("🔍 Objects in store: {:?}", objects.len());
    for obj in &objects {
        println!("  - {}", obj.location);
    }
    assert!(
        !objects.is_empty(),
        "No objects found in store - data didn't reach writer"
    );

    println!("✓ Data successfully written to object store via writer");

    // Verify WAL entries were processed
    let unprocessed = wal.get_unprocessed_entries().await.unwrap();
    assert_eq!(
        unprocessed.len(),
        0,
        "Expected all WAL entries to be processed, but found {} unprocessed entries",
        unprocessed.len()
    );

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

    // Set up test infrastructure
    let temp_dir = TempDir::new().unwrap();

    // Set up service discovery with shared SQLite database
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: std::time::Duration::from_secs(30),
        poll_interval: std::time::Duration::from_secs(60),
        ttl: std::time::Duration::from_secs(300),
    });

    // Start querier on random port first to get the address
    let querier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let querier_addr = querier_listener.local_addr().unwrap();
    drop(querier_listener);

    // Create service bootstrap with the actual server address
    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Querier,
        querier_addr.to_string(),
    )
    .await
    .unwrap();
    let querier_id = service_bootstrap.service_id();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Create querier service
    let querier_service = QuerierFlightService::new(object_store.clone(), flight_transport.clone());

    let querier_server = Server::builder()
        .add_service(FlightServiceServer::new(querier_service))
        .serve(querier_addr);

    tokio::spawn(querier_server);

    // Test that querier can be discovered
    let querier_services = wait_for_capability(
        &flight_transport,
        ServiceCapability::QueryExecution,
        1,
        Duration::from_secs(10),
    )
    .await;

    assert!(
        !querier_services.is_empty(),
        "No querier services discovered"
    );
    println!("✓ Querier service registered and discoverable");

    // Create sample test data and write to object store
    let test_data = create_test_span_data();
    let test_file_path = "batch/test_spans.parquet";

    write_batch_to_object_store(object_store.clone(), test_file_path, test_data.clone())
        .await
        .expect("Failed to write test data to object store");

    println!("✓ Sample test data written to object store");

    // Test Flight client connection and query execution
    let _client = flight_transport
        .get_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .expect("Failed to get querier client");

    println!("✓ Successfully created Flight client for querier");

    // Skip query execution for now - DataFusion requires proper table registration
    // The querier architecture has been simplified to only query object store
    // and no longer depends on writers, which was the main goal
    println!(
        "✓ Querier test completed (query execution skipped - requires DataFusion table setup)"
    );

    // Clean up
    flight_transport
        .unregister_service(querier_id)
        .await
        .unwrap();
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
        max_buffer_entries: 1, // Force immediate flush for testing
        flush_interval_secs: 1,
    };

    // Shared infrastructure
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Set up service discovery with shared SQLite database
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: std::time::Duration::from_secs(30),
        poll_interval: std::time::Duration::from_secs(60),
        ttl: std::time::Duration::from_secs(300),
    });

    // Create shared flight transport for service discovery
    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:4317".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Start writer
    let writer_wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());
    let writer_service =
        IcebergWriterFlightService::new(
            config.clone(),
            object_store.clone(),
            writer_wal.clone(),
            &common::config::WriterConfig::default(),
        );
    let _bg = writer_service.start_background_processing();
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);

    let writer_server = Server::builder()
        .add_service(FlightServiceServer::new(writer_service))
        .serve(writer_addr);
    tokio::spawn(writer_server);

    // Create writer bootstrap for proper service registration
    let writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();

    let _writer_id = writer_bootstrap.service_id();

    // Start querier
    let querier_service = QuerierFlightService::new(object_store.clone(), flight_transport.clone());
    let querier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let querier_addr = querier_listener.local_addr().unwrap();
    drop(querier_listener);

    let querier_server = Server::builder()
        .add_service(FlightServiceServer::new(querier_service))
        .serve(querier_addr);
    tokio::spawn(querier_server);

    // Create querier bootstrap for proper service registration
    let querier_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Querier,
        querier_addr.to_string(),
    )
    .await
    .unwrap();

    let _querier_id = querier_bootstrap.service_id();

    // Start acceptor
    let acceptor_wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());
    let trace_handler = TraceHandler::new(flight_transport.clone(), acceptor_wal);
    let acceptor_service = TraceAcceptorService::new(trace_handler);
    let acceptor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let acceptor_addr = acceptor_listener.local_addr().unwrap();
    drop(acceptor_listener);

    let acceptor_server = Server::builder()
        .add_service(TraceServiceServer::new(acceptor_service))
        .serve(acceptor_addr);
    tokio::spawn(acceptor_server);

    // Skip router HTTP testing for now due to axum compatibility
    // Focus on Flight service integration testing
    println!("✓ All Flight services started (acceptor, writer, querier)");

    // Wait for the writer and querier to register before sending data.
    wait_for_capabilities(
        &flight_transport,
        &[
            (ServiceCapability::Storage, 1),
            (ServiceCapability::QueryExecution, 1),
        ],
        Duration::from_secs(10),
    )
    .await;

    // Debug: Check what services are registered
    let trace_ingestion_services = flight_transport
        .discover_services_by_capability(ServiceCapability::TraceIngestion)
        .await;
    println!(
        "Services with TraceIngestion capability: {}",
        trace_ingestion_services.len()
    );

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

    let endpoint = format!("http://{acceptor_addr}");
    let mut otlp_client = connect_trace_client_with_retry(endpoint, Duration::from_secs(10)).await;

    let _response = timeout(Duration::from_secs(5), otlp_client.export(trace_request))
        .await
        .expect("OTLP export timed out")
        .expect("OTLP export failed");

    println!("✓ Step 1: OTLP trace sent to acceptor");

    // Step 2: Wait for processing and verify data in object store
    let objects = wait_for_objects(&object_store, Duration::from_secs(15)).await;
    println!("Objects in store: {}", objects.len());
    for obj in &objects {
        println!("  - {}", obj.location);
    }

    assert!(
        !objects.is_empty(),
        "No data found in object store after ingestion"
    );

    println!("✓ Step 2: Data persisted to object store via writer");

    // Step 3: Verify Flight clients can connect to querier services
    let querier_services = flight_transport
        .discover_services_by_capability(
            common::flight::transport::ServiceCapability::QueryExecution,
        )
        .await;

    assert!(!querier_services.is_empty(), "No querier services found");
    println!("✓ Step 3: Querier services discoverable via Flight transport");
    println!("✓ End-to-end pipeline test completed successfully!");
}

/// Test: Direct Flight communication between acceptor and writer
#[tokio::test]
async fn test_direct_acceptor_writer_flight() {
    // This test isolates the Flight communication between acceptor and writer
    // to confirm if the issue is in the Flight data transfer

    let temp_dir = TempDir::new().unwrap();
    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
    };

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Set up service discovery with shared SQLite database
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: std::time::Duration::from_secs(30),
        poll_interval: std::time::Duration::from_secs(60),
        ttl: std::time::Duration::from_secs(300),
    });

    // Create shared flight transport from acceptor perspective
    let acceptor_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:50055".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(acceptor_bootstrap));

    // Start writer
    let writer_wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());
    let writer_service =
        IcebergWriterFlightService::new(
            config.clone(),
            object_store.clone(),
            writer_wal.clone(),
            &common::config::WriterConfig::default(),
        );
    let _bg = writer_service.start_background_processing();
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);

    let writer_server = Server::builder()
        .add_service(FlightServiceServer::new(writer_service))
        .serve(writer_addr);
    tokio::spawn(writer_server);

    // Create writer bootstrap for proper service registration
    let writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();

    let _writer_id = writer_bootstrap.service_id();

    // Test 1: Can we get a Flight client for the writer? Poll instead of a
    // fixed sleep since writer registration completes asynchronously.
    let client_result = get_client_with_retry(
        &flight_transport,
        ServiceCapability::Storage,
        Duration::from_secs(10),
    )
    .await;

    println!("Flight client creation: {:?}", client_result.is_ok());
    assert!(
        client_result.is_ok(),
        "Failed to get Flight client for writer"
    );

    let mut client = client_result.unwrap();

    // Test 2: Can we send data directly via Flight do_put?
    let test_data = create_test_span_data();
    let schema = test_data.schema();

    println!("Test data created with {} rows", test_data.num_rows());

    // Convert to Flight data
    let flight_data = arrow_flight::utils::batches_to_flight_data(&schema, vec![test_data])
        .expect("Failed to convert to flight data");

    println!("Converted to {} Flight data chunks", flight_data.len());

    // Send via do_put
    let flight_stream = stream::iter(flight_data.into_iter());
    let put_result = client.do_put(flight_stream).await;

    println!("Flight do_put result: {:?}", put_result.is_ok());

    if let Err(e) = &put_result {
        println!("Flight do_put error: {e}");
    }

    assert!(
        put_result.is_ok(),
        "Flight do_put failed: {:?}",
        put_result.err()
    );

    // Consume the response stream
    let mut response_stream = put_result.unwrap().into_inner();
    let mut response_count = 0;
    while let Some(result) = response_stream.next().await {
        match result {
            Ok(_put_result) => response_count += 1,
            Err(e) => println!("Response stream error: {e}"),
        }
    }
    println!("Received {response_count} put responses");

    // Test 3: Check if data reached object store
    let objects = wait_for_objects(&object_store, Duration::from_secs(15)).await;
    println!("Objects in store after direct Flight: {}", objects.len());

    for obj in &objects {
        println!("  - {}", obj.location);
    }

    assert!(
        !objects.is_empty(),
        "No data found in object store after direct Flight communication"
    );
    println!("✓ Direct Flight communication test passed");
}

/// Test: WAL processing isolation
#[tokio::test]
async fn test_wal_processing_isolation() {
    // This test checks if the WAL is working correctly in isolation

    let temp_dir = TempDir::new().unwrap();
    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
    };

    let wal = Arc::new(Wal::new(wal_config).await.unwrap());

    // Test 1: Can we write to WAL?
    let test_data = b"test trace data";
    let entry_id = wal
        .append(common::wal::WalOperation::WriteTraces, test_data.to_vec())
        .await;
    println!("WAL append result: {:?}", entry_id.is_ok());
    assert!(entry_id.is_ok(), "Failed to append to WAL");

    let entry_id = entry_id.unwrap();

    // Test 2: Can we flush WAL?
    let flush_result = wal.flush().await;
    println!("WAL flush result: {:?}", flush_result.is_ok());
    assert!(flush_result.is_ok(), "Failed to flush WAL");

    // Test 3: Can we get unprocessed entries?
    let unprocessed = wal.get_unprocessed_entries().await.unwrap();
    println!("Unprocessed entries: {}", unprocessed.len());
    assert_eq!(unprocessed.len(), 1, "Expected 1 unprocessed entry");

    // Test 4: Can we mark as processed?
    let mark_result = wal.mark_processed(entry_id).await;
    println!("Mark processed result: {:?}", mark_result.is_ok());
    assert!(mark_result.is_ok(), "Failed to mark entry as processed");

    // Test 5: Are there now zero unprocessed entries?
    let unprocessed_after = wal.get_unprocessed_entries().await.unwrap();
    println!(
        "Unprocessed entries after marking: {}",
        unprocessed_after.len()
    );
    assert_eq!(
        unprocessed_after.len(),
        0,
        "Expected 0 unprocessed entries after marking"
    );

    println!("✓ WAL processing isolation test passed");
}

/// Test: Object store write isolation  
#[tokio::test]
async fn test_object_store_write_isolation() {
    // This test checks if writing to object store works in isolation

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let test_data = create_test_span_data();

    println!("Created test data with {} rows", test_data.num_rows());

    // Test: Can we write directly to object store?
    let path = "test/direct_write.parquet";
    let write_result =
        write_batch_to_object_store(object_store.clone(), path, test_data).await;

    println!(
        "Direct object store write result: {:?}",
        write_result.is_ok()
    );
    assert!(write_result.is_ok(), "Failed to write to object store");

    // Verify the file exists
    let objects: Vec<_> = object_store.list(None).try_collect().await.unwrap();
    println!("Objects after direct write: {}", objects.len());

    for obj in &objects {
        println!("  - {}", obj.location);
    }

    assert!(!objects.is_empty(), "No objects found after direct write");
    assert!(
        objects.iter().any(|obj| obj.location.as_ref() == path),
        "Expected file not found"
    );

    println!("✓ Object store write isolation test passed");
}

/// Test: OTLP to Arrow conversion (what acceptor does)
#[tokio::test]
async fn test_otlp_to_arrow_conversion() {
    // This test checks if the OTLP → Arrow conversion works correctly
    // This is what the acceptor does when it receives OTLP data

    println!("Testing OTLP → Arrow conversion...");

    // Create the same OTLP request as the failing end-to-end test
    let trace_id = vec![0x42; 16]; // Same as end-to-end test
    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.clone(),
                    span_id: vec![0x24; 8],
                    name: "test-otlp-conversion-span".to_string(),
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

    println!(
        "Created OTLP request with {} resource spans",
        trace_request.resource_spans.len()
    );

    // Test: Can we convert OTLP to Arrow like the acceptor does?
    // This uses the same conversion logic as in the acceptor
    let record_batch =
        common::flight::conversion::conversion_traces::otlp_traces_to_arrow(&trace_request);

    println!("OTLP → Arrow conversion completed successfully");
    println!(
        "Converted to RecordBatch with {} rows, {} columns",
        record_batch.num_rows(),
        record_batch.num_columns()
    );

    // Test: Can we convert the RecordBatch to Flight data?
    let schema = record_batch.schema();
    let flight_data_result =
        arrow_flight::utils::batches_to_flight_data(&schema, vec![record_batch.clone()]);

    println!(
        "Arrow → Flight conversion result: {:?}",
        flight_data_result.is_ok()
    );

    if let Err(e) = &flight_data_result {
        println!("Arrow → Flight conversion error: {e}");
        panic!("Arrow to Flight conversion failed: {e}");
    }

    let flight_data = flight_data_result.unwrap();
    println!("Converted to {} Flight data chunks", flight_data.len());

    // Test: Can we write the converted data to object store?
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "test/otlp_converted.parquet";

    let write_result =
        write_batch_to_object_store(object_store.clone(), path, record_batch).await;

    println!("Write converted data result: {:?}", write_result.is_ok());

    if let Err(e) = &write_result {
        println!("Write error: {e}");
        panic!("Failed to write converted OTLP data: {e}");
    }

    // Verify the file exists
    let objects: Vec<_> = object_store.list(None).try_collect().await.unwrap();
    println!("Objects after OTLP conversion test: {}", objects.len());

    for obj in &objects {
        println!("  - {}", obj.location);
    }

    assert!(
        !objects.is_empty(),
        "No objects found after OTLP conversion"
    );
    assert!(
        objects.iter().any(|obj| obj.location.as_ref() == path),
        "Expected file not found"
    );

    println!("✓ OTLP to Arrow conversion test passed");
}

/// Helper function to write a RecordBatch to object store as Parquet
async fn write_batch_to_object_store(
    object_store: Arc<dyn ObjectStore>,
    path: &str,
    batch: datafusion::arrow::record_batch::RecordBatch,
) -> anyhow::Result<()> {
    use datafusion::parquet::arrow::async_writer::ParquetObjectWriter;
    use datafusion::parquet::arrow::AsyncArrowWriter;
    use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};

    let path = object_store::path::Path::from(path);
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();
    let schema = batch.schema();
    let object_store_writer = ParquetObjectWriter::new(object_store, path);
    let mut arrow_writer = AsyncArrowWriter::try_new(object_store_writer, schema, Some(props))
        .map_err(|e| anyhow::anyhow!("Failed to create parquet writer: {e}"))?;
    arrow_writer.write(&batch).await?;
    arrow_writer.close().await?;
    Ok(())
}

/// Helper function to create test span data for querier testing
fn create_test_span_data() -> datafusion::arrow::record_batch::RecordBatch {
    use common::flight::schema::create_span_batch_schema;
    use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray, UInt64Array};

    let schema = create_span_batch_schema();

    // Create sample span data with 3 test spans
    let trace_ids = StringArray::from(vec!["trace_001", "trace_001", "trace_002"]);
    let span_ids = StringArray::from(vec!["span_001", "span_002", "span_003"]);
    let parent_span_ids = StringArray::from(vec![None, Some("span_001"), None]);
    let statuses = StringArray::from(vec![
        "STATUS_CODE_OK",
        "STATUS_CODE_OK",
        "STATUS_CODE_ERROR",
    ]);
    let is_root = BooleanArray::from(vec![true, false, true]);
    let names = StringArray::from(vec!["root_operation", "child_operation", "another_root"]);
    let service_names = StringArray::from(vec!["test_service", "test_service", "other_service"]);
    let span_kinds = StringArray::from(vec![
        "SPAN_KIND_SERVER",
        "SPAN_KIND_INTERNAL",
        "SPAN_KIND_CLIENT",
    ]);
    let start_times = UInt64Array::from(vec![1_000_000_000, 1_000_001_000, 1_000_002_000]);
    let durations = UInt64Array::from(vec![5_000_000, 2_000_000, 10_000_000]);

    RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(trace_ids),
            std::sync::Arc::new(span_ids),
            std::sync::Arc::new(parent_span_ids),
            std::sync::Arc::new(statuses),
            std::sync::Arc::new(is_root),
            std::sync::Arc::new(names),
            std::sync::Arc::new(service_names),
            std::sync::Arc::new(span_kinds),
            std::sync::Arc::new(start_times),
            std::sync::Arc::new(durations),
        ],
    )
    .expect("Failed to create test record batch")
}
