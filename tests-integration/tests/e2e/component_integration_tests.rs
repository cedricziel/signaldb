use acceptor::handler::WalManager;
use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::services::otlp_trace_service::TraceAcceptorService;
use arrow_flight::flight_service_client::FlightServiceClient;
use common::CatalogManager;
use common::config::Configuration;
use common::flight::transport::{
    FlightServiceMetadata, InMemoryFlightTransport, ServiceCapability,
};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::ObjectStore;
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

/// Build a WalConfig with an immediate-flush buffer (size 1) so tests don't
/// need to wait out the time-based flush interval, plus an isolated Iceberg
/// catalog path per test to avoid cross-test table conflicts under parallel
/// execution (the default `sqlite::memory:` catalog is shared across
/// connections in the same process).
fn test_tenant_context() -> common::auth::TenantContext {
    common::auth::TenantContext {
        tenant_id: "test-tenant".to_string(),
        dataset_id: "test-dataset".to_string(),
        tenant_slug: "test-tenant".to_string(),
        dataset_slug: "test-dataset".to_string(),
        api_key_name: Some("test-key".to_string()),
        api_key_scopes: None,
        api_key_dataset_id: None,
        user_id: None,
        role: None,
        is_instance_admin: false,
        session_id: None,
        source: common::auth::TenantSource::Config,
    }
}

fn test_wal_config(temp_dir: &TempDir) -> WalConfig {
    WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1, // Force immediate flush for testing
        flush_interval_secs: 1,
        tenant_id: "test-tenant".to_string(),
        dataset_id: "test-dataset".to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    }
}

/// Build a Configuration with a shared SQLite discovery database and an
/// isolated per-test Iceberg catalog file.
fn test_configuration(temp_dir: &TempDir) -> Configuration {
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let iceberg_catalog_db_path = temp_dir.path().join("iceberg_catalog.db");

    let mut config = Configuration::default();
    config.schema.catalog_uri = format!("sqlite://{}", iceberg_catalog_db_path.display());
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: format!("sqlite://{}", catalog_db_path.display()),
        heartbeat_interval: Duration::from_secs(30),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(300),
    });
    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).expect("create storage dir");
    config.storage = common::config::StorageConfig {
        dsn: format!("file://{}", storage_dir.display()),
    };
    config.auth = common::config::AuthConfig {
        tenants: vec![common::config::TenantConfig {
            id: "test-tenant".to_string(),
            slug: "test-tenant".to_string(),
            name: "Test Tenant".to_string(),
            default_dataset: Some("test-dataset".to_string()),
            datasets: vec![common::config::DatasetConfig {
                id: "test-dataset".to_string(),
                slug: "test-dataset".to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![common::config::ApiKeyConfig {
                key: "test-key-123".to_string(),
                name: Some("test-key".to_string()),
            }],
            schema_config: None,
            limits: None,
        }],
        admin_api_key: None,
        internal_service_key: None,
        default_limits: Default::default(),
        storage_usage_refresh_interval: Duration::from_secs(60),
    };
    config
}

/// Pre-create the tenant/dataset Iceberg namespace so writer-side WAL
/// processing can commit tables immediately (matches the wired e2e suites).
async fn precreate_namespace(catalog_manager: &CatalogManager) {
    use iceberg_rust::catalog::namespace::Namespace;

    let namespace = Namespace::try_new(&["test-tenant".to_string(), "test-dataset".to_string()])
        .expect("valid namespace");
    catalog_manager
        .catalog()
        .create_namespace(&namespace, None)
        .await
        .expect("Failed to pre-create Iceberg namespace");
}

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
    let wal_config = test_wal_config(&temp_dir);
    let config = test_configuration(&temp_dir);

    let object_store: Arc<dyn ObjectStore> =
        common::storage::create_object_store_from_dsn(&config.storage.dsn)
            .expect("object store from test storage dsn");

    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:4317".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Start writer Flight service on a random port
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);

    let writer_wal = Arc::new(common::wal::Wal::new(wal_config.clone()).await.unwrap());
    let writer_catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("Failed to create CatalogManager for writer"),
    );
    precreate_namespace(&writer_catalog_manager).await;
    let writer_service = IcebergWriterFlightService::new(
        writer_catalog_manager,
        object_store.clone(),
        writer_wal.clone(),
        &common::config::WriterConfig::default(),
    );
    let _bg = writer_service.start_background_processing();
    let writer_server = Server::builder()
        .add_service(common::flight::flight_service_server(writer_service))
        .serve(writer_addr);

    tokio::spawn(writer_server);

    // Create writer bootstrap for proper service registration
    let writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();

    let writer_id = writer_bootstrap.service_id();

    // Wait for the writer to register as discoverable before pointing the
    // acceptor at it.
    let storage_services = wait_for_capability(
        &flight_transport,
        ServiceCapability::Storage,
        1,
        Duration::from_secs(10),
    )
    .await;
    assert!(
        !storage_services.is_empty(),
        "Writer did not register a Storage-capable service within the timeout"
    );

    // Set up acceptor with flight transport
    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
        wal_config,
    ));
    let trace_handler = TraceHandler::new(flight_transport.clone(), wal_manager.clone());
    let acceptor_service = TraceAcceptorService::new(trace_handler);

    // Start acceptor service on a random port
    let acceptor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let acceptor_addr = acceptor_listener.local_addr().unwrap();
    drop(acceptor_listener);

    // Production wraps this service in grpc_auth_interceptor, which resolves
    // auth headers into a TenantContext request extension. Auth itself is
    // covered by grpc_auth's own tests; here we inject the context directly
    // so this suite stays focused on the acceptor->writer flow.
    let acceptor_server = Server::builder()
        .add_service(TraceServiceServer::with_interceptor(
            acceptor_service,
            |mut req: tonic::Request<()>| {
                req.extensions_mut().insert(test_tenant_context());
                Ok(req)
            },
        ))
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

    // Verify data reached object store via writer
    let objects = wait_for_objects(&object_store, Duration::from_secs(15)).await;
    assert!(
        !objects.is_empty(),
        "No objects found in store - data didn't reach writer"
    );

    // Verify the acceptor's own trace WAL was fully processed (forwarded and
    // acknowledged) rather than just checking the writer's inbound WAL. The
    // WAL is keyed by the injected tenant context — asking for any other
    // tenant/dataset would lazily create an empty WAL and pass vacuously.
    let acceptor_wal = wal_manager
        .get_wal("test-tenant", "test-dataset", "traces")
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let acceptor_unprocessed = loop {
        let unprocessed = acceptor_wal.get_unprocessed_entries().await.unwrap();
        if unprocessed.is_empty() || Instant::now() >= deadline {
            break unprocessed;
        }
        sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        acceptor_unprocessed.len(),
        0,
        "Expected all acceptor WAL entries to be processed, but found {} unprocessed entries",
        acceptor_unprocessed.len()
    );

    // Verify WAL entries get marked processed on the writer side too. The
    // mark happens asynchronously after the Iceberg commit (object-store
    // visibility precedes it), so poll rather than assert instantly.
    let deadline = Instant::now() + Duration::from_secs(15);
    let unprocessed = loop {
        let unprocessed = writer_wal.get_unprocessed_entries().await.unwrap();
        if unprocessed.is_empty() || Instant::now() >= deadline {
            break unprocessed;
        }
        sleep(Duration::from_millis(100)).await;
    };
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

/// Test that a Querier Flight service registers itself as discoverable
/// (ServiceCapability::QueryExecution) via the shared InMemoryFlightTransport,
/// and that a Flight client can be constructed against it.
#[tokio::test]
async fn test_querier_integration() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_configuration(&temp_dir);
    let object_store: Arc<dyn ObjectStore> =
        common::storage::create_object_store_from_dsn(&config.storage.dsn)
            .expect("object store from test storage dsn");

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
        .add_service(common::flight::flight_service_server(querier_service))
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

    // Test Flight client connection
    let _client = flight_transport
        .get_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .expect("Failed to get querier client");

    // Clean up
    flight_transport
        .unregister_service(querier_id)
        .await
        .unwrap();
}

/// Regression test for #1131: the acceptor's OTLP/gRPC servers must accept
/// gzip-compressed requests. Gzip is the default compression for most
/// real-world OTLP/gRPC clients (the OpenTelemetry Collector's `otlp`
/// exporter, many language SDKs), so a client that never disables
/// compression would otherwise hit `Unimplemented: Content is compressed
/// with 'gzip' which isn't supported` against every export call.
///
/// Exercises the real production wiring (`acceptor::init_acceptor_resources`
/// + `acceptor::serve_otlp_grpc`), not a hand-rolled server, so it catches a
/// regression in the actual `*ServiceServer` construction.
#[tokio::test(flavor = "multi_thread")]
async fn test_acceptor_grpc_accepts_gzip_compressed_requests() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_configuration(&temp_dir);
    let wal_dir = temp_dir.path().join("wal");

    let resources =
        acceptor::init_acceptor_resources(config.clone(), "127.0.0.1:4317".to_string(), wal_dir)
            .await
            .expect("Failed to init acceptor resources");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let (init_tx, init_rx) = tokio::sync::oneshot::channel();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, _stopped_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(acceptor::serve_otlp_grpc(
        acceptor::GrpcAcceptorConfig { addr, resources },
        init_tx,
        shutdown_rx,
        stopped_tx,
    ));
    init_rx.await.expect("acceptor did not signal init");

    let endpoint = format!("http://{addr}");
    let client = connect_trace_client_with_retry(endpoint, Duration::from_secs(10)).await;
    // Force gzip compression on the request, matching what real-world
    // OTLP/gRPC clients send by default.
    let mut client = client.send_compressed(tonic::codec::CompressionEncoding::Gzip);

    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "gzip-test-span".to_string(),
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let mut request = tonic::Request::new(trace_request);
    request.metadata_mut().insert(
        "authorization",
        "Bearer test-key-123".parse().expect("valid metadata value"),
    );
    request.metadata_mut().insert(
        "x-tenant-id",
        "test-tenant".parse().expect("valid metadata value"),
    );

    timeout(Duration::from_secs(5), client.export(request))
        .await
        .expect("Request timed out")
        .expect("gzip-compressed export must be accepted by the acceptor");
}

/// Test: Direct Flight communication between acceptor and writer
///
/// Isolates the writer's `do_put` Flight endpoint from the OTLP/WAL-forward
/// path exercised by `test_acceptor_writer_flow`: a raw Arrow RecordBatch is
/// sent directly over Flight, bypassing the acceptor entirely.
#[tokio::test]
async fn test_direct_acceptor_writer_flight() {
    let temp_dir = TempDir::new().unwrap();
    let wal_config = test_wal_config(&temp_dir);
    let config = test_configuration(&temp_dir);

    let object_store: Arc<dyn ObjectStore> =
        common::storage::create_object_store_from_dsn(&config.storage.dsn)
            .expect("object store from test storage dsn");

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
    let writer_wal = Arc::new(common::wal::Wal::new(wal_config.clone()).await.unwrap());
    let writer_catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("Failed to create CatalogManager for writer"),
    );
    precreate_namespace(&writer_catalog_manager).await;
    let writer_service = IcebergWriterFlightService::new(
        writer_catalog_manager,
        object_store.clone(),
        writer_wal.clone(),
        &common::config::WriterConfig::default(),
    );
    let _bg = writer_service.start_background_processing();
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);

    let writer_server = Server::builder()
        .add_service(common::flight::flight_service_server(writer_service))
        .serve(writer_addr);
    tokio::spawn(writer_server);

    // Create writer bootstrap for proper service registration
    let writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();

    let _writer_id = writer_bootstrap.service_id();

    // Can we get a Flight client for the writer? Poll instead of a fixed
    // sleep since writer registration completes asynchronously.
    let client_result = get_client_with_retry(
        &flight_transport,
        ServiceCapability::Storage,
        Duration::from_secs(10),
    )
    .await;

    assert!(
        client_result.is_ok(),
        "Failed to get Flight client for writer"
    );

    let mut client = client_result.unwrap();

    // Can we send data directly via Flight do_put?
    let test_data = create_test_span_data();
    let schema = test_data.schema();

    let flight_data = arrow_flight::utils::batches_to_flight_data(&schema, vec![test_data])
        .expect("Failed to convert to flight data");

    let flight_stream = stream::iter(flight_data.into_iter());
    let put_result = client.do_put(flight_stream).await;

    assert!(
        put_result.is_ok(),
        "Flight do_put failed: {:?}",
        put_result.err()
    );

    // Consume the response stream
    let mut response_stream = put_result.unwrap().into_inner();
    while let Some(result) = response_stream.next().await {
        assert!(result.is_ok(), "Response stream error: {:?}", result.err());
    }

    // Check if data reached object store
    let objects = wait_for_objects(&object_store, Duration::from_secs(15)).await;
    assert!(
        !objects.is_empty(),
        "No data found in object store after direct Flight communication"
    );
}

/// Helper function to create test span data for Flight do_put testing
fn create_test_span_data() -> datafusion::arrow::record_batch::RecordBatch {
    use common::flight::schema::create_span_batch_schema;
    use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray, UInt64Array};

    let schema = create_span_batch_schema();

    // Create sample span data with 3 test spans, columns in the exact order
    // of create_span_batch_schema (13 fields).
    let trace_ids = StringArray::from(vec!["trace_001", "trace_001", "trace_002"]);
    let span_ids = StringArray::from(vec!["span_001", "span_002", "span_003"]);
    let parent_span_ids = StringArray::from(vec![None, Some("span_001"), None]);
    let statuses = StringArray::from(vec![
        "STATUS_CODE_OK",
        "STATUS_CODE_OK",
        "STATUS_CODE_ERROR",
    ]);
    let is_root = BooleanArray::from(vec![true, false, true]);
    let span_names = StringArray::from(vec!["root_operation", "child_operation", "another_root"]);
    let service_names = StringArray::from(vec!["test_service", "test_service", "other_service"]);
    let span_kinds = StringArray::from(vec![
        "SPAN_KIND_SERVER",
        "SPAN_KIND_INTERNAL",
        "SPAN_KIND_CLIENT",
    ]);
    let start_times = UInt64Array::from(vec![1_000_000_000, 1_000_001_000, 1_000_002_000]);
    let durations = UInt64Array::from(vec![5_000_000, 2_000_000, 10_000_000]);
    let span_attributes = StringArray::from(vec![Some("{}"), Some("{}"), Some("{}")]);
    let resource_attributes = StringArray::from(vec![Some("{}"), Some("{}"), Some("{}")]);
    let events: StringArray = StringArray::from(vec![None::<&str>, None, None]);

    RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(trace_ids),
            std::sync::Arc::new(span_ids),
            std::sync::Arc::new(parent_span_ids),
            std::sync::Arc::new(statuses),
            std::sync::Arc::new(is_root),
            std::sync::Arc::new(span_names),
            std::sync::Arc::new(service_names),
            std::sync::Arc::new(span_kinds),
            std::sync::Arc::new(start_times),
            std::sync::Arc::new(durations),
            std::sync::Arc::new(span_attributes),
            std::sync::Arc::new(resource_attributes),
            std::sync::Arc::new(events),
        ],
    )
    .expect("Failed to create test record batch")
}
