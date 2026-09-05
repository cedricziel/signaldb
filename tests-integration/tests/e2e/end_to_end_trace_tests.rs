use acceptor::handler::WalManager;
use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::services::otlp_trace_service::TraceAcceptorService;
use arrow_flight::utils::flight_data_to_batches;
use common::CatalogManager;
use common::auth::{TenantContext, TenantSource};
use common::catalog::Catalog;
use common::config::{
    ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, DefaultSchemas, SchemaConfig,
    StorageConfig, TenantConfig, WriterConfig,
};
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use opentelemetry_proto::tonic::{
    collector::trace::v1::{ExportTraceServiceRequest, trace_service_server::TraceServiceServer},
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use querier::flight::QuerierFlightService;
use router::{RouterAppState, discovery::ServiceRegistry};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use writer::IcebergWriterFlightService;

const TEST_TENANT: &str = "test-tenant";
const TEST_DATASET: &str = "test-dataset";

/// Expected service counts for readiness checking
#[derive(Debug, Clone)]
struct ExpectedServices {
    trace_ingestion: usize,
    query_execution: usize,
    storage: usize,
}

/// Wait for service registration to complete by polling the flight transport
/// until the expected service capabilities are all registered or timeout occurs
async fn wait_for_service_registration(
    flight_transport: &InMemoryFlightTransport,
    expected: ExpectedServices,
    timeout_duration: Duration,
) -> Result<(), String> {
    let start_time = std::time::Instant::now();

    loop {
        let trace_ingestion_services = flight_transport
            .discover_services_by_capability(ServiceCapability::TraceIngestion)
            .await;
        let query_services = flight_transport
            .discover_services_by_capability(ServiceCapability::QueryExecution)
            .await;
        let storage_services = flight_transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;

        let current_counts = ExpectedServices {
            trace_ingestion: trace_ingestion_services.len(),
            query_execution: query_services.len(),
            storage: storage_services.len(),
        };

        if current_counts.trace_ingestion >= expected.trace_ingestion
            && current_counts.query_execution >= expected.query_execution
            && current_counts.storage >= expected.storage
        {
            return Ok(());
        }

        if start_time.elapsed() >= timeout_duration {
            return Err(format!(
                "Service registration timeout after {timeout_duration:?}. Expected {expected:?}, but got {current_counts:?}"
            ));
        }

        sleep(Duration::from_millis(100)).await;
    }
}

/// Test services configuration and shared resources
struct TestServices {
    pub object_store: Arc<dyn ObjectStore>,
    pub flight_transport: Arc<InMemoryFlightTransport>,
    pub acceptor_addr: std::net::SocketAddr,
    pub config: Configuration,
    pub _temp_dir: TempDir,
    // ServiceBootstrap::drop aborts the heartbeat/reaper tasks, so the
    // writer/querier bootstraps must live as long as the test services.
    pub _writer_bootstrap: ServiceBootstrap,
    pub _querier_bootstrap: ServiceBootstrap,
}

/// Set up the full trace pipeline (acceptor, writer, querier) wired the same
/// way production does: a CatalogManager-backed writer and querier sharing an
/// Iceberg catalog, and an acceptor whose gRPC endpoint has a TenantContext
/// injected via a test interceptor (tests don't run the real auth stack).
async fn setup_services() -> TestServices {
    let temp_dir = TempDir::new().unwrap();

    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let storage_dsn = format!("file://{}", storage_dir.display());
    let object_store: Arc<dyn ObjectStore> =
        common::storage::create_object_store_from_dsn(&storage_dsn)
            .expect("Failed to create object store from filesystem DSN");

    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: Duration::from_secs(30),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(300),
    });
    // Each test needs its own isolated Iceberg catalog: the default
    // "sqlite::memory:" is shared across all connections in the same
    // process, causing cross-test Iceberg table conflicts under parallel
    // execution. A per-test on-disk SQLite file gives full isolation.
    let iceberg_catalog_db_path = temp_dir.path().join("iceberg_catalog.db");
    config.schema = SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: format!("sqlite://{}", iceberg_catalog_db_path.display()),
        default_schemas: DefaultSchemas::default(),
        materialized_labels: Default::default(),
    };
    config.storage = StorageConfig {
        dsn: storage_dsn.clone(),
    };
    config.auth = AuthConfig {
        tenants: vec![TenantConfig {
            id: TEST_TENANT.to_string(),
            slug: TEST_TENANT.to_string(),
            name: "Test Tenant".to_string(),
            default_dataset: Some(TEST_DATASET.to_string()),
            datasets: vec![DatasetConfig {
                id: TEST_DATASET.to_string(),
                slug: TEST_DATASET.to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![ApiKeyConfig {
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
        dataset_restriction_rollout_complete: false,
    };

    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
        tenant_id: TEST_TENANT.to_string(),
        dataset_id: TEST_DATASET.to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    };

    // Bind the acceptor listener before registering so the bootstrap
    // advertises the real reachable address, not port 0.
    let acceptor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let acceptor_addr = acceptor_listener.local_addr().unwrap();
    let acceptor_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        acceptor_addr.to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(acceptor_bootstrap));

    // Shared CatalogManager: writer and querier must see the same Iceberg
    // catalog for ingested data to be queryable back.
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("Failed to create CatalogManager"),
    );

    // Pre-create the Iceberg namespace so the querier's catalog cache
    // includes it: QuerierFlightService::new_with_catalog_manager caches
    // namespaces at construction time, before any data has been written.
    {
        use iceberg_rust::catalog::namespace::Namespace;

        let namespace = Namespace::try_new(&[TEST_TENANT.to_string(), TEST_DATASET.to_string()])
            .expect("valid namespace");
        catalog_manager
            .catalog()
            .create_namespace(&namespace, None)
            .await
            .expect("Failed to pre-create Iceberg namespace");
    }

    // Start writer Flight service. Bind first and keep the listener alive
    // (serve_with_incoming) to avoid a TOCTOU port-reuse race under parallel
    // test execution.
    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    let writer_wal = Arc::new(common::wal::manager::WalManager::uniform(
        tests_integration::test_helpers::writer_wal_config(&wal_config),
    ));
    let writer_service = IcebergWriterFlightService::new(
        catalog_manager.clone(),
        object_store.clone(),
        writer_wal,
        &WriterConfig::default(),
    );
    let _writer_bg = writer_service.start_background_processing();
    tokio::spawn(
        Server::builder()
            .add_service(common::flight::flight_service_server(writer_service))
            .serve_with_incoming(TcpListenerStream::new(writer_listener)),
    );
    let writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();

    // Start querier Flight service with per-tenant catalog registration.
    let querier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let querier_addr = querier_listener.local_addr().unwrap();
    let querier_service = QuerierFlightService::new_with_catalog_manager(
        flight_transport.clone(),
        catalog_manager,
        common::config::QuerierConfig::default(),
    )
    .await
    .expect("Failed to create querier service");
    tokio::spawn(
        Server::builder()
            .add_service(common::flight::flight_service_server(querier_service))
            .serve_with_incoming(TcpListenerStream::new(querier_listener)),
    );
    let querier_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Querier,
        querier_addr.to_string(),
    )
    .await
    .unwrap();

    // Start the acceptor. Tests don't run the real auth stack, so a test
    // interceptor injects the fixed TenantContext that the OTLP client's
    // (header-less) requests should have been authenticated to.
    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
        wal_config,
    ));
    let trace_handler = TraceHandler::new(flight_transport.clone(), wal_manager);
    let acceptor_service = TraceAcceptorService::new(trace_handler);
    let acceptor_service_with_auth =
        TraceServiceServer::with_interceptor(acceptor_service, |mut req: tonic::Request<()>| {
            req.extensions_mut().insert(TenantContext {
                tenant_id: TEST_TENANT.to_string(),
                dataset_id: TEST_DATASET.to_string(),
                tenant_slug: TEST_TENANT.to_string(),
                dataset_slug: TEST_DATASET.to_string(),
                api_key_name: Some("test-key".to_string()),
                api_key_scopes: None,
                api_key_dataset_id: None,
                user_id: None,
                role: None,
                is_instance_admin: false,
                session_id: None,
                source: TenantSource::Config,
            });
            Ok(req)
        });
    tokio::spawn(
        Server::builder()
            .add_service(acceptor_service_with_auth)
            .serve_with_incoming(TcpListenerStream::new(acceptor_listener)),
    );

    wait_for_service_registration(
        &flight_transport,
        ExpectedServices {
            trace_ingestion: 1,
            query_execution: 1,
            storage: 1,
        },
        Duration::from_secs(15),
    )
    .await
    .expect("Failed to wait for service registration");

    TestServices {
        object_store,
        flight_transport,
        _writer_bootstrap: writer_bootstrap,
        _querier_bootstrap: querier_bootstrap,
        acceptor_addr,
        config,
        _temp_dir: temp_dir,
    }
}

/// Verify that all services are discoverable
async fn verify_service_discovery(services: &TestServices) {
    let trace_ingestion_services = services
        .flight_transport
        .discover_services_by_capability(ServiceCapability::TraceIngestion)
        .await;
    let query_services = services
        .flight_transport
        .discover_services_by_capability(ServiceCapability::QueryExecution)
        .await;
    let storage_services = services
        .flight_transport
        .discover_services_by_capability(ServiceCapability::Storage)
        .await;

    assert!(
        !trace_ingestion_services.is_empty(),
        "No trace ingestion services found"
    );
    assert!(
        !query_services.is_empty(),
        "No query execution services found"
    );
    assert!(!storage_services.is_empty(), "No storage services found");
}

/// Send a test trace via OTLP and return the trace ID
async fn send_test_trace(services: &TestServices, trace_name: &str) -> Vec<u8> {
    let trace_id = vec![0x42; 16];
    let span_id = vec![0x24; 8];

    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.clone(),
                    span_id: span_id.clone(),
                    parent_span_id: vec![],
                    name: trace_name.to_string(),
                    kind: 1,
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        code: 1,
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

    let endpoint = format!("http://{}", services.acceptor_addr);
    let mut otlp_client = opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient::connect(endpoint)
        .await
        .unwrap();

    timeout(Duration::from_secs(5), otlp_client.export(trace_request))
        .await
        .expect("OTLP export timed out")
        .expect("OTLP export failed");

    trace_id
}

/// Poll the object store until it has persisted data or the timeout elapses.
///
/// This replaces fixed `sleep()` waits with a deterministic condition check:
/// the test proceeds as soon as data lands, and only waits the full timeout
/// when persistence has genuinely failed - avoiding both flakiness on slow
/// CI runners and needless fixed delays on fast ones.
async fn wait_for_objects_persisted(
    object_store: &Arc<dyn ObjectStore>,
    timeout_duration: Duration,
) -> Vec<object_store::ObjectMeta> {
    let start_time = std::time::Instant::now();
    loop {
        let objects: Vec<_> = object_store.list(None).try_collect().await.unwrap();
        if !objects.is_empty() || start_time.elapsed() >= timeout_duration {
            return objects;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Force the writer to commit all pending writes for the test tenant
/// (read-your-writes drain via `do_action("flush")`), so queries that follow
/// observe the ingested data deterministically instead of racing the
/// writer's commit-coalescing interval.
async fn force_writer_commit(services: &TestServices) {
    let mut writer_client = services
        .flight_transport
        .get_client_for_capability(ServiceCapability::Storage)
        .await
        .expect("no Storage-capable Flight client for flush");

    let mut flush_request = tonic::Request::new(arrow_flight::Action {
        r#type: "flush".to_string(),
        body: Default::default(),
    });
    flush_request
        .metadata_mut()
        .insert("x-tenant-id", TEST_TENANT.parse().unwrap());
    flush_request
        .metadata_mut()
        .insert("x-dataset-id", TEST_DATASET.parse().unwrap());

    let mut flush_stream = timeout(
        Duration::from_secs(30),
        writer_client.do_action(flush_request),
    )
    .await
    .expect("writer flush timed out")
    .expect("writer flush failed")
    .into_inner();
    while let Some(result) = flush_stream.next().await {
        result.expect("writer flush stream error");
    }
}

/// Validate that queried trace data matches the original trace
async fn validate_trace_query_data(
    services: &TestServices,
    expected_trace_id: &[u8],
    expected_span_name: &str,
) -> Result<(), String> {
    let mut query_client = services
        .flight_transport
        .get_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .map_err(|e| format!("Failed to get querier client: {e}"))?;

    let query_ticket = arrow_flight::Ticket::new(format!(
        "find_trace:{TEST_TENANT}:{TEST_DATASET}:{}",
        hex::encode(expected_trace_id)
    ));

    let query_result = timeout(Duration::from_secs(10), query_client.do_get(query_ticket))
        .await
        .map_err(|_| "Query timed out".to_string())?;

    match query_result {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut flight_data = Vec::new();

            while let Some(flight_data_result) = stream.next().await {
                match flight_data_result {
                    Ok(data) => flight_data.push(data),
                    Err(e) => return Err(format!("Error reading flight data: {e}")),
                }
            }

            if flight_data.is_empty() {
                return Err("No flight data received".to_string());
            }

            let batches = flight_data_to_batches(&flight_data)
                .map_err(|e| format!("Failed to convert flight data to batches: {e}"))?;

            if batches.is_empty() {
                return Err("No record batches found in flight data".to_string());
            }

            let mut found_matching_trace = false;
            let mut found_matching_span_name = false;

            for batch in &batches {
                if let Some(trace_id_col) = batch.column_by_name("trace_id")
                    && validate_trace_id_column(trace_id_col, expected_trace_id)?
                {
                    found_matching_trace = true;
                }

                if let Some(span_name_col) = batch.column_by_name("span_name")
                    && validate_span_name_column(span_name_col, expected_span_name)?
                {
                    found_matching_span_name = true;
                }
            }

            if !found_matching_trace {
                return Err(format!(
                    "Expected trace_id {} not found in query results",
                    hex::encode(expected_trace_id)
                ));
            }

            if !found_matching_span_name {
                return Err(format!(
                    "Expected span_name '{expected_span_name}' not found in query results"
                ));
            }

            Ok(())
        }
        Err(e) => Err(format!("Query failed: {e}")),
    }
}

/// Validate that a trace_id column contains the expected trace ID
fn validate_trace_id_column(
    column: &datafusion::arrow::array::ArrayRef,
    expected_trace_id: &[u8],
) -> Result<bool, String> {
    use datafusion::arrow::array::Array;

    if let Some(binary_array) = column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BinaryArray>()
    {
        for i in 0..binary_array.len() {
            if let Some(value) = binary_array.value(i).get(0..expected_trace_id.len())
                && value == expected_trace_id
            {
                return Ok(true);
            }
        }
    } else if let Some(string_array) = column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
    {
        let expected_hex = hex::encode(expected_trace_id);
        for i in 0..string_array.len() {
            if let Some(value) = string_array.value(i).get(0..expected_hex.len())
                && value == expected_hex
            {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// Validate that a span_name column contains the expected span name
fn validate_span_name_column(
    column: &datafusion::arrow::array::ArrayRef,
    expected_span_name: &str,
) -> Result<bool, String> {
    use datafusion::arrow::array::Array;

    if let Some(string_array) = column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
    {
        for i in 0..string_array.len() {
            if string_array.value(i) == expected_span_name {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// Complete end-to-end test: OTLP ingestion -> WAL -> Writer -> Iceberg ->
/// Query retrieval, exercised over the real Flight wire protocol (not the
/// Tempo HTTP API, which is covered separately in router_tempo_endpoints.rs).
///
/// Both query paths are asserted for real: a direct Flight `find_trace`
/// against the querier, and the same ticket resolved through the router's
/// `ServiceRegistry` (proving service-discovery-mediated Flight queries
/// work, not just a direct client-to-querier connection).
#[tokio::test]
async fn test_complete_trace_ingestion_and_query_pipeline() {
    let services = setup_services().await;

    let catalog_dsn = services.config.discovery.as_ref().unwrap().dsn.clone();
    let catalog = Catalog::new(&catalog_dsn).await.unwrap();
    let service_registry = ServiceRegistry::with_flight_transport(
        catalog.clone(),
        (*services.flight_transport).clone(),
    );
    let _router_state = RouterAppState::new(catalog, services.config.clone());

    verify_service_discovery(&services).await;

    let trace_id = send_test_trace(&services, "end-to-end-test-span").await;

    let objects = wait_for_objects_persisted(&services.object_store, Duration::from_secs(15)).await;
    assert!(
        !objects.is_empty(),
        "No data found in object store after ingestion"
    );

    // The writer acks `do_put` after its WAL flush and defers the Iceberg
    // commit to the coalescing background loop, so parquet existing in the
    // object store does not yet make the trace queryable. Force the
    // documented read-your-writes drain (`do_action("flush")`) instead of
    // racing the commit interval — the race loses on slow runners.
    force_writer_commit(&services).await;

    // Primary correctness check: the ingested trace_id/span_name must
    // actually round-trip through a direct Flight query against the querier.
    validate_trace_query_data(&services, &trace_id, "end-to-end-test-span")
        .await
        .expect(
            "trace query validation failed - queried data did not match the ingested trace \
             (trace_id/span_name did not round-trip via Flight query)",
        );

    // The router must be able to resolve a QueryExecution-capable Flight
    // client through service discovery and successfully execute the same
    // ticket - not just the direct client used above.
    let mut router_client = service_registry
        .get_flight_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .expect("router could not get a Flight client for QueryExecution");

    let router_ticket = arrow_flight::Ticket::new(format!(
        "find_trace:{TEST_TENANT}:{TEST_DATASET}:{}",
        hex::encode(&trace_id)
    ));

    let mut router_stream = timeout(Duration::from_secs(10), router_client.do_get(router_ticket))
        .await
        .expect("router-mediated query timed out")
        .expect("router-mediated query failed")
        .into_inner();

    let mut router_data_chunks = 0;
    while let Some(chunk) = router_stream.next().await {
        chunk.expect("error reading router-mediated flight data");
        router_data_chunks += 1;
    }
    assert!(
        router_data_chunks > 0,
        "router-mediated query returned no Flight data chunks"
    );
}

/// Performance/volume check for Flight communication: ingest a batch of
/// traces and verify they are all durably persisted. Throughput numbers are
/// informational only - they are not asserted on, since wall-clock timing on
/// a shared CI runner is not a reliable correctness signal and no baseline
/// has been established to compare against.
#[tokio::test]
async fn test_flight_communication_performance() {
    let services = setup_services().await;

    let num_traces: u32 = 50;
    let start_time = std::time::Instant::now();

    let endpoint = format!("http://{}", services.acceptor_addr);
    let mut otlp_client = opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient::connect(endpoint)
        .await
        .unwrap();

    for i in 0..num_traces {
        let mut trace_id = vec![0u8; 16];
        let mut span_id = vec![0u8; 8];

        trace_id[12..16].copy_from_slice(&i.to_be_bytes());
        span_id[4..8].copy_from_slice(&i.to_be_bytes());

        let trace_request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: trace_id.clone(),
                        span_id: span_id.clone(),
                        name: format!("perf-test-span-{i}"),
                        kind: 1,
                        start_time_unix_nano: 1_000_000_000 + (i as u64 * 1_000_000),
                        end_time_unix_nano: 2_000_000_000 + (i as u64 * 1_000_000),
                        ..Default::default()
                    }],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };

        timeout(Duration::from_secs(5), otlp_client.export(trace_request))
            .await
            .expect("OTLP export timed out")
            .expect("OTLP export failed");
    }

    let ingestion_duration = start_time.elapsed();

    // Wait deterministically for processing to complete instead of a fixed
    // sleep: proceed as soon as data is persisted, bounded by a generous
    // timeout so a genuine persistence failure still fails the test promptly.
    let objects = wait_for_objects_persisted(&services.object_store, Duration::from_secs(30)).await;

    println!(
        "Ingested {num_traces} traces in {ingestion_duration:?} (informational, not asserted)"
    );

    assert!(
        !objects.is_empty(),
        "No data was stored after ingesting {num_traces} traces"
    );
}
