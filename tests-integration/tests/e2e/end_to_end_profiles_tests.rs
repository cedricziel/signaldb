//! End-to-end tests for the profiles signal: OTLP ingestion through the
//! gRPC acceptor, WAL durability, Flight forwarding to the writer, Iceberg
//! persistence, and query retrieval — both via a direct Flight
//! `find_profile` ticket against the querier and via the router's
//! Pyroscope-compatible `/pyroscope/render` HTTP endpoint.
//!
//! Modeled on `end_to_end_trace_tests.rs`: a test tenant `AuthConfig`, a
//! `TenantContext`-injecting gRPC interceptor (tests don't run the real auth
//! stack for the acceptor), namespace pre-creation so the querier's catalog
//! cache sees it, and a `file://` storage DSN shared between the writer and
//! the assertions. Profiles are otherwise untested end-to-end: `self_monitoring.rs`
//! only wires a "profiles" WAL subdirectory into `WalManager` as ambient
//! plumbing for the self-monitoring exporter and never ingests or queries
//! actual profile data.

use acceptor::handler::WalManager;
use acceptor::handler::otlp_profiles_handler::ProfileHandler;
use acceptor::services::otlp_profile_service::ProfileAcceptorService;
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
use common::wal::{Wal, WalConfig};
use datafusion::arrow::array::{Array, StringArray};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use opentelemetry_proto::tonic::collector::profiles::v1development::{
    ExportProfilesServiceRequest, profiles_service_server::ProfilesServiceServer,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::profiles::v1development::{
    Function, Line, Location, Profile, ProfilesDictionary, ResourceProfiles, Sample, ScopeProfiles,
    Stack, ValueType,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use querier::flight::QuerierFlightService;
use router::{RouterAppState, create_router};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::{Instant, sleep, timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use writer::IcebergWriterFlightService;

const TEST_TENANT: &str = "test-tenant";
const TEST_DATASET: &str = "test-dataset";

/// The value carried by the fixture's single sample, asserted end-to-end.
const SAMPLE_VALUE: i64 = 4242;
/// The leaf function name in the fixture's single-frame stack, asserted
/// end-to-end via both the Flight query and the flamegraph aggregation.
const LEAF_FUNCTION: &str = "hot_loop";
const SERVICE_NAME: &str = "profiler-service";

/// Test services configuration and shared resources.
struct TestServices {
    pub object_store: Arc<dyn ObjectStore>,
    pub flight_transport: Arc<InMemoryFlightTransport>,
    pub acceptor_addr: std::net::SocketAddr,
    pub config: Configuration,
    pub _temp_dir: TempDir,
}

/// Wait for service registration to complete by polling the flight transport
/// until the expected service capabilities are all registered or timeout occurs.
async fn wait_for_service_registration(
    flight_transport: &InMemoryFlightTransport,
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

        if !trace_ingestion_services.is_empty()
            && !query_services.is_empty()
            && !storage_services.is_empty()
        {
            return Ok(());
        }

        if start_time.elapsed() >= timeout_duration {
            return Err(format!(
                "Service registration timeout after {timeout_duration:?} \
                 (trace_ingestion={}, query_execution={}, storage={})",
                trace_ingestion_services.len(),
                query_services.len(),
                storage_services.len()
            ));
        }

        sleep(Duration::from_millis(100)).await;
    }
}

/// Set up the full profiles pipeline (acceptor, writer, querier) wired the
/// same way production does: a `CatalogManager`-backed writer and querier
/// sharing an Iceberg catalog, and an acceptor whose gRPC endpoint has a
/// `TenantContext` injected via a test interceptor (tests don't run the real
/// auth stack).
///
/// The acceptor is registered under `ServiceCapability::TraceIngestion` here
/// only because that's the capability `wait_for_service_registration` polls
/// for readiness (mirroring the trace test's helper); the profiles gRPC
/// endpoint itself is `ProfilesServiceServer`, independent of that capability
/// name.
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

    let acceptor_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:0".to_string(),
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
    let writer_wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());
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
    ServiceBootstrap::new(
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
    let profile_handler = ProfileHandler::new(flight_transport.clone(), wal_manager);
    let profile_service = ProfileAcceptorService::new(profile_handler);
    let profile_service_with_auth =
        ProfilesServiceServer::with_interceptor(profile_service, |mut req: tonic::Request<()>| {
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
    let acceptor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let acceptor_addr = acceptor_listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(profile_service_with_auth)
            .serve_with_incoming(TcpListenerStream::new(acceptor_listener)),
    );

    wait_for_service_registration(&flight_transport, Duration::from_secs(15))
        .await
        .expect("Failed to wait for service registration");

    TestServices {
        object_store,
        flight_transport,
        acceptor_addr,
        config,
        _temp_dir: temp_dir,
    }
}

fn string_value(s: &str) -> Option<AnyValue> {
    Some(AnyValue {
        value: Some(any_value::Value::StringValue(s.to_string())),
    })
}

/// Build an OTLP profile export with a fully populated dictionary: one
/// function (`hot_loop`), one single-frame stack, and one sample carrying
/// [`SAMPLE_VALUE`]. The profile ID and sample value are the fields whose
/// round-trip fidelity the ingest/query test pins.
fn test_profile_request(profile_id: [u8; 16]) -> ExportProfilesServiceRequest {
    let dictionary = ProfilesDictionary {
        string_table: vec![
            String::new(),             // 0: required empty string
            "cpu".to_string(),         // 1
            "nanoseconds".to_string(), // 2
            LEAF_FUNCTION.to_string(), // 3
            "profiler.rs".to_string(), // 4
        ],
        function_table: vec![
            Function::default(), // 0: null function
            Function {
                name_strindex: 3,
                system_name_strindex: 3,
                filename_strindex: 4,
                start_line: 1,
            },
        ],
        location_table: vec![
            Location::default(), // 0: null location
            Location {
                mapping_index: 0,
                address: 0x1000,
                lines: vec![Line {
                    function_index: 1,
                    line: 42,
                    column: 0,
                }],
                attribute_indices: vec![],
            },
        ],
        stack_table: vec![
            Stack::default(), // 0: null stack
            Stack {
                location_indices: vec![1],
            },
        ],
        ..ProfilesDictionary::default()
    };

    let profile = Profile {
        sample_type: Some(ValueType {
            type_strindex: 1,
            unit_strindex: 2,
        }),
        samples: vec![Sample {
            stack_index: 1,
            values: vec![SAMPLE_VALUE],
            ..Sample::default()
        }],
        time_unix_nano: 1_700_000_000_000_000_000,
        duration_nano: 10_000_000_000,
        profile_id: profile_id.to_vec(),
        ..Profile::default()
    };

    ExportProfilesServiceRequest {
        resource_profiles: vec![ResourceProfiles {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: string_value(SERVICE_NAME),
                    ..KeyValue::default()
                }],
                ..Resource::default()
            }),
            scope_profiles: vec![ScopeProfiles {
                scope: None,
                profiles: vec![profile],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
        dictionary: Some(dictionary),
    }
}

/// Send a test profile via OTLP gRPC and return its profile ID.
async fn send_test_profile(services: &TestServices) -> [u8; 16] {
    let profile_id = [0x77; 16];
    let request = test_profile_request(profile_id);

    let endpoint = format!("http://{}", services.acceptor_addr);
    let mut otlp_client = opentelemetry_proto::tonic::collector::profiles::v1development::profiles_service_client::ProfilesServiceClient::connect(endpoint)
        .await
        .unwrap();

    timeout(Duration::from_secs(5), otlp_client.export(request))
        .await
        .expect("OTLP profiles export timed out")
        .expect("OTLP profiles export failed");

    profile_id
}

/// Poll the object store until it has persisted data or the timeout elapses.
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

fn string_column<'a>(
    batch: &'a datafusion::arrow::record_batch::RecordBatch,
    name: &str,
) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

/// Complete end-to-end test: OTLP profile ingestion -> WAL -> Writer ->
/// Iceberg -> query retrieval over the real Flight `find_profile` ticket
/// against the querier. Round-trip fidelity is checked against the
/// `samples_json` payload (the profile's actual sample value), not just
/// non-emptiness of the response.
#[tokio::test]
async fn otlp_profile_ingestion_round_trips_sample_value_through_query_pipeline() {
    let services = setup_services().await;

    let profile_id = send_test_profile(&services).await;
    let profile_id_hex = hex::encode(profile_id);

    let objects = wait_for_objects_persisted(&services.object_store, Duration::from_secs(15)).await;
    assert!(
        !objects.is_empty(),
        "No data found in object store after profile ingestion"
    );

    let mut query_client = services
        .flight_transport
        .get_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .expect("Failed to get querier client");

    // The first parquet object appearing in the store precedes the Iceberg
    // snapshot commit the querier reads through, so retry NotFound within a
    // bounded window instead of asserting on the first attempt.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut stream = loop {
        let query_ticket = arrow_flight::Ticket::new(format!(
            "find_profile:{TEST_TENANT}:{TEST_DATASET}:{profile_id_hex}"
        ));
        match timeout(Duration::from_secs(10), query_client.do_get(query_ticket))
            .await
            .expect("find_profile query timed out")
        {
            Ok(response) => break response.into_inner(),
            Err(status) if status.code() == tonic::Code::NotFound && Instant::now() < deadline => {
                sleep(Duration::from_millis(200)).await;
            }
            Err(status) => panic!("find_profile query failed: {status:?}"),
        }
    };

    let mut flight_data = Vec::new();
    while let Some(chunk) = stream.next().await {
        flight_data.push(chunk.expect("error reading find_profile flight data"));
    }
    assert!(
        !flight_data.is_empty(),
        "find_profile returned no Flight data for the ingested profile"
    );

    let batches =
        flight_data_to_batches(&flight_data).expect("Failed to convert flight data to batches");
    assert!(
        !batches.is_empty(),
        "find_profile returned no record batches"
    );

    let mut found_profile_id = false;
    let mut found_service_name = false;
    let mut found_sample_type = false;
    let mut found_sample_value = false;

    for batch in &batches {
        if let Some(col) = string_column(batch, "profile_id") {
            for i in 0..col.len() {
                if !col.is_null(i) && col.value(i) == profile_id_hex {
                    found_profile_id = true;
                }
            }
        }
        if let Some(col) = string_column(batch, "service_name") {
            for i in 0..col.len() {
                if !col.is_null(i) && col.value(i) == SERVICE_NAME {
                    found_service_name = true;
                }
            }
        }
        if let Some(col) = string_column(batch, "sample_type") {
            for i in 0..col.len() {
                if !col.is_null(i) && col.value(i) == "cpu" {
                    found_sample_type = true;
                }
            }
        }
        // samples_json is the storage-format payload column carrying the
        // decoded OTLP sample values; asserting the pinned value here proves
        // the sample data itself round-tripped, not merely the profile's
        // identity/metadata columns.
        if let Some(col) = string_column(batch, "samples_json") {
            for i in 0..col.len() {
                if !col.is_null(i) {
                    let samples: serde_json::Value =
                        serde_json::from_str(col.value(i)).expect("valid samples_json");
                    if samples.as_array().is_some_and(|arr| {
                        arr.iter().any(|s| {
                            s["values"].as_array().is_some_and(|values| {
                                values.contains(&serde_json::json!(SAMPLE_VALUE))
                            })
                        })
                    }) {
                        found_sample_value = true;
                    }
                }
            }
        }
    }

    assert!(
        found_profile_id,
        "queried profile_id did not match the ingested profile ({profile_id_hex})"
    );
    assert!(
        found_service_name,
        "queried service_name did not match the ingested profile ({SERVICE_NAME})"
    );
    assert!(
        found_sample_type,
        "queried sample_type did not match the ingested profile (cpu)"
    );
    assert!(
        found_sample_value,
        "queried samples_json did not contain the ingested sample value ({SAMPLE_VALUE}) - \
         sample data did not round-trip through ingestion and storage"
    );
}

/// The router's Pyroscope-compatible `/pyroscope/render` endpoint must
/// return a flamegraph aggregated from the actual ingested profile data,
/// not just any 200 response: the leaf function name and total tick count
/// are pinned against the fixture.
#[tokio::test]
async fn pyroscope_render_endpoint_returns_flamegraph_for_ingested_profile() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let services = setup_services().await;
    send_test_profile(&services).await;

    // Wait for the profile to be durably queryable before hitting the HTTP
    // endpoint: the router's Flight query would otherwise race the writer's
    // background WAL processing.
    wait_for_objects_persisted(&services.object_store, Duration::from_secs(15)).await;

    let catalog_dsn = services.config.discovery.as_ref().unwrap().dsn.clone();
    let catalog = Catalog::new(&catalog_dsn).await.unwrap();
    let router_state = RouterAppState::new_with_flight_transport(
        catalog,
        services.config.clone(),
        (*services.flight_transport).clone(),
    );
    let app = create_router(router_state);

    // Poll the render endpoint: the writer's background WAL processing is
    // asynchronous, so the profile may not be queryable via the querier's
    // Iceberg table the instant object-store persistence is observed.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    // The initial value is only a placeholder for the (rare) case the
    // deadline panic fires before the first response is recorded; the
    // unused_assignments lint can't see that the loop body always reads it
    // on the panic path before overwriting it again.
    #[allow(unused_assignments)]
    let mut last_status = StatusCode::SERVICE_UNAVAILABLE;
    let render_response: pyroscope_api::RenderResponse = loop {
        let request = Request::builder()
            .uri("/pyroscope/render?query=cpu&from=1699999999&until=1700000030")
            .header("authorization", "Bearer test-key-123")
            .header("x-tenant-id", TEST_TENANT)
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        last_status = response.status();
        if last_status == StatusCode::OK {
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let parsed: pyroscope_api::RenderResponse =
                serde_json::from_slice(&body).expect("valid RenderResponse JSON");
            if parsed.flamebearer.num_ticks > 0 {
                break parsed;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "/pyroscope/render never returned a non-empty flamegraph for the ingested profile \
                 within the deadline (last status: {last_status})"
            );
        }
        sleep(Duration::from_millis(200)).await;
    };

    assert_eq!(
        render_response.flamebearer.num_ticks, SAMPLE_VALUE,
        "flamegraph total ticks did not match the ingested sample value"
    );
    assert!(
        render_response
            .flamebearer
            .names
            .iter()
            .any(|n| n == LEAF_FUNCTION),
        "flamegraph name table did not contain the ingested leaf function '{LEAF_FUNCTION}'; got {:?}",
        render_response.flamebearer.names
    );
}

/// Native Query IR reads profile metadata through the router while keeping the
/// storage payload columns out of the generic response.
#[tokio::test]
async fn query_ir_returns_profile_summary_without_raw_payloads() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let services = setup_services().await;
    send_test_profile(&services).await;
    wait_for_objects_persisted(&services.object_store, Duration::from_secs(15)).await;

    let catalog = Catalog::new(&services.config.discovery.as_ref().unwrap().dsn)
        .await
        .unwrap();
    let app = create_router(RouterAppState::new_with_flight_transport(
        catalog,
        services.config.clone(),
        (*services.flight_transport).clone(),
    ));
    let body = serde_json::json!({
        "irVersion": 1,
        "from": "profiles",
        "range": { "from": "1699999999000000000", "to": "1700000030000000000" },
        "result": "rows",
        "fields": ["profile.id", "service.name", "sample.type"],
        "pipeline": []
    });

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header("content-type", "application/json")
            .header("authorization", "Bearer test-key-123")
            .header("x-tenant-id", TEST_TENANT)
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        if response.status() == StatusCode::OK {
            let value: serde_json::Value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap(),
            )
            .unwrap();
            let names: Vec<&str> = value["columns"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|column| column["name"].as_str())
                .collect();
            assert_eq!(names, vec!["profile_id", "service_name", "sample_type"]);
            assert!(!value.to_string().contains("samples_json"));
            assert!(!value.to_string().contains("stacktraces_json"));
            return;
        }
        if Instant::now() >= deadline {
            panic!("profile summary IR query did not become queryable before timeout");
        }
        sleep(Duration::from_millis(200)).await;
    }
}
