//! Cross-service integration coverage for the tenant registry (tasks.md 6.2,
//! `unified-tenant-catalog-registry`): a tenant that exists ONLY in the
//! database — created the way the admin API creates one, `source =
//! "database"`, no `[[auth.tenants]]` config block anywhere — ingests real
//! log data through the OTLP gRPC acceptor and reads it back through the
//! querier's Flight `do_get`.
//!
//! 6.1 (`querier/tests/lazy_tenant_registration.rs`) already proves catalog
//! *resolution* does not error for a database tenant. This test proves
//! *namespace parity*: the write path (acceptor -> WAL -> writer -> Iceberg,
//! namespaced via `CatalogManager::build_namespace`) and the read path (the
//! querier's registry-driven catalog registration, `new_with_catalog_manager`)
//! land in the identical `{tenant}/{dataset}` namespace, so an actual round
//! trip of real data — not just an error-free resolve — succeeds. The
//! object-store path the writer produced is asserted to contain that same
//! namespace, tying the two together directly.

use acceptor::handler::WalManager;
use acceptor::handler::otlp_log_handler::LogHandler;
use acceptor::services::otlp_log_service::LogAcceptorService;
use arrow_flight::Ticket;
use arrow_flight::utils::flight_data_to_batches;
use common::CatalogManager;
use common::auth::{TenantContext, TenantSource};
use common::catalog::Catalog;
use common::config::{Configuration, QuerierConfig, WriterConfig};
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use opentelemetry_proto::tonic::{
    collector::logs::v1::{ExportLogsServiceRequest, logs_service_server::LogsServiceServer},
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use querier::flight::QuerierFlightService;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use writer::IcebergWriterFlightService;

const DB_TENANT: &str = "gamma-tenant";
const DB_DATASET: &str = "production";
const DB_SERVICE_NAME: &str = "gamma-service";
const LOG_BODY: &str = "database tenant namespace parity probe";

struct TestServices {
    object_store: Arc<dyn ObjectStore>,
    flight_transport: Arc<InMemoryFlightTransport>,
    catalog_manager: Arc<CatalogManager>,
    acceptor_addr: std::net::SocketAddr,
    _temp_dir: TempDir,
    _writer_bootstrap: ServiceBootstrap,
    _querier_bootstrap: ServiceBootstrap,
}

/// Boot acceptor + writer + querier wired exactly like production, for a
/// tenant that is registered ONLY in the database `Catalog` — `config.auth`
/// carries no tenant at all — so every namespace decision (write and read)
/// must come from the registry, not a config fallback.
async fn setup_services() -> TestServices {
    let temp_dir = TempDir::new().unwrap();

    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let storage_dsn = format!("file://{}", storage_dir.display());
    let object_store: Arc<dyn ObjectStore> =
        common::storage::create_object_store_from_dsn(&storage_dsn)
            .expect("Failed to create object store from filesystem DSN");

    let discovery_db_path = temp_dir.path().join("discovery.db");
    let discovery_dsn = format!("sqlite://{}", discovery_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: discovery_dsn,
        heartbeat_interval: Duration::from_secs(30),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(300),
    });
    // Each test needs its own isolated Iceberg catalog: the default
    // "sqlite::memory:" is shared across all connections in the same
    // process, causing cross-test Iceberg table conflicts under parallel
    // execution. A per-test on-disk SQLite file gives full isolation.
    let iceberg_catalog_db_path = temp_dir.path().join("iceberg_catalog.db");
    config.schema.catalog_uri = format!("sqlite://{}", iceberg_catalog_db_path.display());
    config.storage.dsn = storage_dsn.clone();
    // Deliberately empty: DB_TENANT/DB_DATASET have no `[[auth.tenants]]`
    // entry anywhere. Every slug/namespace/enumeration decision below must
    // come from the registry union, not a config fallback.
    config.auth.tenants = vec![];

    // The tenant source: a database Catalog holding ONLY the admin-API-style
    // tenant, upserted before any component is constructed so startup
    // enumeration (tasks.md 3.2) picks it up (complementing 6.1, which
    // covers the lazy on-demand path for a tenant created after startup).
    let tenant_source = Arc::new(Catalog::new_in_memory().await.unwrap());
    tenant_source
        .upsert_tenant(DB_TENANT, "Gamma Tenant", Some(DB_DATASET), "database")
        .await
        .unwrap();
    tenant_source
        .create_dataset(DB_TENANT, DB_DATASET)
        .await
        .unwrap();

    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
        tenant_id: DB_TENANT.to_string(),
        dataset_id: DB_DATASET.to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    };

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

    // Shared CatalogManager, registry-wired: writer and querier must resolve
    // the SAME namespace for DB_TENANT/DB_DATASET for data to round-trip.
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("Failed to create CatalogManager")
            .with_tenant_source(tenant_source),
    );

    // Pre-create the Iceberg namespace: QuerierFlightService::new_with_catalog_manager
    // caches namespaces at construction time, before any data has been
    // written (matching the config-tenant e2e harness in
    // end_to_end_trace_tests.rs).
    {
        use iceberg_rust::catalog::namespace::Namespace;

        let namespace = Namespace::try_new(&[DB_TENANT.to_string(), DB_DATASET.to_string()])
            .expect("valid namespace");
        catalog_manager
            .catalog()
            .create_namespace(&namespace, None)
            .await
            .expect("Failed to pre-create Iceberg namespace");
    }

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

    // Querier: registry enumeration (tasks.md 3.2) must register a DataFusion
    // catalog + object store for DB_TENANT even though it has no config entry.
    let querier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let querier_addr = querier_listener.local_addr().unwrap();
    let querier_service = QuerierFlightService::new_with_catalog_manager(
        flight_transport.clone(),
        catalog_manager.clone(),
        QuerierConfig::default(),
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

    // Acceptor: a real gRPC OTLP logs endpoint. Tests don't run the real auth
    // stack, so a test interceptor injects the TenantContext the real
    // Authenticator would have produced for this database tenant (source =
    // Database, slugs falling back to the bare IDs since no config overrides
    // them — exactly what CatalogManager::get_tenant_slug/get_dataset_slug
    // resolve to for a config-less tenant).
    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
        wal_config,
    ));
    let log_handler = LogHandler::new(flight_transport.clone(), wal_manager);
    let log_acceptor_service = LogAcceptorService::new(log_handler);
    let acceptor_service_with_auth =
        LogsServiceServer::with_interceptor(log_acceptor_service, |mut req: tonic::Request<()>| {
            req.extensions_mut().insert(TenantContext {
                tenant_id: DB_TENANT.to_string(),
                dataset_id: DB_DATASET.to_string(),
                tenant_slug: DB_TENANT.to_string(),
                dataset_slug: DB_DATASET.to_string(),
                api_key_name: Some("test-key".to_string()),
                api_key_scopes: None,
                api_key_dataset_ids: None,
                user_id: None,
                role: None,
                is_instance_admin: false,
                session_id: None,
                source: TenantSource::Database,
            });
            Ok(req)
        });
    tokio::spawn(
        Server::builder()
            .add_service(acceptor_service_with_auth)
            .serve_with_incoming(TcpListenerStream::new(acceptor_listener)),
    );

    // Wait for writer + querier to register.
    let start = std::time::Instant::now();
    loop {
        let has_query = !flight_transport
            .discover_services_by_capability(ServiceCapability::QueryExecution)
            .await
            .is_empty();
        let has_storage = !flight_transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await
            .is_empty();
        if has_query && has_storage {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "writer/querier failed to register in time"
        );
        sleep(Duration::from_millis(100)).await;
    }

    TestServices {
        object_store,
        flight_transport,
        catalog_manager,
        acceptor_addr,
        _temp_dir: temp_dir,
        _writer_bootstrap: writer_bootstrap,
        _querier_bootstrap: querier_bootstrap,
    }
}

fn string_value(s: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(s.to_string())),
    }
}

async fn send_test_log(services: &TestServices) {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(string_value(DB_SERVICE_NAME)),
                    ..Default::default()
                }],
                dropped_attributes_count: 0,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    observed_time_unix_nano: 1_700_000_000_000_000_000,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(string_value(LOG_BODY)),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let endpoint = format!("http://{}", services.acceptor_addr);
    let mut otlp_client =
        opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient::connect(endpoint)
            .await
            .unwrap();

    timeout(Duration::from_secs(5), otlp_client.export(request))
        .await
        .expect("OTLP logs export timed out")
        .expect("OTLP logs export failed");
}

/// Force the writer to commit all pending writes for the database tenant
/// (read-your-writes drain via `do_action("flush")`).
async fn force_writer_commit(services: &TestServices) {
    common::testing::flush_storage_writers(&services.flight_transport, DB_TENANT, None)
        .await
        .expect("writer flush failed");
}

#[tokio::test]
async fn database_tenant_log_round_trips_through_matching_namespace() {
    let services = setup_services().await;

    send_test_log(&services).await;

    // Write-side namespace check: the object store must have persisted data
    // under the SAME tenant/dataset namespace CatalogManager::build_namespace
    // resolves for this database tenant, proving the write path did not fall
    // back to some other (e.g. default/global) namespace.
    let expected_namespace = services
        .catalog_manager
        .build_namespace(DB_TENANT, DB_DATASET)
        .expect("database tenant must resolve a namespace");
    let expected_namespace_segments: Vec<String> = expected_namespace
        .to_string()
        .split('.')
        .map(str::to_string)
        .collect();

    let start = std::time::Instant::now();
    let objects = loop {
        let objects: Vec<_> = services
            .object_store
            .list(None)
            .try_collect()
            .await
            .unwrap();
        if !objects.is_empty() || start.elapsed() >= Duration::from_secs(15) {
            break objects;
        }
        sleep(Duration::from_millis(200)).await;
    };
    assert!(
        !objects.is_empty(),
        "expected persisted objects for the database tenant's logs"
    );
    assert!(
        objects.iter().any(|o| {
            let path = o.location.to_string();
            expected_namespace_segments
                .iter()
                .all(|segment| path.contains(segment.as_str()))
        }),
        "expected an object path containing the resolved namespace {expected_namespace_segments:?}, found: {:?}",
        objects
            .iter()
            .map(|o| o.location.to_string())
            .collect::<Vec<_>>()
    );

    // The writer acks `do_put` after its WAL flush and defers the Iceberg
    // commit to the coalescing background loop; force the documented
    // read-your-writes drain instead of racing the commit interval.
    force_writer_commit(&services).await;

    // Read-side round trip: the querier — whose catalog/object-store
    // registration came entirely from the registry (no config entry exists
    // for this tenant) — must return the exact log body that was ingested.
    let mut query_client = services
        .flight_transport
        .get_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .expect("no QueryExecution-capable Flight client");

    let params = serde_json::json!({
        "query": format!("{{service_name=\"{DB_SERVICE_NAME}\"}}"),
        "start": 1_699_999_000_000_000_000i64,
        "end": 1_700_001_000_000_000_000i64,
        "limit": 100,
        "direction": "forward",
    });
    let ticket = Ticket::new(format!(
        "query_logs:{DB_TENANT}:{DB_DATASET}:{}",
        serde_json::to_string(&params).unwrap()
    ));

    let response = timeout(Duration::from_secs(10), query_client.do_get(ticket))
        .await
        .expect("logs query timed out")
        .expect(
            "logs query failed — the querier could not resolve the database tenant's namespace",
        );

    let mut stream = response.into_inner();
    let mut flight_data = Vec::new();
    while let Some(chunk) = stream.next().await {
        flight_data.push(chunk.expect("error reading flight data"));
    }
    assert!(
        !flight_data.is_empty(),
        "expected flight data for the database tenant's logs query"
    );

    let batches = flight_data_to_batches(&flight_data).expect("failed to decode flight data");
    assert!(!batches.is_empty(), "expected at least one record batch");

    use datafusion::arrow::array::Array;

    // The `body` column *stores* the log body's `AnyValue` as a JSON-encoded
    // string (see conversion_logs.rs) so any value type round-trips through a
    // single Utf8 column, but this query goes through the querier's Flight
    // `query_logs` path (`shape_log_query`), which decodes a string-typed
    // body on the way out (issue #1410) — the value observed here is the
    // plain, unquoted text.
    let mut found_body = false;
    let mut found_service = false;
    for batch in &batches {
        if let Some(body_col) = batch.column_by_name("body")
            && let Some(array) = body_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
        {
            found_body |= (0..array.len()).any(|i| array.value(i) == LOG_BODY);
        }
        if let Some(service_col) = batch.column_by_name("service_name")
            && let Some(array) = service_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
        {
            found_service |= (0..array.len()).any(|i| array.value(i) == DB_SERVICE_NAME);
        }
    }

    assert!(
        found_body,
        "expected the ingested log body {LOG_BODY:?} in the query result"
    );
    assert!(
        found_service,
        "expected service_name {DB_SERVICE_NAME:?} in the query result"
    );
}
