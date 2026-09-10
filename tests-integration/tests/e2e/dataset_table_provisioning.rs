//! Cross-service coverage for `dataset-table-provisioning` and the
//! `tenant-catalog-registry` promise that a new tenant is usable — including
//! queryable — the moment it exists (issue #972).
//!
//! Three properties, end to end through the real Flight surfaces:
//!
//! - A tenant created the way the admin API creates one, with no telemetry
//!   ever ingested, answers label/metadata queries for every signal family
//!   with an empty result rather than `Internal: No table named 'logs'` —
//!   both before reconciliation has run (the read guard) and after it has
//!   (the provisioned tables).
//! - A reconciled dataset then accepts a real OTLP write into those
//!   pre-created tables: no duplicate table, no schema conflict, and the data
//!   is queryable afterwards.
//! - A dataset added to that tenant after startup converges on a later pass,
//!   with no restart, and is queryable too.

use acceptor::handler::WalManager;
use acceptor::handler::otlp_log_handler::LogHandler;
use acceptor::services::otlp_log_service::LogAcceptorService;
use arrow_flight::Ticket;
use common::CatalogManager;
use common::auth::{TenantContext, TenantSource};
use common::catalog::Catalog;
use common::config::{Configuration, QuerierConfig, WriterConfig};
use common::flight::decode::flight_data_vec_to_batches;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use futures::StreamExt;
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
use writer::{IcebergWriterFlightService, TableReconciler};

const TENANT: &str = "fresh-tenant";
const DATASET: &str = "production";
const LATE_DATASET: &str = "staging";
const SERVICE_NAME: &str = "fresh-service";
const LOG_BODY: &str = "first telemetry into a pre-provisioned dataset";

/// Every signal table the deployment default enables.
const ALL_SIGNAL_TABLES: usize = 8;

struct TestServices {
    flight_transport: Arc<InMemoryFlightTransport>,
    catalog_manager: Arc<CatalogManager>,
    tenant_source: Arc<Catalog>,
    acceptor_addr: std::net::SocketAddr,
    _object_store: Arc<dyn ObjectStore>,
    _temp_dir: TempDir,
    _writer_bootstrap: ServiceBootstrap,
    _querier_bootstrap: ServiceBootstrap,
}

/// Boot acceptor + writer + querier for a tenant registered ONLY in the
/// database catalog — created the way the admin API creates one, with a
/// `default_dataset` column and (deliberately) no dataset row — so nothing
/// about this test can fall back to a `[[auth.tenants]]` config entry.
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
    // Per-test on-disk catalog: the default "sqlite::memory:" is shared
    // across connections in one process and would collide across tests.
    let iceberg_catalog_db_path = temp_dir.path().join("iceberg_catalog.db");
    config.schema.catalog_uri = format!("sqlite://{}", iceberg_catalog_db_path.display());
    config.storage.dsn = storage_dsn.clone();
    config.auth.tenants = vec![];

    // Admin-API-shaped tenant creation: a tenant row naming a default
    // dataset, and no dataset row at all.
    let tenant_source = Arc::new(Catalog::new_in_memory().await.unwrap());
    tenant_source
        .upsert_tenant(TENANT, "Fresh Tenant", Some(DATASET), "database")
        .await
        .unwrap();

    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
        tenant_id: TENANT.to_string(),
        dataset_id: DATASET.to_string(),
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

    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("Failed to create CatalogManager")
            .with_tenant_source(tenant_source.clone()),
    );

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

    // Acceptor: tests don't run the real auth stack, so a test interceptor
    // injects the TenantContext the Authenticator would have produced.
    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config.clone(),
        wal_config,
    ));
    let log_handler = LogHandler::new(flight_transport.clone(), wal_manager);
    let acceptor_service_with_auth = LogsServiceServer::with_interceptor(
        LogAcceptorService::new(log_handler),
        |mut req: tonic::Request<()>| {
            req.extensions_mut().insert(TenantContext {
                tenant_id: TENANT.to_string(),
                dataset_id: DATASET.to_string(),
                tenant_slug: TENANT.to_string(),
                dataset_slug: DATASET.to_string(),
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
        },
    );
    tokio::spawn(
        Server::builder()
            .add_service(acceptor_service_with_auth)
            .serve_with_incoming(TcpListenerStream::new(acceptor_listener)),
    );

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
        flight_transport,
        catalog_manager,
        tenant_source,
        acceptor_addr,
        _object_store: object_store,
        _temp_dir: temp_dir,
        _writer_bootstrap: writer_bootstrap,
        _querier_bootstrap: querier_bootstrap,
    }
}

/// Run a Flight `do_get` and return the decoded rows, failing the test with
/// the gRPC message if the query errors.
async fn query_rows(services: &TestServices, ticket: String) -> usize {
    let mut client = services
        .flight_transport
        .get_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .expect("no QueryExecution-capable Flight client");

    let response = timeout(
        Duration::from_secs(10),
        client.do_get(Ticket::new(ticket.clone())),
    )
    .await
    .expect("query timed out")
    .unwrap_or_else(|status| {
        panic!(
            "query `{ticket}` must succeed, got: {} ({:?})",
            status.message(),
            status.code()
        )
    });

    let mut stream = response.into_inner();
    let mut flight_data = Vec::new();
    while let Some(chunk) = stream.next().await {
        flight_data.push(chunk.expect("error reading flight data"));
    }
    let batches = flight_data_vec_to_batches(flight_data)
        .await
        .expect("failed to decode flight data");
    batches.iter().map(|b| b.num_rows()).sum()
}

/// The `query_logs_series` result, which is a single JSON row holding the
/// series array rather than one row per series.
async fn query_series_json(services: &TestServices, dataset: &str) -> serde_json::Value {
    let mut client = services
        .flight_transport
        .get_client_for_capability(ServiceCapability::QueryExecution)
        .await
        .expect("no QueryExecution-capable Flight client");

    let ticket = series_ticket(dataset);
    let response = timeout(
        Duration::from_secs(10),
        client.do_get(Ticket::new(ticket.clone())),
    )
    .await
    .expect("query timed out")
    .unwrap_or_else(|status| panic!("query `{ticket}` must succeed, got: {}", status.message()));

    let mut stream = response.into_inner();
    let mut flight_data = Vec::new();
    while let Some(chunk) = stream.next().await {
        flight_data.push(chunk.expect("error reading flight data"));
    }
    let batches = flight_data_vec_to_batches(flight_data)
        .await
        .expect("failed to decode flight data");

    use datafusion::arrow::array::{Array, StringArray};
    let batch = batches.first().expect("series result carries one batch");
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("series column is Utf8");
    assert_eq!(column.len(), 1, "series result is a single JSON row");
    serde_json::from_str(column.value(0)).expect("series payload is JSON")
}

/// One label/metadata/search ticket per signal family, over a wide window.
fn metadata_tickets(dataset: &str) -> Vec<String> {
    let start = 0i64;
    let end = 4_000_000_000_000_000_000i64;
    vec![
        format!("query_logs_labels:{TENANT}:{dataset}:{start}:{end}"),
        format!("query_metric_labels:{TENANT}:{dataset}:{start}:{end}"),
        format!("label_names:{TENANT}:{dataset}"),
        format!("profile_types:{TENANT}:{dataset}"),
        format!(
            "search_traces:{TENANT}:{dataset}:{}",
            serde_json::json!({ "limit": 10 })
        ),
    ]
}

/// `query_logs_series` is JSON-shaped, so it is checked separately from the
/// row-shaped metadata tickets above.
fn series_ticket(dataset: &str) -> String {
    let params = serde_json::json!({
        "selector": format!("{{service_name=\"{SERVICE_NAME}\"}}"),
        "start": 0i64,
        "end": 4_000_000_000_000_000_000i64,
    });
    format!("query_logs_series:{TENANT}:{dataset}:{params}")
}

async fn tables_in(services: &TestServices, dataset: &str) -> Vec<String> {
    let Ok(namespace) = services.catalog_manager.build_namespace(TENANT, dataset) else {
        return Vec::new();
    };
    let mut names: Vec<String> = services
        .catalog_manager
        .catalog()
        .list_tabulars(&namespace)
        .await
        .unwrap_or_default()
        .iter()
        .map(|identifier| identifier.name().to_string())
        .collect();
    names.sort();
    names
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
                    value: Some(string_value(SERVICE_NAME)),
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

/// Task 6.1 — a tenant created through the admin API, with nothing ingested,
/// answers every signal family's metadata query with an empty success.
#[tokio::test]
async fn fresh_tenant_answers_every_signal_with_empty_results() {
    let services = setup_services().await;

    // Before reconciliation: no tables exist at all. This is the read guard.
    assert!(tables_in(&services, DATASET).await.is_empty());
    for ticket in metadata_tickets(DATASET) {
        assert_eq!(
            query_rows(&services, ticket.clone()).await,
            0,
            "`{ticket}` must return no rows"
        );
    }
    assert_eq!(
        query_series_json(&services, DATASET).await,
        serde_json::json!([]),
        "a dataset with no logs table must report no series"
    );

    // After reconciliation: the tables exist, and the same queries still
    // succeed — now by scanning real, empty tables. They no longer have to
    // return *nothing*: an existing logs table reports its well-known label
    // names, which is the same answer any empty-but-present table gives.
    let reconciler = TableReconciler::new(services.catalog_manager.clone());
    reconciler.run_pass().await.expect("reconcile pass");

    assert_eq!(
        tables_in(&services, DATASET).await.len(),
        ALL_SIGNAL_TABLES,
        "the tenant's default dataset must be provisioned from the tenant row alone"
    );
    for ticket in metadata_tickets(DATASET) {
        // No panic from `query_rows` means the query succeeded.
        query_rows(&services, ticket).await;
    }
    assert_eq!(
        query_series_json(&services, DATASET).await,
        serde_json::json!([]),
        "a provisioned-but-empty logs table still has no series"
    );
}

/// Task 6.2 — a reconciled dataset accepts a real OTLP write into the tables
/// provisioning created: no duplicate table, no schema conflict, queryable.
#[tokio::test]
async fn reconciled_dataset_accepts_a_later_write_into_its_tables() {
    let services = setup_services().await;

    let reconciler = TableReconciler::new(services.catalog_manager.clone());
    reconciler.run_pass().await.expect("reconcile pass");
    let provisioned = tables_in(&services, DATASET).await;
    assert_eq!(provisioned.len(), ALL_SIGNAL_TABLES);

    send_test_log(&services).await;
    common::testing::flush_storage_writers(&services.flight_transport, TENANT, None)
        .await
        .expect("writer flush failed");

    // The write went into the pre-created tables: the table set is unchanged.
    assert_eq!(
        tables_in(&services, DATASET).await,
        provisioned,
        "ingest must reuse the provisioned tables, not create new ones"
    );

    // ...and the data is queryable.
    let params = serde_json::json!({
        "query": format!("{{service_name=\"{SERVICE_NAME}\"}}"),
        "start": 1_699_999_000_000_000_000i64,
        "end": 1_700_001_000_000_000_000i64,
        "limit": 100,
        "direction": "forward",
    });
    let rows = query_rows(
        &services,
        format!(
            "query_logs:{TENANT}:{DATASET}:{}",
            serde_json::to_string(&params).unwrap()
        ),
    )
    .await;
    assert!(rows > 0, "the ingested log must be queryable");
}

/// Task 6.3 — a dataset created after startup converges on a later pass, with
/// no restart, and is then queryable.
#[tokio::test]
async fn dataset_created_after_startup_converges_and_is_queryable() {
    let services = setup_services().await;

    let reconciler = TableReconciler::new(services.catalog_manager.clone());
    reconciler.run_pass().await.expect("startup pass");
    assert!(
        tables_in(&services, LATE_DATASET).await.is_empty(),
        "the late dataset does not exist yet"
    );

    // The admin API adds a dataset to the running deployment.
    services
        .tenant_source
        .create_dataset(TENANT, LATE_DATASET)
        .await
        .unwrap();

    // No restart: the next periodic pass picks it up.
    let summary = reconciler.run_pass().await.expect("later pass");
    assert_eq!(summary.tables_created, ALL_SIGNAL_TABLES);
    assert_eq!(
        tables_in(&services, LATE_DATASET).await.len(),
        ALL_SIGNAL_TABLES
    );

    // ...and the querier — already running, never restarted — serves it.
    for ticket in metadata_tickets(LATE_DATASET) {
        query_rows(&services, ticket).await;
    }
    assert_eq!(
        query_series_json(&services, LATE_DATASET).await,
        serde_json::json!([])
    );
}
