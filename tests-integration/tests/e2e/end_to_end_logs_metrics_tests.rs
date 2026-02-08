use acceptor::handler::otlp_log_handler::LogHandler;
use acceptor::handler::otlp_metrics_handler::MetricsHandler;
use acceptor::handler::WalManager;
use arrow_flight::flight_service_server::FlightServiceServer;
use common::CatalogManager;
use common::auth::{TenantContext, TenantSource};
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{Wal, WalConfig};
use futures::TryStreamExt;
use object_store::{local::LocalFileSystem, ObjectStore};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest,
        metrics::v1::ExportMetricsServiceRequest,
    },
    common::v1::{any_value::Value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        metric::Data, number_data_point, AggregationTemporality, Gauge, Histogram,
        HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    },
    resource::v1::Resource,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::transport::Server;
use writer::IcebergWriterFlightService;

struct TestServices {
    object_store: Arc<dyn ObjectStore>,
    log_handler: LogHandler,
    metrics_handler: MetricsHandler,
    _temp_dir: TempDir,
}

fn test_tenant_context() -> TenantContext {
    TenantContext {
        tenant_id: "test-tenant".to_string(),
        dataset_id: "test-dataset".to_string(),
        tenant_slug: "test-tenant".to_string(),
        dataset_slug: "test-dataset".to_string(),
        api_key_name: Some("test-key".to_string()),
        source: TenantSource::Config,
    }
}

async fn wait_for_storage_service(
    flight_transport: &InMemoryFlightTransport,
    timeout_duration: Duration,
) -> Result<(), String> {
    let start = std::time::Instant::now();

    loop {
        let storage_services = flight_transport
            .discover_services_by_capability(ServiceCapability::Storage)
            .await;

        if !storage_services.is_empty() {
            return Ok(());
        }

        if start.elapsed() >= timeout_duration {
            return Err(format!(
                "Timed out waiting for storage service after {timeout_duration:?}"
            ));
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn setup_logs_metrics_services() -> TestServices {
    let temp_dir = TempDir::new().unwrap();
    let wal_config = WalConfig {
        wal_dir: PathBuf::from(temp_dir.path()),
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 1,
        flush_interval_secs: 1,
        tenant_id: "default".to_string(),
        dataset_id: "default".to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    };

    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&storage_dir).unwrap());

    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.storage.dsn = format!("file://{}", storage_dir.display());
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn,
        heartbeat_interval: Duration::from_secs(30),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(300),
    });

    let acceptor_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:50060".to_string(),
    )
    .await
    .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(acceptor_bootstrap));

    let writer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    drop(writer_listener);

    let writer_wal = Arc::new(Wal::new(wal_config.clone()).await.unwrap());
    let writer_catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("Failed to create CatalogManager for writer"),
    );
    let writer_service =
        IcebergWriterFlightService::new(writer_catalog_manager, object_store.clone(), writer_wal);
    let _bg = writer_service.start_background_processing();
    let writer_server = Server::builder()
        .add_service(FlightServiceServer::new(writer_service))
        .serve(writer_addr);
    tokio::spawn(writer_server);

    let _writer_bootstrap =
        ServiceBootstrap::new(config.clone(), ServiceType::Writer, writer_addr.to_string())
            .await
            .unwrap();

    wait_for_storage_service(&flight_transport, Duration::from_secs(10))
        .await
        .expect("writer service registration should complete");

    let wal_manager = Arc::new(WalManager::new(
        wal_config.clone(),
        wal_config.clone(),
        wal_config,
    ));

    let log_handler = LogHandler::new(flight_transport.clone(), wal_manager.clone());
    let metrics_handler = MetricsHandler::new(flight_transport, wal_manager);

    TestServices {
        object_store,
        log_handler,
        metrics_handler,
        _temp_dir: temp_dir,
    }
}

fn make_resource(service_name: &str) -> Resource {
    Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(service_name.to_string())),
            }),
        }],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    }
}

fn build_logs_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(make_resource("test-service")),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000_000_000,
                    observed_time_unix_nano: 1_000_000_000,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("Test log message".to_string())),
                    }),
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
    }
}

fn build_gauge_metrics_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(make_resource("test-service")),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "test.gauge".to_string(),
                    description: "A test gauge".to_string(),
                    unit: "1".to_string(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 1_000_000_000,
                            time_unix_nano: 2_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(42.0)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn build_mixed_metrics_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(make_resource("test-service")),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![
                    Metric {
                        name: "test.gauge".to_string(),
                        description: "A test gauge".to_string(),
                        unit: "1".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1_000_000_000,
                                time_unix_nano: 2_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(42.0)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                        })),
                        metadata: vec![],
                    },
                    Metric {
                        name: "test.sum".to_string(),
                        description: "A test sum".to_string(),
                        unit: "1".to_string(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1_000_000_000,
                                time_unix_nano: 2_000_000_000,
                                value: Some(number_data_point::Value::AsInt(100)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative.into(),
                            is_monotonic: true,
                        })),
                        metadata: vec![],
                    },
                    Metric {
                        name: "test.histogram".to_string(),
                        description: "A test histogram".to_string(),
                        unit: "ms".to_string(),
                        data: Some(Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1_000_000_000,
                                time_unix_nano: 2_000_000_000,
                                count: 3,
                                sum: Some(15.0),
                                bucket_counts: vec![1, 1, 1],
                                explicit_bounds: vec![5.0, 10.0],
                                exemplars: vec![],
                                flags: 0,
                                min: Some(1.0),
                                max: Some(9.0),
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative.into(),
                        })),
                        metadata: vec![],
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

async fn object_locations(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
    objects
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

async fn wait_for_object_locations(
    store: &Arc<dyn ObjectStore>,
    timeout_duration: Duration,
) -> Vec<String> {
    let start = std::time::Instant::now();

    loop {
        let locations = object_locations(store).await;
        if !locations.is_empty() {
            return locations;
        }

        if start.elapsed() >= timeout_duration {
            return locations;
        }

        sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
async fn test_logs_ingestion_and_persistence() {
    let services = setup_logs_metrics_services().await;
    let tenant_context = test_tenant_context();
    let log_request = build_logs_request();

    services
        .log_handler
        .handle_grpc_otlp_logs(&tenant_context, log_request)
        .await;

    let locations = wait_for_object_locations(&services.object_store, Duration::from_secs(20)).await;
    assert!(!locations.is_empty(), "expected persisted objects for logs");
    assert!(
        locations.iter().any(|location| location.contains("logs")),
        "expected at least one logs object path, found: {locations:?}"
    );
}

#[tokio::test]
async fn test_metrics_gauge_ingestion_and_persistence() {
    let services = setup_logs_metrics_services().await;
    let tenant_context = test_tenant_context();
    let metrics_request = build_gauge_metrics_request();

    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&tenant_context, metrics_request)
        .await;

    let locations = wait_for_object_locations(&services.object_store, Duration::from_secs(20)).await;
    assert!(
        locations
            .iter()
            .any(|location| location.contains("metrics_gauge")),
        "expected metrics_gauge object path, found: {locations:?}"
    );
}

#[tokio::test]
async fn test_metrics_mixed_types_ingestion() {
    let services = setup_logs_metrics_services().await;
    let tenant_context = test_tenant_context();
    let metrics_request = build_mixed_metrics_request();

    services
        .metrics_handler
        .handle_grpc_otlp_metrics(&tenant_context, metrics_request)
        .await;

    let locations = wait_for_object_locations(&services.object_store, Duration::from_secs(20)).await;
    assert!(
        locations
            .iter()
            .any(|location| location.contains("metrics_gauge")),
        "expected metrics_gauge object path, found: {locations:?}"
    );
    assert!(
        locations
            .iter()
            .any(|location| location.contains("metrics_sum")),
        "expected metrics_sum object path, found: {locations:?}"
    );
    assert!(
        locations
            .iter()
            .any(|location| location.contains("metrics_histogram")),
        "expected metrics_histogram object path, found: {locations:?}"
    );
}
