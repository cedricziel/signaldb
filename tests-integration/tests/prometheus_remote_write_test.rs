//! Integration tests for Prometheus remote_write endpoint
//!
//! Tests the full flow: HTTP request → decode → convert to OTEL → WAL → response

use acceptor::handler::WalManager;
use acceptor::prometheus_router;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use common::auth::Authenticator;
use common::config::Configuration;
use common::flight::conversion::{
    PrometheusLabel, PrometheusMetricMetadata, PrometheusMetricType, PrometheusSample,
    PrometheusTimeSeries, PrometheusWriteRequest,
};
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::ServiceExt;

/// Encode a PrometheusWriteRequest to snappy-compressed protobuf bytes
fn encode_remote_write_request(request: &PrometheusWriteRequest) -> Vec<u8> {
    // Convert to proto format
    let proto_request = common::flight::conversion::conversion_prometheus::proto::WriteRequest {
        timeseries: request
            .timeseries
            .iter()
            .map(
                |ts| common::flight::conversion::conversion_prometheus::proto::TimeSeries {
                    labels: ts
                        .labels
                        .iter()
                        .map(
                            |l| common::flight::conversion::conversion_prometheus::proto::Label {
                                name: l.name.clone(),
                                value: l.value.clone(),
                            },
                        )
                        .collect(),
                    samples: ts
                        .samples
                        .iter()
                        .map(
                            |s| common::flight::conversion::conversion_prometheus::proto::Sample {
                                value: s.value,
                                timestamp: s.timestamp,
                            },
                        )
                        .collect(),
                    exemplars: vec![],
                    histograms: vec![],
                },
            )
            .collect(),
        metadata: request
            .metadata
            .iter()
            .map(
                |m| common::flight::conversion::conversion_prometheus::proto::MetricMetadata {
                    r#type: match m.metric_type {
                        PrometheusMetricType::Counter => 1,
                        PrometheusMetricType::Gauge => 2,
                        PrometheusMetricType::Summary => 3,
                        PrometheusMetricType::Histogram => 4,
                        _ => 0,
                    },
                    metric_family_name: m.metric_family_name.clone(),
                    help: m.help.clone(),
                    unit: m.unit.clone(),
                },
            )
            .collect(),
    };

    // Encode to protobuf
    let mut proto_bytes = Vec::new();
    proto_request.encode(&mut proto_bytes).unwrap();

    // Compress with snappy
    snap::raw::Encoder::new()
        .compress_vec(&proto_bytes)
        .unwrap()
}

/// Set up test infrastructure for Prometheus endpoint testing
async fn setup_prometheus_test() -> (axum::Router, TempDir) {
    let temp_dir = TempDir::new().unwrap();

    // Set up catalog
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: Duration::from_secs(30),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(300),
    });

    config.schema = common::config::SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: catalog_dsn,
        default_schemas: common::config::DefaultSchemas::default(),
    };

    // Configure test tenant
    config.auth = common::config::AuthConfig {
        enabled: true,
        admin_api_key: None,
        tenants: vec![common::config::TenantConfig {
            id: "test-tenant".to_string(),
            slug: "test-tenant".to_string(),
            name: "Test Tenant".to_string(),
            default_dataset: Some("metrics".to_string()),
            datasets: vec![common::config::DatasetConfig {
                id: "metrics".to_string(),
                slug: "metrics".to_string(),
                is_default: true,
            }],
            api_keys: vec![common::config::ApiKeyConfig {
                key: "test-api-key".to_string(),
                name: Some("Test Key".to_string()),
            }],
            schema_config: None,
        }],
    };

    // Initialize service bootstrap
    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:4317".to_string(),
    )
    .await
    .expect("Failed to initialize service bootstrap");

    let catalog = Arc::new(service_bootstrap.catalog().clone());
    let auth_config = service_bootstrap.config().auth.clone();

    // Initialize Flight transport
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Initialize WAL manager
    let wal_dir = temp_dir.path().join("wal");
    let traces_wal_config = WalConfig::with_defaults(wal_dir.clone());
    let logs_wal_config = WalConfig::with_defaults(wal_dir.clone());
    let metrics_wal_config = WalConfig::with_defaults(wal_dir.clone());
    let wal_manager = Arc::new(WalManager::new(
        traces_wal_config,
        logs_wal_config,
        metrics_wal_config,
    ));

    // Create authenticator
    let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

    // Create Prometheus handler
    let prometheus_handler = Arc::new(acceptor::handler::PrometheusHandler::new(
        flight_transport.clone(),
        wal_manager.clone(),
    ));

    // Build router
    let app = prometheus_router(authenticator, prometheus_handler);

    (app, temp_dir)
}

#[tokio::test]
async fn test_prometheus_remote_write_gauge() {
    let (app, _temp_dir) = setup_prometheus_test().await;

    // Create a simple gauge metric
    let request = PrometheusWriteRequest {
        timeseries: vec![PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "temperature_celsius".to_string(),
                },
                PrometheusLabel {
                    name: "job".to_string(),
                    value: "sensors".to_string(),
                },
                PrometheusLabel {
                    name: "location".to_string(),
                    value: "room1".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 23.5,
                timestamp: chrono::Utc::now().timestamp_millis(),
            }],
            histograms: vec![],
        }],
        metadata: vec![],
    };

    let body = encode_remote_write_request(&request);

    let http_request = Request::builder()
        .method("POST")
        .uri("/api/v1/write")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "snappy")
        .header("Authorization", "Bearer test-api-key")
        .header("X-Tenant-ID", "test-tenant")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(http_request).await.unwrap();

    // Prometheus expects 204 No Content on success
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "Expected 204 No Content for successful remote_write"
    );
}

#[tokio::test]
async fn test_prometheus_remote_write_counter() {
    let (app, _temp_dir) = setup_prometheus_test().await;

    // Create a counter metric with _total suffix
    let request = PrometheusWriteRequest {
        timeseries: vec![PrometheusTimeSeries {
            labels: vec![
                PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_requests_total".to_string(),
                },
                PrometheusLabel {
                    name: "job".to_string(),
                    value: "api".to_string(),
                },
                PrometheusLabel {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
            ],
            samples: vec![PrometheusSample {
                value: 1234.0,
                timestamp: chrono::Utc::now().timestamp_millis(),
            }],
            histograms: vec![],
        }],
        metadata: vec![PrometheusMetricMetadata {
            metric_family_name: "http_requests".to_string(),
            metric_type: PrometheusMetricType::Counter,
            help: "Total HTTP requests".to_string(),
            unit: String::new(),
        }],
    };

    let body = encode_remote_write_request(&request);

    let http_request = Request::builder()
        .method("POST")
        .uri("/api/v1/write")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "snappy")
        .header("Authorization", "Bearer test-api-key")
        .header("X-Tenant-ID", "test-tenant")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(http_request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_prometheus_remote_write_histogram() {
    let (app, _temp_dir) = setup_prometheus_test().await;

    let now = chrono::Utc::now().timestamp_millis();

    // Create histogram series (bucket, count, sum)
    let request = PrometheusWriteRequest {
        timeseries: vec![
            PrometheusTimeSeries {
                labels: vec![
                    PrometheusLabel {
                        name: "__name__".to_string(),
                        value: "http_request_duration_seconds_bucket".to_string(),
                    },
                    PrometheusLabel {
                        name: "le".to_string(),
                        value: "0.1".to_string(),
                    },
                ],
                samples: vec![PrometheusSample {
                    value: 100.0,
                    timestamp: now,
                }],
                histograms: vec![],
            },
            PrometheusTimeSeries {
                labels: vec![
                    PrometheusLabel {
                        name: "__name__".to_string(),
                        value: "http_request_duration_seconds_bucket".to_string(),
                    },
                    PrometheusLabel {
                        name: "le".to_string(),
                        value: "+Inf".to_string(),
                    },
                ],
                samples: vec![PrometheusSample {
                    value: 150.0,
                    timestamp: now,
                }],
                histograms: vec![],
            },
            PrometheusTimeSeries {
                labels: vec![PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_request_duration_seconds_count".to_string(),
                }],
                samples: vec![PrometheusSample {
                    value: 150.0,
                    timestamp: now,
                }],
                histograms: vec![],
            },
            PrometheusTimeSeries {
                labels: vec![PrometheusLabel {
                    name: "__name__".to_string(),
                    value: "http_request_duration_seconds_sum".to_string(),
                }],
                samples: vec![PrometheusSample {
                    value: 12.5,
                    timestamp: now,
                }],
                histograms: vec![],
            },
        ],
        metadata: vec![],
    };

    let body = encode_remote_write_request(&request);

    let http_request = Request::builder()
        .method("POST")
        .uri("/api/v1/write")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "snappy")
        .header("Authorization", "Bearer test-api-key")
        .header("X-Tenant-ID", "test-tenant")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(http_request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_prometheus_remote_write_empty_request() {
    let (app, _temp_dir) = setup_prometheus_test().await;

    // Empty request should succeed (no data to process)
    let request = PrometheusWriteRequest {
        timeseries: vec![],
        metadata: vec![],
    };

    let body = encode_remote_write_request(&request);

    let http_request = Request::builder()
        .method("POST")
        .uri("/api/v1/write")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "snappy")
        .header("Authorization", "Bearer test-api-key")
        .header("X-Tenant-ID", "test-tenant")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(http_request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_prometheus_remote_write_invalid_auth() {
    let (app, _temp_dir) = setup_prometheus_test().await;

    let request = PrometheusWriteRequest {
        timeseries: vec![PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "test_metric".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 1.0,
                timestamp: chrono::Utc::now().timestamp_millis(),
            }],
            histograms: vec![],
        }],
        metadata: vec![],
    };

    let body = encode_remote_write_request(&request);

    let http_request = Request::builder()
        .method("POST")
        .uri("/api/v1/write")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "snappy")
        .header("Authorization", "Bearer invalid-key")
        .header("X-Tenant-ID", "test-tenant")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(http_request).await.unwrap();

    // Should fail with 401 Unauthorized
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_prometheus_remote_write_missing_auth() {
    let (app, _temp_dir) = setup_prometheus_test().await;

    let request = PrometheusWriteRequest {
        timeseries: vec![PrometheusTimeSeries {
            labels: vec![PrometheusLabel {
                name: "__name__".to_string(),
                value: "test_metric".to_string(),
            }],
            samples: vec![PrometheusSample {
                value: 1.0,
                timestamp: chrono::Utc::now().timestamp_millis(),
            }],
            histograms: vec![],
        }],
        metadata: vec![],
    };

    let body = encode_remote_write_request(&request);

    // No Authorization header
    let http_request = Request::builder()
        .method("POST")
        .uri("/api/v1/write")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "snappy")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(http_request).await.unwrap();

    // Should fail - 400 Bad Request for missing required Authorization header
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_prometheus_remote_write_invalid_body() {
    let (app, _temp_dir) = setup_prometheus_test().await;

    // Send invalid data (not valid snappy/protobuf)
    let http_request = Request::builder()
        .method("POST")
        .uri("/api/v1/write")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header(header::CONTENT_ENCODING, "snappy")
        .header("Authorization", "Bearer test-api-key")
        .header("X-Tenant-ID", "test-tenant")
        .body(Body::from(vec![0, 1, 2, 3, 4, 5]))
        .unwrap();

    let response = app.oneshot(http_request).await.unwrap();

    // Should fail with 400 Bad Request (decode error)
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
