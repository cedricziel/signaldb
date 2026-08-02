//! Integration tests for the OTLP/HTTP profiles ingestion endpoint
//!
//! Covers the full flow: HTTP request -> auth middleware -> decode
//! (protobuf and JSON) -> profile handler -> WAL durability. The profiles
//! signal is served at the OTLP development path `/v1development/profiles`.

use std::sync::Arc;
use std::time::Duration;

use acceptor::handler::WalManager;
use acceptor::handler::otlp_profiles_handler::ProfileHandler;
use acceptor::profiles_http_router;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use common::auth::Authenticator;
use common::config::Configuration;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{WalConfig, WalOperation};
use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::profiles::v1development::{
    Function, Line, Location, Profile, ProfilesDictionary, ResourceProfiles, Sample, ScopeProfiles,
    Stack, ValueType,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use tempfile::TempDir;
use tower::ServiceExt;

const TEST_TENANT: &str = "test-tenant";
const TEST_DATASET: &str = "default";
const TEST_API_KEY: &str = "test-api-key";

/// Build a minimal but real OTLP profiles export request: a single resource
/// profile with one sample over a one-frame stack, resolved through a small
/// request-level dictionary.
fn sample_profiles_request() -> ExportProfilesServiceRequest {
    let dictionary = ProfilesDictionary {
        string_table: vec![
            String::new(),             // 0: required empty string
            "cpu".to_string(),         // 1
            "nanoseconds".to_string(), // 2
            "main".to_string(),        // 3
            "app.rs".to_string(),      // 4
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
            values: vec![100],
            ..Sample::default()
        }],
        time_unix_nano: 1_700_000_000_000_000_000,
        duration_nano: 10_000_000_000,
        period_type: Some(ValueType {
            type_strindex: 1,
            unit_strindex: 2,
        }),
        period: 10_000_000,
        profile_id: vec![1; 16],
        ..Profile::default()
    };

    ExportProfilesServiceRequest {
        resource_profiles: vec![ResourceProfiles {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("checkout".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
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

/// Set up the OTLP/HTTP profiles router with an authenticated test tenant.
///
/// Returns the router, the WAL manager (to verify durability), and the
/// temp dir keeping catalog/WAL files alive.
async fn setup_profiles_test() -> (axum::Router, Arc<WalManager>, TempDir) {
    let temp_dir = TempDir::new().unwrap();

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
        materialized_labels: Default::default(),
    };

    config.auth = common::config::AuthConfig {
        admin_api_key: None,
        internal_service_key: None,
        default_limits: Default::default(),
        storage_usage_refresh_interval: Duration::from_secs(60),
        tenants: vec![common::config::TenantConfig {
            id: TEST_TENANT.to_string(),
            slug: TEST_TENANT.to_string(),
            name: "Test Tenant".to_string(),
            default_dataset: Some(TEST_DATASET.to_string()),
            datasets: vec![common::config::DatasetConfig {
                id: TEST_DATASET.to_string(),
                slug: TEST_DATASET.to_string(),
                is_default: true,
                storage: None,
            }],
            api_keys: vec![common::config::ApiKeyConfig {
                key: TEST_API_KEY.to_string(),
                name: Some("Test Key".to_string()),
            }],
            schema_config: None,
            limits: None,
        }],
    };

    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:4317".to_string(),
    )
    .await
    .expect("Failed to initialize service bootstrap");

    let catalog = Arc::new(service_bootstrap.catalog().clone());
    let auth_config = service_bootstrap.config().auth.clone();

    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    let wal_dir = temp_dir.path().join("wal");
    let wal_manager = Arc::new(WalManager::new(
        WalConfig::with_defaults(wal_dir.clone()),
        WalConfig::with_defaults(wal_dir.clone()),
        WalConfig::with_defaults(wal_dir.clone()),
        WalConfig::with_defaults(wal_dir),
    ));

    let rate_limiter = Arc::new(common::ratelimit::TenantRateLimiter::from_auth_config(
        &auth_config,
    ));
    let storage_usage =
        Arc::new(common::storage_usage::StorageUsageTracker::from_auth_config(&auth_config));

    let authenticator = Arc::new(Authenticator::new(auth_config, catalog));

    let profile_handler = Arc::new(ProfileHandler::new(flight_transport, wal_manager.clone()));

    let app = profiles_http_router(authenticator, profile_handler, rate_limiter, storage_usage);

    (app, wal_manager, temp_dir)
}

/// Count WAL entries recorded for the test tenant's profiles WAL.
async fn profiles_wal_entry_count(wal_manager: &WalManager) -> usize {
    let wal = wal_manager
        .get_wal(TEST_TENANT, TEST_DATASET, "profiles")
        .await
        .expect("Failed to open profiles WAL");
    wal.get_entries()
        .await
        .expect("Failed to read WAL entries")
        .iter()
        .filter(|e| matches!(e.operation, WalOperation::WriteProfiles))
        .count()
}

#[tokio::test]
async fn otlp_http_profiles_protobuf_with_auth_lands_in_wal() {
    let (app, wal_manager, _temp_dir) = setup_profiles_test().await;

    let body = sample_profiles_request().encode_to_vec();

    let request = Request::builder()
        .method("POST")
        .uri("/v1development/profiles")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected 200 OK for authenticated protobuf profiles export"
    );
    assert_eq!(
        profiles_wal_entry_count(&wal_manager).await,
        1,
        "Expected the profiles export to be durably recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_profiles_json_with_auth_lands_in_wal() {
    let (app, wal_manager, _temp_dir) = setup_profiles_test().await;

    // OTLP/JSON (protojson) encoding of the profiles export request.
    let body = serde_json::to_vec(&sample_profiles_request()).unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/v1development/profiles")
        .header(header::CONTENT_TYPE, "application/json")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected 200 OK for authenticated JSON profiles export"
    );
    assert_eq!(
        profiles_wal_entry_count(&wal_manager).await,
        1,
        "Expected the profiles export to be durably recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_profiles_invalid_api_key_is_unauthorized() {
    let (app, wal_manager, _temp_dir) = setup_profiles_test().await;

    let body = sample_profiles_request().encode_to_vec();

    let request = Request::builder()
        .method("POST")
        .uri("/v1development/profiles")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", "Bearer wrong-key")
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "Expected 401 Unauthorized for an invalid API key"
    );
    assert_eq!(
        profiles_wal_entry_count(&wal_manager).await,
        0,
        "Rejected exports must not be recorded in the WAL"
    );
}

#[tokio::test]
async fn otlp_http_profiles_missing_auth_headers_is_rejected() {
    let (app, wal_manager, _temp_dir) = setup_profiles_test().await;

    let body = sample_profiles_request().encode_to_vec();

    // No Authorization / X-Tenant-ID headers at all.
    let request = Request::builder()
        .method("POST")
        .uri("/v1development/profiles")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "Expected 401 Unauthorized for a request without auth headers"
    );
    assert_eq!(profiles_wal_entry_count(&wal_manager).await, 0);
}

#[tokio::test]
async fn otlp_http_profiles_malformed_protobuf_is_bad_request() {
    let (app, wal_manager, _temp_dir) = setup_profiles_test().await;

    let request = Request::builder()
        .method("POST")
        .uri("/v1development/profiles")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(vec![0xff, 0xfe, 0xfd, 0xfc]))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request for a malformed protobuf payload"
    );
    assert_eq!(profiles_wal_entry_count(&wal_manager).await, 0);
}

#[tokio::test]
async fn otlp_http_profiles_malformed_json_is_bad_request() {
    let (app, wal_manager, _temp_dir) = setup_profiles_test().await;

    let request = Request::builder()
        .method("POST")
        .uri("/v1development/profiles")
        .header(header::CONTENT_TYPE, "application/json")
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from("{\"resourceProfiles\": \"not-an-array\"}"))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request for a malformed JSON payload"
    );
    assert_eq!(profiles_wal_entry_count(&wal_manager).await, 0);
}
