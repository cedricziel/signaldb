//! Pins the acceptor's per-route API-key scope enforcement (`auth_middleware`'s
//! `required_signal` match in `src/acceptor/src/middleware/auth.rs`).
//!
//! That match is a path-string lookup, not derived from the router
//! automatically: a new ingest route that forgets to appear in it would
//! silently skip scope enforcement. This test builds the full merged HTTP
//! app (the same five routers `serve_otlp_http` mounts) and a real
//! database-backed API key scoped to *only* `traces:write`, then asserts
//! every other ingest route rejects it with 403 while `/v1/traces` does
//! not — so if a new route is added without wiring its scope check here,
//! this test's route list stops matching the router's and calls out the
//! gap.

use std::sync::Arc;
use std::time::Duration;

use acceptor::handler::PrometheusHandler;
use acceptor::handler::WalManager;
use acceptor::handler::otlp_grpc::TraceHandler;
use acceptor::handler::otlp_log_handler::LogHandler;
use acceptor::handler::otlp_metrics_handler::MetricsHandler;
use acceptor::handler::otlp_profiles_handler::ProfileHandler;
use acceptor::{
    acceptor_router, logs_http_router, metrics_http_router, profiles_http_router,
    prometheus_router, traces_http_router,
};
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use common::auth::{Authenticator, TenantContext};
use common::catalog::Catalog;
use common::config::{AuthConfig, Configuration};
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use tempfile::TempDir;
use tower::ServiceExt;

const TEST_TENANT: &str = "test-tenant";
const TEST_DATASET: &str = "production";
const SCOPED_API_KEY: &str = "sk-scoped-to-traces-only";

/// Every path the acceptor's `auth_middleware` scope-checks today. Kept as
/// an explicit list (not derived from the router) so a route this test
/// forgets to add is a visible diff, same as the match in `auth.rs` itself.
const SCOPE_CHECKED_ROUTES: &[(&str, &str)] = &[
    ("/v1/traces", "traces"),
    ("/v1/logs", "logs"),
    ("/v1/metrics", "metrics"),
    ("/v1development/profiles", "profiles"),
    ("/api/v1/write", "metrics"),
];

/// Build the full merged HTTP app (matching `serve_otlp_http`'s route set)
/// with a real database-backed API key scoped to only `traces:write`.
async fn setup_scoped_app() -> (axum::Router, TempDir) {
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
    // No config.auth.tenants: the scoped key only exists in the catalog
    // (config-based keys have no scopes — see common::config::ApiKeyConfig),
    // so authentication is forced through the database path.
    config.auth = AuthConfig::default();

    let service_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Acceptor,
        "127.0.0.1:4317".to_string(),
    )
    .await
    .expect("Failed to initialize service bootstrap");

    let catalog: Arc<Catalog> = Arc::new(service_bootstrap.catalog().clone());
    let auth_config = service_bootstrap.config().auth.clone();

    catalog
        .upsert_tenant_with_default_dataset(
            TEST_TENANT,
            "Test Tenant",
            Some(TEST_DATASET),
            "database",
        )
        .await
        .expect("Failed to create tenant");

    let key_hash = Authenticator::hash_api_key(SCOPED_API_KEY);
    catalog
        .upsert_scoped_api_key(
            TEST_TENANT,
            &key_hash,
            Some("scoped-test-key"),
            None,
            Some(&["traces:write".to_string()]),
            None,
        )
        .await
        .expect("Failed to create scoped API key");

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

    let trace_handler = Arc::new(TraceHandler::new(
        flight_transport.clone(),
        wal_manager.clone(),
    ));
    let log_handler = Arc::new(LogHandler::new(
        flight_transport.clone(),
        wal_manager.clone(),
    ));
    let metrics_handler = Arc::new(MetricsHandler::new(
        flight_transport.clone(),
        wal_manager.clone(),
    ));
    let profile_handler = Arc::new(ProfileHandler::new(
        flight_transport.clone(),
        wal_manager.clone(),
    ));
    let prometheus_handler = Arc::new(PrometheusHandler::new(
        flight_transport.clone(),
        wal_manager.clone(),
    ));

    let app = acceptor_router()
        .merge(traces_http_router(
            authenticator.clone(),
            trace_handler,
            rate_limiter.clone(),
            storage_usage.clone(),
        ))
        .merge(logs_http_router(
            authenticator.clone(),
            log_handler,
            rate_limiter.clone(),
            storage_usage.clone(),
        ))
        .merge(metrics_http_router(
            authenticator.clone(),
            metrics_handler,
            rate_limiter.clone(),
            storage_usage.clone(),
        ))
        .merge(prometheus_router(authenticator.clone(), prometheus_handler))
        .merge(profiles_http_router(
            authenticator,
            profile_handler,
            rate_limiter,
            storage_usage,
        ));

    (app, temp_dir)
}

fn request_for(path: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("Authorization", format!("Bearer {SCOPED_API_KEY}"))
        .header("X-Tenant-ID", TEST_TENANT)
        .body(Body::from(Vec::<u8>::new()))
        .unwrap()
}

#[tokio::test]
async fn traces_scoped_key_is_forbidden_from_every_other_signal() {
    let (app, _temp_dir) = setup_scoped_app().await;

    for (path, signal) in SCOPE_CHECKED_ROUTES {
        let response = app.clone().oneshot(request_for(path)).await.unwrap();

        if *signal == "traces" {
            assert_ne!(
                response.status(),
                StatusCode::FORBIDDEN,
                "a traces:write-scoped key must not be forbidden from {path}"
            );
        } else {
            assert_eq!(
                response.status(),
                StatusCode::FORBIDDEN,
                "a traces:write-scoped key must be forbidden from {path} (signal {signal})"
            );
        }
    }
}

/// Sanity check the fixture actually exercises a real scoped key, not an
/// unrestricted one that happens to pass every check.
#[tokio::test]
async fn scoped_key_resolves_to_the_expected_tenant_context() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());
    let catalog = Arc::new(Catalog::new(&catalog_dsn).await.unwrap());

    catalog
        .upsert_tenant_with_default_dataset(
            TEST_TENANT,
            "Test Tenant",
            Some(TEST_DATASET),
            "database",
        )
        .await
        .unwrap();
    let key_hash = Authenticator::hash_api_key(SCOPED_API_KEY);
    catalog
        .upsert_scoped_api_key(
            TEST_TENANT,
            &key_hash,
            Some("scoped-test-key"),
            None,
            Some(&["traces:write".to_string()]),
            None,
        )
        .await
        .unwrap();

    let authenticator = Authenticator::new(AuthConfig::default(), catalog);
    let ctx: TenantContext = authenticator
        .authenticate(SCOPED_API_KEY, TEST_TENANT, None)
        .await
        .unwrap();

    assert!(ctx.can_ingest("traces"));
    assert!(!ctx.can_ingest("logs"));
    assert!(!ctx.can_ingest("metrics"));
    assert!(!ctx.can_ingest("profiles"));
}
