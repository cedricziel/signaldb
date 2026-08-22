//! Regression tests for finding M1 (acceptor whole-crate review): a
//! `serve_otlp_grpc` / `serve_otlp_http` startup failure must be returned
//! as an `Err`, not a panic, and `init_tx` must only fire *after* a
//! successful bind — never before, which would let the CLI log "listening"
//! for a port that was never actually bound.

use std::sync::Arc;
use std::time::Duration;

use acceptor::handler::WalManager;
use acceptor::{AcceptorResources, GrpcAcceptorConfig, HttpAcceptorConfig};
use common::auth::Authenticator;
use common::config::Configuration;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// Build [`AcceptorResources`] cheaply (in-memory SQLite catalog, no real
/// storage backend) — enough to exercise the gRPC/HTTP server bootstrap
/// code without going through the full `init_acceptor_resources` Iceberg
/// setup this test does not need.
async fn test_resources(temp_dir: &TempDir) -> AcceptorResources {
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let catalog_dsn = format!("sqlite://{}", catalog_db_path.display());

    let mut config = Configuration::default();
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn,
        heartbeat_interval: Duration::from_secs(30),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(300),
    });

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

    AcceptorResources {
        flight_transport,
        wal_manager,
        authenticator,
        rate_limiter,
        storage_usage,
    }
}

/// Occupy a port and return its address without releasing the port, so a
/// subsequent bind to the same address fails with "address in use".
async fn bind_and_hold_port() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_bind_failure_is_returned_as_error_not_a_panic() {
    let temp_dir = TempDir::new().unwrap();
    let resources = test_resources(&temp_dir).await;

    // Hold the port for the whole test so the acceptor's own bind fails.
    let (_held_listener, addr) = bind_and_hold_port().await;

    let (init_tx, init_rx) = tokio::sync::oneshot::channel();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, _stopped_rx) = tokio::sync::oneshot::channel();

    let result = acceptor::serve_otlp_grpc(
        GrpcAcceptorConfig {
            addr,
            resources,
            max_decoding_message_size: 64 * 1024 * 1024,
        },
        init_tx,
        shutdown_rx,
        stopped_tx,
    )
    .await;

    assert!(
        result.is_err(),
        "a bind failure must surface as Err, not a panic"
    );

    // init_tx must never have fired: the sender was dropped without a
    // send once the bind failed, so the receiver observes a RecvError
    // rather than resolving successfully.
    assert!(
        init_rx.await.is_err(),
        "init signal must not fire before a successful bind"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn http_bind_failure_is_returned_as_error_not_a_panic() {
    let temp_dir = TempDir::new().unwrap();
    let resources = test_resources(&temp_dir).await;

    let (_held_listener, addr) = bind_and_hold_port().await;

    let (init_tx, init_rx) = tokio::sync::oneshot::channel();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, _stopped_rx) = tokio::sync::oneshot::channel();

    let result = acceptor::serve_otlp_http(
        HttpAcceptorConfig {
            addr,
            flight_transport: resources.flight_transport,
            wal_manager: resources.wal_manager,
            authenticator: resources.authenticator,
            rate_limiter: resources.rate_limiter,
            storage_usage: resources.storage_usage,
            cors_allowed_origins: None,
            max_request_body_bytes: 64 * 1024 * 1024,
        },
        init_tx,
        shutdown_rx,
        stopped_tx,
    )
    .await;

    assert!(
        result.is_err(),
        "a bind failure must surface as Err, not a panic"
    );
    assert!(
        init_rx.await.is_err(),
        "init signal must not fire before a successful bind"
    );
}
