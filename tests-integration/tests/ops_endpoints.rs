//! Integration tests for the router's operational-control proxy
//! (`/api/v1/ops/*`).
//!
//! These exercise the admin-auth gate, the no-compactor path (no compactor is
//! registered, so the proxy surfaces `503`), and the reachable-compactor path
//! through real capability discovery. The compactor's own `do_action` behavior
//! is covered by the compactor crate; the router side is a thin admin-gated
//! forward, so a stub Flight service stands in for it here.
//!
//! The RPC CLIENT span the router opens around `do_action` is covered
//! separately in `ops_endpoints_tracing.rs`, which needs process-global OTel
//! state and so lives in its own test binary.

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use common::catalog::Catalog;
use common::config::Configuration;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use futures::stream::{self, BoxStream};
use router::RouterState;
use router::discovery::ServiceRegistry;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

const ADMIN_KEY: &str = "admin-key-123";

/// Body the stub compactor answers every `do_action` with.
const STUB_STATUS_BODY: &str = r#"{"active_leases":[],"metrics":{"jobs_completed":7}}"#;

/// Minimal stand-in for the compactor's Flight control surface: answers
/// `do_action` with a fixed JSON body and nothing else.
struct StubCompactorFlightService;

#[tonic::async_trait]
impl FlightService for StubCompactorFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let result = arrow_flight::Result {
            body: Bytes::from_static(STUB_STATUS_BODY.as_bytes()),
        };
        Ok(Response::new(Box::pin(stream::once(
            async move { Ok(result) },
        ))))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

#[derive(Clone)]
struct State {
    catalog: Catalog,
    service_registry: ServiceRegistry,
    config: Configuration,
    authenticator: Arc<common::auth::Authenticator>,
}
impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("State")
    }
}
impl RouterState for State {
    fn catalog(&self) -> &Catalog {
        &self.catalog
    }
    fn service_registry(&self) -> &ServiceRegistry {
        &self.service_registry
    }
    fn config(&self) -> &Configuration {
        &self.config
    }
    fn authenticator(&self) -> &Arc<common::auth::Authenticator> {
        &self.authenticator
    }
}

/// Serve the full router (no compactor registered) and return its base URL.
async fn serve_router() -> (String, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let catalog_dsn = format!("sqlite://{}", temp_dir.path().join("catalog.db").display());
    let catalog = Catalog::new(&catalog_dsn).await.unwrap();

    let mut config = Configuration::default();
    config.auth.admin_api_key = Some(ADMIN_KEY.to_string());

    let service_registry = ServiceRegistry::new(catalog.clone());
    let authenticator = Arc::new(common::auth::Authenticator::new(
        config.auth.clone(),
        Arc::new(catalog.clone()),
    ));
    let state = State {
        catalog,
        service_registry,
        config,
        authenticator,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router::create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Wait until the listener accepts connections.
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    (format!("http://{addr}"), temp_dir)
}

/// A registered, reachable compactor must be found by capability discovery and
/// its response forwarded verbatim.
///
/// This is the path that silently never worked: the compactor registered with
/// `StorageMaintenance`, but the catalog dropped the capability on read, so
/// discovery came up empty and every ops call 503'd regardless of deployment.
#[tokio::test]
async fn ops_compact_status_forwards_a_reachable_compactor_response() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_dsn = format!("sqlite://{}", temp_dir.path().join("catalog.db").display());

    let mut config = Configuration::default();
    config.auth.admin_api_key = Some(ADMIN_KEY.to_string());
    config.discovery = Some(common::config::DiscoveryConfig {
        dsn: catalog_dsn.clone(),
        heartbeat_interval: std::time::Duration::from_secs(5),
        poll_interval: std::time::Duration::from_secs(60),
        ttl: std::time::Duration::from_secs(30),
    });

    let catalog = Catalog::new(&catalog_dsn).await.unwrap();

    // Serve the stub compactor and register it exactly as the real one does.
    let compactor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let compactor_addr = compactor_listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            // Same builder the real compactor uses, so the stub negotiates
            // compression identically to production.
            .add_service(common::flight::flight_service_server(
                StubCompactorFlightService,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                compactor_listener,
            ))
            .await
            .unwrap();
    });
    catalog
        .register_ingester(
            Uuid::new_v4(),
            &compactor_addr.to_string(),
            ServiceType::Compactor,
            &[ServiceCapability::StorageMaintenance],
        )
        .await
        .expect("compactor registered");

    let router_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Router,
        "127.0.0.1:50053".to_string(),
    )
    .await
    .expect("router bootstrap");
    let service_registry = ServiceRegistry::with_flight_transport(
        catalog.clone(),
        InMemoryFlightTransport::new(router_bootstrap),
    );

    let authenticator = Arc::new(common::auth::Authenticator::new(
        config.auth.clone(),
        Arc::new(catalog.clone()),
    ));
    let state = State {
        catalog,
        service_registry,
        config,
        authenticator,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router::create_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/v1/ops/compact/status"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await
        .expect("request sent");

    let status = resp.status();
    let body = resp.text().await.expect("body");
    assert_eq!(
        status.as_u16(),
        200,
        "/api/v1/ops/compact/status should reach the registered compactor, got {status}: {body}"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).expect("json body"),
        serde_json::from_str::<serde_json::Value>(STUB_STATUS_BODY).unwrap(),
        "the compactor's response must be forwarded verbatim"
    );
}

#[tokio::test]
async fn ops_compact_requires_admin_auth() {
    let (base, _tmp) = serve_router().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/ops/compact"))
        .send()
        .await
        .expect("request sent");
    // No admin credential → rejected by the admin-auth layer.
    assert!(
        resp.status().is_client_error(),
        "expected 4xx without admin auth, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn ops_compact_without_compactor_is_unavailable() {
    let (base, _tmp) = serve_router().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/ops/compact"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await
        .expect("request sent");
    // Authorized, but no compactor is registered → 503.
    assert_eq!(
        resp.status().as_u16(),
        503,
        "/api/v1/ops/compact should be 503 with no compactor, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn ops_compact_dry_run_without_compactor_is_unavailable() {
    let (base, _tmp) = serve_router().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/ops/compact/dry-run"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await
        .expect("request sent");
    // Authorized, but no compactor is registered → 503.
    assert_eq!(
        resp.status().as_u16(),
        503,
        "/api/v1/ops/compact/dry-run should be 503 with no compactor, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn ops_compact_status_without_compactor_is_unavailable() {
    let (base, _tmp) = serve_router().await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/ops/compact/status"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await
        .expect("request sent");
    // Authorized, but no compactor is registered → 503.
    assert_eq!(
        resp.status().as_u16(),
        503,
        "/api/v1/ops/compact/status should be 503 with no compactor, got {}",
        resp.status()
    );
}
