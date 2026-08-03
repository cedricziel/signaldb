//! Integration tests for the router's operational-control proxy
//! (`/api/v1/ops/*`).
//!
//! These exercise the admin-auth gate and the no-compactor path deterministically
//! (no compactor is registered, so the proxy surfaces `503`). The compactor's
//! own `do_action` behavior is covered by the compactor crate; the router side
//! is a thin admin-gated forward.

use std::sync::Arc;

use common::catalog::Catalog;
use common::config::Configuration;
use router::RouterState;
use router::discovery::ServiceRegistry;
use tempfile::TempDir;
use tokio::net::TcpListener;

const ADMIN_KEY: &str = "admin-key-123";

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
    for path in [
        "/api/v1/ops/compact",
        "/api/v1/ops/compact/dry-run",
        "/api/v1/ops/compact/status",
    ] {
        let is_status = path.ends_with("/status");
        let client = reqwest::Client::new();
        let req = if is_status {
            client.get(format!("{base}{path}"))
        } else {
            client.post(format!("{base}{path}"))
        };
        let resp = req
            .bearer_auth(ADMIN_KEY)
            .send()
            .await
            .expect("request sent");
        // Authorized, but no compactor is registered → 503.
        assert_eq!(
            resp.status().as_u16(),
            503,
            "{path} should be 503 with no compactor, got {}",
            resp.status()
        );
    }
}
