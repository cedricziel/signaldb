//! End-to-end verification (through the real Flight `do_get` entry point) that a
//! tenant created via the admin API — i.e. a `source = "database"` tenant with
//! no `[[auth.tenants]]` config block — is queryable with no restart. Before
//! this change every query for such a tenant failed with
//! `failed to resolve catalog: <tenant>`; the querier now resolves and registers
//! the tenant's catalog on demand from the tenant registry.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::Ticket;
use arrow_flight::flight_service_server::FlightService;
use common::CatalogManager;
use common::catalog::Catalog;
use common::config::{Configuration, DatabaseConfig, DiscoveryConfig, QuerierConfig};
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use querier::QuerierFlightService;
use tonic::Request;

#[tokio::test]
async fn database_tenant_is_queryable_without_restart() {
    // A shared database catalog that starts with NO tenants, so nothing is
    // registered when the querier is constructed. This forces the on-demand
    // (lazy) registration path rather than startup enumeration.
    let source = Arc::new(Catalog::new_in_memory().await.unwrap());

    let config = Configuration {
        database: DatabaseConfig {
            dsn: "sqlite::memory:".to_string(),
        },
        discovery: Some(DiscoveryConfig {
            dsn: "sqlite::memory:".to_string(),
            heartbeat_interval: Duration::from_secs(5),
            poll_interval: Duration::from_secs(10),
            ttl: Duration::from_secs(60),
        }),
        ..Default::default()
    };
    let bootstrap =
        ServiceBootstrap::new(config, ServiceType::Querier, "localhost:50054".to_string())
            .await
            .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
    let catalog_manager = Arc::new(
        CatalogManager::new_in_memory()
            .await
            .unwrap()
            .with_tenant_source(source.clone()),
    );
    let service = QuerierFlightService::new_with_catalog_manager(
        flight_transport,
        catalog_manager,
        QuerierConfig::default(),
    )
    .await
    .unwrap();

    // Create the tenant AFTER the querier is running, exactly as the admin API
    // does against the shared catalog — no restart, no config edit.
    source
        .upsert_tenant(
            "matter-survey",
            "Matter Survey",
            Some("production"),
            "database",
        )
        .await
        .unwrap();
    source
        .create_dataset("matter-survey", "production")
        .await
        .unwrap();

    // A well-formed logs label-names ticket for the freshly-created tenant.
    // The logs path resolves the `matter-survey` catalog (it has no early
    // "no tables" guard), so before the fix this failed with
    // `failed to resolve catalog: matter-survey`.
    let ticket = Ticket::new("query_logs_labels:matter-survey:production:0:100000000000");
    let result = service.do_get(Request::new(ticket)).await;

    // With on-demand registration the catalog resolves and the query must
    // succeed (returning the known logs labels, empty for a fresh dataset).
    result.expect("lazily-registered database tenant must be queryable without restart");

    // Negative control: a tenant that was never created must still fail catalog
    // resolution. This proves the ticket format genuinely exercises catalog
    // resolution, so the assertion above isn't passing for an unrelated reason.
    let ghost = Ticket::new("query_logs_labels:ghost-tenant:production:0:100000000000");
    let ghost_err = match service.do_get(Request::new(ghost)).await {
        Ok(_) => panic!("an unregistered tenant must fail"),
        Err(status) => status,
    };
    assert!(
        ghost_err.message().contains("resolve catalog"),
        "expected catalog-resolution failure for an unknown tenant, got: {}",
        ghost_err.message()
    );
}
