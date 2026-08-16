//! `signaldb-cli tenant table` against a real router (task 4.3 of
//! `mcp-admin-tool-parity`). `tenant table list`/`provision` are the only
//! `tenant` subcommands reachable with a plain tenant API key — see
//! `signaldb_cli::commands::tenant_self`'s module doc for why the
//! dataset/api-key/membership nouns were dropped from the CLI.

use clap::Parser;
use common::auth::Authenticator;
use common::catalog::Catalog;
use common::config::{
    ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig, TenantsConfig,
};
use router::{RouterAppState, create_router};
use tokio::net::TcpListener;

const TENANT: &str = "acme";
const KEY: &str = "sk-acme-tenant-table-cli";

fn tenant_config() -> TenantConfig {
    TenantConfig {
        id: TENANT.to_string(),
        slug: TENANT.to_string(),
        name: "Acme Inc".to_string(),
        default_dataset: Some("production".to_string()),
        datasets: vec![DatasetConfig {
            id: "production".to_string(),
            slug: "production".to_string(),
            is_default: true,
            storage: None,
        }],
        api_keys: vec![ApiKeyConfig {
            key: KEY.to_string(),
            name: Some("cli-test".to_string()),
        }],
        schema_config: None,
        limits: None,
    }
}

/// Serve the full router with tenant `acme` and a scoped write key. The
/// schema (Iceberg) catalog is a file-backed temp SQLite database — a
/// `sqlite::memory:` uri would give every request its own throwaway
/// in-memory database (each HTTP request opens a fresh pool), so `list`
/// after `provision` would see nothing.
async fn serve_router() -> (String, common::testing::TempCatalog, common::CatalogManager) {
    let temp_catalog = common::testing::TempCatalog::new();
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    let mut config = Configuration {
        auth: AuthConfig {
            tenants: vec![tenant_config()],
            ..Default::default()
        },
        // `TenantSchemaRegistry::get_catalog_for_tenant` (used by
        // `list_tenant_tables`, unlike `create_tenant_tables`) checks
        // `Configuration::is_tenant_enabled`, which reads this legacy
        // `tenants` map/`default_tenant`, not `auth.tenants` — a pre-existing
        // split between the two tenant registries this test has to satisfy.
        tenants: TenantsConfig {
            default_tenant: TENANT.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    config.schema.catalog_uri = temp_catalog.uri().to_string();
    // Runs the Iceberg SQL catalog's migrations against the temp file before
    // any request hits it; without this, list/create 500 on an
    // unmigrated database.
    let manager = common::CatalogManager::new(config.clone()).await.unwrap();
    catalog.sync_config_tenants(&config.auth).await.unwrap();
    catalog
        .upsert_scoped_api_key(
            TENANT,
            &Authenticator::hash_api_key(KEY),
            Some("cli-test"),
            None,
            Some(&["traces:write".to_string()]),
            None,
        )
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = create_router(RouterAppState::new(catalog, config));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for attempt in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        assert!(attempt < 49, "router never became reachable");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    (format!("http://{addr}"), temp_catalog, manager)
}

fn cli_args<'a>(router_url: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["signaldb-cli", "tenant", "table"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--url", router_url, "--api-key", KEY, "--tenant-id", TENANT]);
    args
}

#[tokio::test]
async fn tenant_table_list_and_provision_succeed_with_a_tenant_key() {
    let (router_url, _temp_catalog, manager) = serve_router().await;

    // `tenant table list` succeeds with just a tenant API key carrying the
    // required (any valid) scopes — no table has been provisioned yet.
    // (`GET /tenants/{id}/tables` is itself a stub that always answers `[]`
    // — see `TenantSchemaRegistry::list_tables_for_tenant` — so this call
    // only proves the command reaches the endpoint without an auth error;
    // real provisioning is verified against the Iceberg catalog below.)
    let list_before = signaldb_cli::commands::Cli::try_parse_from(cli_args(&router_url, &["list"]))
        .expect("tenant table list parses");
    list_before
        .run()
        .await
        .expect("tenant table list succeeds before provisioning");

    // `tenant table provision` creates the tenant's enabled signal tables.
    let provision =
        signaldb_cli::commands::Cli::try_parse_from(cli_args(&router_url, &["provision"]))
            .expect("tenant table provision parses");
    provision
        .run()
        .await
        .expect("tenant table provision succeeds");

    // Verify against the Iceberg catalog directly that the tables now exist
    // (mirrors `create_tenant_tables_actually_creates_them` in
    // `router/src/endpoints/tenant.rs`).
    let namespace = manager.build_namespace(TENANT, "production").unwrap();
    let mut tables: Vec<String> = manager
        .catalog()
        .list_tabulars(&namespace)
        .await
        .unwrap()
        .iter()
        .map(|identifier| identifier.name().to_string())
        .collect();
    tables.sort();
    assert!(
        tables.contains(&"traces".to_string()),
        "provisioned tables must include 'traces': {tables:?}"
    );
}
