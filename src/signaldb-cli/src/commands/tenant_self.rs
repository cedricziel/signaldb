//! The `tenant` command group: the caller's own tenant, through the tenant
//! self-service table endpoints (`/api/v1/tenants/{id}/tables...`),
//! authenticated with a tenant API key — mirroring the MCP server's
//! `tenant_list_tables` / `tenant_create_tables` / `tenant_list_table_schemas`
//! / `list_available_table_schemas` tools (change `mcp-admin-tool-parity`).
//!
//! - `signaldb-cli tenant table list|provision|schemas|available-schemas`
//!
//! **Scope note**: the design for this change originally also called for
//! `tenant dataset`/`tenant api-key`/`tenant membership`/`tenant schema get`,
//! mirroring the management API's `manage_*` operations. Those operations
//! (`router::endpoints::management`) require a human-authenticated principal
//! — a browser session cookie or an OAuth access token, carrying a real
//! per-tenant membership role — and explicitly reject a bare API key
//! (`authorize_tenant`; see `manage_get_schema`'s `ctx.is_instance_admin`
//! check, stricter still). This is deliberate, existing, and tested
//! (`ingestion_api_key_cannot_use_human_management_endpoints` in
//! `router/src/endpoints/session.rs`) — not a gap this change should paper
//! over. The CLI has no session/OAuth login of its own, only
//! `--api-key`/`SIGNALDB_API_KEY`, so no CLI command could ever reach those
//! endpoints; they are correspondingly excluded from the whole-SDK parity
//! manifest (`tests-integration/tests/query_parity.rs`) rather than shipped
//! as commands that always fail. The MCP server's equivalent `tenant_*`
//! tools are kept — an OAuth-authenticated MCP session is a real,
//! already-supported human credential — with descriptions noting the
//! requirement.
//!
//! Every subcommand authenticates with a tenant API key (`--api-key` /
//! `--tenant-id`, or the `SIGNALDB_*` environment) and operates on that
//! tenant only — the tenant table endpoints reject a path `tenant_id` that
//! does not match the authenticated one. `--tenant-id` therefore doubles as
//! both the `X-Tenant-Id` header and the path parameter these operations
//! require.

use clap::Subcommand;

use super::discover::ConnectArgs;
use super::query::print_json_response;

/// `signaldb-cli tenant <noun> <verb>`.
#[derive(Subcommand)]
pub enum TenantSelfAction {
    /// List or provision the tenant's signal tables
    Table {
        #[command(subcommand)]
        action: TableAction,
    },
}

impl TenantSelfAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            TenantSelfAction::Table { action } => action.run().await,
        }
    }
}

/// A tenant id required for the `/api/v1/tenants/{tenant_id}/...` path
/// segment — the endpoint checks it against the authenticated tenant, so a
/// mismatch (or omission) is always an error, never ambiguous.
fn require_tenant_id(connect: &ConnectArgs) -> anyhow::Result<&str> {
    connect.tenant_id.as_deref().ok_or_else(|| {
        anyhow::anyhow!("--tenant-id (or SIGNALDB_TENANT_ID) is required for `tenant` commands")
    })
}

#[derive(Subcommand)]
pub enum TableAction {
    /// List the tenant's provisioned signal tables
    List(ConnectArgs),
    /// Provision (create) the tenant's enabled signal tables
    Provision(ConnectArgs),
    /// List the tenant's configured table schema types
    Schemas(ConnectArgs),
    /// List every table schema type SignalDB knows how to provision
    #[command(name = "available-schemas")]
    AvailableSchemas(ConnectArgs),
}

impl TableAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            TableAction::List(connect) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .list_tenant_tables()
                    .tenant_id(tenant_id)
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "list_tenant_tables")
            }
            TableAction::Provision(connect) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .create_tenant_tables()
                    .tenant_id(tenant_id)
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "create_tenant_tables")
            }
            TableAction::Schemas(connect) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .list_tenant_schemas()
                    .tenant_id(tenant_id)
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "list_tenant_schemas")
            }
            TableAction::AvailableSchemas(connect) => {
                let v = connect
                    .build_client()?
                    .list_available_schemas()
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "list_available_schemas")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        action: TenantSelfAction,
    }

    #[test]
    fn table_subcommands_parse() {
        assert!(TestCli::try_parse_from(["tenant", "table", "list"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "table", "provision"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "table", "schemas"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "table", "available-schemas"]).is_ok());
    }

    #[test]
    fn management_only_nouns_are_not_reachable() {
        // dataset/api-key/membership/schema management requires a
        // human-authenticated principal the CLI cannot present; see the
        // module doc for why these were removed rather than shipped broken.
        assert!(TestCli::try_parse_from(["tenant", "dataset", "list"]).is_err());
        assert!(TestCli::try_parse_from(["tenant", "api-key", "list"]).is_err());
        assert!(TestCli::try_parse_from(["tenant", "membership", "list"]).is_err());
        assert!(TestCli::try_parse_from(["tenant", "schema", "get"]).is_err());
    }

    #[tokio::test]
    async fn table_list_requires_tenant_id() {
        let result = TableAction::List(ConnectArgs {
            url: "http://127.0.0.1:1".to_string(),
            api_key: Some("sk-test".to_string()),
            tenant_id: None,
            dataset_id: None,
        })
        .run()
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn table_list_sends_tenant_id_as_path_and_header() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/tenants/acme/tables")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"tenant_id":"acme","tables":[]}"#)
            .create_async()
            .await;

        TableAction::List(ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        })
        .run()
        .await
        .expect("table list succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn table_provision_hits_the_create_endpoint() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/tenants/acme/tables/create")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(r#"{"message":"ok","tenant_id":"acme"}"#)
            .create_async()
            .await;

        TableAction::Provision(ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        })
        .run()
        .await
        .expect("table provision succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn table_schemas_and_available_schemas_hit_the_right_endpoints() {
        let mut server = mockito::Server::new_async().await;
        let schemas_mock = server
            .mock("GET", "/api/v1/tenants/acme/schemas")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"tenant_id":"acme","tables":[]}"#)
            .create_async()
            .await;
        let available_mock = server
            .mock("GET", "/api/v1/schemas/available")
            .match_header("authorization", "Bearer sk-test")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"schemas":[]}"#)
            .create_async()
            .await;

        TableAction::Schemas(ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        })
        .run()
        .await
        .expect("table schemas succeeds");
        schemas_mock.assert_async().await;

        TableAction::AvailableSchemas(ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: None,
            dataset_id: None,
        })
        .run()
        .await
        .expect("available-schemas succeeds");
        available_mock.assert_async().await;
    }
}
