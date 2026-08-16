//! The `tenant` command group: the caller's own tenant, through the
//! management API (`/api/v1/manage/...`) and the tenant self-service table
//! endpoints (`/api/v1/tenants/{id}/tables...`), authenticated with a tenant
//! API key — mirroring the MCP server's `tenant_*` tools (change
//! `mcp-admin-tool-parity`).
//!
//! - `signaldb-cli tenant dataset list|create|delete`
//! - `signaldb-cli tenant api-key list|create|update|revoke`
//! - `signaldb-cli tenant membership list|set|remove`
//! - `signaldb-cli tenant schema get`
//! - `signaldb-cli tenant table list|provision|schemas|available-schemas`
//!
//! Every subcommand authenticates with a tenant API key (`--api-key` /
//! `--tenant-id`, or the `SIGNALDB_*` environment) and operates on that
//! tenant only — the management API rejects a path `tenant_id` that does not
//! match the authenticated one. `--tenant-id` therefore doubles as both the
//! `X-Tenant-Id` header and the path parameter these operations require.

use clap::Subcommand;
use signaldb_sdk::types::{
    ManageCreateApiKeyRequest, ManageCreateDatasetRequest, ManageUpdateApiKeyRequest,
    MembershipRole, UpsertMembershipRequest,
};

use super::discover::ConnectArgs;
use super::query::print_json_response;

/// `signaldb-cli tenant <noun> <verb>`.
#[derive(Subcommand)]
pub enum TenantSelfAction {
    /// Manage the tenant's own datasets
    Dataset {
        #[command(subcommand)]
        action: DatasetAction,
    },
    /// Manage the tenant's own API keys
    ApiKey {
        #[command(subcommand)]
        action: ApiKeyAction,
    },
    /// Manage the tenant's own memberships
    Membership {
        #[command(subcommand)]
        action: MembershipAction,
    },
    /// Read the tenant's logical + physical schema
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },
    /// List or provision the tenant's signal tables
    Table {
        #[command(subcommand)]
        action: TableAction,
    },
}

impl TenantSelfAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            TenantSelfAction::Dataset { action } => action.run().await,
            TenantSelfAction::ApiKey { action } => action.run().await,
            TenantSelfAction::Membership { action } => action.run().await,
            TenantSelfAction::Schema { action } => action.run().await,
            TenantSelfAction::Table { action } => action.run().await,
        }
    }
}

/// A tenant id required for the `/api/v1/manage/tenants/{tenant_id}/...` and
/// `/api/v1/tenants/{tenant_id}/...` path segments — the management API
/// checks it against the authenticated tenant, so a mismatch (or omission)
/// is always an error, never ambiguous.
fn require_tenant_id(connect: &ConnectArgs) -> anyhow::Result<&str> {
    connect.tenant_id.as_deref().ok_or_else(|| {
        anyhow::anyhow!("--tenant-id (or SIGNALDB_TENANT_ID) is required for `tenant` commands")
    })
}

#[derive(Subcommand)]
pub enum DatasetAction {
    /// List the tenant's datasets
    List(ConnectArgs),
    /// Create a dataset for the tenant
    Create {
        /// Dataset name
        #[arg(long)]
        name: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Delete a dataset
    Delete {
        /// Dataset name
        dataset: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

impl DatasetAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            DatasetAction::List(connect) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_list_datasets()
                    .tenant_id(tenant_id)
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_list_datasets")
            }
            DatasetAction::Create { name, connect } => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_create_dataset()
                    .tenant_id(tenant_id)
                    .body(ManageCreateDatasetRequest { name })
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_create_dataset")
            }
            DatasetAction::Delete { dataset, connect } => {
                let tenant_id = require_tenant_id(&connect)?;
                connect
                    .build_client()?
                    .manage_delete_dataset()
                    .tenant_id(tenant_id)
                    .dataset_name(&dataset)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("manage_delete_dataset failed: {e}"))?;
                println!("Dataset '{dataset}' deleted.");
                Ok(())
            }
        }
    }
}

#[derive(Subcommand)]
pub enum ApiKeyAction {
    /// List the tenant's own API keys (with their scopes)
    List(ConnectArgs),
    /// Create an API key for the tenant
    Create {
        /// Optional key name
        #[arg(long)]
        name: Option<String>,
        /// Scope the key carries (repeatable, at least one)
        #[arg(long = "scope", required = true, value_name = "SCOPE")]
        scopes: Vec<String>,
        /// Restrict the key to one dataset of the tenant
        #[arg(long)]
        dataset: Option<String>,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Update the scopes and/or dataset restriction of a live API key
    Update {
        /// API key ID to update
        key_id: String,
        /// Replacement scope list (repeatable); omit to keep the current scopes
        #[arg(long = "scope", value_name = "SCOPE")]
        scopes: Vec<String>,
        /// Replacement dataset restriction; omit to keep the current one
        #[arg(long)]
        dataset: Option<String>,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Revoke an API key
    Revoke {
        /// API key ID to revoke
        key_id: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

impl ApiKeyAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            ApiKeyAction::List(connect) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_list_api_keys()
                    .tenant_id(tenant_id)
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_list_api_keys")
            }
            ApiKeyAction::Create {
                name,
                scopes,
                dataset,
                connect,
            } => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_create_api_key()
                    .tenant_id(tenant_id)
                    .body(ManageCreateApiKeyRequest {
                        name,
                        scopes,
                        dataset_id: dataset,
                    })
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_create_api_key")
            }
            ApiKeyAction::Update {
                key_id,
                scopes,
                dataset,
                connect,
            } => {
                if scopes.is_empty() && dataset.is_none() {
                    anyhow::bail!("nothing to update: pass --scope and/or --dataset");
                }
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_update_api_key()
                    .tenant_id(tenant_id)
                    .key_id(&key_id)
                    .body(ManageUpdateApiKeyRequest {
                        scopes: (!scopes.is_empty()).then_some(scopes),
                        dataset_id: dataset,
                    })
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_update_api_key")
            }
            ApiKeyAction::Revoke { key_id, connect } => {
                let tenant_id = require_tenant_id(&connect)?;
                connect
                    .build_client()?
                    .manage_revoke_api_key()
                    .tenant_id(tenant_id)
                    .key_id(&key_id)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("manage_revoke_api_key failed: {e}"))?;
                println!("API key '{key_id}' revoked.");
                Ok(())
            }
        }
    }
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum RoleArg {
    Admin,
    Member,
    Viewer,
}

impl From<RoleArg> for MembershipRole {
    fn from(role: RoleArg) -> Self {
        match role {
            RoleArg::Admin => MembershipRole::Admin,
            RoleArg::Member => MembershipRole::Member,
            RoleArg::Viewer => MembershipRole::Viewer,
        }
    }
}

#[derive(Subcommand)]
pub enum MembershipAction {
    /// List the tenant's memberships
    List(ConnectArgs),
    /// Create or update a member's role (upsert)
    Set {
        /// Member's login email
        #[arg(long)]
        email: String,
        /// Role to grant
        #[arg(long, value_enum)]
        role: RoleArg,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Remove a member from the tenant
    Remove {
        /// User ID to remove
        user_id: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

impl MembershipAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            MembershipAction::List(connect) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_list_memberships()
                    .tenant_id(tenant_id)
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_list_memberships")
            }
            MembershipAction::Set {
                email,
                role,
                connect,
            } => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_upsert_membership()
                    .tenant_id(tenant_id)
                    .body(UpsertMembershipRequest {
                        email,
                        role: role.into(),
                    })
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_upsert_membership")
            }
            MembershipAction::Remove { user_id, connect } => {
                let tenant_id = require_tenant_id(&connect)?;
                connect
                    .build_client()?
                    .manage_remove_membership()
                    .tenant_id(tenant_id)
                    .user_id(&user_id)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("manage_remove_membership failed: {e}"))?;
                println!("Membership for user '{user_id}' removed.");
                Ok(())
            }
        }
    }
}

#[derive(Subcommand)]
pub enum SchemaAction {
    /// The tenant's logical + physical schema (materialized labels, custom
    /// fields)
    Get(ConnectArgs),
}

impl SchemaAction {
    pub async fn run(self) -> anyhow::Result<()> {
        let SchemaAction::Get(connect) = self;
        let v = connect.build_client()?.manage_get_schema().send().await;
        print_json_response(v.map(|r| r.into_inner()), "manage_get_schema")
    }
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
    fn dataset_subcommands_parse() {
        assert!(TestCli::try_parse_from(["tenant", "dataset", "list"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "dataset", "create", "--name", "prod"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "dataset", "delete", "prod"]).is_ok());
    }

    #[test]
    fn api_key_subcommands_parse() {
        assert!(TestCli::try_parse_from(["tenant", "api-key", "list"]).is_ok());
        assert!(
            TestCli::try_parse_from(["tenant", "api-key", "create", "--scope", "traces:write"])
                .is_ok()
        );
        // Create requires at least one --scope.
        assert!(TestCli::try_parse_from(["tenant", "api-key", "create"]).is_err());
        assert!(TestCli::try_parse_from(["tenant", "api-key", "update", "k1"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "api-key", "revoke", "k1"]).is_ok());
    }

    #[test]
    fn membership_subcommands_parse() {
        assert!(TestCli::try_parse_from(["tenant", "membership", "list"]).is_ok());
        assert!(
            TestCli::try_parse_from([
                "tenant",
                "membership",
                "set",
                "--email",
                "a@example.com",
                "--role",
                "member"
            ])
            .is_ok()
        );
        assert!(TestCli::try_parse_from(["tenant", "membership", "remove", "u1"]).is_ok());
    }

    #[test]
    fn schema_get_parses() {
        assert!(TestCli::try_parse_from(["tenant", "schema", "get"]).is_ok());
    }

    #[test]
    fn table_subcommands_parse() {
        assert!(TestCli::try_parse_from(["tenant", "table", "list"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "table", "provision"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "table", "schemas"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "table", "available-schemas"]).is_ok());
    }

    #[tokio::test]
    async fn dataset_list_requires_tenant_id() {
        let result = DatasetAction::List(ConnectArgs {
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
    async fn dataset_list_sends_tenant_id_as_path_and_header() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/manage/tenants/acme/datasets")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"[]"#)
            .create_async()
            .await;

        DatasetAction::List(ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        })
        .run()
        .await
        .expect("dataset list succeeds");
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
}
