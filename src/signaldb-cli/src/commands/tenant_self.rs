//! The `tenant` command group: the caller's own tenant, authenticated with a
//! tenant API key (`--api-key` / `--tenant-id`, or the `SIGNALDB_*`
//! environment). Every subcommand operates on that tenant only.
//!
//! - `signaldb-cli tenant show` — the tenant's self view
//!   (`GET /api/v1/tenants/{id}`; any valid key of the tenant)
//! - `signaldb-cli tenant table list|provision|schemas|available-schemas` —
//!   signal tables (`/api/v1/tenants/{id}/tables...`; any valid key of the
//!   tenant)
//! - `signaldb-cli tenant dataset list|create|delete`,
//!   `tenant api-key list|create|update|revoke`,
//!   `tenant membership list|set|remove`, `tenant schema get` — the tenant
//!   management API (`/api/v1/manage/...`), which requires the key to carry
//!   the explicit `tenant:manage` scope. Ingest-only keys and legacy unscoped
//!   keys are refused by the router with `403`; the CLI surfaces that error
//!   and exits non-zero (see `router::endpoints::management::authorize_tenant`).
//!
//! These mirror the MCP server's `tenant_*` tools. Destructive verbs
//! (`dataset delete`, `api-key revoke`, `membership remove`) ask for
//! confirmation on a TTY unless `--yes` is passed, and refuse to run
//! non-interactively without `--yes`.
//!
//! `--tenant-id` doubles as both the `X-Tenant-Id` header and the path
//! parameter these operations require; the router checks that they match.

use std::io::{IsTerminal, Write as _};

use clap::{ArgAction, Args, Subcommand};
use signaldb_sdk::types::{
    ManageApiKeyResponse, ManageCreateApiKeyRequest, ManageCreateDatasetRequest,
    ManageUpdateApiKeyRequest, MembershipRole, UpsertMembershipRequest,
};

use super::discover::ConnectArgs;
use super::query::print_json_response;
use signaldb_sdk::types::ListTablesResponse;

/// `signaldb-cli tenant <noun> <verb>`.
#[derive(Subcommand)]
pub enum TenantSelfAction {
    /// Show the authenticated tenant (id, name, default dataset)
    Show(ConnectArgs),
    /// List, create, or delete the tenant's datasets (needs `tenant:manage`)
    Dataset {
        #[command(subcommand)]
        action: DatasetAction,
    },
    /// List, create, update, or revoke the tenant's API keys (needs `tenant:manage`)
    #[command(name = "api-key")]
    ApiKey {
        #[command(subcommand)]
        action: ApiKeyAction,
    },
    /// List, set, or remove the tenant's user memberships (needs `tenant:manage`)
    Membership {
        #[command(subcommand)]
        action: MembershipAction,
    },
    /// Read the registered logical and physical schema (needs `tenant:manage`)
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
            TenantSelfAction::Show(connect) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .get_tenant_self()
                    .tenant_id(tenant_id)
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "get_tenant_self")
            }
            TenantSelfAction::Dataset { action } => action.run().await,
            TenantSelfAction::ApiKey { action } => action.run().await,
            TenantSelfAction::Membership { action } => action.run().await,
            TenantSelfAction::Schema { action } => action.run().await,
            TenantSelfAction::Table { action } => action.run().await,
        }
    }
}

/// A tenant id required for the `/api/v1/tenants/{tenant_id}/...` and
/// `/api/v1/manage/tenants/{tenant_id}/...` path segments — the endpoints
/// check it against the authenticated tenant, so a mismatch (or omission) is
/// always an error, never ambiguous.
fn require_tenant_id(connect: &ConnectArgs) -> anyhow::Result<&str> {
    connect.tenant_id.as_deref().ok_or_else(|| {
        anyhow::anyhow!("--tenant-id (or SIGNALDB_TENANT_ID) is required for `tenant` commands")
    })
}

/// `tenant table list` connection args plus the output-format toggle.
#[derive(Args)]
pub struct TableListArgs {
    #[command(flatten)]
    connect: ConnectArgs,
    /// Print raw JSON instead of the DATASET/TABLE/TYPE table
    #[arg(long)]
    json: bool,
}

/// `--yes` for destructive verbs: skip the interactive confirmation.
#[derive(Args, Debug, Default, Clone)]
pub struct ConfirmArgs {
    /// Skip the confirmation prompt (required when stdin is not a terminal)
    #[arg(long, short = 'y')]
    pub yes: bool,
}

/// Confirm a destructive action: pass with `--yes`; otherwise, on a TTY, ask
/// the user to type `expected`; refuse non-interactively.
fn confirm_destructive(confirm: &ConfirmArgs, what: &str, expected: &str) -> anyhow::Result<()> {
    if confirm.yes {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("refusing to {what} '{expected}' without --yes (stdin is not a terminal)");
    }
    eprint!("Type '{expected}' to confirm you want to {what} it: ");
    std::io::stderr().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    if input.trim() == expected {
        Ok(())
    } else {
        anyhow::bail!("confirmation did not match; aborting");
    }
}

/// `signaldb-cli tenant dataset <verb>` — the caller's own datasets through
/// the management API (`tenant:manage`).
#[derive(Subcommand)]
pub enum DatasetAction {
    /// List the tenant's datasets
    List(ConnectArgs),
    /// Create a dataset
    Create {
        /// Dataset name
        name: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Delete a dataset (never the default or a configuration-backed one)
    Delete {
        /// Dataset name
        name: String,
        #[command(flatten)]
        confirm: ConfirmArgs,
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
            DatasetAction::Delete {
                name,
                confirm,
                connect,
            } => {
                let tenant_id = require_tenant_id(&connect)?;
                confirm_destructive(&confirm, "delete dataset", &name)?;
                connect
                    .build_client()?
                    .manage_delete_dataset()
                    .tenant_id(tenant_id)
                    .dataset_name(&name)
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("manage_delete_dataset failed"))?;
                println!("Dataset '{name}' deleted.");
                Ok(())
            }
        }
    }
}

/// `signaldb-cli tenant api-key <verb>` — the caller's own API keys through
/// the management API (`tenant:manage`).
/// `tenant api-key list` connection args plus the output-format toggle.
#[derive(Args)]
pub struct ApiKeyListArgs {
    #[command(flatten)]
    connect: ConnectArgs,
    /// Print raw JSON instead of the ID/NAME/SCOPES/DATASETS table
    #[arg(long)]
    json: bool,
}

#[derive(Subcommand)]
pub enum ApiKeyAction {
    /// List the tenant's API keys (with their scopes and dataset
    /// restriction; never the key material)
    List(ApiKeyListArgs),
    /// Create an API key; the key material is printed exactly once
    Create {
        /// Optional key name
        #[arg(long)]
        name: Option<String>,
        /// Scope the key carries (repeatable, at least one): metrics:write,
        /// logs:write, traces:write, profiles:write, traces:read, logs:read,
        /// metrics:read, profiles:read, schema:read, schema:write,
        /// tenant:manage (manage this tenant's datasets, keys, and members)
        #[arg(long = "scope", required = true, value_name = "SCOPE")]
        scopes: Vec<String>,
        /// Restrict the key to these datasets of the tenant (repeatable);
        /// omit for an unrestricted key
        #[arg(long = "dataset", action = ArgAction::Append, value_name = "DATASET")]
        dataset: Option<Vec<String>>,
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
        /// Replacement dataset restriction (repeatable); omit to leave the
        /// current restriction unchanged
        #[arg(long = "dataset", action = ArgAction::Append, value_name = "DATASET")]
        dataset: Option<Vec<String>>,
        /// Clear an existing dataset restriction back to unrestricted;
        /// cannot be combined with --dataset
        #[arg(long, conflicts_with = "dataset")]
        clear_dataset_restriction: bool,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Revoke an API key
    Revoke {
        /// API key ID to revoke
        key_id: String,
        #[command(flatten)]
        confirm: ConfirmArgs,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

/// Render `ID  NAME  SCOPES  DATASETS` rows, column-aligned.
fn format_tenant_api_key_list(keys: &[ManageApiKeyResponse]) -> String {
    if keys.is_empty() {
        return "No API keys.".to_string();
    }

    let rows: Vec<(String, String, String, String)> = keys
        .iter()
        .map(|k| {
            let name = k.name.clone().unwrap_or_else(|| "-".to_string());
            let scopes = k
                .scopes
                .as_ref()
                .filter(|s| !s.is_empty())
                .map(|s| s.join(", "))
                .unwrap_or_else(|| "-".to_string());
            let datasets = crate::commands::format_dataset_restriction(k.dataset_ids.as_deref());
            (k.id.clone(), name, scopes, datasets)
        })
        .collect();

    let widths = [
        rows.iter()
            .map(|(v, ..)| v.len())
            .chain(std::iter::once("ID".len()))
            .max()
            .unwrap_or(0),
        rows.iter()
            .map(|(_, v, ..)| v.len())
            .chain(std::iter::once("NAME".len()))
            .max()
            .unwrap_or(0),
        rows.iter()
            .map(|(_, _, v, _)| v.len())
            .chain(std::iter::once("SCOPES".len()))
            .max()
            .unwrap_or(0),
    ];

    let mut out = format!(
        "{:w0$}  {:w1$}  {:w2$}  DATASETS\n",
        "ID",
        "NAME",
        "SCOPES",
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2]
    );
    for (id, name, scopes, datasets) in rows {
        out.push_str(&format!(
            "{id:w0$}  {name:w1$}  {scopes:w2$}  {datasets}\n",
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2]
        ));
    }
    out.trim_end().to_string()
}

impl ApiKeyAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            ApiKeyAction::List(ApiKeyListArgs { connect, json }) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_list_api_keys()
                    .tenant_id(tenant_id)
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("manage_list_api_keys failed"))?
                    .into_inner();
                if json {
                    crate::commands::print_json(&v)?;
                } else {
                    println!("{}", format_tenant_api_key_list(&v));
                }
                Ok(())
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
                        dataset_ids: dataset,
                    })
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_create_api_key")
            }
            ApiKeyAction::Update {
                key_id,
                scopes,
                dataset,
                clear_dataset_restriction,
                connect,
            } => {
                if scopes.is_empty() && dataset.is_none() && !clear_dataset_restriction {
                    anyhow::bail!(
                        "nothing to update: pass --scope, --dataset, and/or --clear-dataset-restriction"
                    );
                }
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_update_api_key()
                    .tenant_id(tenant_id)
                    .key_id(&key_id)
                    .body(ManageUpdateApiKeyRequest {
                        scopes: (!scopes.is_empty()).then_some(scopes),
                        dataset_ids: dataset,
                        clear_dataset_restriction: clear_dataset_restriction.then_some(true),
                    })
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_update_api_key")
            }
            ApiKeyAction::Revoke {
                key_id,
                confirm,
                connect,
            } => {
                let tenant_id = require_tenant_id(&connect)?;
                confirm_destructive(&confirm, "revoke API key", &key_id)?;
                connect
                    .build_client()?
                    .manage_revoke_api_key()
                    .tenant_id(tenant_id)
                    .key_id(&key_id)
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("manage_revoke_api_key failed"))?;
                println!("API key '{key_id}' revoked.");
                Ok(())
            }
        }
    }
}

/// `signaldb-cli tenant membership <verb>` — the caller's own user
/// memberships through the management API (`tenant:manage`).
#[derive(Subcommand)]
pub enum MembershipAction {
    /// List the tenant's memberships
    List(ConnectArgs),
    /// Grant or change a user's role in the tenant
    Set {
        /// User email
        email: String,
        /// Role: admin, member, or viewer
        #[arg(long)]
        role: MembershipRole,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Remove a user's membership from the tenant
    Remove {
        /// User ID
        user_id: String,
        #[command(flatten)]
        confirm: ConfirmArgs,
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
                    .body(UpsertMembershipRequest { email, role })
                    .send()
                    .await;
                print_json_response(v.map(|r| r.into_inner()), "manage_upsert_membership")
            }
            MembershipAction::Remove {
                user_id,
                confirm,
                connect,
            } => {
                let tenant_id = require_tenant_id(&connect)?;
                confirm_destructive(&confirm, "remove membership of user", &user_id)?;
                connect
                    .build_client()?
                    .manage_remove_membership()
                    .tenant_id(tenant_id)
                    .user_id(&user_id)
                    .send()
                    .await
                    .map_err(|e| {
                        anyhow::Error::new(e).context("manage_remove_membership failed")
                    })?;
                println!("Membership of user '{user_id}' removed.");
                Ok(())
            }
        }
    }
}

/// `signaldb-cli tenant schema <verb>` — the registered logical and physical
/// schema through the management API (`tenant:manage`).
#[derive(Subcommand)]
pub enum SchemaAction {
    /// Print the logical (client-visible) and physical (storage) schema
    Get(ConnectArgs),
}

impl SchemaAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            SchemaAction::Get(connect) => {
                let v = connect.build_client()?.manage_get_schema().send().await;
                print_json_response(v.map(|r| r.into_inner()), "manage_get_schema")
            }
        }
    }
}

#[derive(Subcommand)]
pub enum TableAction {
    /// List the tenant's provisioned signal tables
    List(TableListArgs),
    /// Provision (create) the tenant's enabled signal tables
    Provision(ConnectArgs),
    /// List the tenant's configured table schema types
    Schemas(ConnectArgs),
    /// List every table schema type SignalDB knows how to provision
    #[command(name = "available-schemas")]
    AvailableSchemas(ConnectArgs),
}

/// Render `DATASET  TABLE  TYPE` rows, column-aligned, sorted by
/// dataset then table name.
fn format_table_list(response: &ListTablesResponse) -> String {
    if response.tables.is_empty() {
        return "No signal tables provisioned yet.".to_string();
    }

    let mut rows: Vec<(&str, &str, &str)> = response
        .tables
        .iter()
        .map(|t| {
            (
                t.dataset.as_deref().unwrap_or(""),
                t.name.as_str(),
                t.schema_type.as_str(),
            )
        })
        .collect();
    rows.sort();

    let dataset_width = rows
        .iter()
        .map(|(d, _, _)| d.len())
        .chain(std::iter::once("DATASET".len()))
        .max()
        .unwrap_or(0);
    let table_width = rows
        .iter()
        .map(|(_, t, _)| t.len())
        .chain(std::iter::once("TABLE".len()))
        .max()
        .unwrap_or(0);

    let mut out = format!(
        "{:dataset_width$}  {:table_width$}  TYPE\n",
        "DATASET", "TABLE"
    );
    for (dataset, table, schema_type) in rows {
        out.push_str(&format!(
            "{dataset:dataset_width$}  {table:table_width$}  {schema_type}\n"
        ));
    }
    out.trim_end().to_string()
}

impl TableAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            TableAction::List(TableListArgs { connect, json }) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .list_tenant_tables()
                    .tenant_id(tenant_id)
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("list_tenant_tables failed"))?
                    .into_inner();
                if json {
                    crate::commands::print_json(&v)?;
                } else {
                    println!("{}", format_table_list(&v));
                }
                Ok(())
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
    fn management_nouns_parse() {
        for args in [
            vec!["tenant", "show"],
            vec!["tenant", "dataset", "list"],
            vec!["tenant", "dataset", "create", "staging"],
            vec!["tenant", "dataset", "delete", "staging", "--yes"],
            vec!["tenant", "api-key", "list"],
            vec![
                "tenant",
                "api-key",
                "create",
                "--name",
                "ci",
                "--scope",
                "traces:write",
                "--scope",
                "tenant:manage",
            ],
            vec!["tenant", "api-key", "update", "k1", "--scope", "logs:write"],
            vec!["tenant", "api-key", "revoke", "k1", "-y"],
            vec!["tenant", "membership", "list"],
            vec![
                "tenant",
                "membership",
                "set",
                "bob@example.com",
                "--role",
                "member",
            ],
            vec!["tenant", "membership", "remove", "user-1", "--yes"],
            vec!["tenant", "schema", "get"],
        ] {
            assert!(
                TestCli::try_parse_from(&args).is_ok(),
                "{args:?} must parse"
            );
        }
        // Scopes stay required on key creation; the role is a closed enum.
        assert!(TestCli::try_parse_from(["tenant", "api-key", "create", "--name", "ci"]).is_err());
        assert!(
            TestCli::try_parse_from(["tenant", "membership", "set", "x@y", "--role", "owner"])
                .is_err()
        );
    }

    #[test]
    fn api_key_create_accepts_repeated_dataset_flag() {
        let parsed = TestCli::try_parse_from([
            "tenant",
            "api-key",
            "create",
            "--name",
            "ci",
            "--scope",
            "traces:write",
            "--dataset",
            "production",
            "--dataset",
            "staging",
        ])
        .expect("parses");
        let TenantSelfAction::ApiKey {
            action: ApiKeyAction::Create { dataset, .. },
        } = parsed.action
        else {
            panic!("expected api-key create");
        };
        assert_eq!(
            dataset,
            Some(vec!["production".to_string(), "staging".to_string()])
        );
    }

    #[test]
    fn api_key_update_accepts_repeated_dataset_flag_and_clear_flag() {
        let parsed = TestCli::try_parse_from([
            "tenant",
            "api-key",
            "update",
            "k1",
            "--dataset",
            "production",
            "--dataset",
            "staging",
        ])
        .expect("parses");
        let TenantSelfAction::ApiKey {
            action: ApiKeyAction::Update { dataset, .. },
        } = parsed.action
        else {
            panic!("expected api-key update");
        };
        assert_eq!(
            dataset,
            Some(vec!["production".to_string(), "staging".to_string()])
        );

        let parsed =
            TestCli::try_parse_from(["tenant", "api-key", "update", "k1", "--dataset", "x"])
                .expect("parses");
        let TenantSelfAction::ApiKey {
            action:
                ApiKeyAction::Update {
                    clear_dataset_restriction,
                    ..
                },
        } = parsed.action
        else {
            panic!("expected api-key update");
        };
        assert!(!clear_dataset_restriction);
    }

    #[test]
    fn api_key_update_rejects_dataset_and_clear_dataset_restriction_together() {
        let parsed = TestCli::try_parse_from([
            "tenant",
            "api-key",
            "update",
            "k1",
            "--dataset",
            "production",
            "--clear-dataset-restriction",
        ]);
        assert!(
            parsed.is_err(),
            "--dataset and --clear-dataset-restriction must conflict at the CLI level"
        );
    }

    #[tokio::test]
    async fn destructive_verbs_refuse_without_yes_when_not_a_tty() {
        // Under `cargo test`, stdin is not a terminal.
        let result = DatasetAction::Delete {
            name: "staging".into(),
            confirm: ConfirmArgs { yes: false },
            connect: ConnectArgs {
                url: "http://127.0.0.1:1".to_string(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
        }
        .run()
        .await;
        let err = result.expect_err("must refuse").to_string();
        assert!(err.contains("--yes"), "{err}");
    }

    #[tokio::test]
    async fn dataset_create_and_list_hit_the_management_api() {
        let mut server = mockito::Server::new_async().await;
        let create = server
            .mock("POST", "/api/v1/manage/tenants/acme/datasets")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .match_body(mockito::Matcher::Json(
                serde_json::json!({ "name": "staging" }),
            ))
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(r#"{"id":"staging","name":"staging"}"#)
            .create_async()
            .await;
        let list = server
            .mock("GET", "/api/v1/manage/tenants/acme/datasets")
            .match_header("authorization", "Bearer sk-test")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id":"production","name":"production"},{"id":"staging","name":"staging"}]"#,
            )
            .create_async()
            .await;
        let connect = || ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        };
        DatasetAction::Create {
            name: "staging".into(),
            connect: connect(),
        }
        .run()
        .await
        .expect("create succeeds");
        DatasetAction::List(connect())
            .run()
            .await
            .expect("list succeeds");
        create.assert_async().await;
        list.assert_async().await;
    }

    #[tokio::test]
    async fn dataset_create_surfaces_the_403_from_a_key_without_tenant_manage() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/api/v1/manage/tenants/acme/datasets")
            .with_status(403)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"Tenant administrator role or tenant:manage scope required"}"#)
            .create_async()
            .await;
        let result = DatasetAction::Create {
            name: "staging".into(),
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-ingest".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
        }
        .run()
        .await;
        let err = format!("{:#}", result.expect_err("must fail"));
        assert!(err.contains("manage_create_dataset failed"), "{err}");
        assert!(
            err.contains("403") || err.contains("tenant:manage"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn api_key_create_membership_set_and_schema_get_hit_the_management_api() {
        let mut server = mockito::Server::new_async().await;
        let key = server
            .mock("POST", "/api/v1/manage/tenants/acme/api-keys")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "name": "ci",
                "scopes": ["traces:write"]
            })))
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(r#"{"id":"k1","key":"sdbk_1","name":"ci","scopes":["traces:write"]}"#)
            .create_async()
            .await;
        let update = server
            .mock("PATCH", "/api/v1/manage/tenants/acme/api-keys/k1")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "scopes": ["logs:write"]
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"id":"k1","name":"ci","scopes":["logs:write"],"created_at":"2026-01-01T00:00:00Z","revoked":false}"#)
            .create_async()
            .await;
        let revoke = server
            .mock("DELETE", "/api/v1/manage/tenants/acme/api-keys/k1")
            .with_status(204)
            .create_async()
            .await;
        let membership = server
            .mock("PUT", "/api/v1/manage/tenants/acme/memberships")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "email": "bob@example.com",
                "role": "member"
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"user_id":"u1","email":"bob@example.com","role":"member"}"#)
            .create_async()
            .await;
        let remove = server
            .mock("DELETE", "/api/v1/manage/tenants/acme/memberships/u1")
            .with_status(204)
            .create_async()
            .await;
        let schema = server
            .mock("GET", "/api/v1/manage/schema")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"logical":[],"logical_schema_version":"1","physical":[]}"#)
            .create_async()
            .await;
        let show = server
            .mock("GET", "/api/v1/tenants/acme")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"tenant_id":"acme","enabled":true,"schema":null}"#)
            .create_async()
            .await;
        let connect = || ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        };
        ApiKeyAction::Create {
            name: Some("ci".into()),
            scopes: vec!["traces:write".into()],
            dataset: None,
            connect: connect(),
        }
        .run()
        .await
        .expect("api-key create");
        ApiKeyAction::Update {
            key_id: "k1".into(),
            scopes: vec!["logs:write".into()],
            dataset: None,
            clear_dataset_restriction: false,
            connect: connect(),
        }
        .run()
        .await
        .expect("api-key update");
        ApiKeyAction::Revoke {
            key_id: "k1".into(),
            confirm: ConfirmArgs { yes: true },
            connect: connect(),
        }
        .run()
        .await
        .expect("api-key revoke");
        MembershipAction::Set {
            email: "bob@example.com".into(),
            role: MembershipRole::Member,
            connect: connect(),
        }
        .run()
        .await
        .expect("membership set");
        MembershipAction::Remove {
            user_id: "u1".into(),
            confirm: ConfirmArgs { yes: true },
            connect: connect(),
        }
        .run()
        .await
        .expect("membership remove");
        SchemaAction::Get(connect())
            .run()
            .await
            .expect("schema get");
        TenantSelfAction::Show(connect())
            .run()
            .await
            .expect("tenant show");
        for m in [key, update, revoke, membership, remove, schema, show] {
            m.assert_async().await;
        }
    }

    #[tokio::test]
    async fn table_list_requires_tenant_id() {
        let result = TableAction::List(TableListArgs {
            connect: ConnectArgs {
                url: "http://127.0.0.1:1".to_string(),
                api_key: Some("sk-test".to_string()),
                tenant_id: None,
                dataset_id: None,
            },
            json: false,
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
            .with_body(r#"{"tenant_id":"acme","tables":[],"datasets":[]}"#)
            .create_async()
            .await;

        TableAction::List(TableListArgs {
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
            json: false,
        })
        .run()
        .await
        .expect("table list succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn table_list_json_flag_prints_raw_json() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/api/v1/tenants/acme/tables")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"tenant_id":"acme","tables":[{"name":"traces","schema_type":"traces","description":"d","dataset":"production"}],"datasets":[{"dataset":"production","tables":[{"name":"traces","schema_type":"traces","description":"d","dataset":"production"}]}]}"#,
            )
            .create_async()
            .await;

        TableAction::List(TableListArgs {
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
            json: true,
        })
        .run()
        .await
        .expect("table list --json succeeds");
    }

    #[test]
    fn format_table_list_prints_dataset_table_type_columns() {
        let response: ListTablesResponse = serde_json::from_str(
            r#"{"tenant_id":"acme","tables":[
                {"name":"traces","schema_type":"traces","description":"d","dataset":"production"},
                {"name":"logs","schema_type":"logs","description":"d","dataset":"production"},
                {"name":"profiles","schema_type":"profiles","description":"d","dataset":"archive"}
            ],"datasets":[]}"#,
        )
        .unwrap();

        let rendered = format_table_list(&response);

        assert!(rendered.starts_with("DATASET"));
        assert!(rendered.contains("TABLE"));
        assert!(rendered.contains("TYPE"));
        assert!(rendered.contains("production") && rendered.contains("traces"));
        assert!(rendered.contains("archive") && rendered.contains("profiles"));
        // Sorted by dataset then table: "archive" precedes "production".
        assert!(rendered.find("archive").unwrap() < rendered.find("production").unwrap());
    }

    #[test]
    fn format_table_list_reports_empty_state() {
        let response: ListTablesResponse =
            serde_json::from_str(r#"{"tenant_id":"acme","tables":[],"datasets":[]}"#).unwrap();
        assert_eq!(
            format_table_list(&response),
            "No signal tables provisioned yet."
        );
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

    fn connect_acme(server: &mockito::ServerGuard) -> ConnectArgs {
        ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        }
    }

    #[tokio::test]
    async fn tenant_api_key_update_sends_multiple_datasets() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("PATCH", "/api/v1/manage/tenants/acme/api-keys/k1")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "dataset_ids": ["production", "staging"]
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"id":"k1","name":"ci","scopes":["traces:write"],"dataset_ids":["production","staging"],"created_at":"2026-01-01T00:00:00Z","revoked":false}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::Update {
            key_id: "k1".into(),
            scopes: vec![],
            dataset: Some(vec!["production".into(), "staging".into()]),
            clear_dataset_restriction: false,
            connect: connect_acme(&server),
        }
        .run()
        .await
        .expect("update succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn tenant_api_key_update_clears_dataset_restriction() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("PATCH", "/api/v1/manage/tenants/acme/api-keys/k1")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "clear_dataset_restriction": true
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"id":"k1","name":"ci","scopes":["traces:write"],"created_at":"2026-01-01T00:00:00Z","revoked":false}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::Update {
            key_id: "k1".into(),
            scopes: vec![],
            dataset: None,
            clear_dataset_restriction: true,
            connect: connect_acme(&server),
        }
        .run()
        .await
        .expect("update succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn tenant_api_key_update_without_changes_is_an_error() {
        let server = mockito::Server::new_async().await;
        let result = ApiKeyAction::Update {
            key_id: "k1".into(),
            scopes: vec![],
            dataset: None,
            clear_dataset_restriction: false,
            connect: connect_acme(&server),
        }
        .run()
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tenant_api_key_list_defaults_to_a_human_readable_table_with_dataset_restriction() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/api/v1/manage/tenants/acme/api-keys")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[
                    {"id":"k1","name":"ci","scopes":["traces:write"],"dataset_ids":["production","staging"],"created_at":"2026-01-01T00:00:00Z","revoked":false},
                    {"id":"k2","name":"full","scopes":["schema:read"],"dataset_ids":null,"created_at":"2026-01-01T00:00:00Z","revoked":false}
                ]"#,
            )
            .create_async()
            .await;

        ApiKeyAction::List(ApiKeyListArgs {
            connect: connect_acme(&server),
            json: false,
        })
        .run()
        .await
        .expect("list succeeds");
    }

    #[test]
    fn format_tenant_api_key_list_shows_dataset_restriction_or_unrestricted() {
        let keys: Vec<ManageApiKeyResponse> = serde_json::from_str(
            r#"[
                {"id":"k1","name":"ci","scopes":["traces:write"],"dataset_ids":["production","staging"],"created_at":"2026-01-01T00:00:00Z","revoked":false},
                {"id":"k2","name":"full","scopes":["schema:read"],"dataset_ids":null,"created_at":"2026-01-01T00:00:00Z","revoked":false}
            ]"#,
        )
        .unwrap();

        let rendered = format_tenant_api_key_list(&keys);

        assert!(rendered.contains("production, staging"));
        assert!(rendered.contains("unrestricted"));
    }
}
