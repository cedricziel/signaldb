//! The `tenant` command group: the caller's own tenant, authenticated with a
//! tenant API key (`--api-key` / `--tenant-id`, or the `SIGNALDB_*`
//! environment). Every subcommand operates on that tenant only.
//!
//! - `signaldb-cli tenant show` — the tenant's self view
//!   (`GET /api/v1/tenants/{id}`; any valid key of the tenant)
//! - `signaldb-cli tenant table list|provision|schemas|available-schemas` —
//!   signal tables (`/api/v1/tenants/{id}/tables...`; any valid key of the
//!   tenant)
//! - `signaldb-cli tenant source-context` — a source-code snippet around a
//!   stack-frame location (`/api/v1/tenants/{id}/source-context`; any valid
//!   key of the tenant, not `tenant:manage`)
//! - `signaldb-cli tenant dataset list|create|delete`,
//!   `tenant api-key list|create|update|revoke`,
//!   `tenant membership list|set|remove`, `tenant schema get`,
//!   `tenant github link|list|remove` — the tenant management API
//!   (`/api/v1/manage/...`), which requires the key to carry the explicit
//!   `tenant:manage` scope. Ingest-only keys and legacy unscoped keys are
//!   refused by the router with `403`; the CLI surfaces that error and exits
//!   non-zero (see `router::endpoints::management::authorize_tenant`).
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
    GitHubInstallationResponse, ManageApiKeyResponse, ManageCreateApiKeyRequest,
    ManageCreateDatasetRequest, ManageUpdateApiKeyRequest, MembershipRole, SourceContextRequest,
    SourceContextResponse, SourceContextStatus, UpsertMembershipRequest,
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
    /// Link, list, or remove GitHub App installations (needs `tenant:manage`)
    Github {
        #[command(subcommand)]
        action: GithubAction,
    },
    /// Fetch a source-code snippet around a stack-frame location, through
    /// the tenant's linked GitHub App installation(s) (any valid key of the
    /// tenant)
    #[command(name = "source-context")]
    SourceContext(SourceContextArgs),
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
            TenantSelfAction::Github { action } => action.run().await,
            TenantSelfAction::SourceContext(args) => run_source_context(args).await,
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
        /// Restrict the key to these browser origins for CORS (repeatable);
        /// omit for an unrestricted key
        #[arg(long = "allowed-origin", action = ArgAction::Append, value_name = "ORIGIN")]
        allowed_origin: Option<Vec<String>>,
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
        /// Replacement allowed-origins restriction (repeatable); omit to
        /// leave the current restriction unchanged
        #[arg(long = "allowed-origin", action = ArgAction::Append, value_name = "ORIGIN")]
        allowed_origin: Option<Vec<String>>,
        /// Clear an existing allowed-origins restriction back to
        /// unrestricted; cannot be combined with --allowed-origin
        #[arg(long, conflicts_with = "allowed_origin")]
        clear_allowed_origins: bool,
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

/// Render `ID  NAME  SCOPES  DATASETS  ORIGINS` rows, column-aligned.
fn format_tenant_api_key_list(keys: &[ManageApiKeyResponse]) -> String {
    let rows: Vec<(String, String, String, String, String)> = keys
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
            let origins = crate::commands::format_dataset_restriction(k.allowed_origins.as_deref());
            (k.id.clone(), name, scopes, datasets, origins)
        })
        .collect();
    crate::commands::format_api_key_table(&rows)
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
                allowed_origin,
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
                        allowed_origins: allowed_origin,
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
                allowed_origin,
                clear_allowed_origins,
                connect,
            } => {
                if scopes.is_empty()
                    && dataset.is_none()
                    && !clear_dataset_restriction
                    && allowed_origin.is_none()
                    && !clear_allowed_origins
                {
                    anyhow::bail!(
                        "nothing to update: pass --scope, --dataset, --clear-dataset-restriction, \
                         --allowed-origin, and/or --clear-allowed-origins"
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
                        allowed_origins: allowed_origin,
                        clear_allowed_origins: clear_allowed_origins.then_some(true),
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

/// `tenant github link|list` connection args plus the output-format toggle.
#[derive(Args)]
pub struct GithubOutputArgs {
    #[command(flatten)]
    connect: ConnectArgs,
    /// Print raw JSON instead of the human-readable output
    #[arg(long)]
    json: bool,
}

/// `signaldb-cli tenant github <verb>` — the caller's own GitHub App
/// installations through the management API (`tenant:manage`).
#[derive(Subcommand)]
pub enum GithubAction {
    /// Start linking a GitHub App installation: prints the install URL to
    /// open in a browser signed in to SignalDB as an admin of this tenant
    Link(GithubOutputArgs),
    /// List the linked installations and the repositories they cover
    List(GithubOutputArgs),
    /// Attach an installation that already exists for this GitHub App
    /// directly, with no OAuth install flow (instance-admin only — see
    /// docs/operations/github-app.md)
    Attach {
        /// GitHub installation ID to attach
        installation_id: i64,
        #[command(flatten)]
        output: GithubOutputArgs,
    },
    /// Remove a linked installation (SignalDB stops minting tokens for it immediately)
    Remove {
        /// GitHub installation ID to remove
        installation_id: i64,
        #[command(flatten)]
        confirm: ConfirmArgs,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

/// Render `INSTALLATION  ACCOUNT  REPOSITORIES  SYNCED  LINKED BY` rows,
/// column-aligned.
fn format_github_installation_table(installations: &[GitHubInstallationResponse]) -> String {
    let rows: Vec<(String, String, String, String, String)> = installations
        .iter()
        .map(|i| {
            let synced = if i.stale {
                format!("{} (stale)", i.repositories_synced_at.to_rfc3339())
            } else {
                i.repositories_synced_at.to_rfc3339()
            };
            let linked_by = i
                .linked_by_github_login
                .as_deref()
                .map(|login| format!("@{login}"))
                .unwrap_or_else(|| "-".to_string());
            (
                i.installation_id.to_string(),
                format!("{} ({})", i.account_login, i.account_type),
                format_repositories(&i.repositories),
                synced,
                linked_by,
            )
        })
        .collect();
    crate::commands::format_table(
        [
            "INSTALLATION",
            "ACCOUNT",
            "REPOSITORIES",
            "SYNCED",
            "LINKED BY",
        ],
        &rows,
        "No GitHub installations linked.",
    )
}

/// Render a repository list as `<count>: <first three>, …` (no ellipsis when
/// there are three or fewer).
fn format_repositories(repositories: &[String]) -> String {
    let shown: Vec<&str> = repositories.iter().take(3).map(String::as_str).collect();
    let mut out = format!("{}: {}", repositories.len(), shown.join(", "));
    if repositories.len() > 3 {
        out.push_str(", …");
    }
    out
}

impl GithubAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            GithubAction::Link(GithubOutputArgs { connect, json }) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_start_github_link()
                    .tenant_id(tenant_id)
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("manage_start_github_link failed"))?
                    .into_inner();
                if json {
                    crate::commands::print_json(&v)?;
                } else {
                    println!(
                        "Open this URL in a browser where you are signed in to SignalDB as an admin of tenant {tenant_id}:\n\n  {}\n\nThe link expires at {}. After GitHub redirects back, run `signaldb-cli tenant github list` to see the installation.",
                        v.install_url,
                        v.expires_at.to_rfc3339()
                    );
                }
                Ok(())
            }
            GithubAction::List(GithubOutputArgs { connect, json }) => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_list_github_installations()
                    .tenant_id(tenant_id)
                    .send()
                    .await
                    .map_err(|e| {
                        anyhow::Error::new(e).context("manage_list_github_installations failed")
                    })?
                    .into_inner();
                if json {
                    crate::commands::print_json(&v)?;
                } else if !v.configured {
                    println!(
                        "GitHub integration is not configured on this server ([github] section)."
                    );
                } else {
                    println!("{}", format_github_installation_table(&v.installations));
                }
                Ok(())
            }
            GithubAction::Attach {
                installation_id,
                output: GithubOutputArgs { connect, json },
            } => {
                let tenant_id = require_tenant_id(&connect)?;
                let v = connect
                    .build_client()?
                    .manage_attach_github_installation()
                    .tenant_id(tenant_id)
                    .body(signaldb_sdk::types::AttachGitHubInstallationRequest { installation_id })
                    .send()
                    .await
                    .map_err(|e| {
                        anyhow::Error::new(e).context("manage_attach_github_installation failed")
                    })?
                    .into_inner();
                if json {
                    crate::commands::print_json(&v)?;
                } else {
                    println!(
                        "GitHub installation {} ({}) attached.",
                        v.installation_id, v.account_login
                    );
                }
                Ok(())
            }
            GithubAction::Remove {
                installation_id,
                confirm,
                connect,
            } => {
                let tenant_id = require_tenant_id(&connect)?;
                confirm_destructive(
                    &confirm,
                    "remove GitHub installation",
                    &installation_id.to_string(),
                )?;
                connect
                    .build_client()?
                    .manage_remove_github_installation()
                    .tenant_id(tenant_id)
                    .installation_id(installation_id)
                    .send()
                    .await
                    .map_err(|e| {
                        anyhow::Error::new(e).context("manage_remove_github_installation failed")
                    })?;
                println!("GitHub installation {installation_id} removed.");
                Ok(())
            }
        }
    }
}

/// `tenant source-context` args: connection plus the lookup parameters.
#[derive(Args)]
pub struct SourceContextArgs {
    /// File path within the repository
    #[arg(long)]
    path: String,
    /// 1-based line number to center the snippet on
    #[arg(long)]
    line: u32,
    /// `owner/name`, or a GitHub URL naming the repository; omit to probe
    /// every repository covered by the tenant's linked installations by
    /// path alone
    #[arg(long)]
    repository: Option<String>,
    /// The ref (branch, tag, or commit SHA) to read the file at; omit for
    /// the repository's default branch
    #[arg(long = "ref")]
    git_ref: Option<String>,
    /// Lines of context on each side of `--line`; omit for the router's
    /// default
    #[arg(long)]
    context: Option<u32>,
    #[command(flatten)]
    connect: ConnectArgs,
    /// Print the raw JSON response instead of the human-readable snippet
    #[arg(long)]
    json: bool,
}

/// Render `repository path (@ref | default branch)` followed by the
/// numbered snippet lines with a `>` marker on the target line, or the
/// unavailable-reason sentence. A malformed `available` response missing
/// its snippet degrades to the unavailable rendering rather than panicking
/// — same defensive stance as `flamegraph_or_not_found` in the MCP server.
/// Pure and synchronous, so it's directly unit-testable without a server.
fn format_source_context(response: &SourceContextResponse) -> String {
    match (response.status, response.snippet.as_ref()) {
        (SourceContextStatus::Available, Some(snippet)) => {
            let ref_label = snippet
                .ref_
                .as_deref()
                .map(|r| format!("@{r}"))
                .unwrap_or_else(|| "default branch".to_string());
            let mut out = format!("{} {} ({})\n", snippet.repository, snippet.path, ref_label);
            for (offset, text) in snippet.lines.iter().enumerate() {
                let line_no = snippet.start_line + offset as i32;
                let marker = if line_no == snippet.line { '>' } else { ' ' };
                out.push_str(&format!("{marker} {line_no:>5} | {text}\n"));
            }
            out.trim_end().to_string()
        }
        _ => {
            let reason = response
                .reason
                .map(|r| r.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            format!("Source context unavailable: {reason}")
        }
    }
}

async fn run_source_context(args: SourceContextArgs) -> anyhow::Result<()> {
    let tenant_id = require_tenant_id(&args.connect)?;
    let v = args
        .connect
        .build_client()?
        .source_context()
        .tenant_id(tenant_id)
        .body(SourceContextRequest {
            repository: args.repository,
            ref_: args.git_ref,
            path: args.path,
            line: args.line as i32,
            context_lines: args.context.map(|c| c as i32),
        })
        .send()
        .await
        .map_err(|e| anyhow::Error::new(e).context("source_context failed"))?
        .into_inner();
    if args.json {
        crate::commands::print_json(&v)?;
    } else {
        println!("{}", format_source_context(&v));
    }
    Ok(())
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

    #[test]
    fn api_key_create_accepts_repeated_allowed_origin_flag() {
        let parsed = TestCli::try_parse_from([
            "tenant",
            "api-key",
            "create",
            "--name",
            "ci",
            "--scope",
            "traces:write",
            "--allowed-origin",
            "https://a.example",
            "--allowed-origin",
            "https://b.example",
        ])
        .expect("parses");
        let TenantSelfAction::ApiKey {
            action: ApiKeyAction::Create { allowed_origin, .. },
        } = parsed.action
        else {
            panic!("expected api-key create");
        };
        assert_eq!(
            allowed_origin,
            Some(vec![
                "https://a.example".to_string(),
                "https://b.example".to_string()
            ])
        );
    }

    #[test]
    fn api_key_update_accepts_repeated_allowed_origin_flag_and_clear_flag() {
        let parsed = TestCli::try_parse_from([
            "tenant",
            "api-key",
            "update",
            "k1",
            "--allowed-origin",
            "https://a.example",
            "--allowed-origin",
            "https://b.example",
        ])
        .expect("parses");
        let TenantSelfAction::ApiKey {
            action: ApiKeyAction::Update { allowed_origin, .. },
        } = parsed.action
        else {
            panic!("expected api-key update");
        };
        assert_eq!(
            allowed_origin,
            Some(vec![
                "https://a.example".to_string(),
                "https://b.example".to_string()
            ])
        );

        let parsed = TestCli::try_parse_from([
            "tenant",
            "api-key",
            "update",
            "k1",
            "--allowed-origin",
            "https://a.example",
        ])
        .expect("parses");
        let TenantSelfAction::ApiKey {
            action:
                ApiKeyAction::Update {
                    clear_allowed_origins,
                    ..
                },
        } = parsed.action
        else {
            panic!("expected api-key update");
        };
        assert!(!clear_allowed_origins);
    }

    #[test]
    fn api_key_update_rejects_allowed_origin_and_clear_allowed_origins_together() {
        let parsed = TestCli::try_parse_from([
            "tenant",
            "api-key",
            "update",
            "k1",
            "--allowed-origin",
            "https://a.example",
            "--clear-allowed-origins",
        ]);
        assert!(
            parsed.is_err(),
            "--allowed-origin and --clear-allowed-origins must conflict at the CLI level"
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
            .with_body(
                r#"{"user_id":"u1","email":"bob@example.com","role":"member","granted_by":"local"}"#,
            )
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
            allowed_origin: None,
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
            allowed_origin: None,
            clear_allowed_origins: false,
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
            allowed_origin: None,
            clear_allowed_origins: false,
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
            allowed_origin: None,
            clear_allowed_origins: false,
            connect: connect_acme(&server),
        }
        .run()
        .await
        .expect("update succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn tenant_api_key_update_sends_allowed_origins() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("PATCH", "/api/v1/manage/tenants/acme/api-keys/k1")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "allowed_origins": ["https://a.example", "https://b.example"]
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"id":"k1","name":"ci","scopes":["traces:write"],"allowed_origins":["https://a.example","https://b.example"],"created_at":"2026-01-01T00:00:00Z","revoked":false}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::Update {
            key_id: "k1".into(),
            scopes: vec![],
            dataset: None,
            clear_dataset_restriction: false,
            allowed_origin: Some(vec!["https://a.example".into(), "https://b.example".into()]),
            clear_allowed_origins: false,
            connect: connect_acme(&server),
        }
        .run()
        .await
        .expect("update succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn tenant_api_key_update_clears_allowed_origins() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("PATCH", "/api/v1/manage/tenants/acme/api-keys/k1")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "clear_allowed_origins": true
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
            clear_dataset_restriction: false,
            allowed_origin: None,
            clear_allowed_origins: true,
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
            allowed_origin: None,
            clear_allowed_origins: false,
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

    #[test]
    fn format_tenant_api_key_list_shows_allowed_origins_restriction_or_unrestricted() {
        let keys: Vec<ManageApiKeyResponse> = serde_json::from_str(
            r#"[
                {"id":"k1","name":"ci","scopes":["traces:write"],"allowed_origins":["https://a.example","https://b.example"],"created_at":"2026-01-01T00:00:00Z","revoked":false},
                {"id":"k2","name":"full","scopes":["schema:read"],"allowed_origins":null,"created_at":"2026-01-01T00:00:00Z","revoked":false}
            ]"#,
        )
        .unwrap();

        let rendered = format_tenant_api_key_list(&keys);

        assert!(rendered.contains("https://a.example, https://b.example"));
        assert!(rendered.contains("unrestricted"));
    }

    #[test]
    fn github_subcommands_parse() {
        assert!(TestCli::try_parse_from(["tenant", "github", "link"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "github", "list"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "github", "attach", "42"]).is_ok());
        assert!(TestCli::try_parse_from(["tenant", "github", "remove", "42", "--yes"]).is_ok());
        // Attach and removal need an installation id.
        assert!(TestCli::try_parse_from(["tenant", "github", "attach"]).is_err());
        assert!(TestCli::try_parse_from(["tenant", "github", "remove"]).is_err());
    }

    #[test]
    fn source_context_subcommand_parses() {
        assert!(
            TestCli::try_parse_from([
                "tenant",
                "source-context",
                "--path",
                "src/main.rs",
                "--line",
                "42",
            ])
            .is_ok()
        );
        assert!(
            TestCli::try_parse_from([
                "tenant",
                "source-context",
                "--path",
                "src/main.rs",
                "--line",
                "42",
                "--repository",
                "octo/api",
                "--ref",
                "main",
                "--context",
                "5",
            ])
            .is_ok()
        );
        // `--path` and `--line` are required.
        assert!(TestCli::try_parse_from(["tenant", "source-context"]).is_err());
        assert!(
            TestCli::try_parse_from(["tenant", "source-context", "--path", "src/main.rs"]).is_err()
        );
    }

    fn source_context_response(json: &str) -> SourceContextResponse {
        serde_json::from_str(json).expect("valid SourceContextResponse fixture")
    }

    #[test]
    fn format_source_context_renders_the_snippet_with_a_marker_on_the_target_line() {
        let response = source_context_response(
            r#"{
                "status": "available",
                "snippet": {
                    "repository": "octo/api",
                    "path": "src/f.rs",
                    "ref": "main",
                    "line": 3,
                    "start_line": 2,
                    "lines": ["two", "three", "four"],
                    "sha": "sha-abc",
                    "html_url": "https://github.com/octo/api/blob/main/src/f.rs#L3"
                }
            }"#,
        );

        let rendered = format_source_context(&response);

        assert!(rendered.starts_with("octo/api src/f.rs (@main)"));
        assert!(rendered.contains("> "));
        assert!(rendered.contains("three"));
        // Only the target line carries the marker.
        let marked_lines: Vec<&str> = rendered.lines().filter(|l| l.starts_with('>')).collect();
        assert_eq!(marked_lines.len(), 1);
        assert!(marked_lines[0].contains("three"));
    }

    #[test]
    fn format_source_context_labels_an_omitted_ref_as_the_default_branch() {
        let response = source_context_response(
            r#"{
                "status": "available",
                "snippet": {
                    "repository": "octo/api",
                    "path": "f.rs",
                    "line": 1,
                    "start_line": 1,
                    "lines": ["a"],
                    "sha": "sha",
                    "html_url": "https://github.com/octo/api/blob/main/f.rs#L1"
                }
            }"#,
        );

        assert!(format_source_context(&response).contains("(default branch)"));
    }

    #[test]
    fn format_source_context_reports_the_unavailable_reason() {
        let response =
            source_context_response(r#"{ "status": "unavailable", "reason": "no_installation" }"#);

        assert_eq!(
            format_source_context(&response),
            "Source context unavailable: no_installation"
        );
    }

    #[tokio::test]
    async fn source_context_requires_tenant_id() {
        let result = run_source_context(SourceContextArgs {
            path: "f.rs".into(),
            line: 1,
            repository: None,
            git_ref: None,
            context: None,
            connect: ConnectArgs {
                url: "http://127.0.0.1:1".to_string(),
                api_key: Some("sk-test".to_string()),
                tenant_id: None,
                dataset_id: None,
            },
            json: false,
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn source_context_sends_the_request_and_prints_the_snippet() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/tenants/acme/source-context")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "repository": "octo/api",
                "ref": "main",
                "path": "src/f.rs",
                "line": 3,
                "context_lines": 1
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"status":"available","snippet":{"repository":"octo/api","path":"src/f.rs","ref":"main","line":3,"start_line":2,"lines":["two","three","four"],"sha":"sha","html_url":"https://github.com/octo/api/blob/main/src/f.rs#L3"}}"#,
            )
            .create_async()
            .await;

        run_source_context(SourceContextArgs {
            path: "src/f.rs".into(),
            line: 3,
            repository: Some("octo/api".into()),
            git_ref: Some("main".into()),
            context: Some(1),
            connect: connect_acme(&server),
            json: false,
        })
        .await
        .expect("source-context succeeds");
        mock.assert_async().await;
    }

    fn sample_installation(
        overrides: impl FnOnce(&mut GitHubInstallationResponse),
    ) -> GitHubInstallationResponse {
        let mut installation: GitHubInstallationResponse = serde_json::from_str(
            r#"{
                "account_login": "octo",
                "account_type": "Organization",
                "created_at": "2026-01-01T00:00:00Z",
                "installation_id": 1,
                "manage_url": "https://github.com/organizations/octo/settings/installations/1",
                "repositories": ["octo/api"],
                "repositories_synced_at": "2026-01-01T00:00:00Z",
                "stale": false,
                "updated_at": "2026-01-01T00:00:00Z"
            }"#,
        )
        .unwrap();
        overrides(&mut installation);
        installation
    }

    #[test]
    fn format_github_installation_table_reports_empty_state() {
        assert_eq!(
            format_github_installation_table(&[]),
            "No GitHub installations linked."
        );
    }

    #[test]
    fn format_github_installation_table_renders_one_org_row() {
        let installation = sample_installation(|_| {});

        let rendered = format_github_installation_table(&[installation]);

        assert!(rendered.starts_with("INSTALLATION"));
        assert!(rendered.contains("ACCOUNT"));
        assert!(rendered.contains("REPOSITORIES"));
        assert!(rendered.contains("SYNCED"));
        assert!(rendered.contains("LINKED BY"));
        assert!(rendered.contains('1'));
        assert!(rendered.contains("octo (Organization)"));
        assert!(rendered.contains("1: octo/api"));
        assert!(rendered.contains("2026-01-01T00:00:00Z"));
        assert!(!rendered.contains("stale"));
        assert!(rendered.trim_end().ends_with('-'), "{rendered}");
    }

    #[test]
    fn format_github_installation_table_marks_stale_rows() {
        let installation = sample_installation(|installation| installation.stale = true);

        let rendered = format_github_installation_table(&[installation]);

        assert!(rendered.contains("(stale)"));
    }

    #[test]
    fn format_github_installation_table_truncates_more_than_three_repositories() {
        let installation = sample_installation(|installation| {
            installation.repositories = vec![
                "octo/api".to_string(),
                "octo/web".to_string(),
                "octo/worker".to_string(),
                "octo/docs".to_string(),
            ];
            installation.linked_by_github_login = Some("alice".to_string());
        });

        let rendered = format_github_installation_table(&[installation]);

        assert!(rendered.contains("4: octo/api, octo/web, octo/worker, …"));
        assert!(!rendered.contains("octo/docs"));
        assert!(rendered.contains("@alice"));
    }

    #[tokio::test]
    async fn github_link_requires_tenant_id() {
        let result = GithubAction::Link(GithubOutputArgs {
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
    async fn github_link_prints_the_install_url() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "POST",
                "/api/v1/manage/tenants/acme/github-installations/link",
            )
            .match_header("authorization", "Bearer sk-test")
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"install_url":"https://github.com/apps/signaldb/installations/new?state=abc","expires_at":"2026-01-01T00:10:00Z"}"#,
            )
            .create_async()
            .await;

        GithubAction::Link(GithubOutputArgs {
            connect: connect_acme(&server),
            json: false,
        })
        .run()
        .await
        .expect("link succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn github_attach_hits_the_attach_endpoint() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "POST",
                "/api/v1/manage/tenants/acme/github-installations/attach",
            )
            .match_header("authorization", "Bearer sk-test")
            .match_body(r#"{"installation_id":42}"#)
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"installation_id":42,"account_login":"acme-org","account_type":"Organization","repositories":[],"repositories_synced_at":"2026-01-01T00:00:00Z","stale":false,"linked_by_github_login":null,"manage_url":"https://github.com/organizations/acme-org/settings/installations/42","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}"#,
            )
            .create_async()
            .await;

        GithubAction::Attach {
            installation_id: 42,
            output: GithubOutputArgs {
                connect: connect_acme(&server),
                json: false,
            },
        }
        .run()
        .await
        .expect("attach succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn github_list_reports_unconfigured_server() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/api/v1/manage/tenants/acme/github-installations")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"configured":false,"installations":[]}"#)
            .create_async()
            .await;

        GithubAction::List(GithubOutputArgs {
            connect: connect_acme(&server),
            json: false,
        })
        .run()
        .await
        .expect("list succeeds");
    }

    #[tokio::test]
    async fn github_remove_hits_the_delete_endpoint() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "DELETE",
                "/api/v1/manage/tenants/acme/github-installations/42",
            )
            .match_header("authorization", "Bearer sk-test")
            .with_status(204)
            .create_async()
            .await;

        GithubAction::Remove {
            installation_id: 42,
            confirm: ConfirmArgs { yes: true },
            connect: connect_acme(&server),
        }
        .run()
        .await
        .expect("remove succeeds");
        mock.assert_async().await;
    }
}
