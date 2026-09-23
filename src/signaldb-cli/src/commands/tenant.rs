use clap::Subcommand;
use clap_complete::engine::ArgValueCompleter;
use signaldb_sdk::Client;
use signaldb_sdk::types::{CreateTenantRequest, UpdateTenantRequest};

use super::completions::tenant_id_completer;

#[derive(Subcommand)]
pub enum TenantAction {
    /// List all tenants
    List,
    /// Create a new tenant
    Create {
        /// Tenant ID
        id: String,
        /// Tenant name
        #[arg(long)]
        name: String,
        /// Default dataset name
        #[arg(long)]
        default_dataset: Option<String>,
    },
    /// Get tenant details
    Get {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        id: String,
    },
    /// Update a tenant
    Update {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        id: String,
        /// New tenant name
        #[arg(long)]
        name: Option<String>,
        /// New default dataset
        #[arg(long)]
        default_dataset: Option<String>,
    },
    /// Delete a tenant
    Delete {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        id: String,
    },
}

impl TenantAction {
    pub async fn run(self, client: &Client) -> anyhow::Result<()> {
        match self {
            TenantAction::List => {
                let resp = client.list_tenants().send().await?.into_inner();
                crate::commands::print_json(&resp)?;
            }
            TenantAction::Create {
                id,
                name,
                default_dataset,
            } => {
                let resp = client
                    .create_tenant()
                    .body(CreateTenantRequest {
                        id,
                        name,
                        default_dataset,
                    })
                    .send()
                    .await?
                    .into_inner();
                crate::commands::print_json(&resp)?;
            }
            TenantAction::Get { id } => {
                let resp = client
                    .get_tenant()
                    .tenant_id(&id)
                    .send()
                    .await?
                    .into_inner();
                crate::commands::print_json(&resp)?;
            }
            TenantAction::Update {
                id,
                name,
                default_dataset,
            } => {
                let resp = client
                    .update_tenant()
                    .tenant_id(&id)
                    .body(UpdateTenantRequest {
                        name,
                        default_dataset,
                    })
                    .send()
                    .await?
                    .into_inner();
                crate::commands::print_json(&resp)?;
            }
            TenantAction::Delete { id } => {
                client.delete_tenant().tenant_id(&id).send().await?;
                println!("Tenant '{id}' deleted.");
            }
        }
        Ok(())
    }
}
