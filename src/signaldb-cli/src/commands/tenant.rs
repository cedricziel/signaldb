use clap::Subcommand;
use signaldb_sdk::Client;
use signaldb_sdk::types::{CreateTenantRequest, UpdateTenantRequest};

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
        id: String,
    },
    /// Update a tenant
    Update {
        /// Tenant ID
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
        id: String,
    },
}

impl TenantAction {
    pub async fn run(self, client: &Client) -> anyhow::Result<()> {
        match self {
            TenantAction::List => {
                let resp = client.list_tenants().send().await?.into_inner();
                println!("{}", serde_json::to_string_pretty(&resp)?);
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
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            TenantAction::Get { id } => {
                let resp = client
                    .get_tenant()
                    .tenant_id(&id)
                    .send()
                    .await?
                    .into_inner();
                println!("{}", serde_json::to_string_pretty(&resp)?);
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
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            TenantAction::Delete { id } => {
                client.delete_tenant().tenant_id(&id).send().await?;
                println!("Tenant '{id}' deleted.");
            }
        }
        Ok(())
    }
}
