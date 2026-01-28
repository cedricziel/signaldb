use clap::Subcommand;
use signaldb_api::{CreateTenantRequest, UpdateTenantRequest};
use signaldb_sdk::SignalDbClient;

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
    pub async fn run(self, client: &SignalDbClient) -> anyhow::Result<()> {
        match self {
            TenantAction::List => {
                let resp = client.list_tenants().await?;
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            TenantAction::Create {
                id,
                name,
                default_dataset,
            } => {
                let resp = client
                    .create_tenant(CreateTenantRequest {
                        id,
                        name,
                        default_dataset,
                    })
                    .await?;
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            TenantAction::Get { id } => {
                let resp = client.get_tenant(&id).await?;
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            TenantAction::Update {
                id,
                name,
                default_dataset,
            } => {
                let resp = client
                    .update_tenant(
                        &id,
                        UpdateTenantRequest {
                            name,
                            default_dataset,
                        },
                    )
                    .await?;
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            TenantAction::Delete { id } => {
                client.delete_tenant(&id).await?;
                println!("Tenant '{id}' deleted.");
            }
        }
        Ok(())
    }
}
