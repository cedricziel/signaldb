use clap::Subcommand;
use clap_complete::engine::ArgValueCompleter;
use signaldb_sdk::Client;
use signaldb_sdk::types::ManageCreateDatasetRequest;

use super::completions::tenant_id_completer;

#[derive(Subcommand)]
pub enum DatasetAction {
    /// List datasets for a tenant
    List {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        tenant_id: String,
    },
    /// Create a new dataset for a tenant
    Create {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        tenant_id: String,
        /// Dataset name
        #[arg(long)]
        name: String,
    },
    /// Delete a dataset
    Delete {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        tenant_id: String,
        /// Dataset ID
        dataset_id: String,
    },
}

impl DatasetAction {
    pub async fn run(self, client: &Client) -> anyhow::Result<()> {
        match self {
            DatasetAction::List { tenant_id } => {
                let resp = client
                    .manage_list_datasets()
                    .tenant_id(&tenant_id)
                    .send()
                    .await?
                    .into_inner();
                crate::commands::print_json(&resp)?;
            }
            DatasetAction::Create { tenant_id, name } => {
                let resp = client
                    .manage_create_dataset()
                    .tenant_id(&tenant_id)
                    .body(ManageCreateDatasetRequest { name })
                    .send()
                    .await?
                    .into_inner();
                crate::commands::print_json(&resp)?;
            }
            DatasetAction::Delete {
                tenant_id,
                dataset_id,
            } => {
                client
                    .manage_delete_dataset()
                    .tenant_id(&tenant_id)
                    .dataset_name(&dataset_id)
                    .send()
                    .await?;
                println!("Dataset '{dataset_id}' deleted.");
            }
        }
        Ok(())
    }
}
