use clap::Subcommand;
use clap_complete::engine::ArgValueCompleter;
use signaldb_sdk::Client;
use signaldb_sdk::types::CreateDatasetRequest;

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
                    .list_datasets()
                    .tenant_id(&tenant_id)
                    .send()
                    .await?
                    .into_inner();
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            DatasetAction::Create { tenant_id, name } => {
                let resp = client
                    .create_dataset()
                    .tenant_id(&tenant_id)
                    .body(CreateDatasetRequest { name })
                    .send()
                    .await?
                    .into_inner();
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            DatasetAction::Delete {
                tenant_id,
                dataset_id,
            } => {
                client
                    .delete_dataset()
                    .tenant_id(&tenant_id)
                    .dataset_id(&dataset_id)
                    .send()
                    .await?;
                println!("Dataset '{dataset_id}' deleted.");
            }
        }
        Ok(())
    }
}
