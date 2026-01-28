use clap::Subcommand;
use signaldb_api::CreateDatasetRequest;
use signaldb_sdk::SignalDbClient;

#[derive(Subcommand)]
pub enum DatasetAction {
    /// List datasets for a tenant
    List {
        /// Tenant ID
        tenant_id: String,
    },
    /// Create a new dataset for a tenant
    Create {
        /// Tenant ID
        tenant_id: String,
        /// Dataset name
        #[arg(long)]
        name: String,
    },
    /// Delete a dataset
    Delete {
        /// Tenant ID
        tenant_id: String,
        /// Dataset ID
        dataset_id: String,
    },
}

impl DatasetAction {
    pub async fn run(self, client: &SignalDbClient) -> anyhow::Result<()> {
        match self {
            DatasetAction::List { tenant_id } => {
                let resp = client.list_datasets(&tenant_id).await?;
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            DatasetAction::Create { tenant_id, name } => {
                let resp = client
                    .create_dataset(&tenant_id, CreateDatasetRequest { name })
                    .await?;
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            DatasetAction::Delete {
                tenant_id,
                dataset_id,
            } => {
                client.delete_dataset(&tenant_id, &dataset_id).await?;
                println!("Dataset '{dataset_id}' deleted.");
            }
        }
        Ok(())
    }
}
