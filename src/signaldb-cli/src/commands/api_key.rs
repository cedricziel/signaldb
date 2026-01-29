use clap::Subcommand;
use signaldb_sdk::Client;
use signaldb_sdk::types::CreateApiKeyRequest;

#[derive(Subcommand)]
pub enum ApiKeyAction {
    /// List API keys for a tenant
    List {
        /// Tenant ID
        tenant_id: String,
    },
    /// Create a new API key for a tenant
    Create {
        /// Tenant ID
        tenant_id: String,
        /// Optional key name
        #[arg(long)]
        name: Option<String>,
    },
    /// Revoke an API key
    Revoke {
        /// Tenant ID
        tenant_id: String,
        /// API key ID to revoke
        key_id: String,
    },
}

impl ApiKeyAction {
    pub async fn run(self, client: &Client) -> anyhow::Result<()> {
        match self {
            ApiKeyAction::List { tenant_id } => {
                let resp = client
                    .list_api_keys()
                    .tenant_id(&tenant_id)
                    .send()
                    .await?
                    .into_inner();
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            ApiKeyAction::Create { tenant_id, name } => {
                let resp = client
                    .create_api_key()
                    .tenant_id(&tenant_id)
                    .body(CreateApiKeyRequest { name })
                    .send()
                    .await?
                    .into_inner();
                println!("{}", serde_json::to_string_pretty(&resp)?);
            }
            ApiKeyAction::Revoke { tenant_id, key_id } => {
                client
                    .revoke_api_key()
                    .tenant_id(&tenant_id)
                    .key_id(&key_id)
                    .send()
                    .await?;
                println!("API key '{key_id}' revoked.");
            }
        }
        Ok(())
    }
}
