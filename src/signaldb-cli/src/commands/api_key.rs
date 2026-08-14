use clap::Subcommand;
use clap_complete::engine::ArgValueCompleter;
use signaldb_sdk::Client;
use signaldb_sdk::types::CreateApiKeyRequest;

use super::completions::tenant_id_completer;

#[derive(Subcommand)]
pub enum ApiKeyAction {
    /// List API keys for a tenant
    List {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        tenant_id: String,
    },
    /// Create a new API key for a tenant
    Create {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        tenant_id: String,
        /// Optional key name
        #[arg(long)]
        name: Option<String>,
    },
    /// Revoke an API key
    Revoke {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
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
                crate::commands::print_json(&resp)?;
            }
            ApiKeyAction::Create { tenant_id, name } => {
                let resp = client
                    .create_api_key()
                    .tenant_id(&tenant_id)
                    .body(CreateApiKeyRequest { name })
                    .send()
                    .await?
                    .into_inner();
                crate::commands::print_json(&resp)?;
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
