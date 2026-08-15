use clap::Subcommand;
use clap_complete::engine::ArgValueCompleter;
use signaldb_sdk::Client;
use signaldb_sdk::types::{CreateApiKeyRequest, UpdateApiKeyRequest};

use super::completions::tenant_id_completer;

#[derive(Subcommand)]
pub enum ApiKeyAction {
    /// List API keys for a tenant (with their scopes)
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
        /// Scope the key carries (repeatable, at least one): metrics:write,
        /// logs:write, traces:write, profiles:write, traces:read, logs:read,
        /// metrics:read, profiles:read, schema:read, schema:write
        #[arg(long = "scope", required = true, value_name = "SCOPE")]
        scopes: Vec<String>,
        /// Restrict the key to one dataset of the tenant
        #[arg(long)]
        dataset: Option<String>,
    },
    /// Update the scopes and/or dataset restriction of a live API key
    Update {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        tenant_id: String,
        /// API key ID to update
        key_id: String,
        /// Replacement scope list (repeatable); omit to keep the current scopes
        #[arg(long = "scope", value_name = "SCOPE")]
        scopes: Vec<String>,
        /// Replacement dataset restriction; omit to keep the current one
        #[arg(long)]
        dataset: Option<String>,
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
            ApiKeyAction::Create {
                tenant_id,
                name,
                scopes,
                dataset,
            } => {
                let resp = client
                    .create_api_key()
                    .tenant_id(&tenant_id)
                    .body(CreateApiKeyRequest {
                        name,
                        scopes,
                        dataset_id: dataset,
                    })
                    .send()
                    .await?
                    .into_inner();
                crate::commands::print_json(&resp)?;
            }
            ApiKeyAction::Update {
                tenant_id,
                key_id,
                scopes,
                dataset,
            } => {
                if scopes.is_empty() && dataset.is_none() {
                    anyhow::bail!("nothing to update: pass --scope and/or --dataset");
                }
                let resp = client
                    .update_api_key()
                    .tenant_id(&tenant_id)
                    .key_id(&key_id)
                    .body(UpdateApiKeyRequest {
                        scopes: (!scopes.is_empty()).then_some(scopes),
                        dataset_id: dataset,
                    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Harness {
        #[command(subcommand)]
        action: ApiKeyAction,
    }

    fn sdk_client(server: &mockito::ServerGuard) -> Client {
        Client::new_with_client(server.url().trim_end_matches('/'), reqwest::Client::new())
    }

    #[test]
    fn create_requires_at_least_one_scope() {
        let parsed = Harness::try_parse_from(["h", "create", "acme", "--name", "ci"]);
        assert!(parsed.is_err(), "--scope must be required");

        let parsed = Harness::try_parse_from([
            "h",
            "create",
            "acme",
            "--name",
            "ci",
            "--scope",
            "traces:write",
            "--scope",
            "schema:read",
            "--dataset",
            "production",
        ])
        .expect("parses");
        match parsed.action {
            ApiKeyAction::Create {
                scopes, dataset, ..
            } => {
                assert_eq!(scopes, vec!["traces:write", "schema:read"]);
                assert_eq!(dataset.as_deref(), Some("production"));
            }
            _ => panic!("expected create"),
        }
    }

    #[tokio::test]
    async fn create_sends_scopes_and_dataset_to_admin_api() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/admin/tenants/acme/api-keys")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "name": "ci",
                "scopes": ["traces:write", "schema:read"],
                "dataset_id": "production"
            })))
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"id":"k1","key":"sk-acme-1","name":"ci","scopes":["traces:write","schema:read"],"dataset_id":"production","created_at":"2026-01-01T00:00:00Z"}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::Create {
            tenant_id: "acme".into(),
            name: Some("ci".into()),
            scopes: vec!["traces:write".into(), "schema:read".into()],
            dataset: Some("production".into()),
        }
        .run(&sdk_client(&server))
        .await
        .expect("create succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn update_patches_scopes_and_dataset() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("PATCH", "/api/v1/admin/tenants/acme/api-keys/k1")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "scopes": ["schema:read", "schema:write"]
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"id":"k1","name":"ci","scopes":["schema:read","schema:write"],"created_at":"2026-01-01T00:00:00Z"}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::Update {
            tenant_id: "acme".into(),
            key_id: "k1".into(),
            scopes: vec!["schema:read".into(), "schema:write".into()],
            dataset: None,
        }
        .run(&sdk_client(&server))
        .await
        .expect("update succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn update_without_changes_is_an_error() {
        let server = mockito::Server::new_async().await;
        let result = ApiKeyAction::Update {
            tenant_id: "acme".into(),
            key_id: "k1".into(),
            scopes: vec![],
            dataset: None,
        }
        .run(&sdk_client(&server))
        .await;
        assert!(result.is_err());
    }
}
