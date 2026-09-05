use clap::{ArgAction, Subcommand};
use clap_complete::engine::ArgValueCompleter;
use signaldb_sdk::Client;
use signaldb_sdk::types::{ApiKeyResponse, CreateApiKeyRequest, UpdateApiKeyRequest};

use super::completions::tenant_id_completer;

#[derive(Subcommand)]
pub enum ApiKeyAction {
    /// List API keys for a tenant (with their scopes and dataset restriction)
    List {
        /// Tenant ID
        #[arg(add = ArgValueCompleter::new(tenant_id_completer))]
        tenant_id: String,
        /// Print raw JSON instead of the ID/NAME/SCOPES/DATASETS table
        #[arg(long)]
        json: bool,
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
        /// metrics:read, profiles:read, schema:read, schema:write,
        /// tenant:manage (manage this tenant's datasets, keys, and members)
        #[arg(long = "scope", required = true, value_name = "SCOPE")]
        scopes: Vec<String>,
        /// Restrict the key to these datasets of the tenant (repeatable);
        /// omit for an unrestricted key
        #[arg(long = "dataset", action = ArgAction::Append, value_name = "DATASET")]
        dataset: Option<Vec<String>>,
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
        /// Replacement dataset restriction (repeatable); omit to leave the
        /// current restriction unchanged
        #[arg(long = "dataset", action = ArgAction::Append, value_name = "DATASET")]
        dataset: Option<Vec<String>>,
        /// Clear an existing dataset restriction back to unrestricted;
        /// cannot be combined with --dataset
        #[arg(long, conflicts_with = "dataset")]
        clear_dataset_restriction: bool,
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

/// Render `ID  NAME  SCOPES  DATASETS` rows, column-aligned.
fn format_api_key_list(keys: &[ApiKeyResponse]) -> String {
    if keys.is_empty() {
        return "No API keys.".to_string();
    }

    let rows: Vec<(String, String, String, String)> = keys
        .iter()
        .map(|k| {
            let name = k.name.clone().unwrap_or_else(|| "-".to_string());
            let scopes = k
                .scopes
                .as_ref()
                .filter(|s| !s.is_empty())
                .map(|s| s.join(", "))
                .unwrap_or_else(|| "-".to_string());
            let datasets = super::format_dataset_restriction(k.dataset_ids.as_deref());
            (k.id.clone(), name, scopes, datasets)
        })
        .collect();

    let widths = [
        rows.iter()
            .map(|(v, ..)| v.len())
            .chain(std::iter::once("ID".len()))
            .max()
            .unwrap_or(0),
        rows.iter()
            .map(|(_, v, ..)| v.len())
            .chain(std::iter::once("NAME".len()))
            .max()
            .unwrap_or(0),
        rows.iter()
            .map(|(_, _, v, _)| v.len())
            .chain(std::iter::once("SCOPES".len()))
            .max()
            .unwrap_or(0),
    ];

    let mut out = format!(
        "{:w0$}  {:w1$}  {:w2$}  DATASETS\n",
        "ID",
        "NAME",
        "SCOPES",
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2]
    );
    for (id, name, scopes, datasets) in rows {
        out.push_str(&format!(
            "{id:w0$}  {name:w1$}  {scopes:w2$}  {datasets}\n",
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2]
        ));
    }
    out.trim_end().to_string()
}

impl ApiKeyAction {
    pub async fn run(self, client: &Client) -> anyhow::Result<()> {
        match self {
            ApiKeyAction::List { tenant_id, json } => {
                let resp = client
                    .list_api_keys()
                    .tenant_id(&tenant_id)
                    .send()
                    .await?
                    .into_inner();
                if json {
                    crate::commands::print_json(&resp)?;
                } else {
                    println!("{}", format_api_key_list(&resp.api_keys));
                }
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
                        dataset_ids: dataset,
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
                clear_dataset_restriction,
            } => {
                if scopes.is_empty() && dataset.is_none() && !clear_dataset_restriction {
                    anyhow::bail!(
                        "nothing to update: pass --scope, --dataset, and/or --clear-dataset-restriction"
                    );
                }
                let resp = client
                    .update_api_key()
                    .tenant_id(&tenant_id)
                    .key_id(&key_id)
                    .body(UpdateApiKeyRequest {
                        scopes: (!scopes.is_empty()).then_some(scopes),
                        dataset_ids: dataset,
                        clear_dataset_restriction: clear_dataset_restriction.then_some(true),
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
        signaldb_sdk::ClientBuilder::new(server.url())
            .build()
            .unwrap()
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
            "--dataset",
            "staging",
        ])
        .expect("parses");
        match parsed.action {
            ApiKeyAction::Create {
                scopes, dataset, ..
            } => {
                assert_eq!(scopes, vec!["traces:write", "schema:read"]);
                assert_eq!(
                    dataset,
                    Some(vec!["production".to_string(), "staging".to_string()])
                );
            }
            _ => panic!("expected create"),
        }
    }

    #[test]
    fn create_without_dataset_flag_is_unrestricted() {
        let parsed = Harness::try_parse_from([
            "h", "create", "acme", "--name", "ci", "--scope", "traces:write",
        ])
        .expect("parses");
        match parsed.action {
            ApiKeyAction::Create { dataset, .. } => assert_eq!(dataset, None),
            _ => panic!("expected create"),
        }
    }

    #[test]
    fn update_accepts_repeated_dataset_flag() {
        let parsed = Harness::try_parse_from([
            "h",
            "update",
            "acme",
            "k1",
            "--dataset",
            "production",
            "--dataset",
            "staging",
        ])
        .expect("parses");
        match parsed.action {
            ApiKeyAction::Update { dataset, .. } => {
                assert_eq!(
                    dataset,
                    Some(vec!["production".to_string(), "staging".to_string()])
                );
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn update_without_dataset_flag_leaves_restriction_unchanged() {
        let parsed = Harness::try_parse_from(["h", "update", "acme", "k1", "--scope", "logs:write"])
            .expect("parses");
        match parsed.action {
            ApiKeyAction::Update { dataset, .. } => assert_eq!(dataset, None),
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn update_accepts_clear_dataset_restriction_alone() {
        let parsed = Harness::try_parse_from([
            "h",
            "update",
            "acme",
            "k1",
            "--clear-dataset-restriction",
        ])
        .expect("parses");
        match parsed.action {
            ApiKeyAction::Update {
                dataset,
                clear_dataset_restriction,
                ..
            } => {
                assert_eq!(dataset, None);
                assert!(clear_dataset_restriction);
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn update_rejects_dataset_and_clear_dataset_restriction_together() {
        let parsed = Harness::try_parse_from([
            "h",
            "update",
            "acme",
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

    #[tokio::test]
    async fn create_sends_scopes_and_multiple_datasets_to_admin_api() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/admin/tenants/acme/api-keys")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "name": "ci",
                "scopes": ["traces:write", "schema:read"],
                "dataset_ids": ["production", "staging"]
            })))
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"id":"k1","key":"sk-acme-1","name":"ci","scopes":["traces:write","schema:read"],"dataset_ids":["production","staging"],"created_at":"2026-01-01T00:00:00Z"}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::Create {
            tenant_id: "acme".into(),
            name: Some("ci".into()),
            scopes: vec!["traces:write".into(), "schema:read".into()],
            dataset: Some(vec!["production".into(), "staging".into()]),
        }
        .run(&sdk_client(&server))
        .await
        .expect("create succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn update_patches_scopes_and_dataset_set() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("PATCH", "/api/v1/admin/tenants/acme/api-keys/k1")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "scopes": ["schema:read", "schema:write"],
                "dataset_ids": ["production", "staging"]
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"id":"k1","name":"ci","scopes":["schema:read","schema:write"],"dataset_ids":["production","staging"],"created_at":"2026-01-01T00:00:00Z"}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::Update {
            tenant_id: "acme".into(),
            key_id: "k1".into(),
            scopes: vec!["schema:read".into(), "schema:write".into()],
            dataset: Some(vec!["production".into(), "staging".into()]),
            clear_dataset_restriction: false,
        }
        .run(&sdk_client(&server))
        .await
        .expect("update succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn update_clears_dataset_restriction() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("PATCH", "/api/v1/admin/tenants/acme/api-keys/k1")
            .match_body(mockito::Matcher::Json(serde_json::json!({
                "clear_dataset_restriction": true
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"id":"k1","name":"ci","scopes":["schema:read"],"created_at":"2026-01-01T00:00:00Z"}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::Update {
            tenant_id: "acme".into(),
            key_id: "k1".into(),
            scopes: vec![],
            dataset: None,
            clear_dataset_restriction: true,
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
            clear_dataset_restriction: false,
        }
        .run(&sdk_client(&server))
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn list_defaults_to_a_human_readable_table_with_dataset_restriction() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/api/v1/admin/tenants/acme/api-keys")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"api_keys":[
                    {"id":"k1","name":"ci","scopes":["traces:write"],"dataset_ids":["production","staging"],"created_at":"2026-01-01T00:00:00Z"},
                    {"id":"k2","name":"full","scopes":["schema:read"],"dataset_ids":null,"created_at":"2026-01-01T00:00:00Z"}
                ]}"#,
            )
            .create_async()
            .await;

        ApiKeyAction::List {
            tenant_id: "acme".into(),
            json: false,
        }
        .run(&sdk_client(&server))
        .await
        .expect("list succeeds");
    }

    #[test]
    fn format_api_key_list_shows_dataset_restriction_or_unrestricted() {
        let keys: Vec<ApiKeyResponse> = serde_json::from_str(
            r#"[
                {"id":"k1","name":"ci","scopes":["traces:write"],"dataset_ids":["production","staging"],"created_at":"2026-01-01T00:00:00Z"},
                {"id":"k2","name":"full","scopes":["schema:read"],"dataset_ids":null,"created_at":"2026-01-01T00:00:00Z"}
            ]"#,
        )
        .unwrap();

        let rendered = format_api_key_list(&keys);

        assert!(rendered.contains("production, staging"));
        assert!(rendered.contains("unrestricted"));
    }
}
