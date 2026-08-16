//! The `schema` command group and `admin schema`: schema-registry lookup and
//! custom-registry management via `signaldb-sdk` (openspec change
//! `schema-registry`, D7). Mirrors the MCP server's `list_schema_registries`,
//! `resolve_attribute` / `resolve_entity` / `resolve_metric`, `search_schema`
//! and `create/replace/delete_schema_registry` tools.
//!
//! - `signaldb-cli schema registry list|get <ns> <version>`
//! - `signaldb-cli schema attribute|entity|metric get <name>` — every
//!   definition of the name across the tenant's visible registries, in
//!   precedence order (custom → signaldb → otel), `primary` first
//! - `signaldb-cli schema attribute|entity|metric search <prefix> [--limit]`
//! - `signaldb-cli admin schema create|replace|validate --file <path>` (YAML
//!   or JSON) and `admin schema delete <ns> <version>`
//!
//! Every subcommand authenticates with a tenant API key (`--api-key` /
//! `--tenant-id`, or the `SIGNALDB_*` environment): reads need `schema:read`,
//! mutations `schema:write`. Output is the HTTP response JSON, unchanged.

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Args, Subcommand};

use super::discover::ConnectArgs;
use super::query::print_json_response;

/// `signaldb-cli schema <noun> <verb>` — read-only registry lookup.
#[derive(Subcommand)]
pub enum SchemaAction {
    /// List registries or fetch one registry document
    Registry {
        #[command(subcommand)]
        action: RegistryAction,
    },
    /// Resolve or search attribute keys (e.g. `k8s.pod.uid`)
    Attribute {
        #[command(subcommand)]
        action: LookupAction,
    },
    /// Resolve or search entity types (e.g. `k8s.pod`)
    Entity {
        #[command(subcommand)]
        action: LookupAction,
    },
    /// Resolve or search metric names (e.g. `k8s.pod.cpu.time`)
    Metric {
        #[command(subcommand)]
        action: LookupAction,
    },
}

#[derive(Subcommand)]
pub enum RegistryAction {
    /// List the registries visible to the tenant (bundled + custom), in
    /// precedence order, with definition counts
    List(ConnectArgs),
    /// Fetch one registry's summary and its document
    Get {
        /// Registry namespace (e.g. `otel`, `signaldb`, or a custom name)
        namespace: String,
        /// Registry version (e.g. `1.43.0`)
        version: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

#[derive(Subcommand)]
pub enum LookupAction {
    /// Resolve one name: every definition across the visible registries,
    /// precedence-ordered, `primary` first (empty when unknown)
    Get {
        /// Attribute key, entity type, or metric name
        name: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// List definitions whose name starts with a prefix
    Search {
        /// Name prefix (empty lists from the top)
        #[arg(default_value = "")]
        prefix: String,
        /// Maximum hits (server default 50, max 200)
        #[arg(long)]
        limit: Option<u64>,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

/// `signaldb-cli admin schema <verb>` — custom-registry management.
#[derive(Subcommand)]
pub enum AdminSchemaAction {
    /// Create a custom registry from a Weaver-model document (YAML or JSON)
    Create(DocumentArgs),
    /// Replace a custom registry's document (name/version must match)
    Replace {
        /// Registry namespace
        namespace: String,
        /// Registry version
        version: String,
        #[command(flatten)]
        document: DocumentArgs,
    },
    /// Delete a custom registry
    Delete {
        /// Registry namespace
        namespace: String,
        /// Registry version
        version: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    /// Validate a document without storing it (errors carry document paths)
    Validate(DocumentArgs),
}

#[derive(Args)]
pub struct DocumentArgs {
    /// Registry document: `.yaml`/`.yml` is parsed as YAML, `.json` as JSON;
    /// any other extension is tried as YAML (a superset of JSON)
    #[arg(long, value_name = "PATH")]
    file: PathBuf,
    #[command(flatten)]
    connect: ConnectArgs,
}

/// A registry document as the JSON object the API takes.
pub(crate) type Document = serde_json::Map<String, serde_json::Value>;

/// Read a registry document from disk, converting YAML to JSON. The API's
/// create/replace/validate bodies are the raw Weaver-model document as JSON.
pub(crate) fn read_document(path: &Path) -> anyhow::Result<Document> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_document(&text, path)
}

fn parse_document(text: &str, path: &Path) -> anyhow::Result<Document> {
    let is_json = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    let value: serde_json::Value = if is_json {
        serde_json::from_str(text)
            .with_context(|| format!("{} is not valid JSON", path.display()))?
    } else {
        serde_norway::from_str(text)
            .with_context(|| format!("{} is not valid YAML", path.display()))?
    };
    match value {
        serde_json::Value::Object(map) => Ok(map),
        _ => anyhow::bail!(
            "{} must contain a registry document object (name, version, groups)",
            path.display()
        ),
    }
}

impl SchemaAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            SchemaAction::Registry { action } => action.run().await,
            SchemaAction::Attribute { action } => action.run(Kind::Attribute).await,
            SchemaAction::Entity { action } => action.run(Kind::Entity).await,
            SchemaAction::Metric { action } => action.run(Kind::Metric).await,
        }
    }
}

impl RegistryAction {
    async fn run(self) -> anyhow::Result<()> {
        match self {
            RegistryAction::List(connect) => {
                let client = connect.build_client()?;
                let result = client
                    .schema_list_registries()
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "schema registry list")
            }
            RegistryAction::Get {
                namespace,
                version,
                connect,
            } => {
                let client = connect.build_client()?;
                let result = client
                    .schema_get_registry()
                    .namespace(namespace)
                    .version(version)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "schema registry get")
            }
        }
    }
}

/// Which definition kind a `get`/`search` targets.
#[derive(Clone, Copy)]
enum Kind {
    Attribute,
    Entity,
    Metric,
}

impl LookupAction {
    async fn run(self, kind: Kind) -> anyhow::Result<()> {
        // Each kind has its own generated response type, so every arm maps and
        // prints its own result.
        match (self, kind) {
            (LookupAction::Get { name, connect }, Kind::Attribute) => {
                let client = connect.build_client()?;
                let result = client
                    .schema_resolve_attribute()
                    .key(name)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "schema attribute get")
            }
            (LookupAction::Get { name, connect }, Kind::Entity) => {
                let client = connect.build_client()?;
                let result = client
                    .schema_resolve_entity()
                    .name(name)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "schema entity get")
            }
            (LookupAction::Get { name, connect }, Kind::Metric) => {
                let client = connect.build_client()?;
                let result = client
                    .schema_resolve_metric()
                    .name(name)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "schema metric get")
            }
            (
                LookupAction::Search {
                    prefix,
                    limit,
                    connect,
                },
                Kind::Attribute,
            ) => {
                let client = connect.build_client()?;
                let mut req = client.schema_search_attributes().prefix(prefix);
                if let Some(limit) = limit {
                    req = req.limit(limit);
                }
                let result = req.send().await.map(|r| r.into_inner());
                print_json_response(result, "schema attribute search")
            }
            (
                LookupAction::Search {
                    prefix,
                    limit,
                    connect,
                },
                Kind::Entity,
            ) => {
                let client = connect.build_client()?;
                let mut req = client.schema_search_entities().prefix(prefix);
                if let Some(limit) = limit {
                    req = req.limit(limit);
                }
                let result = req.send().await.map(|r| r.into_inner());
                print_json_response(result, "schema entity search")
            }
            (
                LookupAction::Search {
                    prefix,
                    limit,
                    connect,
                },
                Kind::Metric,
            ) => {
                let client = connect.build_client()?;
                let mut req = client.schema_search_metrics().prefix(prefix);
                if let Some(limit) = limit {
                    req = req.limit(limit);
                }
                let result = req.send().await.map(|r| r.into_inner());
                print_json_response(result, "schema metric search")
            }
        }
    }
}

impl AdminSchemaAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            AdminSchemaAction::Create(args) => {
                let document = read_document(&args.file)?;
                let client = args.connect.build_client()?;
                let result = client
                    .schema_create_registry()
                    .body(document)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "admin schema create")
            }
            AdminSchemaAction::Replace {
                namespace,
                version,
                document: args,
            } => {
                let document = read_document(&args.file)?;
                let client = args.connect.build_client()?;
                let result = client
                    .schema_replace_registry()
                    .namespace(namespace)
                    .version(version)
                    .body(document)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "admin schema replace")
            }
            AdminSchemaAction::Delete {
                namespace,
                version,
                connect,
            } => {
                let client = connect.build_client()?;
                client
                    .schema_delete_registry()
                    .namespace(&namespace)
                    .version(&version)
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("admin schema delete failed"))?;
                println!("Schema registry '{namespace}@{version}' deleted.");
                Ok(())
            }
            AdminSchemaAction::Validate(args) => {
                let document = read_document(&args.file)?;
                let client = args.connect.build_client()?;
                let result = client
                    .schema_validate_registry()
                    .body(document)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "admin schema validate")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct SchemaCli {
        #[command(subcommand)]
        action: SchemaAction,
    }

    #[derive(Parser)]
    struct AdminSchemaCli {
        #[command(subcommand)]
        action: AdminSchemaAction,
    }

    const ACME_YAML: &str = include_str!("../../../schema-model/tests/fixtures/acme.yaml");

    fn connect(url: &str) -> ConnectArgs {
        ConnectArgs {
            url: url.to_string(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        }
    }

    fn write_temp(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("signaldb-cli-schema-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join(name);
        std::fs::write(&path, contents).expect("write fixture");
        path
    }

    #[test]
    fn schema_registry_list_and_get_parse() {
        assert!(SchemaCli::try_parse_from(["schema", "registry", "list"]).is_ok());
        let cli = SchemaCli::try_parse_from(["schema", "registry", "get", "otel", "1.43.0"])
            .expect("parses");
        let SchemaAction::Registry {
            action: RegistryAction::Get {
                namespace, version, ..
            },
        } = cli.action
        else {
            panic!("expected registry get");
        };
        assert_eq!(namespace, "otel");
        assert_eq!(version, "1.43.0");
        // `get` needs both positionals.
        assert!(SchemaCli::try_parse_from(["schema", "registry", "get", "otel"]).is_err());
    }

    #[test]
    fn schema_lookup_get_and_search_parse_for_every_kind() {
        for noun in ["attribute", "entity", "metric"] {
            let cli = SchemaCli::try_parse_from(["schema", noun, "get", "k8s.pod"]).expect(noun);
            let name = match cli.action {
                SchemaAction::Attribute {
                    action: LookupAction::Get { name, .. },
                }
                | SchemaAction::Entity {
                    action: LookupAction::Get { name, .. },
                }
                | SchemaAction::Metric {
                    action: LookupAction::Get { name, .. },
                } => name,
                _ => panic!("expected {noun} get"),
            };
            assert_eq!(name, "k8s.pod");

            let cli = SchemaCli::try_parse_from(["schema", noun, "search", "k8s.", "--limit", "5"])
                .expect(noun);
            let (prefix, limit) = match cli.action {
                SchemaAction::Attribute {
                    action: LookupAction::Search { prefix, limit, .. },
                }
                | SchemaAction::Entity {
                    action: LookupAction::Search { prefix, limit, .. },
                }
                | SchemaAction::Metric {
                    action: LookupAction::Search { prefix, limit, .. },
                } => (prefix, limit),
                _ => panic!("expected {noun} search"),
            };
            assert_eq!(prefix, "k8s.");
            assert_eq!(limit, Some(5));
        }
        // `get` requires a name; `search` prefix defaults to empty.
        assert!(SchemaCli::try_parse_from(["schema", "attribute", "get"]).is_err());
        assert!(SchemaCli::try_parse_from(["schema", "attribute", "search"]).is_ok());
    }

    #[test]
    fn admin_schema_subcommands_parse() {
        assert!(
            AdminSchemaCli::try_parse_from(["admin-schema", "create", "--file", "conv.yaml"])
                .is_ok()
        );
        assert!(
            AdminSchemaCli::try_parse_from([
                "admin-schema",
                "replace",
                "acme",
                "1.0.0",
                "--file",
                "conv.json"
            ])
            .is_ok()
        );
        assert!(
            AdminSchemaCli::try_parse_from(["admin-schema", "delete", "acme", "1.0.0"]).is_ok()
        );
        assert!(
            AdminSchemaCli::try_parse_from(["admin-schema", "validate", "--file", "conv.yaml"])
                .is_ok()
        );
        // `--file` is required for create/replace/validate.
        assert!(AdminSchemaCli::try_parse_from(["admin-schema", "create"]).is_err());
        assert!(
            AdminSchemaCli::try_parse_from(["admin-schema", "replace", "acme", "1.0.0"]).is_err()
        );
    }

    #[test]
    fn read_document_accepts_yaml_and_json_by_extension() {
        let yaml = write_temp("acme.yaml", ACME_YAML);
        let from_yaml = read_document(&yaml).expect("yaml parses");
        assert_eq!(from_yaml["name"], "acme");
        assert_eq!(from_yaml["version"], "1.0.0");
        assert!(from_yaml["groups"].as_array().is_some_and(|g| g.len() == 5));

        let json_text = serde_json::to_string(&from_yaml).expect("serialize");
        let json = write_temp("acme.json", &json_text);
        let from_json = read_document(&json).expect("json parses");
        assert_eq!(
            from_json, from_yaml,
            "YAML and JSON produce the same document"
        );

        // Anything else is tried as YAML, which is a superset of JSON.
        let other = write_temp("acme.registry", &json_text);
        assert_eq!(
            read_document(&other).expect("json-as-yaml parses"),
            from_yaml
        );
    }

    #[test]
    fn read_document_rejects_non_object_and_bad_syntax() {
        let list = write_temp("list.yaml", "- a\n- b\n");
        let err = read_document(&list).expect_err("a list is not a document");
        assert!(
            err.to_string().contains("registry document object"),
            "{err}"
        );

        let broken = write_temp("broken.json", "{ not json");
        assert!(read_document(&broken).is_err());

        assert!(read_document(Path::new("/nonexistent/conv.yaml")).is_err());
    }

    #[tokio::test]
    async fn schema_attribute_get_resolves_via_sdk() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/schema/attributes/service.name")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"key":"service.name","primary":null,"hits":[]}"#)
            .create_async()
            .await;

        SchemaAction::Attribute {
            action: LookupAction::Get {
                name: "service.name".to_string(),
                connect: connect(&server.url()),
            },
        }
        .run()
        .await
        .expect("schema attribute get succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn schema_metric_search_passes_prefix_and_limit() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/schema/metrics")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("prefix".into(), "k8s.".into()),
                mockito::Matcher::UrlEncoded("limit".into(), "3".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"hits":[]}"#)
            .create_async()
            .await;

        SchemaAction::Metric {
            action: LookupAction::Search {
                prefix: "k8s.".to_string(),
                limit: Some(3),
                connect: connect(&server.url()),
            },
        }
        .run()
        .await
        .expect("schema metric search succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn admin_schema_create_uploads_yaml_as_json() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/schema/registries")
            .match_header("authorization", "Bearer sk-test")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "name": "acme",
                "version": "1.0.0"
            })))
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"namespace":"acme","version":"1.0.0","source":"custom","read_only":false,
                    "attribute_count":5,"entity_count":2,"metric_count":1}"#,
            )
            .create_async()
            .await;

        let file = write_temp("upload.yaml", ACME_YAML);
        AdminSchemaAction::Create(DocumentArgs {
            file,
            connect: connect(&server.url()),
        })
        .run()
        .await
        .expect("admin schema create succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn admin_schema_delete_hits_the_registry_path() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("DELETE", "/api/v1/schema/registries/acme/1.0.0")
            .with_status(204)
            .create_async()
            .await;

        AdminSchemaAction::Delete {
            namespace: "acme".to_string(),
            version: "1.0.0".to_string(),
            connect: connect(&server.url()),
        }
        .run()
        .await
        .expect("admin schema delete succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn admin_schema_create_surfaces_a_422_as_an_error() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/api/v1/schema/registries")
            .with_status(422)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"registry document is invalid","errors":[]}"#)
            .create_async()
            .await;

        let file = write_temp("invalid.yaml", ACME_YAML);
        let err = AdminSchemaAction::Create(DocumentArgs {
            file,
            connect: connect(&server.url()),
        })
        .run()
        .await
        .expect_err("a 422 must fail the command");
        assert!(
            err.to_string().contains("admin schema create failed"),
            "{err}"
        );
    }
}
