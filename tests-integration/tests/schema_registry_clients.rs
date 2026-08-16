//! Schema registry across the client surfaces (openspec change
//! `schema-registry`, task 6.4; see also the `client-surface-parity` spec).
//!
//! 1. Parity: every schema-registry capability has both a CLI subcommand and
//!    an MCP tool, asserted from one manifest so the surfaces cannot drift.
//! 2. End-to-end against a real router: a custom registry is created through
//!    the CLI (`admin schema create --file acme.yaml`, YAML → SDK → HTTP), then
//!    `service.name` is resolved over HTTP (SDK) and through the MCP
//!    `resolve_attribute` tool driven over the Streamable HTTP transport. Both
//!    return the same precedence-ordered envelope: the tenant's own definition
//!    is `primary`, the bundled `otel` definition stays as an alternative.

use std::collections::HashSet;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::{CommandFactory, Parser};
use common::auth::Authenticator;
use common::catalog::Catalog;
use common::config::{ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig};
use futures::StreamExt;
use mcp_server::{McpAppState, mcp_http_router};
use router::{RouterAppState, create_router};
use serde_json::{Value, json};
use signaldb_sdk::Client;
use tokio::net::TcpListener;
use tower::ServiceExt;

// ---- parity ----------------------------------------------------------------

/// (capability, CLI command path, MCP tool).
const SCHEMA_MANIFEST: &[(&str, &[&str], &str)] = &[
    (
        "list registries",
        &["schema", "registry", "list"],
        "list_schema_registries",
    ),
    (
        "resolve attribute",
        &["schema", "attribute", "get"],
        "resolve_attribute",
    ),
    (
        "resolve entity",
        &["schema", "entity", "get"],
        "resolve_entity",
    ),
    (
        "resolve metric",
        &["schema", "metric", "get"],
        "resolve_metric",
    ),
    (
        "search attributes",
        &["schema", "attribute", "search"],
        "search_schema",
    ),
    (
        "search entities",
        &["schema", "entity", "search"],
        "search_schema",
    ),
    (
        "search metrics",
        &["schema", "metric", "search"],
        "search_schema",
    ),
    (
        "create registry",
        &["admin", "schema", "create"],
        "create_schema_registry",
    ),
    (
        "replace registry",
        &["admin", "schema", "replace"],
        "replace_schema_registry",
    ),
    (
        "delete registry",
        &["admin", "schema", "delete"],
        "delete_schema_registry",
    ),
];

/// Whether the CLI has a subcommand at `path` (e.g. `["schema", "entity", "get"]`).
fn cli_has_subcommand(path: &[&str]) -> bool {
    let mut cmd = signaldb_cli::commands::Cli::command();
    for name in path {
        let Some(sub) = cmd.get_subcommands().find(|c| c.get_name() == *name) else {
            return false;
        };
        let sub = sub.clone();
        cmd = sub;
    }
    true
}

#[test]
fn schema_capabilities_have_cli_and_mcp() {
    use mcp_server::server::McpServer;
    let mut tools = HashSet::new();
    for (cap, cli_path, tool) in SCHEMA_MANIFEST {
        assert!(
            cli_has_subcommand(cli_path),
            "CLI `{}` is missing for {cap}",
            cli_path.join(" ")
        );
        assert!(
            McpServer::has_tool(tool),
            "MCP tool `{tool}` is missing for {cap}"
        );
        tools.insert(*tool);
    }
    // `schema registry get` and `admin schema validate` are CLI conveniences
    // over the same HTTP operations; the MCP surface reaches registries through
    // the list + resolve tools and validates by attempting a create.
    assert!(cli_has_subcommand(&["schema", "registry", "get"]));
    assert!(cli_has_subcommand(&["admin", "schema", "validate"]));
    assert_eq!(tools.len(), 8, "eight distinct MCP schema tools");
}

// ---- end-to-end ------------------------------------------------------------

const TENANT: &str = "acme";
const WRITE_KEY: &str = "sk-acme-schema-write";
const ACME_YAML: &str = include_str!("../../src/schema-model/tests/fixtures/acme.yaml");

fn tenant_config() -> TenantConfig {
    TenantConfig {
        id: TENANT.to_string(),
        slug: TENANT.to_string(),
        name: "Acme Inc".to_string(),
        default_dataset: Some("production".to_string()),
        datasets: vec![DatasetConfig {
            id: "production".to_string(),
            slug: "production".to_string(),
            is_default: true,
            storage: None,
        }],
        api_keys: vec![ApiKeyConfig {
            key: "acme-legacy-key".to_string(),
            name: Some("legacy".to_string()),
        }],
        schema_config: None,
        limits: None,
    }
}

/// Serve the full router with tenant `acme` and a `schema:read`+`schema:write`
/// scoped key; returns its base URL.
async fn serve_router() -> String {
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    let config = Configuration {
        auth: AuthConfig {
            tenants: vec![tenant_config()],
            ..Default::default()
        },
        ..Default::default()
    };
    catalog.sync_config_tenants(&config.auth).await.unwrap();
    catalog
        .upsert_scoped_api_key(
            TENANT,
            &Authenticator::hash_api_key(WRITE_KEY),
            Some("schema-write"),
            None,
            Some(&["schema:read".to_string(), "schema:write".to_string()]),
            None,
        )
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = create_router(RouterAppState::new(catalog, config));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for attempt in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        assert!(attempt < 49, "router never became reachable");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    format!("http://{addr}")
}

/// An SDK client carrying the scoped key and tenant header on every request,
/// exactly as the CLI and MCP server construct it.
fn sdk_client(base_url: &str) -> Client {
    signaldb_sdk::ClientBuilder::new(base_url)
        .bearer(WRITE_KEY)
        .tenant(TENANT)
        .build()
        .unwrap()
}

/// Assert the resolve envelope for `service.name`: the tenant's `acme`
/// definition is primary and the bundled `otel` one is still listed.
fn assert_acme_precedence(resolution: &Value) {
    assert_eq!(resolution["key"], "service.name");
    assert_eq!(
        resolution["primary"]["namespace"], "acme",
        "custom registry wins: {resolution}"
    );
    assert_eq!(resolution["primary"]["source"], "custom");
    assert!(
        resolution["primary"]["brief"]
            .as_str()
            .is_some_and(|b| b.contains("ADR-12")),
        "primary carries the tenant's brief: {resolution}"
    );
    let hits = resolution["hits"].as_array().expect("hits array");
    assert_eq!(hits[0]["namespace"], "acme", "hits are precedence-ordered");
    assert!(
        hits.iter()
            .any(|h| h["namespace"] == "otel" && h["source"] == "bundled"),
        "the otel definition is kept as an alternative: {resolution}"
    );
}

/// Minimal Streamable HTTP MCP client over the in-process `mcp_http_router`:
/// initialize → initialized → tools/call, parsing the SSE response.
struct McpHttpClient {
    app: axum::Router,
    session_id: Option<String>,
}

impl McpHttpClient {
    async fn connect(router_url: &str) -> Self {
        let app = mcp_http_router(McpAppState::new(router_url.to_string()), &[]);
        let mut client = Self {
            app,
            session_id: None,
        };
        let init = client
            .post(json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "tests-integration", "version": "0"}
                }
            }))
            .await;
        assert!(
            init["result"]["serverInfo"].is_object(),
            "initialize response: {init}"
        );
        assert!(client.session_id.is_some(), "server assigns a session id");
        client
            .notify(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .await;
        client
    }

    fn request(&self, body: Value) -> Request<Body> {
        let mut req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", "localhost")
            .header("authorization", format!("Bearer {WRITE_KEY}"))
            .header("x-tenant-id", TENANT)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        if let Some(id) = &self.session_id {
            req = req.header("mcp-session-id", id);
        }
        req.body(Body::from(body.to_string())).unwrap()
    }

    async fn notify(&self, body: Value) {
        let res = self.app.clone().oneshot(self.request(body)).await.unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED, "notification accepted");
    }

    /// Send one JSON-RPC request and return its response object, whether the
    /// transport answered with plain JSON or an SSE stream.
    async fn post(&mut self, body: Value) -> Value {
        let res = self.app.clone().oneshot(self.request(body)).await.unwrap();
        if res.status() != StatusCode::OK {
            let status = res.status();
            let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .unwrap();
            panic!(
                "MCP POST rejected with {status}: {}",
                String::from_utf8_lossy(&bytes)
            );
        }
        if let Some(id) = res
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
        {
            self.session_id = Some(id.to_string());
        }
        let is_sse = res
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.contains("text/event-stream"));
        let mut stream = res.into_body().into_data_stream();
        let mut buffer = String::new();
        let read = async {
            while let Some(chunk) = stream.next().await {
                buffer.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
                if let Some(message) = extract_message(&buffer, is_sse) {
                    return message;
                }
            }
            extract_message(&buffer, is_sse)
                .unwrap_or_else(|| panic!("no JSON-RPC message in response body: {buffer:?}"))
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), read)
            .await
            .expect("MCP response within 30s")
    }

    async fn call_tool(&mut self, name: &str, arguments: Value) -> Value {
        let response = self
            .post(json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": name, "arguments": arguments}
            }))
            .await;
        assert!(
            response.get("error").is_none(),
            "tools/call {name} failed: {response}"
        );
        let result = &response["result"];
        assert_ne!(result["isError"], true, "tool {name} errored: {result}");
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("tool {name} returns a text block: {result}"));
        serde_json::from_str(text).expect("tool text is JSON")
    }
}

/// The first complete JSON-RPC message in `buffer`: for SSE, the first
/// `data:` line that parses; for JSON, the whole body.
fn extract_message(buffer: &str, is_sse: bool) -> Option<Value> {
    if !is_sse {
        return serde_json::from_str(buffer).ok();
    }
    buffer
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
        .find(|message| message.get("result").is_some() || message.get("error").is_some())
}

#[tokio::test]
async fn custom_registry_via_cli_resolves_over_http_and_mcp_with_precedence() {
    let router_url = serve_router().await;

    // Before: only the bundled registries answer, so otel is primary.
    let sdk = sdk_client(&router_url);
    let before = sdk
        .schema_resolve_attribute()
        .key("service.name")
        .send()
        .await
        .expect("resolve before upload")
        .into_inner();
    assert_eq!(
        before.primary.as_ref().map(|h| h.namespace.as_str()),
        Some("otel"),
        "bundled otel is primary until a custom registry defines the key"
    );

    // Create the custom registry through the CLI: YAML file → SDK → HTTP.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("acme.yaml");
    std::fs::write(&file, ACME_YAML).unwrap();
    let cli = signaldb_cli::commands::Cli::try_parse_from([
        "signaldb-cli",
        "admin",
        "schema",
        "create",
        "--file",
        file.to_str().unwrap(),
        "--url",
        &router_url,
        "--api-key",
        WRITE_KEY,
        "--tenant-id",
        TENANT,
    ])
    .expect("admin schema create parses");
    cli.run().await.expect("admin schema create succeeds");

    // The registry is listed as custom, ahead of the bundled ones.
    let registries = sdk
        .schema_list_registries()
        .send()
        .await
        .expect("list registries")
        .into_inner()
        .registries;
    assert_eq!(registries[0].namespace, "acme");
    assert_eq!(registries[0].version, "1.0.0");
    assert!(
        registries
            .iter()
            .any(|r| r.namespace == "otel" && r.read_only),
        "bundled otel stays listed and read-only"
    );

    // Resolve over HTTP via the SDK.
    let via_http = sdk
        .schema_resolve_attribute()
        .key("service.name")
        .send()
        .await
        .expect("resolve via SDK")
        .into_inner();
    let via_http = serde_json::to_value(&via_http).unwrap();
    assert_acme_precedence(&via_http);

    // Resolve through the MCP tool over Streamable HTTP; same envelope.
    let mut mcp = McpHttpClient::connect(&router_url).await;
    let via_mcp = mcp
        .call_tool("resolve_attribute", json!({"key": "service.name"}))
        .await;
    assert_acme_precedence(&via_mcp);
    assert_eq!(
        via_mcp["primary"], via_http["primary"],
        "MCP returns the HTTP result shape unchanged"
    );
    assert_eq!(
        via_mcp["hits"].as_array().map(Vec::len),
        via_http["hits"].as_array().map(Vec::len)
    );

    // Entity and metric lookups + prefix search reach the custom definitions
    // through the MCP tools too.
    let entity = mcp
        .call_tool("resolve_entity", json!({"name": "acme.order"}))
        .await;
    assert_eq!(entity["primary"]["namespace"], "acme");
    let metric = mcp
        .call_tool("resolve_metric", json!({"name": "acme.checkout.latency"}))
        .await;
    assert_eq!(metric["primary"]["instrument"], "histogram");
    let search = mcp
        .call_tool(
            "search_schema",
            json!({"kind": "attribute", "prefix": "acme.order.", "limit": 10}),
        )
        .await;
    let keys: Vec<&str> = search["hits"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|h| h["key"].as_str())
        .collect();
    assert!(keys.contains(&"acme.order.id"), "search hits: {keys:?}");

    // Deleting through MCP restores otel as primary.
    let deleted = mcp
        .call_tool(
            "delete_schema_registry",
            json!({"namespace": "acme", "version": "1.0.0"}),
        )
        .await;
    assert_eq!(deleted["deleted"], true);
    let after = sdk
        .schema_resolve_attribute()
        .key("service.name")
        .send()
        .await
        .expect("resolve after delete")
        .into_inner();
    assert_eq!(
        after.primary.as_ref().map(|h| h.namespace.as_str()),
        Some("otel")
    );
}
