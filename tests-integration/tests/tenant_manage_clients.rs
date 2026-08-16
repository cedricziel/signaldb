//! `tenant:manage` end to end (change `management-api-key-scope`, task 4.2):
//! an API key carrying the scope manages its own tenant's datasets and keys
//! through the CLI (`signaldb-cli tenant ...`, over the SDK) and through the
//! MCP `tenant_*` tools against a real router; an ingest-only key is denied
//! on both surfaces with an error naming the scope.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::Parser;
use common::auth::{Authenticator, TENANT_MANAGE_SCOPE};
use common::catalog::Catalog;
use common::config::{
    ApiKeyConfig, AuthConfig, Configuration, DatasetConfig, TenantConfig, TenantsConfig,
};
use futures::StreamExt;
use mcp_server::{McpAppState, mcp_http_router};
use router::{RouterAppState, create_router};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tower::ServiceExt;

const TENANT: &str = "acme";
const MANAGE_KEY: &str = "sk-acme-tenant-manage";
const INGEST_KEY: &str = "sk-acme-ingest-only";

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
            key: "sk-acme-legacy".to_string(),
            name: Some("legacy".to_string()),
        }],
        schema_config: None,
        limits: None,
    }
}

async fn scoped_key(catalog: &Catalog, secret: &str, scopes: &[&str]) {
    let scopes: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
    catalog
        .upsert_scoped_api_key(
            TENANT,
            &Authenticator::hash_api_key(secret),
            Some(secret),
            None,
            Some(&scopes),
            None,
        )
        .await
        .unwrap();
}

/// Serve the full router with tenant `acme`, a `tenant:manage` key, and an
/// ingest-only key.
async fn serve_router() -> (String, Catalog) {
    let catalog = Catalog::new("sqlite::memory:").await.unwrap();
    let config = Configuration {
        auth: AuthConfig {
            tenants: vec![tenant_config()],
            ..Default::default()
        },
        // `GET /api/v1/tenants/{id}` (`tenant show` / `tenant_info`) answers
        // from `TenantApi`, which reads this legacy `tenants` map /
        // `default_tenant`, not `auth.tenants` — the same pre-existing split
        // `tenant_table_cli.rs` has to satisfy.
        tenants: TenantsConfig {
            default_tenant: TENANT.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    catalog.sync_config_tenants(&config.auth).await.unwrap();
    scoped_key(&catalog, MANAGE_KEY, &["traces:write", TENANT_MANAGE_SCOPE]).await;
    scoped_key(&catalog, INGEST_KEY, &["traces:write"]).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = create_router(RouterAppState::new(catalog.clone(), config));
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
    (format!("http://{addr}"), catalog)
}

fn cli_args<'a>(router_url: &'a str, key: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["signaldb-cli", "tenant"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--url", router_url, "--api-key", key, "--tenant-id", TENANT]);
    args
}

async fn run_cli(router_url: &str, key: &str, extra: &[&str]) -> anyhow::Result<()> {
    signaldb_cli::commands::Cli::try_parse_from(cli_args(router_url, key, extra))
        .unwrap_or_else(|e| panic!("`tenant {extra:?}` parses: {e}"))
        .run()
        .await
}

#[tokio::test]
async fn tenant_manage_key_drives_the_cli_and_an_ingest_key_is_denied() {
    let (router_url, catalog) = serve_router().await;

    run_cli(&router_url, MANAGE_KEY, &["show"])
        .await
        .expect("tenant show succeeds");
    run_cli(&router_url, MANAGE_KEY, &["dataset", "create", "staging"])
        .await
        .expect("tenant dataset create succeeds with tenant:manage");
    run_cli(&router_url, MANAGE_KEY, &["dataset", "list"])
        .await
        .expect("tenant dataset list succeeds");
    let datasets = catalog.get_datasets(TENANT).await.unwrap();
    assert!(
        datasets.iter().any(|d| d.name == "staging"),
        "staging exists after `tenant dataset create`: {datasets:?}"
    );

    run_cli(
        &router_url,
        MANAGE_KEY,
        &[
            "api-key",
            "create",
            "--name",
            "ci",
            "--scope",
            "traces:write",
        ],
    )
    .await
    .expect("tenant api-key create succeeds");
    let keys = catalog.list_api_keys(TENANT).await.unwrap();
    assert!(
        keys.iter().any(|k| k.name.as_deref() == Some("ci")),
        "the ci key exists after `tenant api-key create`: {keys:?}"
    );
    run_cli(&router_url, MANAGE_KEY, &["api-key", "list"])
        .await
        .expect("tenant api-key list succeeds");
    run_cli(&router_url, MANAGE_KEY, &["schema", "get"])
        .await
        .expect("tenant schema get succeeds");

    // An ingest-only key is refused, and the CLI surfaces the router's
    // access-denied error non-zero.
    let denied = run_cli(&router_url, INGEST_KEY, &["dataset", "create", "denied"])
        .await
        .expect_err("ingest-only key must be denied");
    let message = format!("{denied:#}");
    assert!(
        message.contains("manage_create_dataset failed"),
        "names the operation: {message}"
    );
    assert!(
        message.contains("403") || message.contains("tenant:manage"),
        "reads as access denied: {message}"
    );
    let datasets = catalog.get_datasets(TENANT).await.unwrap();
    assert!(
        !datasets.iter().any(|d| d.name == "denied"),
        "the denied create performed no change: {datasets:?}"
    );

    // A legacy unscoped key is refused too: management is opt-in.
    run_cli(&router_url, "sk-acme-legacy", &["dataset", "list"])
        .await
        .expect_err("legacy unscoped key must be denied");
}

/// Minimal Streamable HTTP MCP client over the in-process `mcp_http_router`,
/// forwarding a chosen API key.
struct McpHttpClient {
    app: axum::Router,
    api_key: &'static str,
    session_id: Option<String>,
}

impl McpHttpClient {
    async fn connect(router_url: &str, api_key: &'static str) -> Self {
        let app = mcp_http_router(McpAppState::new(router_url.to_string()), &[]);
        let mut client = Self {
            app,
            api_key,
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
        let res = client
            .app
            .clone()
            .oneshot(
                client.request(json!({"jsonrpc": "2.0", "method": "notifications/initialized"})),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED, "notification accepted");
        client
    }

    fn request(&self, body: Value) -> Request<Body> {
        let mut req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", "localhost")
            .header("authorization", format!("Bearer {}", self.api_key))
            .header("x-tenant-id", TENANT)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        if let Some(id) = &self.session_id {
            req = req.header("mcp-session-id", id);
        }
        req.body(Body::from(body.to_string())).unwrap()
    }

    async fn post(&mut self, body: Value) -> Value {
        let res = self.app.clone().oneshot(self.request(body)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "MCP POST accepted");
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

    /// `tools/call`, returning the raw JSON-RPC response (so callers can
    /// assert on errors as well as results).
    async fn call_tool(&mut self, name: &str, arguments: Value) -> Value {
        self.post(json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": name, "arguments": arguments}
        }))
        .await
    }
}

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

fn error_text(response: &Value) -> String {
    if let Some(error) = response.get("error") {
        return error["message"].as_str().unwrap_or_default().to_string();
    }
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

#[tokio::test]
async fn tenant_manage_key_drives_the_mcp_tenant_tools_and_an_ingest_key_is_denied() {
    let (router_url, catalog) = serve_router().await;

    let mut manage = McpHttpClient::connect(&router_url, MANAGE_KEY).await;
    let created = manage
        .call_tool(
            "tenant_create_dataset",
            json!({"tenant_id": TENANT, "name": "mcp-staging"}),
        )
        .await;
    assert!(created.get("error").is_none(), "{created}");
    assert_ne!(created["result"]["isError"], true, "{created}");
    let dataset: Value = serde_json::from_str(&error_text(&created)).expect("JSON result");
    assert_eq!(
        dataset["name"], "mcp-staging",
        "SDK-shaped result: {dataset}"
    );
    let datasets = catalog.get_datasets(TENANT).await.unwrap();
    assert!(
        datasets.iter().any(|d| d.name == "mcp-staging"),
        "{datasets:?}"
    );

    let key = manage
        .call_tool(
            "tenant_create_api_key",
            json!({"tenant_id": TENANT, "name": "mcp-ci", "scopes": ["logs:write"]}),
        )
        .await;
    let key: Value = serde_json::from_str(&error_text(&key)).expect("JSON result");
    assert!(
        key["key"].as_str().is_some_and(|k| k.starts_with("sdbk_")),
        "key material returned once: {key}"
    );

    let info = manage
        .call_tool("tenant_info", json!({"tenant_id": TENANT}))
        .await;
    let info: Value = serde_json::from_str(&error_text(&info)).expect("JSON result");
    assert_eq!(info["tenant_id"], TENANT, "{info}");

    let mut ingest = McpHttpClient::connect(&router_url, INGEST_KEY).await;
    let denied = ingest
        .call_tool(
            "tenant_create_dataset",
            json!({"tenant_id": TENANT, "name": "mcp-denied"}),
        )
        .await;
    assert!(
        denied.get("error").is_some() || denied["result"]["isError"] == true,
        "ingest-only key must be denied: {denied}"
    );
    let text = error_text(&denied);
    assert!(
        text.contains("tenant:manage"),
        "denial names the scope: {text}"
    );
    let datasets = catalog.get_datasets(TENANT).await.unwrap();
    assert!(
        !datasets.iter().any(|d| d.name == "mcp-denied"),
        "{datasets:?}"
    );
}
