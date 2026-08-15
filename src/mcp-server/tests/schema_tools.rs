//! The schema-registry tools as an MCP client sees them over a real session
//! (openspec change `schema-registry`, task 6.3): every lookup and admin tool
//! is listed, its input schema names the parameters the HTTP API takes, and
//! the lookup descriptions steer a model to resolve before it queries.
//!
//! The router-backed behaviour (results equal to the HTTP response, precedence
//! ordering) is covered end-to-end in `tests-integration`; nothing here dials
//! the router.

use rmcp::{ClientHandler, ServiceExt, model::ClientInfo, service::RunningService};

use mcp_server::server::McpServer;

#[derive(Clone)]
struct TestClient;

impl ClientHandler for TestClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::default()
    }
}

async fn connect() -> RunningService<rmcp::RoleClient, TestClient> {
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);

    tokio::spawn(async move {
        let server = McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(5),
        );
        if let Ok(running) = server.serve(server_transport).await {
            let _ = running.waiting().await;
        }
    });

    TestClient
        .serve(client_transport)
        .await
        .expect("client connects to the in-memory server")
}

/// (tool, required parameters).
const SCHEMA_TOOLS: &[(&str, &[&str])] = &[
    ("list_schema_registries", &[]),
    ("resolve_attribute", &["key"]),
    ("resolve_entity", &["name"]),
    ("resolve_metric", &["name"]),
    ("search_schema", &["kind"]),
    ("create_schema_registry", &["document"]),
    (
        "replace_schema_registry",
        &["namespace", "version", "document"],
    ),
    ("delete_schema_registry", &["namespace", "version"]),
];

#[tokio::test]
async fn schema_tools_are_listed_with_their_parameters() {
    let client = connect().await;
    let tools = client
        .list_tools(None)
        .await
        .expect("tools/list succeeds")
        .tools;

    for (name, required) in SCHEMA_TOOLS {
        let tool = tools
            .iter()
            .find(|t| t.name == *name)
            .unwrap_or_else(|| panic!("tool `{name}` is listed"));
        let schema = serde_json::Value::Object((*tool.input_schema).clone());
        let listed_required: Vec<&str> = schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|r| r.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        for param in *required {
            assert!(
                listed_required.contains(param),
                "`{name}` must require `{param}`; schema: {schema}"
            );
        }
    }

    // `search_schema` optionally narrows by prefix and caps hits, and its
    // `kind` is a closed set.
    let search = tools
        .iter()
        .find(|t| t.name == "search_schema")
        .expect("search_schema is listed");
    let schema = serde_json::Value::Object((*search.input_schema).clone());
    let props = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("search_schema has properties");
    assert!(props.contains_key("prefix") && props.contains_key("limit"));
    let text = schema.to_string();
    for kind in ["\"attribute\"", "\"entity\"", "\"metric\""] {
        assert!(text.contains(kind), "kind names {kind}: {text}");
    }

    client.cancel().await.ok();
}

#[tokio::test]
async fn lookup_tool_descriptions_steer_resolve_before_query() {
    let client = connect().await;
    let tools = client
        .list_tools(None)
        .await
        .expect("tools/list succeeds")
        .tools;

    for name in [
        "resolve_attribute",
        "resolve_entity",
        "resolve_metric",
        "search_schema",
    ] {
        let description = tools
            .iter()
            .find(|t| t.name == name)
            .and_then(|t| t.description.clone())
            .unwrap_or_else(|| panic!("tool `{name}` has a description"));
        let lower = description.to_ascii_lowercase();
        assert!(
            lower.contains("before"),
            "`{name}` must tell the model to look the name up before querying: {description}"
        );
        assert!(
            lower.contains("mean") || lower.contains("definition"),
            "`{name}` must say it explains what a name means: {description}"
        );
    }

    client.cancel().await.ok();
}
