//! The MCP admin toolset for API keys shares the scope vocabulary of every
//! other key-management surface: `create_api_key` takes required `scopes`
//! plus an optional `dataset_ids` restriction set, `update_api_key_scopes`
//! patches a live key (including clearing its restriction via
//! `clear_dataset_restriction`), and `list_api_keys` shows scopes.

mod common;

use common::connect;
use mcp_server::server::McpServer;

#[test]
fn api_key_admin_tools_are_registered() {
    for tool in ["list_api_keys", "create_api_key", "update_api_key_scopes"] {
        assert!(McpServer::has_tool(tool), "MCP tool `{tool}` is missing");
    }
}

#[tokio::test]
async fn create_api_key_requires_scopes_and_offers_dataset_ids() {
    let client = connect().await;
    let tools = client.list_tools(None).await.expect("tools/list succeeds");

    let create = tools
        .tools
        .iter()
        .find(|t| t.name == "create_api_key")
        .expect("create_api_key tool listed");
    let schema = serde_json::to_value(&create.input_schema).expect("schema serializes");
    let required: Vec<&str> = schema["required"]
        .as_array()
        .map(|items| items.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert!(
        required.contains(&"scopes"),
        "create_api_key must require `scopes`: {schema}"
    );
    assert!(
        required.contains(&"tenant_id"),
        "create_api_key must require `tenant_id`: {schema}"
    );
    assert!(
        schema["properties"].get("dataset_ids").is_some(),
        "create_api_key must accept `dataset_ids`: {schema}"
    );

    let update = tools
        .tools
        .iter()
        .find(|t| t.name == "update_api_key_scopes")
        .expect("update_api_key_scopes tool listed");
    let schema = serde_json::to_value(&update.input_schema).expect("schema serializes");
    for field in [
        "tenant_id",
        "key_id",
        "scopes",
        "dataset_ids",
        "clear_dataset_restriction",
    ] {
        assert!(
            schema["properties"].get(field).is_some(),
            "update_api_key_scopes must accept `{field}`: {schema}"
        );
    }

    client.cancel().await.ok();
}
