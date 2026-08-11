//! End-to-end checks for the MCP `prompts` capability and the parts of
//! `completion/complete` that need no router credential, driven over a real
//! MCP session — mirrors the pattern in `tests/apps_extension.rs` and
//! `tests/cache_hints.rs`.
//!
//! Completions that forward to the router (and so need a caller credential)
//! are covered in `src/server.rs`'s own test module instead: this crate's
//! in-memory duplex transport carries no HTTP layer, so nothing ever inserts
//! an `Extension<Parts>` into the request context here — exactly like every
//! other router-forwarding method in this crate, those are tested by calling
//! the handler directly with a manually-built credential.

use rmcp::{
    ClientHandler, ServiceExt,
    model::{
        ArgumentInfo, CacheScope, ClientInfo, CompleteRequestParams, GetPromptRequestParams,
        Reference,
    },
    service::RunningService,
};

use mcp_server::server::McpServer;

#[derive(Clone)]
struct TestClient;

impl ClientHandler for TestClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::default()
    }
}

/// Serve the handler over an in-memory duplex and connect a client to it. The
/// router URL is never dialed: nothing here calls a router-backed tool.
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

#[tokio::test]
async fn prompts_are_listed_with_cache_hints() {
    let client = connect().await;

    let result = client
        .list_prompts(None)
        .await
        .expect("prompts/list succeeds");

    let names: Vec<_> = result.prompts.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "investigate_trace",
            "find_recent_errors",
            "build_promql_query"
        ],
        "the prompt catalog must be advertised in order"
    );
    assert!(
        result.ttl_ms.is_some(),
        "prompts/list must set `ttlMs`, required by protocol 2026-07-28 (SEP-2549)"
    );
    assert_eq!(
        result.cache_scope,
        Some(CacheScope::Public),
        "the prompt catalog is static and identical for every client"
    );

    client.cancel().await.ok();
}

#[tokio::test]
async fn get_prompt_renders_the_requested_template() {
    let client = connect().await;

    let mut arguments = rmcp::model::JsonObject::new();
    arguments.insert("trace_id".to_string(), serde_json::json!("abc123"));

    let result = client
        .get_prompt(GetPromptRequestParams::new("investigate_trace").with_arguments(arguments))
        .await
        .expect("prompts/get succeeds");

    let rmcp::model::ContentBlock::Text(text) = &result.messages[0].content else {
        panic!("prompt message must be text");
    };
    assert!(text.text.contains("abc123"));

    client.cancel().await.ok();
}

#[tokio::test]
async fn get_prompt_rejects_a_missing_required_argument() {
    let client = connect().await;

    let result = client
        .get_prompt(GetPromptRequestParams::new("investigate_trace"))
        .await;
    assert!(
        result.is_err(),
        "omitting the required `trace_id` argument must fail"
    );

    client.cancel().await.ok();
}

#[tokio::test]
async fn completion_is_empty_for_a_reference_this_server_does_not_complete() {
    let client = connect().await;

    let result = client
        .complete(CompleteRequestParams::new(
            Reference::for_prompt("investigate_trace"),
            ArgumentInfo::new("trace_id", "a"),
        ))
        .await
        .expect("completion/complete succeeds even with no suggestions");

    assert!(
        result.completion.values.is_empty(),
        "there is no live data source for a trace ID prefix"
    );

    client.cancel().await.ok();
}

#[tokio::test]
async fn completion_is_empty_for_an_unknown_prompt_argument() {
    let client = connect().await;

    let result = client
        .complete(CompleteRequestParams::new(
            Reference::for_prompt("find_recent_errors"),
            ArgumentInfo::new("not_a_real_argument", "x"),
        ))
        .await
        .expect("completion/complete succeeds even for an unrecognized argument");

    assert!(result.completion.values.is_empty());

    client.cancel().await.ok();
}
