//! Regression test for SEP-2549 cache hints on paginated/read results.
//!
//! Protocol version `2026-07-28` requires `ttlMs`/`cacheScope` on
//! `ListToolsResult`, `ListResourcesResult`, and `ReadResourceResult`. rmcp
//! keeps them optional on the Rust side for backwards compatibility, so a
//! server that forgets to set them still round-trips fine against another
//! rmcp client — but a spec-conformant client that negotiates
//! `2026-07-28` rejects the response outright ("Invalid result for
//! tools/list": `ttlMs` missing, `cacheScope` missing). Assert the fields are
//! actually populated, not just that the call succeeds.

use rmcp::{
    ClientHandler, ServiceExt,
    model::{CacheScope, ClientInfo, ReadResourceRequestParams},
    service::RunningService,
};

use mcp_server::apps::TRACE_APP_URI;
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

#[tokio::test]
async fn list_tools_result_carries_cache_hints() {
    let client = connect().await;

    let result = client.list_tools(None).await.expect("tools/list succeeds");
    assert!(
        result.ttl_ms.is_some(),
        "tools/list must set `ttlMs`, required by protocol 2026-07-28 (SEP-2549)"
    );
    assert_eq!(
        result.cache_scope,
        Some(CacheScope::Private),
        "tool metadata (e.g. UI `_meta`) varies per client capability, so the \
         result must not be marked publicly cacheable"
    );

    client.cancel().await.ok();
}

#[tokio::test]
async fn list_resources_result_carries_cache_hints() {
    let client = connect().await;

    let result = client
        .list_resources(None)
        .await
        .expect("resources/list succeeds");
    assert!(
        result.ttl_ms.is_some(),
        "resources/list must set `ttlMs`, required by protocol 2026-07-28 (SEP-2549)"
    );
    assert!(
        result.cache_scope.is_some(),
        "resources/list must set `cacheScope`, required by protocol 2026-07-28 (SEP-2549)"
    );

    client.cancel().await.ok();
}

#[tokio::test]
async fn read_resource_result_carries_cache_hints() {
    let client = connect().await;

    let result = client
        .read_resource(ReadResourceRequestParams::new(TRACE_APP_URI))
        .await
        .expect("resources/read succeeds");
    assert!(
        result.ttl_ms.is_some(),
        "resources/read must set `ttlMs`, required by protocol 2026-07-28 (SEP-2549)"
    );
    assert!(
        result.cache_scope.is_some(),
        "resources/read must set `cacheScope`, required by protocol 2026-07-28 (SEP-2549)"
    );

    client.cancel().await.ok();
}
