//! End-to-end checks for the MCP Apps surface, driven over a real MCP session.
//!
//! The server's obligations under the extension are: link a UI-backed tool to
//! its `ui://` resource via `_meta.ui.resourceUri`, serve that resource as
//! `text/html;profile=mcp-app`, and leave the tool surface untouched for
//! clients that never negotiated apps. Each is asserted through the protocol,
//! not against internal helpers.

use rmcp::{
    ClientHandler, ServiceExt,
    model::{ClientInfo, ExtensionCapabilities, ReadResourceRequestParams, ResourceContents},
    service::RunningService,
};

use mcp_server::apps::{TRACE_APP_URI, UI_EXTENSION_ID, UI_MIME_TYPE};
use mcp_server::server::McpServer;

/// A client that optionally declares the MCP Apps extension in `initialize`.
#[derive(Clone)]
struct TestClient {
    supports_ui: bool,
}

impl ClientHandler for TestClient {
    fn get_info(&self) -> ClientInfo {
        let mut info = ClientInfo::default();
        if self.supports_ui {
            let mut extensions = ExtensionCapabilities::new();
            extensions.insert(
                UI_EXTENSION_ID.to_string(),
                serde_json::json!({ "mimeTypes": [UI_MIME_TYPE] })
                    .as_object()
                    .expect("capability settings are an object")
                    .clone(),
            );
            info.capabilities.extensions = Some(extensions);
        }
        info
    }
}

/// Serve the handler over an in-memory duplex and connect a client to it. The
/// router URL is never dialed: nothing here calls a router-backed tool.
async fn connect(supports_ui: bool) -> RunningService<rmcp::RoleClient, TestClient> {
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

    TestClient { supports_ui }
        .serve(client_transport)
        .await
        .expect("client connects to the in-memory server")
}

#[tokio::test]
async fn ui_client_sees_the_trace_app_on_get_trace() {
    let client = connect(true).await;
    let tools = client.list_all_tools().await.expect("tools are listed");

    let get_trace = tools
        .iter()
        .find(|tool| tool.name == "get_trace")
        .expect("`get_trace` is registered");
    let meta = get_trace
        .meta
        .as_ref()
        .expect("a UI client sees `_meta` on `get_trace`");
    let meta = serde_json::to_value(meta).expect("meta serializes");
    assert_eq!(meta["ui"]["resourceUri"], TRACE_APP_URI);

    // Only UI-backed tools carry the metadata.
    let search = tools
        .iter()
        .find(|tool| tool.name == "search_traces")
        .expect("`search_traces` is registered");
    assert!(
        search.meta.is_none(),
        "tools without an app must not advertise one"
    );

    client.cancel().await.ok();
}

#[tokio::test]
async fn client_without_the_extension_sees_a_plain_tool_surface() {
    let client = connect(false).await;
    let tools = client.list_all_tools().await.expect("tools are listed");

    let get_trace = tools
        .iter()
        .find(|tool| tool.name == "get_trace")
        .expect("`get_trace` is registered");
    assert!(
        get_trace.meta.is_none(),
        "a client that did not negotiate apps must not be offered a UI resource"
    );

    client.cancel().await.ok();
}

#[tokio::test]
async fn trace_app_is_served_as_an_mcp_apps_document() {
    let client = connect(true).await;

    let listed = client
        .list_all_resources()
        .await
        .expect("resources are listed");
    assert!(
        listed.iter().any(|resource| resource.uri == TRACE_APP_URI),
        "the trace app is advertised in resources/list"
    );

    let read = client
        .read_resource(ReadResourceRequestParams::new(TRACE_APP_URI))
        .await
        .expect("the trace app is readable");

    let contents = read.contents.first().expect("one content item");
    let ResourceContents::TextResourceContents {
        uri,
        mime_type,
        text,
        ..
    } = contents
    else {
        panic!("UI resources are served as text");
    };
    assert_eq!(uri, TRACE_APP_URI);
    assert_eq!(mime_type.as_deref(), Some(UI_MIME_TYPE));
    assert!(
        text.starts_with("<!doctype html>"),
        "the app must be an HTML5 document"
    );

    client.cancel().await.ok();
}

#[tokio::test]
async fn unknown_resource_uri_is_rejected() {
    let client = connect(true).await;

    let result = client
        .read_resource(ReadResourceRequestParams::new(
            "ui://signaldb/does-not-exist",
        ))
        .await;
    assert!(
        result.is_err(),
        "reading an unregistered URI must fail rather than return an empty document"
    );

    client.cancel().await.ok();
}
