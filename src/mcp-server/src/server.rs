//! # MCP server handler
//!
//! Exposes SignalDB over the Model Context Protocol. In this scaffold the only
//! tool is `server_info`, which proves the transport + authentication pipeline
//! end to end; the query tools (`search_traces`, `get_trace`, …) land in a
//! follow-up (epic #620, Phase C).
//!
//! The handler holds no privileged state. The caller's credential arrives per
//! request in the HTTP [`Parts`], where the authentication layer has already
//! attached the resolved [`TenantContext`]; tools read it from there.

use axum::http::request::Parts;
use common::auth::TenantContext;
use rmcp::{
    ErrorData, ServerHandler,
    handler::server::tool::Extension,
    model::{CallToolResult, ContentBlock},
    tool, tool_handler, tool_router,
};

/// The SignalDB MCP server handler. One instance is created per session by the
/// transport's service factory; it is a zero-sized, cheap-to-clone value.
#[derive(Clone, Default)]
pub struct McpServer;

#[tool_router]
impl McpServer {
    /// Construct a handler instance.
    pub fn new() -> Self {
        Self
    }

    #[tool(
        description = "Report the SignalDB MCP server identity and the authenticated tenant/dataset for this session. Use this to confirm connectivity and which tenant your credential resolves to."
    )]
    async fn server_info(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        // The auth layer attaches the resolved context on success; over stdio
        // (dev, no HTTP headers) it is absent and the session is unauthenticated.
        let ctx = parts.extensions.get::<TenantContext>();
        let info = serde_json::json!({
            "server": "signaldb-mcp",
            "version": env!("CARGO_PKG_VERSION"),
            "authenticated": ctx.is_some(),
            "tenant": ctx.map(|c| c.tenant_id.as_str()),
            "dataset": ctx.map(|c| c.dataset_id.as_str()),
        });
        Ok(CallToolResult::success(vec![ContentBlock::text(
            info.to_string(),
        )]))
    }
}

#[tool_handler]
impl ServerHandler for McpServer {}
