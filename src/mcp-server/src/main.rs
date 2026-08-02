//! `signaldb-mcp` — standalone Model Context Protocol server.
//!
//! Serves MCP over Streamable HTTP at `/mcp` (production) or over stdio
//! (`--stdio`, for local development). See the crate docs for the trust model.

use anyhow::{Context, Result};
use clap::Parser;
use common::auth::Authenticator;
use common::catalog::Catalog;
use common::cli::{CommonArgs, utils};
use mcp_server::{McpAppState, mcp_http_router, server::McpServer};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "signaldb-mcp")]
#[command(
    about = "SignalDB MCP server — exposes SignalDB to AI agents over the Model Context Protocol"
)]
#[command(version)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    /// Serve MCP over stdio instead of HTTP (local development). Stdio has no
    /// per-request credential, so tools run unauthenticated — dev only.
    #[arg(long)]
    stdio: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = utils::load_config(cli.common.config.as_ref())?;
    utils::init_logging(&cli.common, None);

    let router_base_url = config
        .mcp
        .router_url
        .clone()
        .unwrap_or_else(|| "http://localhost:3000".to_string());

    if cli.stdio {
        return serve_stdio(router_base_url).await;
    }

    // Catalog is needed to resolve database-backed API keys; config-file keys
    // resolve without it. DSN follows discovery first, then the database config
    // — the same precedence every other service uses.
    let dsn = config
        .discovery
        .as_ref()
        .map(|d| d.dsn.as_str())
        .unwrap_or(config.database.dsn.as_str());
    let catalog = Catalog::new(dsn)
        .await
        .context("Failed to open catalog for MCP authentication")?;
    let authenticator = Arc::new(Authenticator::new(config.auth.clone(), Arc::new(catalog)));

    let state = McpAppState::new(authenticator, router_base_url);

    let addr: std::net::SocketAddr = config
        .mcp
        .bind_address
        .parse()
        .with_context(|| format!("Invalid [mcp].bind_address: {}", config.mcp.bind_address))?;

    let app = mcp_http_router(state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind MCP server on {addr}"))?;
    tracing::info!(address = %addr, "SignalDB MCP server listening (Streamable HTTP at /mcp)");

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("MCP server shutting down gracefully");
        })
        .await
        .context("MCP server error")?;

    Ok(())
}

/// Serve the MCP handler over stdio for local development.
async fn serve_stdio(router_base_url: String) -> Result<()> {
    use rmcp::ServiceExt;

    tracing::info!("SignalDB MCP server starting on stdio (development, unauthenticated)");
    let service = McpServer::new(router_base_url)
        .serve(rmcp::transport::stdio())
        .await
        .context("Failed to start MCP stdio transport")?;
    service.waiting().await.context("MCP stdio server error")?;
    Ok(())
}
