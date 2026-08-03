//! `signaldb-mcp` — standalone Model Context Protocol server.
//!
//! Serves MCP over Streamable HTTP at `/mcp` (production) or over stdio
//! (`--stdio`, local development). It is a sidecar that forwards the caller's
//! credential to a SignalDB router via `signaldb-sdk`; it depends on no
//! SignalDB internal crate. See the crate docs for the trust model.

use anyhow::{Context, Result};
use clap::Parser;
use mcp_server::{McpAppState, mcp_http_router, server::McpServer};
use std::net::SocketAddr;

#[derive(Parser)]
#[command(name = "signaldb-mcp")]
#[command(
    about = "SignalDB MCP server — exposes SignalDB to AI agents over the Model Context Protocol"
)]
#[command(version)]
struct Cli {
    /// Address the Streamable HTTP transport binds to (serves MCP at `/mcp`).
    /// Loopback by default: the server forwards live bearer credentials, so a
    /// non-loopback bind is an explicit opt-in that should sit behind TLS.
    #[arg(
        long,
        env = "SIGNALDB__MCP__BIND_ADDRESS",
        default_value = "127.0.0.1:8228"
    )]
    bind_address: String,

    /// Base URL of the SignalDB router HTTP API to forward calls to.
    #[arg(
        long,
        env = "SIGNALDB__MCP__ROUTER_URL",
        default_value = "http://localhost:3000"
    )]
    router_url: String,

    /// Comma-separated `Host` header allowlist (`host` or `host:port`) for the
    /// Streamable HTTP transport's DNS-rebinding guard. The transport accepts
    /// only loopback hosts by default; set this to the externally-reachable
    /// authority (e.g. `signaldb.example.com` or `10.0.0.5:30228`) when serving
    /// beyond localhost. The single value `*` disables the guard — the server
    /// still authenticates every request, so this drops only rebinding
    /// protection, not authorization.
    #[arg(long, env = "SIGNALDB__MCP__ALLOWED_HOSTS", value_delimiter = ',')]
    allowed_hosts: Vec<String>,

    /// Serve MCP over stdio instead of HTTP (local development). Stdio has no
    /// per-request credential, so downstream calls carry none — dev only.
    #[arg(long)]
    stdio: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing();

    if cli.stdio {
        return serve_stdio(cli.router_url).await;
    }

    let addr: SocketAddr = cli
        .bind_address
        .parse()
        .with_context(|| format!("Invalid bind address: {}", cli.bind_address))?;

    let app = mcp_http_router(McpAppState::new(cli.router_url.clone()), &cli.allowed_hosts);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind MCP server on {addr}"))?;
    tracing::info!(address = %addr, router = %cli.router_url, "SignalDB MCP server listening (Streamable HTTP at /mcp)");

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("MCP server shutting down gracefully");
        })
        .await
        .context("MCP server error")?;

    Ok(())
}

/// Initialize tracing from `RUST_LOG` (default `info`), plain-text to stderr.
fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// Serve the MCP handler over stdio for local development.
async fn serve_stdio(router_url: String) -> Result<()> {
    use rmcp::ServiceExt;

    tracing::info!("SignalDB MCP server starting on stdio (development)");
    let service = McpServer::new(router_url)
        .serve(rmcp::transport::stdio())
        .await
        .context("Failed to start MCP stdio transport")?;
    service.waiting().await.context("MCP stdio server error")?;
    Ok(())
}
