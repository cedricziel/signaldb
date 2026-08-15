//! `signaldb mcp` — the Model Context Protocol server's command-line entry point.
//!
//! Serves MCP over Streamable HTTP at `/mcp` (production) or over stdio
//! (`--stdio`, local development). It is a sidecar that forwards the caller's
//! credential to a SignalDB router via `signaldb-sdk`; it depends on no
//! SignalDB internal crate. See the crate docs for the trust model.

use crate::{McpAppState, mcp_http_router, server::McpServer};
use anyhow::{Context, Result};
use std::net::SocketAddr;

/// Command-line arguments of the `mcp` subcommand (`signaldb mcp`).
#[derive(clap::Args, Debug)]
pub struct Args {
    /// Address the Streamable HTTP transport binds to (serves MCP at `/mcp`).
    /// Loopback by default: the server forwards live bearer credentials, so a
    /// non-loopback bind is an explicit opt-in that should sit behind TLS.
    #[arg(
        long,
        env = "SIGNALDB__MCP__BIND_ADDRESS",
        default_value = "127.0.0.1:8228"
    )]
    pub bind_address: String,

    /// Base URL of the SignalDB router HTTP API to forward calls to.
    #[arg(
        long,
        env = "SIGNALDB__MCP__ROUTER_URL",
        default_value = "http://localhost:3000"
    )]
    pub router_url: String,

    /// Comma-separated `Host` header allowlist (`host` or `host:port`) for the
    /// Streamable HTTP transport's DNS-rebinding guard. The transport accepts
    /// only loopback hosts by default; set this to the externally-reachable
    /// authority (e.g. `signaldb.example.com` or `10.0.0.5:30228`) when serving
    /// beyond localhost. The single value `*` disables the guard — the server
    /// still authenticates every request, so this drops only rebinding
    /// protection, not authorization.
    #[arg(long, env = "SIGNALDB__MCP__ALLOWED_HOSTS", value_delimiter = ',')]
    pub allowed_hosts: Vec<String>,

    /// Overall timeout, in seconds, for each HTTP request forwarded to the
    /// router, so a hung router fails MCP tool calls instead of hanging them.
    #[arg(long, env = "SIGNALDB__MCP__ROUTER_TIMEOUT", default_value_t = 30)]
    pub router_timeout: u64,

    /// Serve MCP over stdio instead of HTTP (local development). Stdio has no
    /// per-request credential, so downstream calls carry none — dev only.
    #[arg(long)]
    pub stdio: bool,

    /// This MCP resource's own public URL (e.g. `https://signaldb.example.com/mcp`).
    /// Set together with `--oauth-issuer-url` to advertise OAuth: the server then
    /// serves the Protected Resource Metadata document and challenges
    /// unauthenticated requests toward it. Tokens are audience-bound to this URL.
    #[arg(long, env = "SIGNALDB__MCP__OAUTH__RESOURCE_URL")]
    pub oauth_resource_url: Option<String>,

    /// Public URL of the OAuth authorization server (the router) clients are
    /// directed to (e.g. `https://signaldb.example.com`). Required to advertise
    /// OAuth; see `--oauth-resource-url`.
    #[arg(long, env = "SIGNALDB__MCP__OAUTH__ISSUER_URL")]
    pub oauth_issuer_url: Option<String>,
}

/// Run the `mcp` server with its own arguments.
pub async fn run(args: Args) -> Result<()> {
    init_tracing();

    let router_timeout = std::time::Duration::from_secs(args.router_timeout);

    if args.stdio {
        return serve_stdio(args.router_url, router_timeout).await;
    }

    let addr: SocketAddr = args
        .bind_address
        .parse()
        .with_context(|| format!("Invalid bind address: {}", args.bind_address))?;

    let mut state = McpAppState::new(args.router_url.clone()).with_router_timeout(router_timeout);
    match (
        args.oauth_resource_url.clone(),
        args.oauth_issuer_url.clone(),
    ) {
        (Some(resource_url), Some(issuer_url)) => {
            tracing::info!(%resource_url, %issuer_url, "OAuth resource metadata enabled");
            state = state.with_oauth(resource_url, issuer_url);
        }
        (None, None) => {}
        _ => {
            tracing::warn!(
                "Ignoring OAuth config: both --oauth-resource-url and --oauth-issuer-url must be set"
            );
        }
    }
    let app = mcp_http_router(state, &args.allowed_hosts);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind MCP server on {addr}"))?;
    tracing::info!(address = %addr, router = %args.router_url, "SignalDB MCP server listening (Streamable HTTP at /mcp)");

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
async fn serve_stdio(router_url: String, router_timeout: std::time::Duration) -> Result<()> {
    use rmcp::ServiceExt;

    tracing::info!("SignalDB MCP server starting on stdio (development)");
    let service = McpServer::new(router_url, router_timeout)
        .serve(rmcp::transport::stdio())
        .await
        .context("Failed to start MCP stdio transport")?;
    service.waiting().await.context("MCP stdio server error")?;
    Ok(())
}
