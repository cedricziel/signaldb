//! Cross-surface query parity (see the `client-surface-parity` spec).
//!
//! Every query language SignalDB supports must be reachable via a CLI
//! `query --<flag>`. Every language served over the router's HTTP surface
//! (TraceQL/LogQL/PromQL/Query IR) must also be reachable via an MCP tool. SQL
//! is served over Arrow Flight (gRPC); the MCP server is an HTTP forwarder with
//! no Flight client, so SQL is intentionally CLI-only. Asserting the boundary
//! here keeps the surfaces from drifting apart silently.

use clap::CommandFactory;
use std::collections::HashSet;

/// (language, CLI `query` flag id, MCP tool name — `None` when CLI-only).
const MANIFEST: &[(&str, &str, Option<&str>)] = &[
    ("traceql", "traceql", Some("search_traces")),
    ("promql", "promql", Some("query_metrics")),
    ("logql", "logql", Some("search_logs")),
    ("ir", "ir", Some("query_ir")),
    ("sql", "sql", None), // Flight transport → CLI-only by design.
];

/// The argument ids on the CLI's `query` subcommand.
fn cli_query_flags() -> HashSet<String> {
    let cmd = signaldb_cli::commands::Cli::command();
    let query = cmd
        .get_subcommands()
        .find(|c| c.get_name() == "query")
        .expect("`query` subcommand exists");
    query
        .get_arguments()
        .map(|a| a.get_id().to_string())
        .collect()
}

#[test]
fn every_language_has_a_cli_flag() {
    let flags = cli_query_flags();
    for (lang, flag, _) in MANIFEST {
        assert!(
            flags.contains(*flag),
            "CLI `query` is missing the --{flag} flag for {lang}"
        );
    }
}

#[test]
fn http_languages_have_mcp_tools_and_sql_stays_cli_only() {
    use mcp_server::server::McpServer;
    for (lang, _flag, tool) in MANIFEST {
        match tool {
            Some(t) => assert!(
                McpServer::has_tool(t),
                "MCP tool `{t}` for {lang} is missing (HTTP languages must have a tool)"
            ),
            None => assert!(
                !McpServer::has_tool("query_sql") && !McpServer::has_tool("sql"),
                "SQL must stay CLI-only — the HTTP-forwarding MCP holds no Flight client"
            ),
        }
    }
}
