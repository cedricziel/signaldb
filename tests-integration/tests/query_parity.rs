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

/// Operational (compaction) capabilities: (name, CLI `ops compact` subcommand,
/// MCP tool). Ops is HTTP-transport, so every capability is on both surfaces.
const OPS_MANIFEST: &[(&str, &str, &str)] = &[
    ("run", "run", "compact_run"),
    ("status", "status", "compact_status"),
    ("dry-run", "dry-run", "compact_dry_run"),
];

/// The subcommands of the CLI's `ops compact` command.
fn cli_ops_compact_subcommands() -> HashSet<String> {
    let cmd = signaldb_cli::commands::Cli::command();
    let ops = cmd
        .get_subcommands()
        .find(|c| c.get_name() == "ops")
        .expect("`ops` subcommand exists");
    let compact = ops
        .get_subcommands()
        .find(|c| c.get_name() == "compact")
        .expect("`ops compact` subcommand exists");
    compact
        .get_subcommands()
        .map(|c| c.get_name().to_string())
        .collect()
}

#[test]
fn ops_capabilities_have_cli_and_mcp() {
    use mcp_server::server::McpServer;
    let cli = cli_ops_compact_subcommands();
    for (cap, cli_sub, tool) in OPS_MANIFEST {
        assert!(
            cli.contains(*cli_sub),
            "CLI `ops compact {cli_sub}` is missing for {cap}"
        );
        assert!(
            McpServer::has_tool(tool),
            "MCP tool `{tool}` is missing for {cap}"
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
            None => {
                // Only SQL is CLI-only; guard the invariant so an HTTP language
                // can never be silently marked CLI-only by setting its tool to
                // `None`.
                assert_eq!(
                    *lang, "sql",
                    "only SQL may be CLI-only; `{lang}` is an HTTP language and needs an MCP tool"
                );
                assert!(
                    !McpServer::has_tool("query_sql") && !McpServer::has_tool("sql"),
                    "SQL must stay CLI-only — the HTTP-forwarding MCP holds no Flight client"
                );
            }
        }
    }
}
