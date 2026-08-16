//! Whole-SDK cross-surface parity (see the `client-surface-parity` spec,
//! design D3 of `mcp-admin-tool-parity`).
//!
//! Every operation `signaldb_sdk::OPERATIONS` lists (derived from the OpenAPI
//! document by `cargo xtask generate`) must be reachable through a CLI
//! command and an MCP tool, unless it is on the reviewed `EXCLUDED` list. Two
//! narrower rules from the original query-language-only version of this test
//! are kept as explicit assertions: every query language has a CLI `query`
//! flag, and SQL — served over Arrow Flight, which the MCP server (an HTTP
//! forwarder with no Flight client) cannot reach — stays CLI-only.

use clap::CommandFactory;
use std::collections::HashSet;

use mcp_server::server::McpServer;

/// Operations intentionally reachable from only one surface (or from
/// neither CLI nor MCP), with the reason. Every entry must still name an
/// operation `signaldb_sdk::OPERATIONS` actually has — `stale_exclusions_are_rejected`
/// below fails the moment one doesn't.
const EXCLUDED: &[(&str, &str)] = &[
    (
        "oauth_consent_context",
        "browser OAuth 2.1 consent flow, authenticated by the session cookie — no CLI or MCP surface makes sense",
    ),
    (
        "oauth_consent_decision",
        "browser OAuth 2.1 consent flow, authenticated by the session cookie — no CLI or MCP surface makes sense",
    ),
    (
        "manage_create_tenant",
        "requires a signed-in human instance-administrator session (`TenantContext::is_instance_admin`, set only by password login); unreachable from any API-key-authenticated client, which is the only auth the CLI supports (the MCP server can reach it via an OAuth-authenticated session, but no CLI surface is possible)",
    ),
    (
        "list_tenants_self",
        "tenant.rs's self-view tenant list, filtered to exactly the caller's own tenant — redundant with `whoami`/`server_info` identity lookup; not given a dedicated CLI/MCP surface (mcp-admin-tool-parity rescoping)",
    ),
    (
        "get_tenant_self",
        "tenant.rs's self-view tenant get — same rationale as `list_tenants_self`",
    ),
    // The following ten `manage_*` operations (datasets, API keys,
    // memberships) and `manage_get_schema` are gated by
    // `router::endpoints::management::authorize_tenant` (or, for
    // `manage_get_schema`, the stricter `ctx.is_instance_admin`), which
    // requires a human-authenticated principal — a browser session cookie or
    // an OAuth access token carrying a real per-tenant membership role — and
    // explicitly rejects a bare API key. This is deliberate and tested
    // (`ingestion_api_key_cannot_use_human_management_endpoints` in
    // `router/src/endpoints/session.rs`), discovered while implementing this
    // change's CLI `tenant` group (which was scoped down to `tenant table`
    // as a result — see `signaldb_cli::commands::tenant_self`'s module doc).
    // The CLI has no session/OAuth login, only `--api-key`, so no CLI
    // command can ever reach these; MCP keeps `tenant_*` tools for them
    // (functional for an OAuth-authenticated MCP session), so they are
    // excluded here rather than mapped with a nonexistent CLI surface.
    (
        "manage_list_datasets",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible — see the block comment above",
    ),
    (
        "manage_create_dataset",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_delete_dataset",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_list_api_keys",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_create_api_key",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_update_api_key",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_revoke_api_key",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_list_memberships",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_upsert_membership",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_remove_membership",
        "human-session-only management endpoint (authorize_tenant); no CLI surface possible",
    ),
    (
        "manage_get_schema",
        "human-session-only, and stricter still: requires ctx.is_instance_admin, not just a tenant-admin role; no CLI surface possible",
    ),
    (
        "search_tags_v2",
        "Tempo v2 tag search, added to the OpenAPI document by a concurrent change (trace-tag-discovery, #1258) merged into main while this change was in flight; neither the CLI's `discover attributes` nor the MCP `discover_attributes` tool has been updated to the v2 endpoints yet (they still call v1 search_tags/search_tag_values) — out of this change's scope (tempo.rs tag handlers), tracked separately",
    ),
    (
        "search_tag_values_v2",
        "Tempo v2 tag value search — same rationale as search_tags_v2",
    ),
];

/// How an operation is reached through the CLI: either a subcommand path
/// (`["admin", "tenant", "list"]`) or a flag id on the `query` command
/// (`--promql`, `--ir`, `--trace-id`, ...).
#[derive(Clone, Copy)]
enum CliSurface {
    Path(&'static [&'static str]),
    QueryFlag(&'static str),
}

/// `(operation, CLI surface, MCP tool name)`. Every non-excluded operation in
/// `signaldb_sdk::OPERATIONS` must appear here exactly once; stale entries
/// (naming an operation that no longer exists) fail loudly, same as a
/// missing one.
const MANIFEST: &[(&str, CliSurface, &str)] = &[
    // ---- Admin (platform-admin, admin API) ----
    (
        "list_tenants",
        CliSurface::Path(&["admin", "tenant", "list"]),
        "list_tenants",
    ),
    (
        "get_tenant",
        CliSurface::Path(&["admin", "tenant", "get"]),
        "get_tenant",
    ),
    (
        "create_tenant",
        CliSurface::Path(&["admin", "tenant", "create"]),
        "create_tenant",
    ),
    (
        "update_tenant",
        CliSurface::Path(&["admin", "tenant", "update"]),
        "update_tenant",
    ),
    (
        "delete_tenant",
        CliSurface::Path(&["admin", "tenant", "delete"]),
        "delete_tenant",
    ),
    (
        "create_user",
        CliSurface::Path(&["user", "create"]),
        "create_user",
    ),
    (
        "list_datasets",
        CliSurface::Path(&["admin", "dataset", "list"]),
        "list_datasets",
    ),
    (
        "create_dataset",
        CliSurface::Path(&["admin", "dataset", "create"]),
        "create_dataset",
    ),
    (
        "delete_dataset",
        CliSurface::Path(&["admin", "dataset", "delete"]),
        "delete_dataset",
    ),
    (
        "list_api_keys",
        CliSurface::Path(&["admin", "api-key", "list"]),
        "list_api_keys",
    ),
    (
        "create_api_key",
        CliSurface::Path(&["admin", "api-key", "create"]),
        "create_api_key",
    ),
    (
        "update_api_key",
        CliSurface::Path(&["admin", "api-key", "update"]),
        "update_api_key_scopes",
    ),
    (
        "revoke_api_key",
        CliSurface::Path(&["admin", "api-key", "revoke"]),
        "revoke_api_key",
    ),
    // ---- Tenant self-management: tables/schemas only (tenant self-service
    // API, works with a plain tenant API key). manage_list_datasets,
    // manage_create_dataset, manage_delete_dataset, manage_list_api_keys,
    // manage_create_api_key, manage_update_api_key, manage_revoke_api_key,
    // manage_list_memberships, manage_upsert_membership,
    // manage_remove_membership, and manage_get_schema are EXCLUDED below,
    // not mapped here — the management API's `authorize_tenant` requires a
    // human-authenticated principal and rejects a bare API key, which is
    // the CLI's only auth mechanism. Their MCP `tenant_*` tools still exist
    // (functional for an OAuth-authenticated MCP session) but the CLI has
    // no matching command, so the whole-SDK check cannot require both
    // surfaces for them. ----
    (
        "list_tenant_tables",
        CliSurface::Path(&["tenant", "table", "list"]),
        "tenant_list_tables",
    ),
    (
        "create_tenant_tables",
        CliSurface::Path(&["tenant", "table", "provision"]),
        "tenant_create_tables",
    ),
    (
        "list_tenant_schemas",
        CliSurface::Path(&["tenant", "table", "schemas"]),
        "tenant_list_table_schemas",
    ),
    (
        "list_available_schemas",
        CliSurface::Path(&["tenant", "table", "available-schemas"]),
        "list_available_table_schemas",
    ),
    // ---- Operational control ----
    (
        "ops_compact",
        CliSurface::Path(&["ops", "compact", "run"]),
        "compact_run",
    ),
    (
        "ops_compact_status",
        CliSurface::Path(&["ops", "compact", "status"]),
        "compact_status",
    ),
    (
        "ops_compact_dry_run",
        CliSurface::Path(&["ops", "compact", "dry-run"]),
        "compact_dry_run",
    ),
    // ---- Schema registry ----
    (
        "schema_list_registries",
        CliSurface::Path(&["schema", "registry", "list"]),
        "list_schema_registries",
    ),
    (
        "schema_get_registry",
        CliSurface::Path(&["schema", "registry", "get"]),
        "get_schema_registry",
    ),
    (
        "schema_create_registry",
        CliSurface::Path(&["admin", "schema", "create"]),
        "create_schema_registry",
    ),
    (
        "schema_replace_registry",
        CliSurface::Path(&["admin", "schema", "replace"]),
        "replace_schema_registry",
    ),
    (
        "schema_delete_registry",
        CliSurface::Path(&["admin", "schema", "delete"]),
        "delete_schema_registry",
    ),
    (
        "schema_validate_registry",
        CliSurface::Path(&["admin", "schema", "validate"]),
        "validate_schema_registry",
    ),
    (
        "schema_resolve_attribute",
        CliSurface::Path(&["schema", "attribute", "get"]),
        "resolve_attribute",
    ),
    (
        "schema_search_attributes",
        CliSurface::Path(&["schema", "attribute", "search"]),
        "search_schema",
    ),
    (
        "schema_resolve_entity",
        CliSurface::Path(&["schema", "entity", "get"]),
        "resolve_entity",
    ),
    (
        "schema_search_entities",
        CliSurface::Path(&["schema", "entity", "search"]),
        "search_schema",
    ),
    (
        "schema_resolve_metric",
        CliSurface::Path(&["schema", "metric", "get"]),
        "resolve_metric",
    ),
    (
        "schema_search_metrics",
        CliSurface::Path(&["schema", "metric", "search"]),
        "search_schema",
    ),
    // ---- Query languages (also covered by the language-specific assertions
    // below) ----
    ("search", CliSurface::QueryFlag("traceql"), "search_traces"),
    (
        "promql_query",
        CliSurface::QueryFlag("promql"),
        "query_metrics",
    ),
    (
        "promql_query_range",
        CliSurface::QueryFlag("promql"),
        "query_metrics",
    ),
    ("logql_query", CliSurface::QueryFlag("logql"), "search_logs"),
    (
        "logql_query_range",
        CliSurface::QueryFlag("logql"),
        "search_logs",
    ),
    ("query_ir", CliSurface::QueryFlag("ir"), "query_ir"),
    (
        "query_single_trace",
        CliSurface::QueryFlag("trace_id"),
        "get_trace",
    ),
    // ---- Discovery auxiliaries (label/tag names and values) — all reachable
    // through `discover attributes` / `discover_attributes`, signal-selected ----
    (
        "search_tags",
        CliSurface::Path(&["discover", "attributes"]),
        "discover_attributes",
    ),
    (
        "search_tag_values",
        CliSurface::Path(&["discover", "attributes"]),
        "discover_attributes",
    ),
    (
        "logql_labels",
        CliSurface::Path(&["discover", "attributes"]),
        "discover_attributes",
    ),
    (
        "logql_label_values",
        CliSurface::Path(&["discover", "attributes"]),
        "discover_attributes",
    ),
    (
        "promql_labels",
        CliSurface::Path(&["discover", "attributes"]),
        "discover_attributes",
    ),
    (
        "promql_label_values",
        CliSurface::Path(&["discover", "attributes"]),
        "discover_attributes",
    ),
    // ---- Identity ----
    ("whoami", CliSurface::Path(&["whoami"]), "server_info"),
];

/// Walks `cmd`'s subcommand tree along `path`, returning whether every
/// segment resolves to a nested subcommand.
fn subcommand_path_exists(cmd: &clap::Command, path: &[&str]) -> bool {
    let mut current = cmd;
    for segment in path {
        match current.get_subcommands().find(|c| c.get_name() == *segment) {
            Some(next) => current = next,
            None => return false,
        }
    }
    true
}

/// Whether the CLI's `query` subcommand declares an argument with this id.
fn query_flag_exists(cmd: &clap::Command, flag: &str) -> bool {
    let Some(query) = cmd.get_subcommands().find(|c| c.get_name() == "query") else {
        return false;
    };
    query.get_arguments().any(|a| a.get_id() == flag)
}

fn cli_covers(cmd: &clap::Command, surface: CliSurface) -> bool {
    match surface {
        CliSurface::Path(path) => subcommand_path_exists(cmd, path),
        CliSurface::QueryFlag(flag) => query_flag_exists(cmd, flag),
    }
}

#[test]
fn manifest_is_total_over_operations_minus_excluded() {
    let operations: HashSet<&str> = signaldb_sdk::OPERATIONS.iter().copied().collect();
    let excluded: HashSet<&str> = EXCLUDED.iter().map(|(op, _)| *op).collect();
    let manifest: HashSet<&str> = MANIFEST.iter().map(|(op, _, _)| *op).collect();

    // Every excluded name must be a real, current operation — otherwise the
    // exclusion list is rotting (D3's "stale exclusion" scenario).
    let mut stale_exclusions: Vec<&str> = excluded
        .iter()
        .filter(|op| !operations.contains(*op))
        .copied()
        .collect();
    stale_exclusions.sort();
    assert!(
        stale_exclusions.is_empty(),
        "EXCLUDED names operations that no longer exist in signaldb_sdk::OPERATIONS: {stale_exclusions:?}"
    );

    // Every manifest entry must also be a real, current operation.
    let mut stale_manifest: Vec<&str> = manifest
        .iter()
        .filter(|op| !operations.contains(*op))
        .copied()
        .collect();
    stale_manifest.sort();
    assert!(
        stale_manifest.is_empty(),
        "MANIFEST names operations that no longer exist in signaldb_sdk::OPERATIONS: {stale_manifest:?}"
    );

    // Excluded and mapped must not overlap.
    let mut both: Vec<&str> = excluded.intersection(&manifest).copied().collect();
    both.sort();
    assert!(
        both.is_empty(),
        "operations both excluded and mapped (pick one): {both:?}"
    );

    // Every operation must be excluded or mapped — nothing falls through.
    let mut uncovered: Vec<&str> = operations
        .iter()
        .filter(|op| !excluded.contains(*op) && !manifest.contains(*op))
        .copied()
        .collect();
    uncovered.sort();
    assert!(
        uncovered.is_empty(),
        "operations with neither a MANIFEST entry nor an EXCLUDED reason: {uncovered:?}"
    );
}

#[test]
fn every_manifest_operation_has_a_cli_surface_and_an_mcp_tool() {
    let cmd = signaldb_cli::commands::Cli::command();
    let mut missing_cli = Vec::new();
    let mut missing_mcp = Vec::new();

    for (operation, cli_surface, mcp_tool) in MANIFEST {
        if !cli_covers(&cmd, *cli_surface) {
            missing_cli.push(*operation);
        }
        if !McpServer::has_tool(mcp_tool) {
            missing_mcp.push(*operation);
        }
    }

    assert!(
        missing_cli.is_empty(),
        "operations with no CLI surface: {missing_cli:?}"
    );
    assert!(
        missing_mcp.is_empty(),
        "operations with no MCP tool: {missing_mcp:?}"
    );
}

/// SQL is intentionally absent from `signaldb_sdk::OPERATIONS` (it is served
/// over Arrow Flight, which OpenAPI cannot describe — see
/// `query::QueryClient`), so it can never appear in `MANIFEST`/`EXCLUDED`.
/// This is the explicit boundary assertion the whole-SDK check cannot derive
/// on its own: SQL has a CLI flag, and no MCP tool may claim it.
#[test]
fn sql_is_cli_only_by_design() {
    let cmd = signaldb_cli::commands::Cli::command();
    assert!(
        query_flag_exists(&cmd, "sql"),
        "CLI `query` is missing the --sql flag"
    );
    assert!(
        !signaldb_sdk::OPERATIONS.contains(&"sql"),
        "SQL is Flight-transport and must never gain an OpenAPI operation"
    );
    assert!(
        !McpServer::has_tool("sql") && !McpServer::has_tool("query_sql"),
        "SQL must stay CLI-only — the HTTP-forwarding MCP server holds no Flight client"
    );
}

/// Every query language keeps its own explicit CLI-flag assertion (the
/// original, narrower shape of this test), independent of the whole-SDK
/// walk above.
#[test]
fn every_language_has_a_cli_flag() {
    let cmd = signaldb_cli::commands::Cli::command();
    for flag in ["sql", "promql", "logql", "traceql", "ir"] {
        assert!(
            query_flag_exists(&cmd, flag),
            "CLI `query` is missing the --{flag} flag"
        );
    }
}

/// Every HTTP query language additionally needs an MCP tool (SQL is the sole,
/// explicitly asserted exception above).
#[test]
fn http_languages_have_mcp_tools() {
    for tool in ["search_traces", "query_metrics", "search_logs", "query_ir"] {
        assert!(
            McpServer::has_tool(tool),
            "MCP tool `{tool}` is missing for an HTTP query language"
        );
    }
}
