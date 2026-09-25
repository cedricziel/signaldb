//! The `services` command group: `signaldb-cli services map` renders the
//! Query IR `graph` envelope (see `docs/users/querying-ir.md`) as a table,
//! JSON, Graphviz `dot`, or Mermaid `flowchart`.

use clap::{Args, Subcommand, ValueEnum};
use signaldb_sdk::types::{
    GraphNodeKind, QueryIrRequest, QueryIrResponse, QueryRange, ServiceGraph,
};

use super::discover::ConnectArgs;
use super::print_json;

#[derive(Subcommand)]
pub enum ServicesAction {
    /// Render the service dependency graph
    Map(MapArgs),
}

impl ServicesAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            ServicesAction::Map(args) => args.run().await,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum GraphFormat {
    /// One row per edge (default).
    Table,
    /// The raw `graph` query response.
    Json,
    /// Graphviz `digraph`, renderable with `dot -Tsvg`.
    Dot,
    /// Mermaid `flowchart LR`.
    Mermaid,
}

#[derive(Args)]
pub struct MapArgs {
    /// Restrict to this service's neighbourhood
    #[arg(long, conflicts_with = "trace_id")]
    service: Option<String>,
    /// Hops from `--service` (1-3, default 1). Requires `--service`.
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=3))]
    depth: Option<u8>,
    /// Restrict to the services and calls of one trace
    #[arg(long, value_name = "TRACE_ID")]
    trace_id: Option<String>,
    /// Range start (RFC3339, `now-1h`, or epoch nanoseconds)
    #[arg(long, default_value = "now-1h")]
    from: String,
    /// Range end
    #[arg(long, default_value = "now")]
    to: String,
    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    format: GraphFormat,
    #[command(flatten)]
    connect: ConnectArgs,
}

impl MapArgs {
    pub async fn run(self) -> anyhow::Result<()> {
        if self.depth.is_some() && self.service.is_none() {
            anyhow::bail!("--depth requires --service");
        }

        // The `depth`-requires-`service` check above means `self.depth` is
        // only `Some` here when `self.service` is too.
        let request = QueryIrRequest {
            depth: self.depth.map(i64::from),
            fields: None,
            focus: self.service.clone(),
            from: "traces".to_string(),
            ir_version: 8,
            pipeline: Vec::new(),
            range: QueryRange {
                from: self.from.clone(),
                to: self.to.clone(),
            },
            result: "graph".to_string(),
            trace_id: self.trace_id.clone(),
        };

        let client = self.connect.build_client()?;
        let response = client
            .query_ir()
            .body(request)
            .send()
            .await
            .map_err(|e| anyhow::Error::new(e).context("services map failed"))?
            .into_inner();

        render(&response, self.format)
    }
}

/// Print warnings to stderr, then either a stderr note (empty graph) or the
/// requested format to stdout.
fn render(response: &QueryIrResponse, format: GraphFormat) -> anyhow::Result<()> {
    for warning in &response.warnings {
        eprintln!("warning: {}: {}", warning.code, warning.message);
    }

    let Some(graph) = &response.graph else {
        anyhow::bail!("server response carried no graph envelope");
    };

    if graph.nodes.is_empty() {
        eprintln!("No services found for the given scope.");
        return Ok(());
    }

    match format {
        GraphFormat::Table => {
            println!("{}", render_table(graph));
        }
        GraphFormat::Json => print_json(response)?,
        GraphFormat::Dot => println!("{}", render_dot(graph)),
        GraphFormat::Mermaid => println!("{}", render_mermaid(graph)),
    }
    Ok(())
}

/// `name -> (display name, is_external)` for edge rendering.
fn node_index(graph: &ServiceGraph) -> std::collections::HashMap<&str, (&str, bool)> {
    graph
        .nodes
        .iter()
        .map(|n| {
            (
                n.id.as_str(),
                (n.name.as_str(), matches!(n.kind, GraphNodeKind::External)),
            )
        })
        .collect()
}

fn format_rate(rate: f64) -> String {
    format!("{rate:.2}")
}

fn format_error_rate(error_rate: f64) -> String {
    format!("{:.1}%", error_rate * 100.0)
}

fn format_p95(p95_ns: Option<i64>) -> String {
    match p95_ns {
        None => "-".to_string(),
        Some(ns) => format!("{:.2}ms", ns as f64 / 1_000_000.0),
    }
}

/// One row per edge, sorted by call rate descending; external targets are
/// suffixed ` (external)`.
fn render_table(graph: &ServiceGraph) -> String {
    let index = node_index(graph);
    let mut edges: Vec<_> = graph.edges.iter().collect();
    edges.sort_by(|a, b| b.rate.total_cmp(&a.rate));

    let rows: Vec<(String, String, String, String, String)> = edges
        .iter()
        .map(|edge| {
            let (source_name, _) = index
                .get(edge.source.as_str())
                .copied()
                .unwrap_or((edge.source.as_str(), false));
            let (target_name, target_external) = index
                .get(edge.target.as_str())
                .copied()
                .unwrap_or((edge.target.as_str(), false));
            let target_label = if target_external {
                format!("{target_name} (external)")
            } else {
                target_name.to_string()
            };
            (
                source_name.to_string(),
                target_label,
                format_rate(edge.rate),
                format_error_rate(edge.error_rate),
                format_p95(edge.p95_ns),
            )
        })
        .collect();

    super::format_table(
        ["SOURCE", "TARGET", "CALLS/S", "ERROR %", "P95"],
        &rows,
        "No edges.",
    )
}

/// Escape a string for a Graphviz double-quoted id or label.
fn escape_dot(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn render_dot(graph: &ServiceGraph) -> String {
    let mut out = String::from("digraph service_map {\n");
    for node in &graph.nodes {
        let id = escape_dot(&node.id);
        let label = escape_dot(&node.name);
        if matches!(node.kind, GraphNodeKind::External) {
            out.push_str(&format!("  \"{id}\" [label=\"{label}\", style=dashed];\n"));
        } else {
            out.push_str(&format!("  \"{id}\" [label=\"{label}\"];\n"));
        }
    }
    for edge in &graph.edges {
        let source = escape_dot(&edge.source);
        let target = escape_dot(&edge.target);
        let label = escape_dot(&format!(
            "{}/s, {}",
            format_rate(edge.rate),
            format_error_rate(edge.error_rate)
        ));
        out.push_str(&format!(
            "  \"{source}\" -> \"{target}\" [label=\"{label}\"];\n"
        ));
    }
    out.push_str("}\n");
    out
}

/// A Mermaid-safe node identifier: non-alphanumeric characters become `_`.
fn mermaid_id(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Escape a string for a Mermaid double-quoted label.
fn escape_mermaid_label(s: &str) -> String {
    s.replace('"', "&quot;")
}

fn render_mermaid(graph: &ServiceGraph) -> String {
    let mut out = String::from("flowchart LR\n");
    for node in &graph.nodes {
        let id = mermaid_id(&node.id);
        let label = escape_mermaid_label(&node.name);
        out.push_str(&format!("  {id}[\"{label}\"]\n"));
    }
    for edge in &graph.edges {
        let source = mermaid_id(&edge.source);
        let target = mermaid_id(&edge.target);
        let label = escape_mermaid_label(&format!(
            "{}/s, {}",
            format_rate(edge.rate),
            format_error_rate(edge.error_rate)
        ));
        out.push_str(&format!("  {source} -->|\"{label}\"| {target}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use signaldb_sdk::types::{GraphEdge, GraphNode};

    fn sample_graph() -> ServiceGraph {
        ServiceGraph {
            dropped_nodes: None,
            nodes: vec![
                GraphNode {
                    dependency_kind: None,
                    error_rate: Some(0.25),
                    id: "service:checkout".to_string(),
                    kind: GraphNodeKind::Service,
                    name: "checkout".to_string(),
                    p95_ns: Some(20_000_000),
                    request_rate: Some(0.4),
                },
                GraphNode {
                    dependency_kind: None,
                    error_rate: Some(0.0),
                    id: "service:frontend".to_string(),
                    kind: GraphNodeKind::Service,
                    name: "frontend".to_string(),
                    p95_ns: Some(12_000_000),
                    request_rate: Some(0.4),
                },
                GraphNode {
                    dependency_kind: Some("database".to_string()),
                    error_rate: None,
                    id: "external:database:orders-db".to_string(),
                    kind: GraphNodeKind::External,
                    name: "orders-db".to_string(),
                    p95_ns: None,
                    request_rate: None,
                },
            ],
            edges: vec![
                GraphEdge {
                    count: 4,
                    error_rate: 0.25,
                    p95_ns: Some(20_000_000),
                    rate: 0.4,
                    source: "service:frontend".to_string(),
                    target: "service:checkout".to_string(),
                },
                GraphEdge {
                    count: 4,
                    error_rate: 0.0,
                    p95_ns: Some(3_000_000),
                    rate: 0.4,
                    source: "service:checkout".to_string(),
                    target: "external:database:orders-db".to_string(),
                },
            ],
        }
    }

    fn odd_name_graph() -> ServiceGraph {
        ServiceGraph {
            dropped_nodes: None,
            nodes: vec![GraphNode {
                dependency_kind: None,
                error_rate: Some(0.0),
                id: "service:\"weird\" name\\here".to_string(),
                kind: GraphNodeKind::Service,
                name: "\"weird\" name\\here".to_string(),
                p95_ns: None,
                request_rate: None,
            }],
            edges: vec![],
        }
    }

    fn empty_graph() -> ServiceGraph {
        ServiceGraph {
            dropped_nodes: None,
            nodes: vec![],
            edges: vec![],
        }
    }

    #[test]
    fn table_lists_one_row_per_edge_sorted_by_rate_and_marks_external() {
        let table = render_table(&sample_graph());
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines.len(), 3, "header + 2 edge rows, got:\n{table}");
        assert!(lines[0].starts_with("SOURCE") && lines[0].contains("TARGET"));
        // Row order: highest rate first; both edges tie at 0.40/s, so the
        // original edge order (frontend->checkout, then checkout->orders-db)
        // is preserved by a stable sort.
        assert!(
            lines[1].starts_with("frontend") && lines[1].contains("checkout"),
            "got: {}",
            lines[1]
        );
        assert!(lines[1].contains("25.0%") && lines[1].contains("20.00ms"));
        assert!(
            lines[2].starts_with("checkout") && lines[2].contains("orders-db (external)"),
            "got: {}",
            lines[2]
        );
        assert!(lines[2].contains("0.0%") && lines[2].contains("3.00ms"));
    }

    #[test]
    fn json_format_serializes_the_response_unchanged() {
        let response = QueryIrResponse {
            columns: vec![],
            flamegraph: None,
            graph: Some(sample_graph()),
            heatmap: None,
            metadata: None,
            result: "graph".to_string(),
            rows: vec![],
            series: vec![],
            step_ns: None,
            warnings: vec![],
            window: signaldb_sdk::types::ResolvedWindow {
                start_ns: 0,
                end_ns: 3_600_000_000_000,
            },
        };
        let value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&response).unwrap()).unwrap();
        assert_eq!(value["result"], "graph");
        assert_eq!(value["graph"]["nodes"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn dot_output_is_a_valid_digraph_with_escaped_ids() {
        let dot = render_dot(&sample_graph());
        assert!(dot.starts_with("digraph service_map {\n"));
        assert!(dot.ends_with("}\n"));
        assert!(dot.contains("\"service:checkout\" [label=\"checkout\"];"));
        assert!(
            dot.contains("\"external:database:orders-db\" [label=\"orders-db\", style=dashed];")
        );
        assert!(dot.contains("\"service:frontend\" -> \"service:checkout\""));
    }

    #[test]
    fn dot_escapes_quotes_and_backslashes_in_ids_and_labels() {
        let dot = render_dot(&odd_name_graph());
        assert!(dot.contains(r#""service:\"weird\" name\\here" [label="\"weird\" name\\here"];"#));
    }

    #[test]
    fn mermaid_output_uses_flowchart_lr_and_sanitized_ids() {
        let mermaid = render_mermaid(&sample_graph());
        assert!(mermaid.starts_with("flowchart LR\n"));
        assert!(mermaid.contains("service_checkout[\"checkout\"]"));
        assert!(mermaid.contains("external_database_orders_db[\"orders-db\"]"));
        assert!(mermaid.contains("service_frontend -->|\"0.40/s, 25.0%\"| service_checkout"));
    }

    #[test]
    fn mermaid_escapes_quotes_in_labels() {
        let mermaid = render_mermaid(&odd_name_graph());
        assert!(mermaid.contains("&quot;weird&quot; name\\here"));
    }

    #[test]
    fn empty_graph_renders_no_stdout_and_a_stderr_note() {
        let response = QueryIrResponse {
            columns: vec![],
            flamegraph: None,
            graph: Some(empty_graph()),
            heatmap: None,
            metadata: None,
            result: "graph".to_string(),
            rows: vec![],
            series: vec![],
            step_ns: None,
            warnings: vec![],
            window: signaldb_sdk::types::ResolvedWindow {
                start_ns: 0,
                end_ns: 60,
            },
        };
        // `render` only writes to real stdout/stderr; the emptiness branch is
        // exercised here to confirm it returns Ok(()) rather than erroring or
        // panicking on an empty node list.
        assert!(render(&response, GraphFormat::Table).is_ok());
    }

    #[test]
    fn service_and_trace_id_are_mutually_exclusive() {
        #[derive(clap::Parser)]
        struct Harness {
            #[command(flatten)]
            args: MapArgs,
        }
        use clap::Parser as _;
        assert!(
            Harness::try_parse_from(["h", "--service", "checkout", "--trace-id", "abc"]).is_err()
        );
        assert!(Harness::try_parse_from(["h", "--service", "checkout"]).is_ok());
        assert!(Harness::try_parse_from(["h", "--trace-id", "abc"]).is_ok());
    }

    #[test]
    fn depth_is_bounded_to_one_through_three() {
        #[derive(clap::Parser)]
        struct Harness {
            #[command(flatten)]
            args: MapArgs,
        }
        use clap::Parser as _;
        assert!(Harness::try_parse_from(["h", "--service", "x", "--depth", "0"]).is_err());
        assert!(Harness::try_parse_from(["h", "--service", "x", "--depth", "4"]).is_err());
        assert!(Harness::try_parse_from(["h", "--service", "x", "--depth", "2"]).is_ok());
    }

    #[tokio::test]
    async fn depth_without_service_is_rejected_at_runtime() {
        let args = MapArgs {
            service: None,
            depth: Some(2),
            trace_id: None,
            from: "now-1h".to_string(),
            to: "now".to_string(),
            format: GraphFormat::Table,
            connect: ConnectArgs {
                url: "http://127.0.0.1:1".to_string(),
                api_key: None,
                tenant_id: None,
                dataset_id: None,
            },
        };
        let err = args
            .run()
            .await
            .expect_err("depth without service must be rejected");
        assert!(err.to_string().contains("--service"), "got {err}");
    }

    #[tokio::test]
    async fn map_submits_a_graph_ir_document_via_sdk() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/query")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "irVersion": 8,
                "from": "traces",
                "result": "graph",
                "focus": "checkout",
                "depth": 2
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"result":"graph","window":{"start_ns":0,"end_ns":60},
                    "graph":{"nodes":[{"id":"service:checkout","name":"checkout","kind":"service"}],"edges":[]}}"#,
            )
            .create_async()
            .await;

        let args = MapArgs {
            service: Some("checkout".to_string()),
            depth: Some(2),
            trace_id: None,
            from: "now-1h".to_string(),
            to: "now".to_string(),
            format: GraphFormat::Json,
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
        };
        args.run().await.expect("services map succeeds");
        mock.assert_async().await;
    }
}
