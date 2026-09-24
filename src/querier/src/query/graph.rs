//! The Query IR `graph` envelope: a service dependency graph assembled from
//! fixed internal pipelines over `traces` (see
//! `openspec/changes/service-map`).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::query_ir::Document;
use common::service_graph::{GRAPH_JSON_COLUMN, GraphEdge, GraphNode, GraphNodeKind, ServiceGraph};
use datafusion::arrow::array::{Array, AsArray, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use datafusion::functions::core::expr_fn::{coalesce, nullif};
use datafusion::functions_aggregate::expr_fn::{approx_percentile_cont, count, sum};
use datafusion::logical_expr::{Expr, JoinType, col, lit, when};
use datafusion::prelude::{DataFrame, SessionContext};
use serde_json::json;

use super::error::QuerierError;
use super::ir_planner::{PlanRequest, ResolvedWindow, plan_document};

/// Server-side bounds on a graph query.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GraphLimits {
    pub correlate_max_rows: usize,
    pub max_nodes: usize,
}

/// External node name attributes, first present wins (the order
/// `src/ui/src/api/dependencyTargets.ts` also uses).
const NAME_ATTRS: [&str; 5] = [
    "db.namespace",
    "messaging.destination.name",
    "rpc.service",
    "server.address",
    "peer.service",
];

/// Dependency-kind attributes and the kind each one signals.
const KIND_ATTRS: [(&str, &str); 4] = [
    ("db.system.name", "database"),
    ("messaging.system", "messaging"),
    ("rpc.system", "rpc"),
    ("http.request.method", "http"),
];

const CALLEE_KINDS: [&str; 2] = ["Server", "Consumer"];
const CALLER_KINDS: [&str; 2] = ["Client", "Producer"];

/// Build the graph for a `graph` document, or `None` when the dataset has no
/// traces table. Returns the graph, the resolved window, and whether the
/// edge query's `correlate` join hit `correlate_max_rows`.
///
/// Three internal IR pipelines run over the document's `where`-filtered
/// span set: service edges (a `correlate` join grouped by caller and callee
/// service), node metrics (server/consumer spans per service), and external
/// edges (client/producer spans anti-joined against every server/consumer
/// span in the window). Each aggregates inside DataFusion; only the grouped
/// rows are collected, then merged, scoped and capped here.
pub(crate) async fn build_graph(
    ctx: &SessionContext,
    doc: &Document,
    tenant_slug: &str,
    dataset_slug: &str,
    now_ns: i64,
    limits: GraphLimits,
) -> Result<Option<(ServiceGraph, ResolvedWindow, bool)>, QuerierError> {
    let request = || {
        PlanRequest::new(tenant_slug, dataset_slug, now_ns)
            .with_correlate_max_rows(limits.correlate_max_rows)
    };
    // Validates the caller's document and resolves its window; the plan
    // itself is never executed.
    let Some((_, window, _)) = plan_document(ctx, doc, request()).await? else {
        return Ok(None);
    };
    let plan = |internal: Document| async move {
        plan_document(ctx, &internal, request())
            .await?
            .map(|(df, _, truncated)| (df, truncated))
            .ok_or_else(|| QuerierError::InvalidInput("traces table disappeared".into()))
    };

    let (edges_df, truncated) = plan(internal_doc(
        doc,
        true,
        &CALLEE_KINDS,
        json!([
            { "correlate": { "to": "parent", "kind": "inner" } },
            call_aggregate(&["parent.service.name", "service.name"]),
        ]),
        None,
    )?)
    .await?;
    let (nodes_df, _) = plan(internal_doc(
        doc,
        true,
        &CALLEE_KINDS,
        json!([call_aggregate(&["service.name"])]),
        None,
    )?)
    .await?;
    let mut caller_fields = vec!["trace_id", "span_id", "service.name"];
    caller_fields.extend(NAME_ATTRS);
    caller_fields.extend(KIND_ATTRS.map(|(attr, _)| attr));
    caller_fields.extend(["status.code", "duration"]);
    let (callers_df, _) = plan(internal_doc(
        doc,
        true,
        &CALLER_KINDS,
        json!([]),
        Some(&caller_fields),
    )?)
    .await?;
    // The callee side ignores the document's `where` stages: a filtered-out
    // callee is still instrumented, so its caller's call is not external.
    let (callees_df, _) = plan(internal_doc(
        doc,
        false,
        &CALLEE_KINDS,
        json!([]),
        Some(&["trace_id", "parent_span_id"]),
    )?)
    .await?;
    let externals_df = external_calls(callers_df, callees_df)?;

    let window_secs = ((window.end_ns - window.start_ns) as f64 / 1e9).max(f64::MIN_POSITIVE);
    // The three aggregations are independent; run them concurrently.
    let (edge_rows, node_rows, external_rows) = futures::try_join!(
        call_rows(edges_df, 2),
        call_rows(nodes_df, 1),
        call_rows(externals_df, 3),
    )?;
    let mut edges: Vec<GraphEdge> = edge_rows
        .into_iter()
        .filter(|row| row.keys[0] != row.keys[1])
        .map(|row| row.edge(&row.keys[1], window_secs))
        .collect();
    let mut nodes: BTreeMap<String, GraphNode> = BTreeMap::new();
    for row in node_rows {
        let name = row.keys[0].clone();
        nodes.insert(
            name.clone(),
            GraphNode {
                request_rate: Some(row.calls as f64 / window_secs),
                error_rate: Some(row.error_rate()),
                p95_ns: row.p95_ns,
                ..service_node(&name)
            },
        );
    }
    // Two groups (say, differing kind attributes) can share one external
    // name; fold them into one edge.
    let mut external_edges: BTreeMap<(String, String), (GraphEdge, String)> = BTreeMap::new();
    for row in external_rows {
        // A client span with no naming attribute is named after its kind.
        let kind = row.keys[2].clone();
        let target = if row.keys[1].is_empty() {
            &kind
        } else {
            &row.keys[1]
        };
        let edge = row.edge(target, window_secs);
        match external_edges.entry((edge.source.clone(), edge.target.clone())) {
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert((edge, kind));
            }
            std::collections::btree_map::Entry::Occupied(mut o) => {
                merge_edge(&mut o.get_mut().0, &edge, window_secs);
            }
        }
    }
    for (edge, kind) in external_edges.into_values() {
        nodes
            .entry(edge.target.clone())
            .or_insert_with(|| GraphNode {
                kind: GraphNodeKind::External,
                dependency_kind: Some(kind),
                ..service_node(&edge.target)
            });
        edges.push(edge);
    }
    // A caller with no server/consumer spans of its own (a cron job, a
    // frontend) still needs a node.
    for edge in &edges {
        nodes
            .entry(edge.source.clone())
            .or_insert_with(|| service_node(&edge.source));
        nodes
            .entry(edge.target.clone())
            .or_insert_with(|| service_node(&edge.target));
    }
    edges.sort_by(|a, b| (&a.source, &a.target).cmp(&(&b.source, &b.target)));

    let mut graph = ServiceGraph {
        nodes: nodes.into_values().collect(),
        edges,
        dropped_nodes: 0,
    };
    if let Some(focus) = &doc.focus {
        let depth = usize::try_from(doc.depth.unwrap_or(1)).unwrap_or(1);
        graph = scope_to_focus(graph, focus, depth);
    }
    Ok(Some((
        cap_nodes(graph, doc.focus.as_deref(), limits.max_nodes),
        window,
        truncated.is_some_and(|flag| flag.load(Ordering::Relaxed)),
    )))
}

/// The graph as a one-row `graph_json: Utf8` batch, the shape it crosses
/// the Flight wire in (the router decodes it back into the response).
pub(crate) fn encode_graph_batch(graph: &ServiceGraph) -> Result<RecordBatch, QuerierError> {
    let json = serde_json::to_string(graph).map_err(|e| {
        QuerierError::QueryFailed(datafusion::error::DataFusionError::Execution(format!(
            "failed to encode graph: {e}"
        )))
    })?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        GRAPH_JSON_COLUMN,
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![json]))]).map_err(|e| {
        QuerierError::QueryFailed(datafusion::error::DataFusionError::ArrowError(
            Box::new(e),
            None,
        ))
    })
}

fn service_node(name: &str) -> GraphNode {
    GraphNode {
        name: name.to_string(),
        kind: GraphNodeKind::Service,
        dependency_kind: None,
        request_rate: None,
        error_rate: None,
        p95_ns: None,
    }
}

/// Fold `other` into `into`; the p95 keeps the larger of the two.
fn merge_edge(into: &mut GraphEdge, other: &GraphEdge, window_secs: f64) {
    let errors = into.error_rate * into.count as f64 + other.error_rate * other.count as f64;
    into.count += other.count;
    into.rate = into.count as f64 / window_secs;
    into.error_rate = errors / into.count.max(1) as f64;
    into.p95_ns = into.p95_ns.max(other.p95_ns);
}

/// One internal IR document: the caller's range and (optionally) `where`
/// stages, its `trace_id` scope, a span-kind filter, then `tail`. With
/// `fields` it is a `rows` projection, otherwise a `table`.
fn internal_doc(
    doc: &Document,
    keep_where: bool,
    kinds: &[&str],
    tail: serde_json::Value,
    fields: Option<&[&str]>,
) -> Result<Document, QuerierError> {
    let mut pipeline: Vec<serde_json::Value> = Vec::new();
    if keep_where {
        for stage in &doc.pipeline {
            pipeline.push(serde_json::to_value(stage).map_err(invalid)?);
        }
    }
    if let Some(trace_id) = &doc.trace_id {
        pipeline.push(json!({ "where": { "field": "trace_id", "op": "eq", "value": trace_id } }));
    }
    pipeline.push(json!({ "where": { "field": "span_kind", "op": "in", "value": kinds } }));
    pipeline.extend(tail.as_array().into_iter().flatten().cloned());
    serde_json::from_value(json!({
        "irVersion": 8,
        "from": doc.from,
        "range": doc.range,
        "result": if fields.is_some() { "rows" } else { "table" },
        "fields": fields,
        "pipeline": pipeline,
    }))
    .map_err(invalid)
}

fn invalid(e: serde_json::Error) -> QuerierError {
    QuerierError::InvalidInput(format!("graph: {e}"))
}

/// The per-group call metrics every graph query reports.
fn call_aggregate(by: &[&str]) -> serde_json::Value {
    json!({ "aggregate": { "by": by, "aggs": [
        { "fn": "count", "as": "calls" },
        { "fn": "count", "as": "errors",
          "where": { "field": "status.code", "op": "eq", "value": "Error" } },
        { "fn": "quantile", "of": "duration", "arg": 0.95, "as": "p95" },
    ] } })
}

/// Client/producer spans with no server/consumer child in the window,
/// grouped by caller service, external name and dependency kind. Callers
/// arrive as `[trace_id, span_id, service, NAME_ATTRS.., KIND_ATTRS..,
/// status, duration]` and callees as `[trace_id, parent_span_id]`, read by
/// position since the IR projection picks its own output column names.
fn external_calls(callers: DataFrame, callees: DataFrame) -> Result<DataFrame, QuerierError> {
    let callers = rename_positional(callers, "caller")?;
    let callees = rename_positional(callees, "callee")?;
    let at = |i: usize| col(format!("caller_{i}"));
    let calls = callers
        .join_on(
            callees,
            JoinType::LeftAnti,
            vec![at(0).eq(col("callee_0")), at(1).eq(col("callee_1"))],
        )
        .map_err(QuerierError::QueryFailed)?;

    let present = |i: usize| nullif(at(i), lit(""));
    let name = coalesce(
        (3..3 + NAME_ATTRS.len())
            .map(present)
            .chain([lit("")])
            .collect(),
    );
    let kind_base = 3 + NAME_ATTRS.len();
    let mut kind = when(present(kind_base).is_not_null(), lit(KIND_ATTRS[0].1));
    for (i, (_, label)) in KIND_ATTRS.iter().enumerate().skip(1) {
        kind = kind.when(present(kind_base + i).is_not_null(), lit(*label));
    }
    let kind = kind
        .otherwise(lit("other"))
        .map_err(QuerierError::QueryFailed)?;
    let status = at(kind_base + KIND_ATTRS.len());
    let duration = at(kind_base + KIND_ATTRS.len() + 1);
    let is_error = when(status.eq(lit("Error")), lit(1_i64))
        .otherwise(lit(0_i64))
        .map_err(QuerierError::QueryFailed)?;
    calls
        .aggregate(
            vec![
                at(2).alias("service"),
                name.alias("name"),
                kind.alias("kind"),
            ],
            vec![
                count(lit(1)).alias("calls"),
                sum(is_error).alias("errors"),
                approx_percentile_cont(duration.sort(true, false), lit(0.95), None).alias("p95"),
            ],
        )
        .map_err(QuerierError::QueryFailed)
}

fn rename_positional(df: DataFrame, prefix: &str) -> Result<DataFrame, QuerierError> {
    let exprs: Vec<Expr> = df
        .schema()
        .columns()
        .into_iter()
        .enumerate()
        .map(|(i, c)| Expr::Column(c).alias(format!("{prefix}_{i}")))
        .collect();
    df.select(exprs).map_err(QuerierError::QueryFailed)
}

/// One aggregated group: its key columns, then calls, errors and p95.
struct CallRow {
    keys: Vec<String>,
    calls: u64,
    errors: u64,
    p95_ns: Option<i64>,
}

impl CallRow {
    fn error_rate(&self) -> f64 {
        self.errors as f64 / self.calls.max(1) as f64
    }

    /// An edge from `keys[0]` to `target`.
    fn edge(&self, target: &str, window_secs: f64) -> GraphEdge {
        GraphEdge {
            source: self.keys[0].clone(),
            target: target.to_string(),
            count: self.calls,
            rate: self.calls as f64 / window_secs,
            error_rate: self.error_rate(),
            p95_ns: self.p95_ns,
        }
    }
}

/// Collect an aggregated frame whose first `n_keys` columns are group keys
/// followed by calls, errors and p95. Groups with a null key are skipped.
async fn call_rows(df: DataFrame, n_keys: usize) -> Result<Vec<CallRow>, QuerierError> {
    let batches = df.collect().await.map_err(QuerierError::QueryFailed)?;
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = (0..n_keys)
            .map(|i| cast_column(batch, i, &DataType::Utf8))
            .collect::<Result<Vec<_>, _>>()?;
        let calls = cast_column(batch, n_keys, &DataType::Int64)?;
        let errors = cast_column(batch, n_keys + 1, &DataType::Int64)?;
        let p95 = cast_column(batch, n_keys + 2, &DataType::Float64)?;
        let (calls, errors, p95) = (
            calls.as_primitive::<Int64Type>(),
            errors.as_primitive::<Int64Type>(),
            p95.as_primitive::<Float64Type>(),
        );
        for row in 0..batch.num_rows() {
            if keys.iter().any(|k| k.is_null(row)) {
                continue;
            }
            let count = |a: &datafusion::arrow::array::Int64Array| {
                if a.is_null(row) {
                    0
                } else {
                    a.value(row).max(0) as u64
                }
            };
            rows.push(CallRow {
                keys: keys
                    .iter()
                    .map(|k| k.as_string::<i32>().value(row).to_string())
                    .collect(),
                calls: count(calls),
                errors: count(errors),
                p95_ns: (!p95.is_null(row)).then(|| p95.value(row).round() as i64),
            });
        }
    }
    Ok(rows)
}

fn cast_column(
    batch: &RecordBatch,
    i: usize,
    to: &DataType,
) -> Result<Arc<dyn Array>, QuerierError> {
    cast(batch.column(i), to).map_err(|e| {
        QuerierError::QueryFailed(datafusion::error::DataFusionError::ArrowError(
            Box::new(e),
            None,
        ))
    })
}

/// Keep the nodes within `depth` hops of `focus` (either direction), and the
/// edges the walk crossed: those with an endpoint closer than `depth`. An
/// unknown focus is an empty graph.
fn scope_to_focus(graph: ServiceGraph, focus: &str, depth: usize) -> ServiceGraph {
    if !graph.nodes.iter().any(|n| n.name == focus) {
        return ServiceGraph::default();
    }
    let mut dist: HashMap<&str, usize> = HashMap::from([(focus, 0)]);
    let mut queue = VecDeque::from([focus]);
    while let Some(at) = queue.pop_front() {
        let d = dist[at];
        if d == depth {
            continue;
        }
        for e in &graph.edges {
            let next = if e.source == at {
                e.target.as_str()
            } else if e.target == at {
                e.source.as_str()
            } else {
                continue;
            };
            dist.entry(next).or_insert_with(|| {
                queue.push_back(next);
                d + 1
            });
        }
    }
    let crossed = |e: &GraphEdge| {
        matches!(
            (dist.get(e.source.as_str()), dist.get(e.target.as_str())),
            (Some(a), Some(b)) if (*a).min(*b) < depth
        )
    };
    ServiceGraph {
        nodes: graph
            .nodes
            .iter()
            .filter(|n| dist.contains_key(n.name.as_str()))
            .cloned()
            .collect(),
        edges: graph.edges.iter().filter(|e| crossed(e)).cloned().collect(),
        dropped_nodes: 0,
    }
}

/// Cap the graph at `max_nodes`: the focus first, then the nodes with the
/// most call traffic (the sum of counts on their edges), name as the tie
/// break. Edges touching a dropped node go with it.
fn cap_nodes(mut graph: ServiceGraph, focus: Option<&str>, max_nodes: usize) -> ServiceGraph {
    if graph.nodes.len() <= max_nodes {
        return graph;
    }
    let mut traffic: HashMap<&str, u64> = HashMap::new();
    for e in &graph.edges {
        *traffic.entry(&e.source).or_default() += e.count;
        *traffic.entry(&e.target).or_default() += e.count;
    }
    let rank = |n: &GraphNode| {
        (
            focus != Some(n.name.as_str()),
            std::cmp::Reverse(traffic.get(n.name.as_str()).copied().unwrap_or(0)),
        )
    };
    let mut ranked: Vec<&GraphNode> = graph.nodes.iter().collect();
    ranked.sort_by(|a, b| rank(a).cmp(&rank(b)).then_with(|| a.name.cmp(&b.name)));
    let kept: HashSet<String> = ranked
        .into_iter()
        .take(max_nodes)
        .map(|n| n.name.clone())
        .collect();
    graph.dropped_nodes = (graph.nodes.len() - kept.len()) as u64;
    graph.nodes.retain(|n| kept.contains(&n.name));
    graph
        .edges
        .retain(|e| kept.contains(&e.source) && kept.contains(&e.target));
    graph
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::service_graph::{GraphEdge, GraphNode, GraphNodeKind};
    use datafusion::arrow::array::{
        ArrayRef, Int64Array, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};
    use std::sync::Arc;

    const SECOND: i64 = 1_000_000_000;
    /// The test window, `[0, 10s]`: every rate is a count divided by 10.
    const WINDOW_END: i64 = 10 * SECOND;

    struct Span {
        trace: &'static str,
        span: &'static str,
        parent: Option<&'static str>,
        service: &'static str,
        kind: &'static str,
        status: &'static str,
        duration_ms: i64,
        attrs: &'static [(&'static str, &'static str)],
    }

    fn span(
        trace: &'static str,
        span: &'static str,
        parent: Option<&'static str>,
        service: &'static str,
        kind: &'static str,
    ) -> Span {
        Span {
            trace,
            span,
            parent,
            service,
            kind,
            status: "Ok",
            duration_ms: 10,
            attrs: &[],
        }
    }

    const HTTP_CHECKOUT: &[(&str, &str)] = &[
        ("server.address", "checkout:8080"),
        ("http.request.method", "GET"),
    ];
    const CHECKOUT_DB: &[(&str, &str)] = &[
        ("db.system.name", "postgresql"),
        ("db.namespace", "checkout-db"),
    ];
    const ORDERS_DB: &[(&str, &str)] = &[
        ("db.system.name", "postgresql"),
        ("db.namespace", "orders-db"),
    ];

    /// Four `frontend → checkout → checkout-db` traces (`t1`..`t4`, the
    /// checkout server span failing in `t4`), plus `t5`: an `orders` server
    /// span whose `orders-db` client span has only a nested client child.
    fn fixture() -> Vec<Span> {
        let mut spans = Vec::new();
        for (trace, status) in [("t1", "Ok"), ("t2", "Ok"), ("t3", "Ok"), ("t4", "Error")] {
            spans.push(span(trace, "fs", None, "frontend", "Server"));
            spans.push(Span {
                attrs: HTTP_CHECKOUT,
                ..span(trace, "fc", Some("fs"), "frontend", "Client")
            });
            spans.push(Span {
                status,
                duration_ms: 20,
                ..span(trace, "cs", Some("fc"), "checkout", "Server")
            });
            spans.push(Span {
                attrs: CHECKOUT_DB,
                ..span(trace, "cdb", Some("cs"), "checkout", "Client")
            });
        }
        spans.push(span("t5", "os", None, "orders", "Server"));
        spans.push(Span {
            attrs: ORDERS_DB,
            ..span("t5", "odb", Some("os"), "orders", "Client")
        });
        spans.push(Span {
            attrs: ORDERS_DB,
            ..span("t5", "odb2", Some("odb"), "orders", "Client")
        });
        spans
    }

    fn attrs_map(rows: &[&[(&str, &str)]]) -> ArrayRef {
        let names = MapFieldNames {
            entry: "entries".to_string(),
            key: "keys".to_string(),
            value: "values".to_string(),
        };
        let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
        for row in rows {
            for (k, v) in *row {
                b.keys().append_value(k);
                b.values().append_value(v);
            }
            b.append(true).unwrap();
        }
        Arc::new(b.finish())
    }

    fn ctx_with(spans: &[Span]) -> SessionContext {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("start_time_unix_nano", DataType::Int64, false),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, false),
            Field::new(
                "span_attributes",
                DataType::Map(Arc::new(entries), false),
                true,
            ),
        ]));
        let col = |f: fn(&Span) -> &str| -> ArrayRef {
            Arc::new(StringArray::from(spans.iter().map(f).collect::<Vec<_>>()))
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                col(|s| s.trace),
                col(|s| s.span),
                Arc::new(StringArray::from(
                    spans.iter().map(|s| s.parent).collect::<Vec<_>>(),
                )),
                col(|s| s.span),
                col(|s| s.service),
                col(|s| s.kind),
                Arc::new(Int64Array::from(vec![SECOND; spans.len()])),
                Arc::new(Int64Array::from(
                    spans
                        .iter()
                        .map(|s| s.duration_ms * 1_000_000)
                        .collect::<Vec<_>>(),
                )),
                col(|s| s.status),
                attrs_map(&spans.iter().map(|s| s.attrs).collect::<Vec<_>>()),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table(
            "traces".to_string(),
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    fn graph_doc(extra: serde_json::Value) -> Document {
        let mut v = serde_json::json!({
            "irVersion": 8, "from": "traces",
            "range": { "from": 0, "to": WINDOW_END },
            "result": "graph", "pipeline": []
        });
        for (k, val) in extra.as_object().into_iter().flatten() {
            v[k] = val.clone();
        }
        serde_json::from_value(v).unwrap()
    }

    async fn run_with(extra: serde_json::Value, max_nodes: usize) -> ServiceGraph {
        let ctx = ctx_with(&fixture());
        let limits = GraphLimits {
            correlate_max_rows: 1_000,
            max_nodes,
        };
        build_graph(&ctx, &graph_doc(extra), "t", "d", 0, limits)
            .await
            .unwrap()
            .expect("traces table is registered")
            .0
    }

    async fn run(extra: serde_json::Value) -> ServiceGraph {
        run_with(extra, 200).await
    }

    fn node<'a>(g: &'a ServiceGraph, name: &str) -> &'a GraphNode {
        g.nodes
            .iter()
            .find(|n| n.name == name)
            .unwrap_or_else(|| panic!("no node {name} in {:?}", g.nodes))
    }

    fn edge<'a>(g: &'a ServiceGraph, source: &str, target: &str) -> &'a GraphEdge {
        g.edges
            .iter()
            .find(|e| e.source == source && e.target == target)
            .unwrap_or_else(|| panic!("no edge {source} -> {target} in {:?}", g.edges))
    }

    fn names(g: &ServiceGraph) -> Vec<&str> {
        let mut n: Vec<&str> = g.nodes.iter().map(|n| n.name.as_str()).collect();
        n.sort_unstable();
        n
    }

    #[tokio::test]
    async fn service_edge_carries_count_rate_error_rate_and_p95() {
        let g = run(serde_json::json!({})).await;
        let e = edge(&g, "frontend", "checkout");
        assert_eq!(e.count, 4);
        assert!((e.rate - 0.4).abs() < 1e-9, "4 calls over 10s: {}", e.rate);
        assert!((e.error_rate - 0.25).abs() < 1e-9, "{}", e.error_rate);
        assert_eq!(e.p95_ns, Some(20_000_000));
    }

    #[tokio::test]
    async fn service_nodes_carry_server_span_metrics() {
        let g = run(serde_json::json!({})).await;
        let checkout = node(&g, "checkout");
        assert_eq!(checkout.kind, GraphNodeKind::Service);
        assert!((checkout.request_rate.unwrap() - 0.4).abs() < 1e-9);
        assert!((checkout.error_rate.unwrap() - 0.25).abs() < 1e-9);
        assert_eq!(checkout.p95_ns, Some(20_000_000));
        assert_eq!(node(&g, "frontend").error_rate, Some(0.0));
    }

    #[tokio::test]
    async fn uninstrumented_database_is_an_external_node() {
        let g = run(serde_json::json!({})).await;
        let db = node(&g, "checkout-db");
        assert_eq!(db.kind, GraphNodeKind::External);
        assert_eq!(db.dependency_kind.as_deref(), Some("database"));
        assert_eq!(edge(&g, "checkout", "checkout-db").count, 4);
    }

    #[tokio::test]
    async fn nested_client_span_is_not_an_instrumented_callee() {
        let g = run(serde_json::json!({})).await;
        assert_eq!(node(&g, "orders-db").kind, GraphNodeKind::External);
        edge(&g, "orders", "orders-db");
    }

    #[tokio::test]
    async fn instrumented_http_callee_yields_no_external_node() {
        let g = run(serde_json::json!({})).await;
        assert!(
            g.nodes.iter().all(|n| n.name != "checkout:8080"),
            "{:?}",
            g.nodes
        );
        assert_eq!(
            names(&g),
            ["checkout", "checkout-db", "frontend", "orders", "orders-db"]
        );
        assert_eq!(g.dropped_nodes, 0);
    }

    #[tokio::test]
    async fn one_hop_focus_keeps_only_edges_touching_the_focus() {
        let g = run(serde_json::json!({ "focus": "frontend" })).await;
        assert_eq!(names(&g), ["checkout", "frontend"]);
        assert_eq!(g.edges.len(), 1);

        let g = run(serde_json::json!({ "focus": "checkout", "depth": 1 })).await;
        assert_eq!(names(&g), ["checkout", "checkout-db", "frontend"]);
        assert_eq!(g.edges.len(), 2);
    }

    #[tokio::test]
    async fn depth_two_focus_walks_two_hops() {
        let g = run(serde_json::json!({ "focus": "frontend", "depth": 2 })).await;
        assert_eq!(names(&g), ["checkout", "checkout-db", "frontend"]);
        assert_eq!(g.edges.len(), 2);
    }

    #[tokio::test]
    async fn unknown_focus_is_an_empty_graph() {
        let g = run(serde_json::json!({ "focus": "nope" })).await;
        assert!(g.nodes.is_empty() && g.edges.is_empty(), "{g:?}");
    }

    #[tokio::test]
    async fn trace_scope_keeps_only_that_trace() {
        let g = run(serde_json::json!({ "trace_id": "t5" })).await;
        assert_eq!(names(&g), ["orders", "orders-db"]);
        assert_eq!(edge(&g, "orders", "orders-db").count, 2);
    }

    #[tokio::test]
    async fn where_stages_filter_the_span_set() {
        let g = run(serde_json::json!({ "pipeline": [
            { "where": { "field": "service.name", "op": "eq", "value": "orders" } }
        ] }))
        .await;
        assert_eq!(names(&g), ["orders", "orders-db"]);
    }

    #[tokio::test]
    async fn node_cap_keeps_highest_traffic_and_counts_the_rest() {
        // Traffic: checkout 8, frontend 4, checkout-db 4, orders 2, orders-db 2.
        let g = run_with(serde_json::json!({}), 1).await;
        assert_eq!(names(&g), ["checkout"]);
        assert!(g.edges.is_empty());
        assert_eq!(g.dropped_nodes, 4);
    }

    #[tokio::test]
    async fn ir_service_ships_the_graph_as_one_json_cell() {
        let svc =
            super::super::ir_planner::IrService::new(ctx_with(&fixture())).with_graph_max_nodes(2);
        let params = super::super::IrQueryParams {
            document: serde_json::to_value(graph_doc(serde_json::json!({}))).unwrap(),
            now_ns: 0,
        };
        let (batches, window, truncated) = svc.query(&params, "t", "d").await.unwrap();
        assert_eq!(window.end_ns, WINDOW_END);
        assert!(!truncated);
        let cell = batches[0]
            .column_by_name(GRAPH_JSON_COLUMN)
            .unwrap()
            .as_string::<i32>()
            .value(0);
        let g: ServiceGraph = serde_json::from_str(cell).unwrap();
        assert_eq!(g.nodes.len(), 2);
        assert_eq!(g.dropped_nodes, 3);
    }

    #[tokio::test]
    async fn node_cap_always_keeps_the_focus() {
        let g = run_with(serde_json::json!({ "focus": "frontend" }), 1).await;
        assert_eq!(names(&g), ["frontend"]);
        assert_eq!(g.dropped_nodes, 1);
    }
}
