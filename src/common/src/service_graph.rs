//! The Query IR `graph` result: a service dependency graph built from trace
//! data. The querier assembles it and ships it to the router as one JSON
//! cell; the router returns it as the response's `graph` field.

use serde::{Deserialize, Serialize};

/// The single column of the one-row batch a `graph` result crosses the
/// Flight wire in: the [`ServiceGraph`] as JSON.
pub const GRAPH_JSON_COLUMN: &str = "graph_json";

/// Whether a node reported spans of its own or was inferred from a client
/// or producer span with no instrumented callee.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GraphNodeKind {
    Service,
    External,
}

/// A service or an uninstrumented dependency.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GraphNode {
    /// The service name, or for an external node the first present of
    /// `db.namespace`, `messaging.destination.name`, `rpc.service`,
    /// `server.address`, `peer.service`.
    pub name: String,
    pub kind: GraphNodeKind,
    /// External nodes only: `database`, `messaging`, `rpc`, `http` or `other`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_kind: Option<String>,
    /// Service nodes with server/consumer spans only: requests per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_rate: Option<f64>,
    /// Share (0..1) of the node's server/consumer spans with error status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_rate: Option<f64>,
    /// p95 duration of the node's server/consumer spans, in nanoseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_ns: Option<i64>,
}

/// Calls from `source` to `target` in the window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    pub count: u64,
    /// Calls per second: `count` over the window length in seconds.
    pub rate: f64,
    /// Share (0..1) of the calls with error status.
    pub error_rate: f64,
    /// p95 call duration in nanoseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_ns: Option<i64>,
}

/// The assembled graph. `dropped_nodes` counts the nodes removed by the
/// server-side node cap (`[querier].graph_max_nodes`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ServiceGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    #[serde(default)]
    pub dropped_nodes: u64,
}
