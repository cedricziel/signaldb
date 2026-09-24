## Purpose

Defines the Query IR `graph` result envelope: a service dependency graph built from trace data, with call metrics on nodes and edges, used by the UI, MCP and CLI service maps.

## ADDED Requirements

### Requirement: Graph envelope returns services and calls

A Query IR document over `traces` at IR version 8 or later SHALL accept `"result": "graph"`. The response SHALL contain a list of nodes and a list of edges. Each node SHALL carry a name, a kind of `service` or `external`, and — for `service` nodes — request rate, error rate and p95 duration of its server and consumer spans. Each edge SHALL carry a source node, a target node, call count, call rate, error rate and p95 duration of the calls it represents. Every rate SHALL be in events per second: the count divided by the query window's length in seconds. An edge from service A to service B SHALL exist when a span of B has a parent span of A in the window.

#### Scenario: Two services calling each other

- **WHEN** traces in the window hold `frontend` spans whose child spans belong to `checkout`
- **THEN** the graph holds `service` nodes `frontend` and `checkout` and an edge `frontend → checkout` whose call count equals the number of such child spans and whose call rate is that count divided by the window's length in seconds

#### Scenario: Error rate on an edge

- **WHEN** 3 of 100 `checkout` spans with a `frontend` parent have error status
- **THEN** the `frontend → checkout` edge reports an error rate of 3%

### Requirement: Uninstrumented dependencies appear as external nodes

A client or producer span with no `server` or `consumer` child span in the window SHALL produce an edge to an `external` node. Other child spans (for example a nested client span) SHALL NOT count as an instrumented callee. A call still in progress at the window's end, whose callee span starts after the window, is reported as external; this limitation SHALL be stated in the user documentation. The node SHALL be named from the first present OpenTelemetry attribute of: `db.namespace`, `messaging.destination.name`, `rpc.service`, `server.address`, `peer.service`; and SHALL carry the dependency kind (`database`, `messaging`, `rpc`, `http`, `other`) from `db.system.name`, `messaging.system`, `rpc.system` or `http.request.method`. When a client span does have an instrumented child, the edge SHALL point at the child's service and no external node SHALL be created for it.

#### Scenario: Database call

- **WHEN** `orders` emits client spans with `db.system.name=postgresql` and `db.namespace=orders-db` and no child spans
- **THEN** the graph holds an `external` node `orders-db` of kind `database` and an edge `orders → orders-db`

#### Scenario: Nested client span is not a callee

- **WHEN** an `orders` client span for `db.namespace=orders-db` has only a child client span of its own and no `server` or `consumer` child
- **THEN** the graph still holds the `external` node `orders-db` and the edge `orders → orders-db`

#### Scenario: Instrumented HTTP callee

- **WHEN** `orders` emits a client span with `server.address=inventory:8080` whose child `server` span belongs to service `inventory`
- **THEN** the edge is `orders → inventory` and no external node `inventory:8080` exists

### Requirement: Graph scoping

For `"result": "graph"`, the Query IR document SHALL accept optional top-level scoping fields `focus`, `depth` and `trace_id`, siblings of `result`: a `focus` service with a `depth` of 1 to 3 hops (default 1), returning only nodes within that many edges of the focus in either direction; or a single `trace_id`, returning only the services and calls in that trace. Without scoping the graph covers every service in the window. Scoping by a service that has no spans in the window SHALL return an empty graph, not an error.

#### Scenario: One-hop neighbourhood

- **WHEN** a client asks for the graph with `focus=orders` and `depth=1`
- **THEN** the result holds `orders`, every node with an edge into `orders`, every node `orders` has an edge to, and only the edges touching `orders`

#### Scenario: Single trace

- **WHEN** a client asks for the graph with a `trace_id`
- **THEN** the result holds only the services with spans in that trace, and edge counts are call counts within that trace

### Requirement: Graph size bound

The graph SHALL be capped at a server-side maximum number of nodes. When the cap is hit, the result SHALL always keep the `focus` node if one was given, then keep the remaining nodes with the highest traffic, where a node's traffic is the sum of call counts on all edges touching it (defined for both `service` and `external` nodes). It SHALL report how many nodes were dropped in a warning and SHALL NOT fail.

#### Scenario: Focus node survives the cap

- **WHEN** a client asks for the graph with `focus` set to a low-traffic service whose neighbourhood exceeds the cap
- **THEN** the result still holds the focus node, plus the highest-traffic neighbours up to the cap

#### Scenario: Too many services

- **WHEN** the window holds more services than the node cap
- **THEN** the result holds the nodes with the highest traffic up to the cap, only edges between kept nodes, and a warning naming the dropped node count

### Requirement: Graph respects tenant and dataset

The graph SHALL be built only from the caller's tenant and dataset, as every other Query IR result is.

#### Scenario: Other tenant's services are absent

- **WHEN** tenant A requests a graph and tenant B has services with the same names
- **THEN** tenant A's graph metrics reflect only tenant A's spans
