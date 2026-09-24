## ADDED Requirements

### Requirement: Service map tool

The MCP server SHALL expose a `get_service_map` tool taking an optional `service` and `depth`, an optional time range, and the usual optional `dataset` and `tenant` arguments. It SHALL return the Query IR `graph` result and a short text summary naming the busiest edges and the edges with the highest error rate. For clients that support MCP Apps, the tool SHALL point at a `ui://signaldb/service-map` resource that renders the graph interactively, and the result SHALL include a link to the same map in the web UI.

#### Scenario: Neighbourhood of a service

- **WHEN** a model calls `get_service_map` with `service=payments`
- **THEN** it receives the one-hop graph around `payments`, a summary naming its highest-error edge, and a web UI link to the service map

#### Scenario: Client without MCP Apps

- **WHEN** the client does not advertise MCP Apps support
- **THEN** the tool still returns the graph data and summary without the `ui://` metadata

### Requirement: Trace results summarise the services involved

`get_trace` output SHALL include a list of the services in the trace with the time spent in each and the calls between them, marking calls that failed.

#### Scenario: Failing hop in a trace

- **WHEN** a model calls `get_trace` for a trace in which `api-gateway → payments` failed
- **THEN** the services summary lists that call as failed
