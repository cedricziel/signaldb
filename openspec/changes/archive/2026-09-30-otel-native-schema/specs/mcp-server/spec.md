## MODIFIED Requirements

### Requirement: Query and exploration tools

The MCP server SHALL expose read-only tools that wrap the SignalDB query API: trace search (`search_traces`), single-trace retrieval (`get_trace`), log search (`search_logs`), metric query (`query_metrics`), attribute discovery (`discover_attributes`), and metric-name discovery (`discover_metrics`). Each tool SHALL return structured JSON derived from the API response. In v1 these tools SHALL be visible to every authenticated tenant session without role-based filtering.

**Describe-backed attribute discovery.** `discover_attributes` SHALL accept an optional `signal` argument (`traces` | `logs` | `metrics` | `profiles`, default `traces`) selecting the Query IR source it describes, and SHALL answer through the IR `describe` stage (`POST /api/v1/query`), never through the Tempo, Loki, Prometheus or Pyroscope metadata endpoints. Called without a `tag` argument it SHALL return the `describe: fields` result for that source (logical dotted names with type, level, and origin); called with a `tag` it SHALL return the `describe: values` result for that field. With `signal: "traces"` an optional `scope` (`resource` | `span` | `intrinsic`) SHALL narrow the listing to fields at that attribute level (`resource`, `record`, or declared fields carrying no level) and, with a `tag`, SHALL look up the level-qualified field (`resource.<tag>`, `span.<tag>`); `scope: "intrinsic"` combined with a `tag`, and an empty or whitespace `tag`, SHALL be rejected as invalid parameters. A scope SHALL list only typed keys at that level: untyped keys (no attribute level) and scope-level attributes are never listed, `limit` applies before the scope filter so fewer rows can come back, and a qualified tag can resolve to an intrinsic such as `span.kind`. Values SHALL come from a declared set or maintained statistics; a field nothing covers SHALL return no values and a `hint`, and data SHALL be read only when the caller passes `sample: true`. It SHALL accept optional `from`, `to`, and `limit`. Results SHALL be scoped to the caller's tenant regardless of signal, and the tool SHALL carry the read-only annotation.

**Metric-name discovery.** `discover_metrics` SHALL return the distinct metric names visible to the caller's tenant as the sampled `describe: values` result for the `metric.name` field of the `metrics` source, over `from`/`to` (default the last hour) bounded by an optional `limit`; its description SHALL say it samples stored data, and it SHALL carry the read-only annotation. It SHALL accept the same optional `dataset` argument, dataset-scoping, and payload-cap rules as the other query tools.

**Dataset selection.** Each tool SHALL accept an optional `dataset` argument. When omitted, the session's default dataset (from the resolved tenant context) is used. When provided, it SHALL be forwarded as `X-Dataset-ID` and validated server-side against the caller's tenant context; a dataset the caller may not access SHALL be rejected with an access-denied error rather than silently substituting the default.

**Bounded payloads.** Each tool SHALL cap its serialized result at a fixed byte budget. When the downstream response exceeds the cap, the tool SHALL return valid structured JSON truncated at a record boundary, carrying a `truncated: true` flag and a hint to narrow the query; it SHALL NOT return an unbounded or malformed payload. Clients detect truncation via the flag.

#### Scenario: Trace search returns matching traces

- **WHEN** an authenticated session calls `search_traces` with a TraceQL query and time range
- **THEN** the tool returns the matching traces scoped to the caller's tenant as structured JSON

#### Scenario: Omitted dataset uses the session default

- **WHEN** a session calls a query tool without a `dataset` argument
- **THEN** the query is forwarded for the session's default dataset

#### Scenario: Explicit accessible dataset is forwarded

- **WHEN** a session calls a query tool with a `dataset` the caller's tenant may access
- **THEN** the query is forwarded with that dataset as `X-Dataset-ID`

#### Scenario: Inaccessible dataset is rejected

- **WHEN** a session calls a query tool with a `dataset` the caller's tenant may not access
- **THEN** the tool returns an access-denied error and forwards no query

#### Scenario: Oversized result is truncated with a flag

- **WHEN** a query tool's downstream result exceeds the payload cap
- **THEN** the tool returns valid JSON marked `truncated: true` with a narrowing hint, not an unbounded blob

#### Scenario: Get trace by id when absent

- **WHEN** a session calls `get_trace` with a trace id that does not exist for the caller's tenant
- **THEN** the tool returns a clean MCP "not found" error rather than an empty success or a transport failure

#### Scenario: Invalid query surfaces an actionable error

- **WHEN** a session calls a query tool with a malformed query expression
- **THEN** the tool returns an MCP tool error describing the problem, not a generic internal error

#### Scenario: Rate-limited call is reported as retryable

- **WHEN** a query tool call is rejected by the router's per-tenant rate limit
- **THEN** the tool returns an MCP error indicating the request was throttled and can be retried

#### Scenario: Attribute discovery defaults to traces

- **WHEN** a session calls `discover_attributes` without a `signal` argument
- **THEN** the tool sends a `describe: fields` document for the `traces` source and returns the trace field names for the caller's tenant

#### Scenario: Attribute discovery for logs

- **WHEN** a session calls `discover_attributes` with `signal: "logs"` and no `tag`
- **THEN** the tool sends a `describe: fields` document for the `logs` source and returns its fields

#### Scenario: Attribute discovery for metrics

- **WHEN** a session calls `discover_attributes` with `signal: "metrics"` and a `tag`
- **THEN** the tool sends a `describe: values` document for that field of the `metrics` source, scoped to the caller's tenant

#### Scenario: Discover metric names

- **WHEN** a session calls `discover_metrics`
- **THEN** the tool sends a sampled `describe: values` document for `metric.name` over the last hour and returns the metric names visible to the caller's tenant

#### Scenario: Scope narrows trace discovery by level

- **WHEN** a session calls `discover_attributes` with `signal: "traces"` and `scope: "resource"`
- **THEN** only fields whose level is `resource` are listed, and `scope: "intrinsic"` lists only declared fields that carry no level

#### Scenario: Scope with a tag qualifies the field

- **WHEN** a session calls `discover_attributes` with `signal: "traces"`, `scope: "span"`, and `tag: "env"`
- **THEN** the tool describes the values of `span.env`

#### Scenario: Untyped keys are not listed under a scope

- **WHEN** a session calls `discover_attributes` with `signal: "traces"` and any `scope`, and the source has a key with no attribute level
- **THEN** that key is absent from the scoped listing (it still appears in the unscoped one)

#### Scenario: Intrinsic scope with a tag is rejected

- **WHEN** a session calls `discover_attributes` with `scope: "intrinsic"` and a `tag`
- **THEN** the tool returns an invalid-parameters error and sends no request

#### Scenario: Uncovered values read data only on request

- **WHEN** a session calls `discover_attributes` with a `tag` no declared set or statistics cover, without `sample`
- **THEN** the result has no values and a `hint`, and no signal data is read; with `sample: true` the bounded read runs

#### Scenario: Tools are listed for any authenticated tenant session

- **WHEN** any authenticated tenant session issues `tools/list`
- **THEN** every advertised query/exploration tool is present in the returned list
