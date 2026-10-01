## MODIFIED Requirements

### Requirement: Every discovery surface sees the same names and values

The MCP `discover_attributes` tool with `signal: "traces"` and the CLI `discover attributes` command SHALL both answer through the Query IR `describe` stage, so they list the same names and values as each other and as `POST /api/v1/query`. The Tempo tag endpoints are a separate compatibility surface for external clients such as Grafana's Tempo datasource (as is the UI's traces attribute-key suggestion source); they remain unchanged and MAY list different keys than the `describe` surface.

#### Scenario: MCP and CLI agree with the API

- **WHEN** a tenant's spans carry `http.route` in the window
- **THEN** `discover_attributes(signal="traces")` and `signaldb-cli discover attributes --signal traces` both list `http.route` from the `describe: fields` result, and `discover_attributes(signal="traces", tag="http.route")` returns its values from `describe: values` (from statistics, or a bounded read with `sample: true`)

#### Scenario: UI suggests observed keys

- **WHEN** a user types an attribute key on the traces tab (the "group by attribute" input)
- **THEN** the suggestions include the attribute keys observed in the current window, merged with registry hits as on the logs tab
