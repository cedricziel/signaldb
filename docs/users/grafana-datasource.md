---
audience: user
type: how-to
status: living
sources:
  - src/grafana-plugin/src/plugin.json
  - src/grafana-plugin/src/types.ts
  - src/grafana-plugin/backend/src/main.rs
---

# Use SignalDB from Grafana

Goal: view SignalDB data in Grafana. There are two options:

1. **Grafana's built-in Tempo datasource** pointed at SignalDB's Tempo
   API — the practical choice today for viewing traces.
2. **The native SignalDB plugin** (`signaldb-signaldb-datasource`) — early
   stage; read [its limitations](#native-plugin-limitations) before using it.

## Prerequisites

- Grafana with network access to the SignalDB router (HTTP port 3000,
  Flight port 50053).
- An API key and tenant ID — see [Authentication](authentication.md).

## Option 1: built-in Tempo datasource

1. In Grafana, add a **Tempo** datasource.
2. Set the URL to the router's Tempo endpoint:

   ```text
   http://<router-host>:3000/tempo
   ```

3. Under HTTP headers, add:

   | Header | Value |
   |---|---|
   | `Authorization` | `Bearer <api-key>` |
   | `X-Tenant-ID` | your tenant ID |
   | `X-Dataset-ID` | (optional) dataset ID |

4. Save & test.

Trace-by-ID lookup, trace search, and tag autocomplete work within the
limits listed in the [Tempo API reference](tempo-api-reference.md) — in
particular, TraceQL metrics queries return 501.

### Logs: built-in Loki datasource

For logs, add a **Loki** datasource pointed at the router's Loki
endpoint with the same headers:

```text
http://<router-host>:3000/loki
```

Explore log queries, label/value autocomplete, and metric queries
(`rate`, `count_over_time`, `sum by (...)`) work within the limits in the
[LogQL reference](logql-reference.md) — live tail (`/tail`) is not
implemented yet, and set a panel step equal to the range window for exact
metric results.

## Option 2: native SignalDB plugin

### Build and install

From the repository root:

```bash
npm install               # install workspace dependencies
npm run grafana:build     # build the frontend
cd src/grafana-plugin && npm run build:backend   # build the Rust backend
```

Copy the resulting `src/grafana-plugin/dist/` directory into Grafana's
plugin directory as `signaldb-signaldb-datasource`, and allow the unsigned
plugin:

```ini
[plugins]
allow_loading_unsigned_plugins = signaldb-signaldb-datasource
```

### Configure

| Field | Meaning |
|---|---|
| Router URL | Flight endpoint of the router, e.g. `http://<router-host>:50053` |
| Protocol | `http` or `flight`; the backend currently only supports `flight` |
| Timeout | Query timeout in seconds |
| Tenant ID | Tenant to query |
| Dataset ID | Dataset within the tenant (optional) |
| API Key | Stored in Grafana's secure JSON store |

### Native plugin limitations

Be aware of what the backend actually does before building dashboards on
it:

- The query editor accepts free-text queries, but the backend does **not**
  parse TraceQL, PromQL, or LogQL. It dispatches fixed Flight tickets by
  signal type: `traces`, `logs`, `metrics`, or `trace_by_id?id=<text>`
  when the traces query text is non-empty.
- The router currently answers those fixed tickets with **empty result
  sets** (placeholder handlers), so panels backed by the native plugin
  return no data against a standard deployment.

For working trace views use the Tempo datasource (option 1); for ad-hoc
analysis use [SQL over Flight](querying-sql.md).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Tempo datasource "Save & test" fails with 400/401 | Auth headers missing or wrong | Set `Authorization` and `X-Tenant-ID` headers on the datasource |
| Tempo metrics/TraceQL-metrics panels show errors | Endpoints return 501 | Not implemented — see [Tempo API reference](tempo-api-reference.md) |
| Native plugin panels are always empty | Router answers the plugin's tickets with empty placeholders | Expected today; use the Tempo datasource or SQL instead |
| Plugin not loading in Grafana | Unsigned plugin blocked | Add it to `allow_loading_unsigned_plugins` |
