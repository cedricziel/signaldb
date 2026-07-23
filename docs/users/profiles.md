---
audience: user
type: how-to
status: living
sources:
  - src/acceptor/src/handler/otlp_profiles_handler.rs
  - src/router/src/endpoints/pyroscope.rs
  - src/common/src/flight/conversion/conversion_profiles.rs
  - src/querier/src/query/profile.rs
---

# Profiles

SignalDB stores OpenTelemetry continuous profiles as a fourth signal type
alongside traces, logs, and metrics, and serves them back through a
Pyroscope-compatible query API that Grafana renders natively.

## Sending profiles

Profiles are ingested over OTLP using the `v1development` profiles signal.
Both acceptor ports accept them:

- **gRPC** on `:4317` — the standard `ProfilesService/Export` RPC
- **HTTP** on `:4318` — `POST /v1development/profiles` with an
  `application/x-protobuf` or `application/json` body

Authentication works exactly like the other signals: `Authorization:
Bearer <api-key>` plus `X-Tenant-ID` (and optionally `X-Dataset-ID`). See
[authentication](authentication.md).

An OpenTelemetry Collector with the OTLP exporter forwards profiles
without extra configuration, e.g. from the `ebpf` profiler receiver:

```yaml
exporters:
  otlp:
    endpoint: signaldb:4317
    headers:
      authorization: Bearer sk-my-key
      x-tenant-id: acme
service:
  pipelines:
    profiles:
      receivers: [profiling]
      exporters: [otlp]
```

An export is acknowledged only after it is durably written to the
profiles write-ahead log; a rejected export is safe to retry.

## How profiles are stored

The OTLP profiles wire format shares one dictionary (strings, functions,
locations, stacks, links) per request. SignalDB resolves that dictionary
at ingest so every stored row is self-contained: stack traces arrive as
readable function names, and each profile row carries its service name,
sample type/unit, and — when the profiler linked a span — hex-encoded
`trace_id`/`span_id` columns that join directly against the traces table.

Profiles land in the tenant- and dataset-scoped `profiles` Iceberg table,
hour-partitioned like every other signal. Table creation is automatic for
new tenants and can be disabled per deployment:

```toml
[schema.default_schemas]
profiles_enabled = false
```

## Querying profiles

### Pyroscope API (Grafana-compatible)

The router serves a Pyroscope-compatible surface under `/pyroscope`:

| Endpoint | Purpose |
|----------|---------|
| `GET /pyroscope/render` | Flamegraph for a query and time range |
| `GET /pyroscope/render-diff` | Differential flamegraph between two ranges |
| `GET /pyroscope/profile-types` | Available profile types |
| `GET /pyroscope/label-names` | Label discovery |
| `GET /pyroscope/label-values?label=…` | Values for one label |

Queries use Pyroscope selector syntax; time bounds accept unix seconds,
unix milliseconds, or `now-1h` style expressions:

```
GET /pyroscope/render?query=cpu{service_name="checkout"}&from=now-1h&until=now
```

The response is a flamebearer document that Grafana's flamegraph panel
(and the bundled SignalDB datasource plugin) renders directly.

### SQL

The profiles table is queryable with SQL through the querier's Flight
interface, either fully qualified (`SELECT … FROM acme.prod.profiles`) or
through the tenant-pinned `sql_profiles` ticket where a bare `profiles`
resolves inside your tenant. Useful starting points:

```sql
-- Which services profile the most CPU?
SELECT service_name, count(*) AS profiles, sum(duration_nano) AS total_ns
FROM profiles GROUP BY 1 ORDER BY 3 DESC;

-- What profile types exist?
SELECT DISTINCT sample_type, sample_unit FROM profiles;
```

See [querying with SQL](querying-sql.md) for the general SQL surface.

### Trace correlation

When profiles carry span links, both directions are connected:

- `GET /api/profiles/trace/{trace_id}` lists summaries of the profiles
  linked to a trace.
- The Tempo trace endpoint accepts `include_profiles=true` and attaches
  the same summaries to the trace response.

## Grafana

The SignalDB datasource plugin has a **Profiles** signal type: enter a
Pyroscope-style selector (`cpu{service_name="my-service"}`) and the
backend returns a ready-to-render flamegraph frame. Tenant and dataset
must be set in the datasource configuration. See
[the Grafana datasource guide](grafana-datasource.md).
