---
audience: user
type: how-to
status: living
sources:
  - src/acceptor/src/handler/otlp_profiles_handler.rs
  - src/router/src/endpoints/pyroscope.rs
  - src/common/src/flight/conversion/conversion_profiles.rs
  - src/querier/src/query/profile.rs
  - src/signaldb-cli/src/commands/profiles.rs
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
profiles write-ahead log; a rejected export is safe to retry. Accepted
requests are logged at `DEBUG` (`Handling OTLP profiles request`), like the
other signals' per-request lines, so an idle-looking `INFO` log is normal.

Per-tenant ingest rate limits and storage quotas cover profiles like
every other signal: gRPC exports over the limit fail with
`RESOURCE_EXHAUSTED` (retryable — back off), HTTP exports with `429 Too
Many Requests`, carrying `Retry-After` (whole seconds, rounded up, at
least 1), `X-RateLimit-Limit`, and `X-RateLimit-Burst` computed from the
tenant's actual token-bucket state, so a client can back off precisely
instead of guessing. An error mentioning `quota_exceeded` means the tenant
is at or over its storage quota (`max_storage_bytes`); retrying will not
help until data is deleted, retention shortens, or the quota is raised
(the storage quota has no token bucket, so its `429` carries no
`Retry-After`).

## Profiling SignalDB itself

SignalDB can continuously profile its own CPU and store the result as
regular profile signals under the self-monitoring tenant — the same
dogfooding pipeline that self-monitoring uses for traces, logs, and
metrics:

```toml
[self_monitoring]
profiles_enabled = true
# profile_sample_rate_hz = 99   # sampling frequency (default 99 Hz)
# profile_interval = "60s"      # one profile per window (default 60s)
```

Each window is exported as an OTLP profile (`cpu` / `nanoseconds`, one
sample value per stack = observed samples × sampling period) with
`service.name` set to the emitting service and
`deployment.environment = "self-monitoring"`. Query it like any other
profile data, scoped to the `_system` tenant:

```bash
curl -s "http://localhost:3000/pyroscope/render?query=cpu&from=now-15m&until=now" \
  -H "Authorization: Bearer <admin-api-key>" \
  -H "X-Tenant-ID: _system" -H "X-Dataset-ID: _monitoring"
```

Self-profiling requires `auth.admin_api_key` (the export authenticates
with it) and works even when the rest of `[self_monitoring]` is
disabled. CPU self-profiling is mutually exclusive with the
external-Pyroscope `[profiling]` section — both drive the same SIGPROF
sampler; if both are enabled, `[profiling]` wins and CPU self-profiling
is skipped with a warning.

### Heap profiles

Set `heap_profiles_enabled = true` to also export a jemalloc live-heap
profile (`inuse_space` / `bytes`) each window. This uses jemalloc rather
than the SIGPROF sampler, so it runs alongside CPU self-profiling or the
external `[profiling]` agent.

Heap profiling needs a binary **built with the `jemalloc-profiling`
feature**, and that rules out the default container images: they are
musl-based, where jemalloc's stack unwinder is ABI-mismatched with the
runtime and crashes the process the moment profiling is switched on. The
default images do CPU profiling only. Use the dedicated glibc image
instead:

```text
ghcr.io/cedricziel/signaldb:main-glibc-profiling
```

It ships the monolithic `signaldb` binary on a Debian runtime and is a
drop-in swap for the monolithic image (**amd64 only**, branch/PR tags only —
see the linked doc for the tagging caveat). See
[Binary runtime characteristics](../operations/binaries.md#heap-profiling-and-the-glibc-image)
for why the split exists.

The process must also be started with jemalloc's sampling profiler
enabled — `MALLOC_CONF=prof:true` on Linux, or the prefixed
`_RJEM_MALLOC_CONF=prof:true` on platforms where jemalloc keeps its symbol
prefix (e.g. macOS). Without both the feature and the env var, the setting
logs a warning and does nothing. Query it by profile type:

```bash
curl -s "http://localhost:3000/pyroscope/render?query=inuse_space:inuse_space:bytes&from=now-15m&until=now" \
  -H "Authorization: Bearer <admin-api-key>" \
  -H "X-Tenant-ID: _system" -H "X-Dataset-ID: _monitoring"
```

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

The router serves a Pyroscope-compatible surface under `/pyroscope` (plus
trace correlation at `/api/profiles`). Every endpoint below is part of the
OpenAPI contract (operation ids in parentheses), so it is generated into the
Rust SDK and the TypeScript client, not hand-maintained:

| Endpoint                              | Purpose                                                           | Operation id              |
| ------------------------------------- | ----------------------------------------------------------------- | ------------------------- |
| `GET /pyroscope/render`               | Flamegraph for a query and time range                             | `pyroscope_render`        |
| `GET /pyroscope/render-diff`          | Differential flamegraph between two ranges                        | `pyroscope_render_diff`   |
| `GET /pyroscope/profile-types`        | Available profile types                                           | `pyroscope_profile_types` |
| `GET /pyroscope/label-names`          | Label discovery (reads JSON-string or map-typed attribute tables) | `pyroscope_label_names`   |
| `GET /pyroscope/label-values?label=…` | Values for one label                                              | `pyroscope_label_values`  |
| `GET /api/profiles/trace/{trace_id}`  | Profiles linked to a trace                                        | `profiles_by_trace`       |

Queries use Pyroscope selector syntax; time bounds accept unix seconds,
unix milliseconds, or `now-1h` style expressions:

```
GET /pyroscope/render?query=cpu{service_name="checkout"}&from=now-1h&until=now
```

The response is a flamebearer document that Grafana's flamegraph panel
(and the bundled SignalDB datasource plugin) renders directly. A window
with no matching profiles returns an empty flamegraph with HTTP 200, not
an error.

Failures (including `GET /api/profiles/trace/{trace_id}`) return a JSON
body in the same error shape as the other query APIs, with the reason in
`error` — e.g. `{"status":"error","errorType":"bad_data","error":"missing
or empty 'label' parameter"}`. `errorType` is `bad_data` (400),
`not_found` (404), `rate_limited` (429), `timeout` (504), `unavailable`
(503, no querier), or `internal` (500). A `429` here is the router's query
budget (`max_query_requests_per_sec`), not the ingest limit above; it
carries `retryAfterMs` in the body and the same `Retry-After` /
`X-RateLimit-Limit` / `X-RateLimit-Burst` headers.

#### CLI

```bash
signaldb profiles types [--from now-1h --until now]
signaldb profiles labels [--from now-1h --until now]
signaldb profiles label-values service_name [--from now-1h --until now]
signaldb profiles render 'cpu{service_name="checkout"}' --from now-1h --until now
signaldb profiles diff 'cpu' \
  --left-from now-2h --left-until now-1h --right-from now-1h --right-until now
signaldb profiles by-trace <trace_id>
```

Each verb dispatches through `signaldb-sdk` and prints the native Pyroscope
JSON response unchanged, consistent with the other compat query surfaces.
`profiles` is a standalone group (not under `signaldb query`) because
Pyroscope has no single query-language flag — the selector and ranges are
per-verb parameters.

#### MCP

Agent sessions reach the same surface through dedicated tools:
`discover_profile_types` (profile types with data), `discover_attributes`
with `signal: "profiles"` (label names, or values with `tag`),
`search_profiles` (selector + range → the aggregated flame graph, subject to
the same payload cap and `truncated` flag as the other query tools),
`compare_profiles` (two ranges → the diff flame graph), and
`profiles_for_trace` (profiles correlated with a trace id). See
[MCP server](mcp.md).

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

Note that `samples_json` and `stacktraces_json` — the raw stack-sample
payload — are ordinary columns here: nothing restricts selecting them via
SQL. Contrast the native Query IR below, which deliberately keeps them
unaddressable as fields and instead offers a bounded, aggregated retrieval
path.

See [querying with SQL](querying-sql.md) for the general SQL surface.

### Native Query IR

The [native Query IR](querying-ir.md) reads `profiles` as one summary row per
stored profile (`profile.id`, `timestamp`, `duration`, `sample.type`,
`service.name`, and more) for `rows`/`table`/`series` results, and — via the
`flamegraph` result envelope — retrieves an actual profile payload: the same
bounded, aggregated flamegraph `/pyroscope/render` returns, not raw
`samples_json`/`stacktraces_json`. See
[Profile summaries](querying-ir.md#profile-summaries) and the
[flamegraph envelope](querying-ir.md#flamegraph-envelope-profiles-only) for
the full field/envelope reference.

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
