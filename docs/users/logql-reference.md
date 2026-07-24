---
audience: user
type: reference
status: living
sources:
  - src/router/src/endpoints/logql.rs
  - src/querier/src/query/logql.rs
  - src/querier/src/query/logql_metric.rs
  - src/querier/src/query/logs.rs
  - src/logql/src/**
---

# LogQL reference

SignalDB exposes a Grafana Loki-compatible query API for the logs signal,
served by the router at `http://<router-host>:3000/loki`. Point Grafana's
**Loki** datasource at that base URL, or call the endpoints directly.

This page documents the supported LogQL surface and — importantly — the
approximations and gaps, so you know what to expect. For sending logs,
see [Sending OTLP data](sending-otlp.md); for the trace API, the
[Tempo API reference](tempo-api-reference.md).

## Authentication

Every request is authenticated and tenant-scoped, like the other query
APIs (see [Authentication](authentication.md)):

- `Authorization: Bearer <api-key>`
- `X-Tenant-ID: <tenant>`
- `X-Dataset-ID: <dataset>` (optional; the tenant's default dataset is
  used otherwise)

## Endpoints

| Endpoint | Status |
|----------|--------|
| `GET /loki/api/v1/query_range` | Range query. Returns a **streams** result for log queries, a **matrix** for metric queries |
| `GET /loki/api/v1/query` | Instant query. Returns the most recent lines over a one-hour window ending at `time` (log queries only) |
| `GET /loki/api/v1/labels` | Label names available in the window |
| `GET /loki/api/v1/label/{name}/values` | Distinct values of one label |
| `GET /loki/api/v1/series` | Series (label sets) matching a selector |
| `GET /loki/api/v1/tail` | Not implemented — live tail is tracked separately |

Common query parameters: `query` (the LogQL string), `start`/`end`
(unix nanoseconds, unix seconds, or RFC3339), `limit`, `direction`
(`forward`/`backward`), and `step` (metric queries; Go duration like
`30s` or a number of seconds).

## Labels and how they map to storage

SignalDB stores logs in columnar form, not as free-form label sets. LogQL
labels resolve as follows:

| LogQL label | Resolves to |
|-------------|-------------|
| `service_name`, `service`, `job` | the `service_name` column |
| `level`, `severity`, `detected_level` | the `severity_text` column |
| `trace_id`, `span_id` | the matching columns |
| any other label | a match inside the `log_attributes` / `resource_attributes` JSON |

Labels backed by a column are exact. Any other label is matched by the
serialized `"key":"value"` fragment inside the attribute JSON — the same
approximation the trace search API uses. It can occasionally over-match
when another attribute's text embeds the same fragment; this is a known
limitation until attributes are indexed.

Series identity (in `/series` results and un-grouped metric queries) is
the `service_name` and `level` labels.

## Log queries

A log query is a **stream selector** followed by an optional **pipeline**,
and returns log lines.

```logql
{service_name="api"}
{service_name="api", level="error"}
{namespace=~"prod-.*"}
{service_name="api"} |= "timeout"
{service_name="api"} |= "error" != "healthcheck" |~ "5\\d\\d"
{service_name="api"} | json | level="error"
```

### Stream selectors

All four matchers are supported: `=`, `!=`, `=~`, `!~`. Regex matchers on
a column (`service_name=~"api.*"`) are pushed down as `regexp_like`;
regex against an attribute label is **not** supported (attribute labels
support only `=` and `!=`).

### Line filters

`|=`, `!=`, `|~`, `!~` filter the log `body` (`|~`/`!~` via `regexp_like`).
The `ip("...")` filter form is **not** supported yet.

### Pipeline (parser) stages

Parser and formatter stages parse — `json`, `logfmt` (with flags),
`regexp`, `pattern`, `unpack`, `decolorize`, `line_format`,
`label_format`, `drop`, `keep`, `distinct` — but do **not** themselves
filter rows: they are accepted and pass through. A **label filter** after
a stage (`| level="error"`, `| status="500"`) does filter, resolving
labels via the mapping above.

Label filters support `=`, `!=`, `=~`, `!~` against columns and `=`, `!=`
against attributes, combined with `and`/`or`/comma. Ordered comparisons
(`>`, `>=`, `<`, `<=`, e.g. `| duration > 1s`) are **not** supported yet.

## Metric queries

Metric queries aggregate log streams into a numeric matrix and are
evaluated through `query_range`. Supported forms:

```logql
count_over_time({service_name="api"}[5m])
rate({service_name="api"} |= "error" [5m])
bytes_over_time({service_name="api"}[5m])
bytes_rate({service_name="api"}[5m])
sum_over_time({service_name="api"} | unwrap duration_ms [5m])
avg_over_time({service_name="api"} | unwrap duration_ms [5m])
sum by (level) (rate({service_name="api"}[5m]))
```

- **Range functions**: `count_over_time`, `rate`, `bytes_over_time`,
  `bytes_rate`, and the unwrap-based `sum_over_time` / `avg_over_time` /
  `min_over_time` / `max_over_time`.
- **Vector aggregation**: a single outer `sum by (...)` /
  `sum without ()` wrapping one of the above.
- **Grouping** is only supported by labels backed by a column
  (`service_name`, `level`, ...).

### Time bucketing — the key approximation

Loki evaluates a range aggregation over a *sliding* `[range]` window at
each step. SignalDB instead buckets into **fixed, step-aligned windows**
with `date_bin(step, timestamp)`. This is **exact when `step` equals the
range** (Grafana's default for `count_over_time` panels) and an
approximation otherwise. Set the panel step equal to the range window for
exact results.

### Not supported yet

These parse but return an "unsupported" error at execution:

- `topk` / `bottomk`, `quantile_over_time`, and non-`sum` outer
  aggregations (`avg`/`min`/`max`/`count` over a range aggregation)
- Binary operations between metric queries (`a / b`), `vector(N)`,
  `label_replace`
- `sum without (labels)` with a non-empty label list
- Grouping by an attribute (non-column) label

## Examples

Range log query for one service, newest first:

```bash
curl -G 'http://localhost:3000/loki/api/v1/query_range' \
  -H 'Authorization: Bearer sk-...' -H 'X-Tenant-ID: acme' \
  --data-urlencode 'query={service_name="api"} |= "error"' \
  --data-urlencode 'start=1700000000' --data-urlencode 'end=1700003600' \
  --data-urlencode 'limit=100'
```

Error rate per level over five-minute buckets:

```bash
curl -G 'http://localhost:3000/loki/api/v1/query_range' \
  -H 'Authorization: Bearer sk-...' -H 'X-Tenant-ID: acme' \
  --data-urlencode 'query=sum by (level) (rate({service_name="api"}[5m]))' \
  --data-urlencode 'start=1700000000' --data-urlencode 'end=1700003600' \
  --data-urlencode 'step=5m'
```

## Related

- [Grafana datasource](grafana-datasource.md) — connecting Grafana
- [Sending OTLP data](sending-otlp.md) — ingesting logs
- [Authentication](authentication.md) — API keys and tenant headers
