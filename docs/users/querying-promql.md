---
audience: user
type: how-to
status: living
sources:
  - src/router/src/endpoints/promql.rs
  - src/ql-ir/src/promql_lower.rs
  - src/prometheus-api/src/lib.rs
---

# Query metrics with PromQL

Goal: query your stored metrics with PromQL over SignalDB's
Prometheus-compatible HTTP API, so a Grafana Prometheus data source (or `curl`)
can read them back.

The endpoints are nested under `/prometheus` on the router and speak the
Prometheus `api/v1` response format. `query` and `query_range` lower the PromQL
expression to a [Query IR](querying-ir.md) document over the `metrics` source
and execute it exactly as `POST /api/v1/query` would, so a PromQL expression and
its IR equivalent return the same series, labels and values. The metadata
endpoints (`labels`, `label/{name}/values`, `series`, `label_stats`) read the
metrics tables directly.

## Prerequisites

- A running SignalDB deployment (`./scripts/run-dev.sh` is enough locally); the
  router listens on port 3000.
- Metrics already ingested (via OTLP or [Prometheus
  remote_write](prometheus-remote-write.md)).
- An API key and tenant, sent as `Authorization: Bearer <key>` and
  `X-Tenant-ID: <tenant>` headers (see [Authentication](authentication.md)).

## Range query (matrix)

`query_range` returns a matrix — one time series per label set, sampled at
`step`:

```bash
curl -sG http://localhost:3000/prometheus/api/v1/query_range \
  -H "Authorization: Bearer $SIGNALDB_API_KEY" \
  -H "X-Tenant-ID: $SIGNALDB_TENANT" \
  --data-urlencode 'query=sum(rate(http_requests_total[5m]))' \
  --data-urlencode "start=$(date -d '-1 hour' +%s)" \
  --data-urlencode "end=$(date +%s)" \
  --data-urlencode 'step=60'
```

`start`/`end` are unix seconds; `step` is a duration (`60`, `1m`, `1h`).

Supported so far: instant/range selectors with label matchers
(`=`, `!=`, `=~`, `!~`); the aggregations `sum`, `avg`, `min`, `max`, `count`,
optionally with `by (label)`; the range functions `rate`, `increase`, and the
`<agg>_over_time` family; `histogram_quantile(phi, metric)` (see below); the
unary math functions (`abs`, `ceil`, `floor`, `round`, `sqrt`, `clamp*`, …);
and scalar arithmetic (`metric * 8`, `1024 / metric`). For the full list of
what is and isn't supported, see the
[PromQL function support reference](promql-functions.md).

## Quantiles from histograms

`histogram_quantile(phi, …)` estimates the `phi`-quantile of a histogram
metric — e.g. p95 request latency per service:

```bash
curl -sG http://localhost:3000/prometheus/api/v1/query_range \
  -H "Authorization: Bearer $SIGNALDB_API_KEY" \
  -H "X-Tenant-ID: $SIGNALDB_TENANT" \
  --data-urlencode 'query=histogram_quantile(0.95, sum by (job) (rate(http_request_duration_seconds[5m])))' \
  --data-urlencode "start=$(date -d '-1 hour' +%s)" \
  --data-urlencode "end=$(date +%s)" \
  --data-urlencode 'step=60'
```

Unlike Prometheus text-format histograms (a fan of `_bucket` series keyed by
`le`), SignalDB stores each OTLP histogram whole, so you name the **histogram
metric itself**, not its `_bucket` series, and `le` is implicit (`sum by (le,
job)` and `sum by (job)` mean the same). The quantile is interpolated from the
stored buckets, assuming a uniform spread within the containing bucket — the
same estimate Prometheus's `histogram_quantile` produces.

Over a bare selector or an un-summed `rate(metric[w])`, the quantile is
computed per stored series and labelled by the series' labels less
`__name__`, as in Prometheus. A selector reads each series' latest point in
the 5-minute lookback; `rate(metric[w])` differences each series against
itself over the window. Under `sum [by (…)]` the series are merged per group
after that. `offset` and `@` on the histogram operand are a `400`.

`histogram_quantile` and `histogram_fraction` read explicit-bucket and
exponential histograms (exponential buckets merge by the OTel rule). Gauge
and sum rows are ignored, as Prometheus ignores non-histogram series. A
**summary** point in the queried range is a `400` (`<function> is not
supported on summary metrics`): it carries precomputed quantiles, not
buckets. Unlike Prometheus, which drops such series with an annotation, the
whole query fails, even when histograms are selected beside the summary (for
example a nameless `sum(rate({job="api"}[5m]))`), so narrow the selector with
`__name__`.
Histograms with different explicit bounds merge over the union of their
bounds, and malformed rows are skipped.

`histogram_count` and `histogram_sum` read the stored count and sum, which
histograms, exponential histograms and summaries all carry, so they sum rows
of all three types.

## Instant query (vector)

`query` evaluates once, at `time` (default: now), returning a vector — each
series' value at that instant, read from its latest point in the 5-minute
lookback. An expression that is a scalar (`time()`, `scalar(x)`, `1 + 2`)
returns `resultType` `scalar`; over `query_range` the same expression is a
matrix of one label-less series.

```bash
curl -sG http://localhost:3000/prometheus/api/v1/query \
  -H "Authorization: Bearer $SIGNALDB_API_KEY" \
  -H "X-Tenant-ID: $SIGNALDB_TENANT" \
  --data-urlencode 'query=up' \
  --data-urlencode "time=$(date +%s)"
```

## Discover labels and series

```bash
# label names in a window
curl -sG http://localhost:3000/prometheus/api/v1/labels ...
# values of one label
curl -sG http://localhost:3000/prometheus/api/v1/label/__name__/values ...
# series ({__name__, job}) matching a selector
curl -sG http://localhost:3000/prometheus/api/v1/series \
  --data-urlencode 'match[]=http_requests_total' ...
```

Labels and metric names are also reachable without raw HTTP: `signaldb-cli
discover attributes --signal metrics [--tag NAME]` / `discover metrics`, and
the MCP `discover_attributes`(`signal: "metrics"`) / `discover_metrics` tools
for AI agents — see [the MCP server doc](mcp.md).

Prometheus labels map onto SignalDB fields: `__name__` is the metric name, and
`job`, `service` and `service_name` all address the service name. Any other
label, dotted names included, is a resource or point attribute. In query
results the metric name comes back as `__name__` (where Prometheus keeps it)
and the service name as `service_name`; every other label keeps its SignalDB
name, e.g. `otel.scope.name` or `resource.host.name`.

### Label cardinality

`/api/v1/label_stats` is a SignalDB extension (not part of the Prometheus API)
that returns per-label cardinality so a client can warn before grouping by a
high-cardinality label:

```bash
curl -sG http://localhost:3000/prometheus/api/v1/label_stats ...
# { "status": "success", "data": [
#   { "name": "service", "distinct_estimate": 12, "presence": 1.0, "capped": false },
#   { "name": "k8s.pod", "distinct_estimate": 10000, "presence": 0.9, "capped": true }
# ] }
```

Each entry carries the label `name` (matching `/api/v1/labels`), a
`distinct_estimate` (a floor when `capped` is true — the analyzer stopped at
its cardinality cap), and `presence`, the fraction of scanned rows carrying
the label. The numbers come from the compactor's advisory attribute-stats
analysis, so a label appears here only after its data has been compacted at
least once; freshly ingested labels may be missing until then. The metrics
explorer in the [Explore UI](explore-ui.md) uses this to flag risky group-by
choices.

## Verify

A successful response is `{"status":"success","data":{...}}`. An empty
result — a window with no matching data — returns an empty `result` array with
HTTP 200, not an error.

A failed query returns the Prometheus error envelope
`{"status":"error","errorType":"...","error":"..."}` with a non-2xx status; the
`error` field carries the reason (e.g. a rejected query is `400` with
`errorType` `bad_data`, as is a missing or empty `query` or a `time`,
`start`, `end` or `step` that does not parse; the step must also be
positive), so the message names the actual cause rather than
leaving you with a bare status code. A `429` (`errorType` `rate_limited`,
per-tenant query rate limit exceeded) additionally carries `retryAfterMs`
in the body and `Retry-After`/`X-RateLimit-Limit`/`X-RateLimit-Burst`
headers computed from the tenant's actual budget state.

## Troubleshooting

- **Empty `result` when you expect data**: check that `start`/`end` bracket the
  sample timestamps (they are unix seconds, not milliseconds) and that the
  `X-Tenant-ID` matches the tenant the metrics were ingested under.
- **`resultType` is `vector` but you wanted `matrix`**: use `query_range`, not
  `query`.
- **404**: confirm the path is nested under `/prometheus` (e.g.
  `/prometheus/api/v1/query_range`).
- **400 `bad_data`**: read the `error` field in the response body — it carries
  the reason. An expression that does not parse, or that parses but has no IR
  equivalent, is rejected before it runs; the message names the construct
  (e.g. `a negative offset has no query-IR equivalent`). See
  [constructs that return 400](promql-functions.md#constructs-that-return-400).

## Configure Grafana

Point a Grafana **Prometheus** data source at
`http://<router-host>:3000/prometheus` and add the `Authorization` and
`X-Tenant-ID` headers under _Custom HTTP Headers_.
