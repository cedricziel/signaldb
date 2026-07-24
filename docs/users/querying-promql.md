---
audience: user
type: how-to
status: living
sources:
  - src/router/src/endpoints/promql.rs
  - src/prometheus-api/src/lib.rs
---

# Query metrics with PromQL

Goal: query your stored metrics with PromQL over SignalDB's
Prometheus-compatible HTTP API, so a Grafana Prometheus data source (or `curl`)
can read them back.

The endpoints are nested under `/prometheus` on the router and speak the
Prometheus `api/v1` response format. They translate a PromQL expression into a
querier plan over the `metrics_gauge` and `metrics_sum` Iceberg tables — the
same query path used for traces and logs.

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
optionally with `by (label)`; the range functions `rate` and `increase`; and
`histogram_quantile(phi, metric)` (see below). `histogram_quantile` over an
inner `rate()`, binary operators, and `topk` are not implemented yet (#335).

## Quantiles from histograms

`histogram_quantile(phi, metric)` estimates the `phi`-quantile of a histogram
metric — e.g. p95 latency:

```bash
curl -sG http://localhost:3000/prometheus/api/v1/query_range \
  -H "Authorization: Bearer $SIGNALDB_API_KEY" \
  -H "X-Tenant-ID: $SIGNALDB_TENANT" \
  --data-urlencode 'query=histogram_quantile(0.95, http_request_duration_seconds)' \
  --data-urlencode "start=$(date -d '-1 hour' +%s)" \
  --data-urlencode "end=$(date +%s)" \
  --data-urlencode 'step=60'
```

Unlike Prometheus text-format histograms (a fan of `_bucket` series keyed by
`le`), SignalDB stores each OTLP histogram whole. So the argument is the
**histogram metric name itself**, not a `sum by (le) (rate(..._bucket[5m]))`
expression. The quantile is interpolated per series from the metric's stored
buckets, assuming a uniform spread within the containing bucket — the same
estimate Prometheus's `histogram_quantile` produces.

## Instant query (vector)

`query` evaluates a single point in time (default: now), returning a vector —
the latest sample of each series:

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

Prometheus labels map onto SignalDB columns: `__name__` is the metric name,
`job` is the service name, and other labels are matched against the metric's
attributes.

## Verify

A successful response is `{"status":"success","data":{...}}`. An empty
result — a window with no matching data — returns an empty `result` array with
HTTP 200, not an error.

## Troubleshooting

- **Empty `result` when you expect data**: check that `start`/`end` bracket the
  sample timestamps (they are unix seconds, not milliseconds) and that the
  `X-Tenant-ID` matches the tenant the metrics were ingested under.
- **`resultType` is `vector` but you wanted `matrix`**: use `query_range`, not
  `query`.
- **404**: confirm the path is nested under `/prometheus` (e.g.
  `/prometheus/api/v1/query_range`).

## Configure Grafana

Point a Grafana **Prometheus** data source at
`http://<router-host>:3000/prometheus` and add the `Authorization` and
`X-Tenant-ID` headers under *Custom HTTP Headers*.
