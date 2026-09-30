---
audience: user
type: how-to
status: living
sources:
  - src/acceptor/src/lib.rs
  - src/acceptor/src/cli.rs
  - src/acceptor/src/middleware/grpc_auth.rs
  - src/router/src/endpoints/session.rs
  - src/common/src/config/mod.rs
  - src/common/src/flight/conversion/conversion_common.rs
  - src/common/src/attrs/typed.rs
  - src/writer/src/storage/iceberg.rs
  - src/acceptor/src/type_warning.rs
---

# Send OTLP data to SignalDB

Goal: point an OpenTelemetry SDK or Collector at SignalDB so traces, logs,
and metrics are ingested.

SignalDB accepts OTLP over **gRPC on port 4317** and **HTTP on port
4318** for all three signals. The OTLP/HTTP endpoints are
`POST /v1/traces`, `POST /v1/logs`, and `POST /v1/metrics` (protobuf and
JSON bodies, authenticated) — see
[OTLP/HTTP support](#otlphttp-support) below. Port 4318 also serves
[Prometheus remote_write](prometheus-remote-write.md).

## Prerequisites

- A running SignalDB acceptor (standalone `signaldb acceptor` or the
  monolithic `signaldb` binary). Default ports: gRPC 4317, HTTP 4318.
- An API key and tenant ID. See [Authentication](authentication.md) for how
  these are provisioned and what the headers mean.

## Steps

### 1. Choose the endpoint

Both protocols support all three signals. gRPC:

```text
http://<acceptor-host>:4317
```

OTLP/HTTP:

```text
http://<acceptor-host>:4318/v1/traces
http://<acceptor-host>:4318/v1/logs
http://<acceptor-host>:4318/v1/metrics
```

`<acceptor-host>` above is a placeholder — for a real deployment, ask the
deployment itself: `GET /api/v1/connection` (any tenant API key) returns this
deployment's actual public OTLP gRPC/HTTP endpoints, ready-to-paste
`OTEL_EXPORTER_OTLP_*` env vars, and the headers below filled in for your
tenant/dataset. The same information is available through the MCP
`connection_info` tool. Operators set these endpoints in `[public]` in
`signaldb.toml`; unset, both `GET /api/v1/connection` and this doc fall back
to the localhost defaults above.

### 2. Attach the auth metadata

Each request — gRPC metadata or HTTP headers alike — must carry these keys
(see [Authentication](authentication.md) for details):

| Metadata key / header | Required | Value                                               |
| --------------------- | -------- | --------------------------------------------------- |
| `authorization`       | yes      | `Bearer <api-key>`                                  |
| `x-tenant-id`         | yes      | your tenant ID                                      |
| `x-dataset-id`        | no       | dataset within the tenant; omitted → tenant default |

OTLP ingest always authenticates with a tenant API key as shown above; the
browser Explore UI's email/password or SSO login (see [Setting up SSO / OIDC
login](../operations/oidc-sso.md)) is a separate, human-facing credential and
has no effect on this path.

### 3. Configure your exporter

OpenTelemetry Collector:

```yaml
exporters:
  otlp/signaldb:
    endpoint: signaldb:4317
    tls:
      insecure: true
    headers:
      authorization: "Bearer sk-acme-prod-key-123"
      x-tenant-id: "acme"
      # x-dataset-id: "production"   # optional

service:
  pipelines:
    traces:
      exporters: [otlp/signaldb]
    logs:
      exporters: [otlp/signaldb]
    metrics:
      exporters: [otlp/signaldb]
```

OpenTelemetry SDK via environment variables:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer sk-acme-prod-key-123,x-tenant-id=acme"
```

## Verify

Export a few spans, then query them back over SQL (see
[Querying with SQL](querying-sql.md)):

```bash
signaldb-cli query --sql "SELECT trace_id, span_name, service_name FROM traces LIMIT 5" \
  --api-key sk-acme-prod-key-123 --tenant-id acme
```

The acceptor writes to its WAL before acknowledging an export, so a
successful export response means the data is durable. Its per-tenant WAL
cache is soft-capped (`[wal].max_instances`) and warns at startup when
`RLIMIT_NOFILE` looks thin for the expected tenant count — see
[WAL Persistence](../operations/wal-persistence.md#instance-cap).

## Attribute types

Each attribute key has one canonical type per tenant, dataset, signal and
attribute level (resource, scope or record). How it is chosen (a pin, a
semantic-convention hint, or the first value SignalDB stored) is described in
[Canonical types](schema-registry.md#canonical-types). It never changes on its
own. A value sent with a different type (say `http.status_code` as the string
`"404"` once the key is an integer) is still stored exactly as sent, except that a non-finite double is stored as null. It can be
retrieved through the raw attribute bag, but it can't be filtered as a typed
value (see [Querying with the IR](querying-ir.md)).

The export still succeeds, but the response carries an OTLP
`partial_success` with nothing rejected and an `error_message` naming the
keys, their canonical type and the type that was sent. The OpenTelemetry
Collector and most SDKs log this as a warning. The acceptor learns new
canonical types within about 30 seconds, so the first exports of a new key
aren't flagged. Operators see the same condition as the
`signaldb.writer.attribute_type_mismatches` counter, and the per-key total
as `off_type_count` on `GET /api/v1/schema/attributes/{key}`.

## What is preserved

- **Attribute values keep their OTLP type.** Strings, integers (full 64-bit),
  doubles and booleans are stored typed. Bytes stay bytes, not text. Arrays and
  key-value lists are stored as sent and can be read back, but cannot be used in
  a filter. A non-finite double attribute (`NaN`, `+Inf`, `-Inf`) is stored as
  null.
- **Log `body` is an `AnyValue`.** A string body is returned as the string; a
  structured body is kept and returned as JSON.
- **Duplicate keys and key order are not preserved.** If one attribute list
  repeats a key, the last value wins, and attributes come back grouped by
  their stored type (string, integer, double, boolean, then anything else),
  not in the order you sent them. Keeping both is deferred to a typed wire
  format.
- **Exemplars keep their trace context.** Each exemplar is stored as its own
  row with `trace_id` and `span_id` as hex strings, the same encoding traces
  use, so an exemplar can be joined to its trace and to logs (the IR's
  `exemplars` source, key `trace.id`/`span.id`).
- **Summary metrics are stored as sent.** Count, sum and the precomputed
  quantiles are kept and readable (`metric.quantiles`,
  `metric.quantile_values`). SignalDB does not treat a Summary as a histogram:
  `histogram_quantile` reads only histogram and exponential-histogram points
  and returns nothing for a Summary.
- **`schema_url` is a hint.** The resource and scope `schema_url` select which
  semantic-convention registry may suggest an attribute's type (see
  [Attribute types](#attribute-types)); it is also stored. It never rewrites
  your values.

## Per-signal support

| Signal   | OTLP/gRPC :4317 | OTLP/HTTP :4318                      | Stored as                                      |
| -------- | --------------- | ------------------------------------ | ---------------------------------------------- |
| Traces   | yes             | yes (`POST /v1/traces`)              | `traces` table                                 |
| Logs     | yes             | yes (`POST /v1/logs`)                | `logs` table                                   |
| Metrics  | yes             | yes (`POST /v1/metrics`)             | `metrics`, `metric_exemplars` tables           |
| Profiles | yes             | yes (`POST /v1development/profiles`) | `profiles` table (see [profiles](profiles.md)) |

## Trace continuity into SignalDB

When the operator has SignalDB's self-monitoring enabled, every ingest
request is itself traced: the acceptor roots each call in an OpenTelemetry
semconv SERVER span (`POST /v1/traces` on HTTP, the fully-qualified gRPC
method on :4317) that **joins your trace** when your exporter propagates W3C
`traceparent`/`tracestate` (gRPC metadata or HTTP headers). Nothing is
required on your side beyond standard context propagation — most OTLP
exporters send `traceparent` automatically when the export happens inside an
active span.

## OTLP/HTTP support

The HTTP server on port 4318 ingests **traces** at `POST /v1/traces`,
**logs** at `POST /v1/logs`, **metrics** at `POST /v1/metrics`, and
**profiles** at `POST /v1development/profiles` (see
[profiles](profiles.md)). All accept `application/x-protobuf` and
`application/json` (protojson encoding: trace and span IDs are hex
strings) request bodies, require the same auth headers as gRPC, and
enforce per-tenant rate limits and storage quotas. Compressed
(`Content-Encoding: gzip` or `zstd`) request bodies are accepted and
transparently decompressed; rate limiting and storage-quota accounting are
based on the decompressed size. Every route enforces a maximum decoded
body size (`[acceptor].max_request_body_bytes` in `signaldb.toml`,
default 64MB, applied after decompression) — a request over the limit
gets `413 Payload Too Large`.

Retrying an export is safe. If your exporter times out waiting for a response
that SignalDB had in fact already accepted, the identical resend is
acknowledged without being stored a second time, even when it reaches a
different acceptor replica or arrives after an acceptor restart, as long as it
arrives within `[writer].ingest_dedup_window` (default 1 hour). This matters
most for the OpenTelemetry Collector, whose `otlp` exporter times out after 5
seconds by default and then retries: without it, every slow export showed up as
every span, log record and data point twice. Only a byte-identical batch counts as
a resend; an export that differs in any record is always stored.

A successful export returns `200 OK` with an `Export*ServiceResponse`
body in the same encoding as the request. Error responses:

| Status | Meaning |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| `400 Bad Request` | Malformed payload, or malformed `Authorization` / `X-Tenant-ID` / `X-Dataset-ID` headers (including a non-UTF-8 `X-Dataset-ID` — it is rejected, not silently treated as absent) |
| `401 Unauthorized` | Missing `Authorization` / `X-Tenant-ID` headers, or API key wrong or revoked |
| `403 Forbidden` | Key does not belong to the tenant/dataset you named |
| `413 Payload Too Large` | Decoded request body exceeds `[acceptor].max_request_body_bytes` |
| Export succeeds with a `partial_success` warning naming attribute keys | Those values were sent with a type other than the key's canonical type | Send the key with its canonical type, or ask your operator to pin a different type; the values are stored as sent either way |
| `429 Too Many Requests` | Per-tenant ingest rate limit or storage quota hit; a rate-limit `429` carries `Retry-After`, `X-RateLimit-Limit`, and `X-RateLimit-Burst` computed from the tenant's actual budget state, so a client can back off precisely instead of guessing |

To use OTLP/HTTP from the OpenTelemetry Collector:

```yaml
exporters:
  otlphttp/signaldb:
    endpoint: http://signaldb:4318
    headers:
      authorization: "Bearer sk-acme-prod-key-123"
      x-tenant-id: "acme"
    compression: gzip # the default; SignalDB decompresses gzip and zstd

service:
  pipelines:
    traces:
      exporters: [otlphttp/signaldb]
    logs:
      exporters: [otlphttp/signaldb]
    metrics:
      exporters: [otlphttp/signaldb]
```

## Browser (CORS) ingestion

The OTLP/HTTP endpoints can be called directly from client-side JavaScript
(e.g. a browser-based RUM/telemetry SDK exporting straight to SignalDB, no
collector in between). Cross-origin requests are always allowed through
preflight (`OPTIONS`) — a preflight carries no `Authorization` header, so
the acceptor can't know which key will be used and grants no authority at
that stage. The real check happens on the actual request, once the API key
is resolved:

- A key with no origin restriction configured behaves exactly as today —
  any origin may use it from a browser.
- A key restricted to a set of origins (see
  [Authentication](authentication.md#origin-restriction-browsercors-ingestion))
  only succeeds from an `Origin` in that set; a mismatched origin gets
  `403 Forbidden` and no `Access-Control-Allow-Origin` header, so the
  browser reports it as a CORS failure.
- Non-browser requests (no `Origin` header — SDKs, Collectors,
  server-to-server calls) are entirely unaffected by this restriction
  either way.

Configure the restriction on the key itself; there is no separate
acceptor-wide CORS setting for ingest. To keep keys out of browser code,
export through a collector instead: see [Instrument a browser
app](instrument-browser-app.md).

## Troubleshooting

| Symptom                                                     | Cause                                                                 | Fix                                                                                                                                                                       |
| ----------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `UNAUTHENTICATED: Missing authorization metadata`           | No `authorization` metadata on the request                            | Add `authorization: Bearer <key>` to exporter headers                                                                                                                     |
| `UNAUTHENTICATED: Missing x-tenant-id metadata`             | No tenant header                                                      | Add `x-tenant-id`                                                                                                                                                         |
| `UNAUTHENTICATED`                                           | API key is wrong or revoked                                           | Check the key with your operator, see [Authentication](authentication.md)                                                                                                 |
| `PERMISSION_DENIED`                                         | Key does not belong to the tenant/dataset you named                   | Use a key issued for that tenant                                                                                                                                          |
| `RESOURCE_EXHAUSTED`                                        | Per-tenant ingest rate limit hit                                      | Back off and retry; ask your operator about tenant limits                                                                                                                 |
| `RESOURCE_EXHAUSTED` mentioning `quota_exceeded`            | Tenant is at or over its storage quota (`max_storage_bytes`)          | Retrying will not help until data is deleted, retention shortens, or the quota is raised — talk to your operator                                                          |
| `429 Too Many Requests` on an OTLP/HTTP endpoint            | HTTP analog of the two `RESOURCE_EXHAUSTED` cases above               | Back off and retry (rate limit), or talk to your operator (quota)                                                                                                         |
| `400 Bad Request` on an OTLP/HTTP endpoint with a JSON body | Payload is not valid protojson (e.g. base64 trace IDs instead of hex) | Use a protojson-compliant encoder; trace/span IDs must be hex strings                                                                                                     |
| `413 Payload Too Large`                                     | Decoded body exceeds `[acceptor].max_request_body_bytes`              | Split the batch, or raise the limit (also raises gRPC's `max_decoding_message_size`)                                                                                      |
| CORS error in the browser console on an OTLP/HTTP request   | The key's `allowed_origins` doesn't include the page's origin         | Add the origin to the key (see [Authentication](authentication.md#origin-restriction-browsercors-ingestion)), or omit `allowed_origins` if the key should be unrestricted |
