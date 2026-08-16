---
audience: user
type: how-to
status: living
sources:
  - src/acceptor/src/lib.rs
  - src/acceptor/src/cli.rs
  - src/acceptor/src/middleware/grpc_auth.rs
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

### 2. Attach the auth metadata

Each request — gRPC metadata or HTTP headers alike — must carry these keys
(see [Authentication](authentication.md) for details):

| Metadata key / header | Required | Value                                               |
| --------------------- | -------- | --------------------------------------------------- |
| `authorization`       | yes      | `Bearer <api-key>`                                  |
| `x-tenant-id`         | yes      | your tenant ID                                      |
| `x-dataset-id`        | no       | dataset within the tenant; omitted → tenant default |

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
successful export response means the data is durable.

## Per-signal support

| Signal   | OTLP/gRPC :4317 | OTLP/HTTP :4318                      | Stored as                                                  |
| -------- | --------------- | ------------------------------------ | ---------------------------------------------------------- |
| Traces   | yes             | yes (`POST /v1/traces`)              | `traces` table                                             |
| Logs     | yes             | yes (`POST /v1/logs`)                | `logs` table                                               |
| Metrics  | yes             | yes (`POST /v1/metrics`)             | `metrics_gauge`, `metrics_sum`, `metrics_histogram` tables |
| Profiles | yes             | yes (`POST /v1development/profiles`) | `profiles` table (see [profiles](profiles.md))             |

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
(`Content-Encoding: gzip`) request bodies are not supported — export
uncompressed.

A successful export returns `200 OK` with an `Export*ServiceResponse`
body in the same encoding as the request. Error responses:

| Status                  | Meaning                                                                                                                                                                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `400 Bad Request`       | Malformed payload, or malformed `Authorization` / `X-Tenant-ID` headers                                                                                                                                                                          |
| `401 Unauthorized`      | Missing `Authorization` / `X-Tenant-ID` headers, or API key wrong or revoked                                                                                                                                                                     |
| `403 Forbidden`         | Key does not belong to the tenant/dataset you named                                                                                                                                                                                              |
| `429 Too Many Requests` | Per-tenant ingest rate limit or storage quota hit; a rate-limit `429` carries `Retry-After`, `X-RateLimit-Limit`, and `X-RateLimit-Burst` computed from the tenant's actual budget state, so a client can back off precisely instead of guessing |

To use OTLP/HTTP from the OpenTelemetry Collector:

```yaml
exporters:
  otlphttp/signaldb:
    endpoint: http://signaldb:4318
    headers:
      authorization: "Bearer sk-acme-prod-key-123"
      x-tenant-id: "acme"
    compression: none # gzip request bodies are not supported

service:
  pipelines:
    traces:
      exporters: [otlphttp/signaldb]
    logs:
      exporters: [otlphttp/signaldb]
    metrics:
      exporters: [otlphttp/signaldb]
```

## Troubleshooting

| Symptom                                                     | Cause                                                                 | Fix                                                                                                              |
| ----------------------------------------------------------- | --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `UNAUTHENTICATED: Missing authorization metadata`           | No `authorization` metadata on the request                            | Add `authorization: Bearer <key>` to exporter headers                                                            |
| `UNAUTHENTICATED: Missing x-tenant-id metadata`             | No tenant header                                                      | Add `x-tenant-id`                                                                                                |
| `UNAUTHENTICATED`                                           | API key is wrong or revoked                                           | Check the key with your operator, see [Authentication](authentication.md)                                        |
| `PERMISSION_DENIED`                                         | Key does not belong to the tenant/dataset you named                   | Use a key issued for that tenant                                                                                 |
| `RESOURCE_EXHAUSTED`                                        | Per-tenant ingest rate limit hit                                      | Back off and retry; ask your operator about tenant limits                                                        |
| `RESOURCE_EXHAUSTED` mentioning `quota_exceeded`            | Tenant is at or over its storage quota (`max_storage_bytes`)          | Retrying will not help until data is deleted, retention shortens, or the quota is raised — talk to your operator |
| `429 Too Many Requests` on an OTLP/HTTP endpoint            | HTTP analog of the two `RESOURCE_EXHAUSTED` cases above               | Back off and retry (rate limit), or talk to your operator (quota)                                                |
| `400 Bad Request` on an OTLP/HTTP endpoint with a JSON body | Payload is not valid protojson (e.g. base64 trace IDs instead of hex) | Use a protojson-compliant encoder; trace/span IDs must be hex strings                                            |
