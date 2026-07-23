---
audience: user
type: how-to
status: living
sources:
  - src/acceptor/src/lib.rs
  - src/acceptor/src/main.rs
  - src/acceptor/src/middleware/grpc_auth.rs
---

# Send OTLP data to SignalDB

Goal: point an OpenTelemetry SDK or Collector at SignalDB so traces, logs,
and metrics are ingested.

SignalDB accepts OTLP over **gRPC on port 4317** for all three signals.
Use gRPC. The OTLP/HTTP port (4318) does not ingest OTLP today — see
[the HTTP limitation](#otlphttp-is-not-implemented) below. Port 4318 does
serve [Prometheus remote_write](prometheus-remote-write.md).

## Prerequisites

- A running SignalDB acceptor (standalone `signaldb-acceptor` or the
  monolithic `signaldb` binary). Default ports: gRPC 4317, HTTP 4318.
- An API key and tenant ID. See [Authentication](authentication.md) for how
  these are provisioned and what the headers mean.

## Steps

### 1. Choose the endpoint

Every OTLP export goes to the gRPC endpoint:

```text
http://<acceptor-host>:4317
```

### 2. Attach the auth metadata

Each gRPC request must carry these metadata keys (see
[Authentication](authentication.md) for details):

| Metadata key | Required | Value |
|---|---|---|
| `authorization` | yes | `Bearer <api-key>` |
| `x-tenant-id` | yes | your tenant ID |
| `x-dataset-id` | no | dataset within the tenant; omitted → tenant default |

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
signaldb-cli query sql "SELECT trace_id, span_name, service_name FROM traces LIMIT 5" \
  --api-key sk-acme-prod-key-123 --tenant-id acme
```

The acceptor writes to its WAL before acknowledging an export, so a
successful export response means the data is durable.

## Per-signal support

| Signal | OTLP/gRPC :4317 | Stored as |
|---|---|---|
| Traces | yes | `traces` table |
| Logs | yes | `logs` table |
| Metrics | yes | `metrics_gauge`, `metrics_sum`, `metrics_histogram` tables |
| Profiles | yes | `profiles` table (see [profiles](profiles.md)) |

## OTLP/HTTP support is partial

The HTTP server on port 4318 exposes `POST /v1development/profiles` for
the profiles signal (protobuf and JSON bodies, authenticated) — see
[profiles](profiles.md). For the other signals it exposes only
`POST /v1/traces`, and that handler is a stub: it logs the payload size
and returns `200 OK` without writing anything. There are no `/v1/logs` or
`/v1/metrics` routes. Do not point an `http/protobuf` or `http/json` OTLP
exporter at SignalDB for traces, logs, or metrics — the export will
appear to succeed but no data is stored.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `INVALID_ARGUMENT: Missing authorization metadata` | No `authorization` metadata on the request | Add `authorization: Bearer <key>` to exporter headers |
| `INVALID_ARGUMENT: Missing x-tenant-id metadata` | No tenant header | Add `x-tenant-id` |
| `UNAUTHENTICATED` | API key is wrong or revoked | Check the key with your operator, see [Authentication](authentication.md) |
| `PERMISSION_DENIED` | Key does not belong to the tenant/dataset you named | Use a key issued for that tenant |
| `RESOURCE_EXHAUSTED` | Per-tenant ingest rate limit hit | Back off and retry; ask your operator about tenant limits |
| `RESOURCE_EXHAUSTED` mentioning `quota_exceeded` | Tenant is at or over its storage quota (`max_storage_bytes`) | Retrying will not help until data is deleted, retention shortens, or the quota is raised — talk to your operator |
| Exports return 200 but no data is queryable | Exporter uses OTLP/HTTP on :4318 | Switch the exporter to gRPC on :4317 |
