---
audience: user
type: how-to
status: living
sources:
  - src/acceptor/src/handler/prometheus_handler.rs
  - src/acceptor/src/middleware/auth.rs
---

# Send Prometheus metrics via remote_write

Goal: configure Prometheus (or any remote_write-compatible agent) to ship
metrics into SignalDB.

The acceptor's HTTP server exposes a Prometheus remote_write endpoint:

```text
POST http://<acceptor-host>:4318/api/v1/write
```

It accepts snappy-compressed protobuf (block format, not framed) with
`Content-Type: application/x-protobuf`, remote_write protocol v1 and v2
(v2 adds native histograms and metadata). Incoming samples are converted
to OpenTelemetry metrics and stored in the same metrics tables as OTLP
metrics (`metrics_gauge`, `metrics_sum`, `metrics_histogram`).

## Prerequisites

- A running SignalDB acceptor (HTTP port 4318 by default).
- An API key and tenant ID — see [Authentication](authentication.md).

## Steps

### 1. Add a remote_write block to Prometheus

```yaml
remote_write:
  - url: http://signaldb:4318/api/v1/write
    authorization:
      type: Bearer
      credentials: sk-acme-prod-key-123
    headers:
      X-Tenant-ID: acme
      # X-Dataset-ID: production   # optional; omitted → tenant default
```

Prometheus sends the required encoding (snappy + protobuf) by default; no
encoding settings are needed.

### 2. Reload Prometheus

Reload or restart Prometheus so the new remote_write target takes effect.

## Verify

- A successful write returns **`204 No Content`**. Prometheus's
  `prometheus_remote_storage_samples_failed_total` metric should stay flat.
- Query the data back over SQL (see [Querying with SQL](querying-sql.md)):

```bash
signaldb-cli query sql "SELECT * FROM metrics_gauge LIMIT 5" \
  --api-key sk-acme-prod-key-123 --tenant-id acme
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `400 Missing Authorization header` / `Missing X-Tenant-ID header` | Auth headers not configured | Add the `authorization` and `headers` blocks shown above |
| `401` | API key invalid or revoked | Check the key — see [Authentication](authentication.md) |
| `403` | Key not valid for that tenant/dataset | Use a key issued for the tenant in `X-Tenant-ID` |
| `400` decode error | Body is not snappy-block-compressed protobuf | Use a standard remote_write client; do not gzip or send framed snappy |
| `429` | Per-tenant ingest rate limit hit | Prometheus retries automatically; ask your operator about tenant limits |
| `429` mentioning `quota_exceeded` | Tenant is at or over its storage quota (`max_storage_bytes`) | Retries will not help until data is deleted, retention shortens, or the quota is raised — talk to your operator |
