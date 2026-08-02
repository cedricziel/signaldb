## Why

SignalDB's ingest surface — the acceptor's OTLP gRPC/HTTP endpoints, the
Prometheus `remote_write` endpoint, and the durability, auth, rate-limit,
and quota behavior wrapped around them — is a load-bearing, externally
observable contract with every OpenTelemetry SDK, Collector, and Prometheus
agent that ships data to SignalDB. That contract exists only in code and
tests today; there is no spec describing it. This backfill captures the
current behavior as OpenSpec specs so future changes have a baseline to
diff against and so the ingest guarantees (durability, isolation,
retryability) are stated explicitly rather than inferred.

This is a documentation-of-existing-behavior change: no code changes, no
behavior changes. The specs describe what the acceptor already does.

## What Changes

- Add specs describing the **OTLP ingest** contract per signal:
  - traces, logs, metrics over OTLP/gRPC (`:4317`) and OTLP/HTTP (`:4318`)
  - profiles over OTLP/HTTP (`/v1development/profiles`) and gRPC (development)
- Add a spec for the **Prometheus `remote_write`** ingest path
  (`POST /api/v1/write`), which is not OTLP but rides the same acceptor,
  auth, rate-limit, quota, and durability machinery.
- Add shared specs for the cross-cutting ingest concerns each signal
  inherits:
  - **auth & tenancy**: Bearer API key + `x-tenant-id` (required) +
    `x-dataset-id` (optional), per-signal write scopes, tenant/dataset
    resolution, self-monitoring `_system` tenant handling.
  - **durability**: WAL-before-ack, Flight forward to a Storage-capable
    writer, background retry consumer, at-least-once semantics, and the
    `UNAVAILABLE`/5xx backpressure signal on write-path failure.
  - **rate limiting & quotas**: per-tenant ingest rate limits and storage
    quotas, both surfaced as `RESOURCE_EXHAUSTED`/HTTP 429.
- These specs document existing behavior; they are the **baseline**. Any
  future modification to them is **BREAKING** for the ingest wire contract.

## Capabilities

### New Capabilities

- `otlp-traces-ingestion`: Accepting OTLP trace exports over gRPC and HTTP,
  encodings (protobuf / protojson), OTel span → storage mapping (including
  span events and exceptions), and per-signal response semantics.
- `otlp-logs-ingestion`: Accepting OTLP log exports over gRPC and HTTP,
  encodings, and OTel log record → storage mapping.
- `otlp-metrics-ingestion`: Accepting OTLP metric exports over gRPC and
  HTTP, encodings, and OTel metric (gauge/sum/histogram/…) → storage
  mapping.
- `otlp-profiles-ingestion`: Accepting OTLP profile exports
  (v1development) over HTTP and gRPC, and profile → storage mapping.
- `prometheus-remote-write`: Accepting Prometheus `remote_write` protobuf
  at `POST /api/v1/write` and converting samples to the metrics store.
- `ingest-auth-tenancy`: Bearer API-key authentication, `x-tenant-id` /
  `x-dataset-id` resolution, per-signal write scopes, and self-monitoring
  tenant handling — shared by all ingest paths.
- `ingest-durability`: WAL-before-ack durability, Flight forwarding to the
  writer, the background retry consumer, at-least-once delivery, and
  write-path backpressure — shared by all ingest paths.
- `ingest-rate-limiting-quotas`: Per-tenant ingest rate limiting and
  storage-quota enforcement, surfaced as `RESOURCE_EXHAUSTED` / HTTP 429 —
  shared by all ingest paths.

### Modified Capabilities

<!-- None. openspec/specs/ is empty; this is the first spec set. -->

## Impact

- **Specs only** — no source changes. Documents behavior in:
  - `acceptor`: OTLP gRPC services + HTTP routes, Prometheus handler, WAL
    manager, retry consumer, auth/rate-limit/quota middleware and service
    guards.
  - `common`: auth/tenancy (`auth`, `ratelimit`, `storage_usage`), WAL
    (`wal`), Flight transport + OTel→Arrow conversions
    (`flight/conversion/*`), Iceberg schemas.
  - `writer`: the Storage-capable Flight `do_put` target that the acceptor
    forwards to (referenced, not specified here).
- **Wire contract**: these specs codify the OTLP + Prometheus ingest wire
  behavior. Once archived, changes to them are **BREAKING** and must be
  marked as such.
- No dependency, migration, or on-disk layout changes.
