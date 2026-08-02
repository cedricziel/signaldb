## Context

This change documents behavior that already ships. The acceptor crate
implements OTLP ingest for traces, logs, metrics, and profiles over both
OTLP/gRPC (`:4317`) and OTLP/HTTP (`:4318`), plus Prometheus `remote_write`
at `POST /api/v1/write`, all sharing one auth, rate-limit/quota, and
WAL-durability path. No spec described this contract, so there was no
baseline to diff future changes against. These specs are that baseline.

Relevant existing constraints that shape how the behavior is expressed:

- **FDAP version alignment.** Ingested OTLP data is converted to Arrow
  `RecordBatch`es and persisted as Parquet in Iceberg tables. Conversions
  use the Arrow/Parquet types re-exported by DataFusion so Arrow, Parquet,
  and DataFusion stay version-compatible; the specs describe observable
  ingest behavior and deliberately avoid pinning column-level Arrow types.
- **Flight v1 wire vs v2 storage schema.** The acceptor forwards batches to
  a Storage-capable writer over Arrow Flight (`do_put`), carrying routing
  metadata as `app_metadata` on the schema message. The v1 wire schema and
  the v2 storage schema differ, with a write-time transform between them;
  the ingest specs stop at "durably accepted and forwarded" and leave the
  storage-schema transform to the writer/flight-schema specs.
- **Durability point.** `handle_grpc_otlp_*` writes to the WAL and flushes
  before returning `Ok`; the Flight forward is best-effort and, on failure,
  is retried by the background WAL retry consumer. This is what makes the
  ingest guarantee at-least-once rather than exactly-once.

## Goals / Non-Goals

**Goals:**

- Capture the externally observable ingest contract (endpoints, encodings,
  status codes, auth/scopes, rate-limit/quota, durability) as specs.
- Keep cross-cutting behavior (auth, durability, rate-limit/quota) in shared
  capabilities that the per-signal specs inherit, so there is one source of
  truth and no drift.
- Reflect the code exactly, including the discovered rate-limit/quota
  behavior surfaced as `429` / `RESOURCE_EXHAUSTED`.

**Non-Goals:**

- No behavior change and no code change.
- No specification of the writer's storage schema, the Iceberg table layout,
  or the query paths — those are separate capabilities.
- No column-level Arrow/Parquet mapping (implementation detail, not
  observable behavior).

## Decisions

- **Per-signal capabilities + shared cross-cutting specs.** Four OTLP signal
  specs and one Prometheus spec each inherit `ingest-auth-tenancy`,
  `ingest-durability`, and `ingest-rate-limiting-quotas`. Alternative
  considered: one monolithic `otlp-ingestion` spec — rejected because the
  signals evolve independently (profiles is `v1development`) and a single
  spec would bury signal-specific semantics.
- **Rate-limiting & quotas as its own capability.** Discovered while reading
  the trace service and HTTP handler: both are observable (`429` /
  `RESOURCE_EXHAUSTED`) and orthogonal to auth and durability. Alternative:
  fold into durability — rejected as a category error (overload control is
  not a durability guarantee).
- **Prometheus `remote_write` included as a sibling, not an OTLP signal.**
  It is not OTLP but rides the same acceptor, auth, and durability path, so
  documenting it alongside keeps the ingest surface complete.
- **Specs describe observable behavior only.** Status codes, endpoints,
  encodings, scopes, and the durability/at-least-once guarantee are in
  scope; internal types and the storage transform are not.

## Risks / Trade-offs

- **Backfill drift risk.** Specs written after the fact can subtly diverge
  from code. Mitigated by grounding every requirement in a specific handler
  or test and by the verification tasks that map requirements to existing
  tests.
- **Shared-spec coupling.** Per-signal specs depend on the three shared
  capabilities; a future change to a shared spec ripples across all signals.
  This is intended — it is the single-source-of-truth trade-off — but means
  shared-spec changes must be reviewed as broadly BREAKING.
- **Profiles instability.** The `v1development` profiles contract may change
  upstream; the spec explicitly marks it development-maturity so a future
  breaking change is expected rather than surprising.
