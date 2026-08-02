## Why

`query-ir-core` lets the UI _submit_ a structured query and get a bounded result
back — but a usable explore experience needs two more UI-facing surfaces the core
deliberately left out:

1. **Build-side — discovery.** The goal "building complex queries should be easy"
   needs the builder to know **what it can build on**: which signals exist, which
   fields are queryable on each, what values those fields take, how signals
   relate. Without it the structured builder is a blank box in structured
   clothing. Today that knowledge is scattered across dialect-specific endpoints
   (Loki label/label-values, Tempo tag values, detected-fields).
2. **Delivery-side — live tail + pagination.** `query-ir-core` returns a single,
   range- and `limit`-bounded envelope. The explore UI roadmap wants **live tail**
   (streaming new matching rows) and **pagination/continuation** for large `rows`/
   `trace` result sets — neither of which a unary, statically-enveloped
   `POST /api/v1/query` expresses. Loki and Tempo both have tail; this is the
   native equivalent over the IR.

Both are UI-facing companions to core execution — build-side and delivery-side —
so they share this change.

> Status: **stub** — scope captured, not yet designed in full. Depends on
> `query-ir-core` (shares the logical namespace, the registry resolver, and the
> IR document/envelope). Relates to the attribute-registry epic (#811), the
> detected-fields / attribute-explorability research, and the explore-UI roadmap
> (tail-WS, `/api/context`).

## What Changes (intended)

### Discovery (build-side)

- A native, tenant-scoped **introspection surface** answering: available signal
  sources; queryable fields per source (logical dotted OTel names + canonical
  type, from the registry); value suggestions for a field within a time range;
  cardinality/coverage hints.
- A **`scalar`/`metadata` result envelope** (deferred from `query-ir-core`) for
  introspection results.
- Time-range- and predicate-scoped discovery ("given these filters so far, what
  can I add next?") rather than a static catalog dump.

### Result delivery (delivery-side)

- **Live tail**: a streaming channel (SSE/WS) that delivers new rows matching an
  IR query as they arrive — the same IR document, a streaming transport.
- **Pagination / continuation**: opaque continuation tokens (cursors) for large
  `rows`/`trace` results, so a bounded page can be walked without re-scanning.

## Open questions (resolve when picked up)

- Reuse/reframe existing dialect metadata endpoints vs. a single native
  `describe` operation on the IR surface.
- Registry-known fields vs. detected (sampled-from-data) fields — present
  promoted vs. unpromoted without ever leaking physical columns.
- Value-suggestion cost: sampled vs. exact; bounded scan + cardinality caps.
- Discovery + tail as distinct endpoints vs. `mode`s of the query surface.
- Transport for tail (SSE vs WS) and how it reconciles with the OpenAPI-unary,
  generated-client shape the core uses.
- Cursor stability across compaction/retention; page-size and total-scan bounds.

## Capabilities

### New Capabilities

- `query-field-discovery`: the UI-facing surfaces that sit around core IR
  execution — build-side discovery (sources, fields, values, relationship hints)
  and delivery-side live tail + pagination — over the same logical namespace,
  registry, and IR document as `query-ir-core`.

## Impact

- **common/router/querier**: a metadata/introspection path (reusing the registry
  resolver and existing label/tag plumbing); a streaming result path + cursor
  handling for tail/pagination.
- **ui**: builder autocomplete/field-picker and a live-tail/paged results view,
  consuming the generated client.
- Additive; no ingest/Flight/on-disk changes expected.
