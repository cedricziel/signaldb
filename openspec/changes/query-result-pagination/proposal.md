## Why

`query-ir-core` returns one range- and `limit`-bounded envelope per request.
There is no way to walk a result that is larger than a page: a client either
raises `limit` until the response is unwieldy (or hits the server's cap and
silently sees a truncated view), or re-issues the query with a shifted time
range and hopes the boundary rows line up. Both are the workarounds a
continuation token exists to remove.

This scope arrived here from the `query-field-discovery` stub, which originally
bundled build-side discovery with delivery-side tail and pagination. Discovery
is now its own designed change; **live tail went to epic #437** (the streaming
substrate that owns WAL broadcast, the Acceptor Flight tail, and the router
WebSocket/SSE endpoints); pagination stayed behind as this stub because it is
neither discovery nor streaming. #437's cursors resume a live stream after a
disconnect; a continuation token walks a completed, bounded result — different
problem, different guarantees.

> Status: **stub** — scope captured, not yet designed. Depends on
> `query-ir-core` (the IR document and the `rows` envelope) and on
> `querier-execution-model` (per-query snapshot pinning, which is what could
> make a cursor mean something stable). Relates to `query-structural-traces`
> (the `trace` envelope this must also page).

## What Changes (intended)

- An opaque **continuation token** on the `rows` (and later `trace`) result
  envelope, returned when more of the result exists, accepted on a subsequent
  request to continue from where the previous page ended.
- Documented **page-size and total-scan bounds**, so paging cannot become an
  unbounded export path by another name.
- Defined behaviour when the underlying data changes between pages — compaction,
  retention expiry, or a snapshot that no longer exists — rather than silently
  skipping or repeating rows.

## Open questions (resolve when picked up)

- Token content: a snapshot-pinned offset, a sort-key high-water mark, or an
  opaque server-side handle. A sort-key cursor needs a total order, which ties
  it to `declared-sort-orders` and the IR's `order` stage.
- Cursor lifetime and what happens when it outlives its snapshot: hard error,
  or documented best-effort continuation.
- Whether paging composes with aggregation at all, or is `rows`/`trace`-only.
- Interaction with the export use case — is bulk extraction a separate,
  explicitly slower surface?
- Whether the `metadata` envelope (`query-field-discovery`) needs paging, or
  stays bounded-and-truncated as designed there.

## Capabilities

### New Capabilities

- `query-result-pagination`: bounded, resumable delivery of a large native query
  result — continuation tokens, page and scan bounds, and the consistency
  guarantee a token carries across data lifecycle events.

## Impact

- **Crates**: `router` (token issue/validation on the native query endpoint),
  `querier` (resumable execution and snapshot pinning across pages), `common`
  (the token type and the envelope field), `signaldb-cli`/`ui` (page walking
  through the generated clients).
- Additive; no ingest, Flight wire schema, or on-disk layout change expected.
