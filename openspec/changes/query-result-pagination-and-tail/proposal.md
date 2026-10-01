# Proposal

## Why

`query-ir-core` returns one range- and `limit`-bounded envelope per request.
A result larger than one response has no supported way through:

- A client raises `limit` until the response is unwieldy, or until it hits the
  router's 256 MiB result cap (HTTP 413).
- Or it re-issues the query with a shifted time range and hopes the boundary
  rows line up.

Watching new data has the same gap. The Explore UI's "live" mode re-runs the
whole query on a timer: TanStack `refetchInterval`, every 2s for logs and 15s
for other signals (`src/ui/src/lib/live.ts`). Each tick re-reads and
re-transfers the full window, and the client cannot tell which rows are new.

When this change was a stub, live tail belonged to the streaming epic #437:
a WAL broadcast, an acceptor Flight tail, and router WebSocket/SSE endpoints.
On 2026-08-15 that epic's sub-issues (#438–#441, #444, #445) were closed as
not planned. Today SignalDB has no SSE or WebSocket endpoint anywhere. So live
tail comes back here, onto the substrate pagination needs anyway: one keyset
cursor over a total order on the result. A page cursor walks a fixed window to
its end. A tail cursor follows a sliding window forward. The two differ only
in how the window is bounded, so they share one design and one implementation.

> Depends on `query-ir-core` (the document and the `rows`/`trace` envelopes).
> Works on `main` as it is. When `unflushed-data-visibility` lands, the live
> tail's minimum settle delay drops from the commit lag (~10s) to ~2s (design
> D7). It does not depend on `querier-execution-model`'s snapshot pinning: the
> keyset design does not need a pinned snapshot (design D3).

## What Changes

- **IR result change — pagination (IR v14, additive):** an optional
  document-level `page: { size, cursor? }`. On a paginatable document, a
  `rows` or `trace` response gains `page: { next_cursor? }`. `next_cursor` is
  present exactly when more of the result exists. The client resends the same
  document with `page.cursor` set to continue.
- **IR result change — live tail (IR v15, additive):** an optional
  document-level `tail: { cursor?, settle? }` on a relative `range` ending at
  `now`. The response carries `tail: { cursor, settled_through_ns, caught_up }`.
  The first call returns the newest `page.size` rows in the window. Each later
  call returns the rows that became visible after the cursor, oldest first,
  up to `page.size`.
- **Transport is plain request/response `POST /api/v1/query`**, polled by the
  client. There is no new endpoint, no SSE, and no WebSocket in this change:
  the generated clients and the UI's existing polling work unchanged. A
  long-poll `wait` and an SSE wrapper are designed as later options and are
  not in this change's tasks (design D6).
- **Cursors are opaque, stateless, and bound to the request.** A cursor is a
  versioned, checksummed base64url token. It carries the frozen absolute
  window, the last row's sort key, and a fingerprint of tenant, dataset and
  document. A cursor presented with another tenant, dataset or document is
  rejected (400). An expired cursor or one from an incompatible server version
  is rejected with **410 Gone**, a new status for this endpoint.
- **What can be paginated or tailed:** non-aggregated `rows` and `trace`
  results. A document using `aggregate`, `topk`/`bottomk`, `rank`, `describe`,
  a non-trailing `limit`, any other envelope (`series`, `table`, `heatmap`,
  `flamegraph`, `graph`, `metadata`, `scalar`), or a multi-query formula
  document is rejected with 400 and a `details` reason `not_paginatable` /
  `not_tailable` naming the offending stage or envelope. In addition, `match`
  and explicit `order` stages cannot be tailed.
- **Bounds:** server limits on page size, page bytes, total rows walked per
  cursor chain, cursor lifetime, tie-group size, tail settle delay and tail lag.
  Exceeding one is an explicit 422 `resource_limit` or a warning, never a
  silent truncation.
- **Surfaces:** OpenAPI and both regenerated clients; the CLI (`signaldb query
ir --all-pages`, `--follow`); the UI (logs and traces live mode switches from
  re-running the window to tail polls; a "load more" on result tables); the
  MCP `query_ir` tool (passes `page`/`tail` through and returns the cursor).

Not breaking: documents without `page`/`tail` keep their exact current
behaviour and response. Ingest, Flight ingest schemas, WAL, and storage do not
change. The new 410 status is only returned to requests that send a cursor.

## Capabilities

### New Capabilities

- `query-result-pagination`: bounded, resumable delivery of a large `rows`/
  `trace` result. Covers keyset continuation cursors, the ordering they
  require, page/walk bounds, what cannot be paginated, and the consistency a
  cursor promises when data changes between pages (compaction, retention,
  late arrivals).
- `query-live-tail`: following new matching records of a `rows`/`trace`
  document over a sliding window. Covers the poll protocol and cursor, the
  settle delay and late data, lag handling, what can be tailed, and
  tenancy/auth.

### Modified Capabilities

- `query-ir-core`: the "Declared and validated result envelope" requirement
  gains the optional `page` and `tail` members of the `rows`/`trace`
  envelopes and the validation that rejects them on any other envelope.

## Impact

- **query-ir** (`src/query-ir`): `page`/`tail` document fields gated at IR
  v14/v15; paginatability/tailability validation; the default pagination
  order per source; the cursor key model.
- **common**: the cursor codec (versioned, SHA-256-checksummed base64url; uses the
  workspace's existing `base64`/`sha2`); the trailer report gains
  `page: { last_key, has_more }`; `[querier]` config keys for the bounds.
- **querier**: applies the keyset predicate and the page cut after the
  pipeline and before projection. Reports the last key in the trailer. For a
  tail, rewrites the window and tail-time predicate.
- **router**: `endpoints/query.rs`. Cursor issue and validation, fingerprint
  and TTL checks, 410 mapping in `api_error.rs`, response members, OpenAPI.
- **signaldb-sdk / ui (`src/ui/src/api/gen`)**: regenerated clients. The UI
  logs/traces live mode and "load more" go through the generated client only.
- **signaldb-cli**: `--all-pages`, `--follow`.
- **mcp**: `query_ir` tool schema and description.
- **tests-integration**: walk completeness across a flush/compaction, tenant
  binding, tail late-arrival and lag behaviour.
- **docs**: `docs/users/querying-ir.md` (new Pagination and Live tail
  sections; the Roadmap entry); the `http-api` skill's pagination note gains
  the IR-body cursor exception.
- No new dependencies.
