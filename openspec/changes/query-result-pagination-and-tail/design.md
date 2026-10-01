# Design

## Context

See proposal.md (Why). The facts below come from the code and shape this
design:

- **One request, one fully buffered result.** `POST /api/v1/query`
  (`router/src/endpoints/query.rs`) resolves `range` once against a
  server-stamped `now_ns`. It sends one `query_ir:{tenant}:{dataset}:{json}`
  Flight ticket to a querier and drains the whole stream into memory, bounded
  by `IR_QUERY_TIMEOUT` (60s) and `MAX_IR_RESULT_BYTES` (256 MiB → 413). Only
  then does it build the envelope. The querier (`IrService::query`) also
  `collect()`s fully.
- **Out-of-band facts already travel in a Flight trailer.** After the last
  batch, the querier appends one data-free `FlightData` whose `app_metadata`
  is `correlate_report:` + JSON. The router reads it while draining and turns
  it into `QueryWarning`s. Unknown JSON members are ignored, so adding members
  is compatible across versions.
- **Result order is only defined where a stage defines it.** An `order` stage
  sorts; `match` emits `(trace_id, start time)`; otherwise row order is
  unspecified. The `trace` envelope groups rows by `trace_id` in order of
  first appearance.
- **No row identity for every source.** Traces have `(trace_id, span_id)`.
  That is not strictly unique: the docs note that a span delivered twice
  yields two rows. Logs have no record id at all.
- **The time window is the scan predicate.** Each source has one time column
  (`timestamp`, or `start_time_unix_nano` for traces). The window filter is
  inclusive `[start, end]` on it. Tables are partitioned by `timestamp`, so a
  predicate on it prunes files.
- **Visibility lag.** Today a row becomes queryable only at its writer's
  coalesced Iceberg commit: `commit_interval` (5s) plus the background loop
  tick (~5s), so roughly ≤10s after ack. `unflushed-data-visibility` (designed,
  not implemented) makes rows visible at ack through a hot/cold union with
  no-duplication and no-omission guarantees, and tags hot batches with a
  per-writer sequence.
- **There is no streaming HTTP surface.** There is no SSE or WebSocket handler
  in the router. The #437 sub-issues (WAL broadcast, acceptor Flight tail,
  router WS/SSE) were closed as not planned on 2026-08-15. The UI's live mode
  is TanStack `refetchInterval` polling (`src/ui/src/lib/live.ts`): 2s for
  logs, 15s otherwise, and never for absolute ranges. The UI and CLI must
  consume only generated clients (OpenAPI → TS client / `signaldb-sdk`).
  Those clients are request/response.
- **Errors.** `ApiError` derives `errorType` from the status: 400 `bad_data`,
  422 `invalid`, or `resource_limit` when flagged, and so on. 410 is not
  mapped yet. `details[]` carries structured reasons.

## Goals / Non-Goals

**Goals:**

- Walk any non-aggregated `rows`/`trace` result to completion, with no row
  returned twice and none skipped among rows that exist for the whole walk.
- Follow a live window with bounded server work per request and no
  per-subscriber server state.
- One cursor mechanism for both. Opaque to clients, so the strategy behind it
  can change without breaking clients (D7, the future sequence-based tail).
- Work on today's `main`, and get better automatically when
  `unflushed-data-visibility` lands.

**Non-Goals:**

- Paging aggregates (`table`/`series`/`heatmap`/…) or `describe` metadata. An
  aggregate is bounded by its group count. Re-aggregating per page would cost
  a full aggregation per page for little gain. (The `metadata` envelope stays
  bounded-and-truncated, as `query-field-discovery` designed it.)
- A bulk-export surface. Paging is bounded (D9). Large extraction belongs to a
  separate, explicitly slower surface, if one is ever added.
- Exactly-once live tail. The v1 tail is at-most-once for rows that arrive
  more than `settle` after their tail-time (D7).
- A new streaming transport (SSE/WebSocket/Flight `DoGet` to clients). This
  change leaves room for one (D6) and does not build it.
- Snapshot-isolated pages. The keyset design does not need them (D3). A
  `strict` mode on top of `querier-execution-model`'s snapshot pinning is a
  possible follow-up.

## Decisions

### D1: One keyset cursor for both pagination and tail

A cursor names a **position in a total order** over the result: "after this
sort key". It does not name an offset or a server-side handle. Continuing
means re-running the same document with the extra predicate `key > cursor`
(lexicographic, honouring each key's direction) plus a page cut. A tail is the
same thing over a window whose end slides forward on each call.

**Alternatives considered:**

- _Offset (`skip N`)_: each page costs O(N). Rows inserted or removed before
  the offset (a late arrival, a retention drop) shift every later page, giving
  silent repeats or skips. Only a pinned snapshot would make it safe, and a
  pinned snapshot does not cover hot (unflushed) data at all. Rejected.
- _Server-side result handle_ (materialize once, page out of it): needs
  per-cursor server state with a TTL, sticky routing to one router/querier in
  microservices mode, and memory for every abandoned walk. Rejected:
  stateless cursors survive router restarts and work behind any load
  balancer.
- _Snapshot-pinned offset_: correct only for committed data, and it ties
  cursor lifetime to `snapshots_to_keep`. Rejected for v1 (see Non-Goals).

### D2: A total order is required, with server-appended tie-breakers and atomic tie groups

A cursor needs a total order. Rules:

1. **With an `order` stage**, its keys are the leading sort keys. Every key
   must be a column of the terminal relation (an `order` key on an aggregate
   output cannot occur, because aggregates cannot be paginated).
2. **Without one**, the server applies the source's **default pagination
   order**: the source time column descending (newest first), then the
   source's tie-breakers ascending. A `match` pipeline keeps its native order
   `(trace_id, start, span_id)` ascending.
3. The server **always appends the source's tie-breaker columns** (below) after
   the leading keys, unless they are already present.

| Source    | Time column            | Tie-breakers (ascending)                                               |
| --------- | ---------------------- | ---------------------------------------------------------------------- |
| traces    | `start_time_unix_nano` | `trace_id`, `span_id`                                                  |
| logs      | `timestamp`            | `trace_id`, `span_id`, `service_name`, `observed_timestamp`, body hash |
| metrics   | `timestamp`            | `series.id`                                                            |
| exemplars | `timestamp`            | `series.id`, `trace.id`, `span.id`                                     |
| profiles  | `timestamp`            | `profile_id`                                                           |

Even after the tie-breakers, keys need not be unique: there are logs without
trace context, and spans delivered twice. So **a page never splits a tie
group**, meaning a run of rows with equal full keys. The page ends at a key
change. It may exceed `size` by the rest of the tie group, up to
`page_max_tie_rows`. A tie group larger than that fails the request with 422
`resource_limit`, telling the caller to add an `order` key. Because tie groups
are atomic, the strict `key > cursor` predicate on the next page is exact. No
"skip k of the tied rows" counter is needed, and none would be correct, since
row order inside a tie is not deterministic.

Nulls sort last in either direction, and the cursor encodes `null`
explicitly. The comparison predicate treats null as greater than every value
for that key, which matches DataFusion's `NULLS LAST`.

**`trace` envelope:** the page unit is a **trace**, not a span. `page.size`
counts traces, and cuts happen only where `trace_id` changes. This requires
`trace_id` to be the leading key. That is true of `match` output and of the
envelope's default order `(trace_id, start, span_id)`. An `order` stage whose
leading key is not `trace_id` makes a `trace` document non-paginatable, since
paging needs each trace's rows to be contiguous.

### D3: Consistency: a frozen window, no snapshot pin

The **first** page resolves `range` to an absolute window (as today), and the
cursor carries that window. Later pages use the window from the cursor and
ignore the clock. Each page re-executes over the current data (latest Iceberg
snapshot, plus hot data once `unflushed-data-visibility` lands) with
`key > cursor`. What this guarantees:

- **No row is returned twice** within one walk. Rows are immutable, so a row's
  key never changes. Compaction rewrites files, not rows, so it is invisible
  to a keyset walk. At the hot/cold flush boundary, `unflushed-data-visibility`
  already guarantees no duplication.
- **No row present for the whole walk is skipped.**
- A row **arriving during the walk** is returned only if its key sorts after
  the cursor at the time its page is read. With the default newest-first
  order, a late row inside the window lands before the cursor and is not
  returned. That is documented, not a warning, because it cannot be detected
  cheaply.
- A row **removed** during the walk by retention or tenant deletion stops
  appearing. It is not an error: the walk returns what still exists.

Cursor expiry is about the cursor, not the data. A cursor expires after
`page_cursor_ttl` (default 15m from issue; each page issues a fresh cursor)
and on a cursor-format version change (D4). Both return **410 Gone**, so the
client restarts the walk.

### D4: Cursor encoding

```
sdbc1.<base64url(payload)>.<base64url(sha256(prefix || payload)[0..16])>
```

The payload is compact JSON:

| Field | Meaning                                                                                               |
| ----- | ----------------------------------------------------------------------------------------------------- |
| `v`   | cursor format version (`1`); a mismatch → 410                                                         |
| `k`   | `"page"` or `"tail"`                                                                                  |
| `fp`  | SHA-256 over `(tenant_id, dataset_id, irVersion, canonical document without page.cursor/tail.cursor)` |
| `w`   | frozen window `[start_ns, end_ns]` (page) / tail lower bound (tail)                                   |
| `key` | `[{c, t, v}]`: column, value type, value of the last emitted row's full sort key (null allowed)       |
| `dir` | per-key direction bits (cross-checked against the re-derived order)                                   |
| `n`   | rows (or traces) emitted so far in the walk (walk bound, D9)                                          |
| `iat` | issued-at, ns (TTL)                                                                                   |
| `st`  | tail only: `settled_through_ns` of the previous call                                                  |

**Signed when the server has a secret.** The checksum catches corruption and
casual editing (→ 400). Every value in a cursor is something the caller could
put in its own document (a `where` on the key, a `range`), the query always
runs under the caller's authenticated tenant and dataset, and the fingerprint
makes a cursor from another tenant, dataset or document fail with 400 rather
than run. What an edited cursor could bypass is the walk-row budget and the
lifetime. So there are two modes. Signed, when `[auth].internal_service_key`
is set (a secret every router replica shares): the checksum is an HMAC-SHA256
under a key derived from it, compared in constant time, and a cursor that
fails it answers 410, not 400, so a client restarts cleanly after a key
rotation or against a replica with another key. Unsigned, without the key:
plain SHA-256, which only catches corruption, so the walk budget and lifetime
are advisory, and the router logs a startup warning saying so. In both, a
cursor over 8 KiB, or issued more than 60s in the future, is corrupt.

Values in a cursor are the caller's own data (timestamps, ids, service
names). Cursors appear only in POST bodies and responses. They are never put
in URLs, logs, or span attributes; only their length and kind are recorded.

### D5: What can be paginated or tailed, and the error

A document can be **paginated** when all of these hold:

- the envelope is `rows` or `trace`;
- it is a single document (not a `queries`/`formulas` multi document);
- the pipeline has no `aggregate`, `topk`, `bottomk`, `rank`, or `describe`;
- any `limit` is the **last** stage (it then caps the whole walk: the cursor's
  `n` counts toward it, and the last page is cut at it);
- for `trace`: the leading key is `trace_id` (D2).

`where`, `extract`, `correlate` (all kinds: row-preserving or row-filtering
joins whose output rows are still keyed), `match`, and `order` are allowed.

A document can be **tailed** when it can be paginated and also:

- `range.to` is the relative anchor `now` (absolute ranges cannot slide; the
  UI already disables live mode for them);
- it has no `order` stage (tail order is fixed, D7) and no `match` (a
  structural match needs whole traces, and a tail sees partial ones);
- it has no `limit` (`page.size` bounds each call).

A violation is a **400** whose `details` carry `reason: "not_paginatable"` or
`"not_tailable"` and name the offending stage index or the envelope:

```jsonc
{
  "status": "error",
  "errorType": "bad_data",
  "error": "page: an aggregate result cannot be paginated",
  "details": [
    { "reason": "not_paginatable", "column": "pipeline[1].aggregate" },
  ],
}
```

`page`/`tail` under too low an `irVersion` (<14 / <15) is the ordinary
version-gate rejection. `tail` together with `page.cursor` is a 400: a tail
call carries its position in `tail.cursor`, and `page.size` only bounds the
call.

### D6: Live-tail transport: client polling over the existing endpoint

A tail is a sequence of ordinary `POST /api/v1/query` calls, each carrying the
previous call's `tail.cursor`. The server keeps no subscription. Each call is
bounded like any query (page size, bytes, deadline).

**Why polling, not SSE/WebSocket/Flight streaming:**

- Nothing in the stack streams today. The router drains the whole querier
  stream before replying, and the #437 streaming substrate was abandoned. An
  SSE endpoint would need a server loop re-running the query per subscriber
  anyway, which is polling moved server-side, plus connection lifecycle,
  per-tenant subscriber limits, and proxy idle timeouts.
- The generated TS client and `signaldb-sdk` are request/response. A
  streaming endpoint would be the only operation the UI could not call
  through its generated client, which breaks the "UI consumes only the
  generated client" rule.
- The UI already polls. Moving it to tail polls replaces "re-read the whole
  window every tick" with "read only what is new", without touching its data
  flow.
- Back-pressure comes for free: the client pulls. A slow consumer simply polls
  less often. Server work per call is bounded by `page.size`, and a consumer
  that falls too far behind is moved forward explicitly (D7) rather than
  buffered for.

**Later, without changing the cursor:**

- A long-poll `tail.wait` (≤ 30s): the router re-runs the call at a fixed
  cadence until rows appear or `wait` elapses. That is server-side polling,
  and worth it only once `unflushed-data-visibility` makes a cheap hot-tier
  probe possible.
- An SSE endpoint `POST /api/v1/query/tail` that loops the same call
  server-side and emits each response as an event. The cursor in each event
  lets a client reconnect exactly where it left off, which is what #444
  wanted.

### D7: Tail semantics: tail-time, settle delay, first call, lag

**Tail-time column.** Rows are tailed in ascending order of a per-source
_tail-time_, then tie-breakers (D2):

| Source                             | Tail-time                                                                                                                                      |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| logs, metrics, exemplars, profiles | the source time column                                                                                                                         |
| traces                             | `end_time_unix_nano`; the scan also bounds `start_time_unix_nano ≥ tail_lower − tail_max_span_duration` so it prunes on the partitioned column |

Traces use the **end** time because a span is exported after it ends. Keyed
on start time, a 60s span would only become visible after the tail had passed
its start, and would be missed. Spans longer than `tail_max_span_duration`
(default 1h) are not tailed. This is documented.

**Settle delay.** A call at server time `T` reads rows with tail-time in
`(cursor, T − settle]` and returns `settled_through_ns = T − settle`. A row
that becomes visible after `T` with tail-time ≤ `T − settle` is never
delivered. So the tail is **at-most-once for rows that arrive more than
`settle` after their tail-time**, and exactly-once otherwise. `settle` is
requested per document and clamped to
`[tail_min_settle, tail_max_settle]`:

- `tail_min_settle` defaults to **10s** while visibility waits for the Iceberg
  commit (commit interval plus loop tick). It is a config key, so a deployment
  that raises `commit_interval` must raise it too. The docs say so.
- When `unflushed-data-visibility` lands, rows are visible at ack. The floor
  drops to **2s**, covering only the SDK export batching window. That is a
  default change in the same PR that enables the hot union, recorded in that
  change's tasks.
- Data still sitting in an acceptor WAL because the writer is unreachable is
  invisible until forwarded, and arrives "late" by definition. The docs list
  this.

**Cursor advance.** When a call drains everything up to `T − settle`
(`caught_up: true`), the next cursor's key is
`(settled_through_ns, +∞ tie-breakers)`, even if no row was returned. The next
scan then starts at the settle line instead of the last row, so an idle tail
stays cheap. When `page.size` cut the call short (`caught_up: false`), the
cursor sits after the last row returned, and the client should call again
right away.

**First call (no cursor).** The first call returns the **newest** `page.size`
rows in `[range.from, T − settle]`, ordered ascending in the response, with a
cursor after the newest. This gives the "show the last N lines, then follow"
behaviour of a live log view in one call. The client does not need a separate
initial query that would overlap the tail.

**Lag.** If the cursor's tail-time is older than `T − tail_max_lag` (default
5m), the call skips forward to `T − tail_max_lag` and adds a `tail_lagged`
warning naming the skipped interval. This mirrors a broadcast channel's
`Lagged`: a slow or suspended client (a backgrounded browser tab) never forces
an unbounded catch-up scan. To read the gap completely, the client can page it
with an ordinary absolute-range query.

**Future exact tail.** `unflushed-data-visibility` tags hot rows with a
per-writer sequence. A tail cursor of `kind: "tail", v: 2` could carry
per-writer sequences and read the hot tier in arrival order, making the tail
exactly-once with detectable gaps when a writer flushes past a cursor.
Because cursors are opaque, clients would need no change. Out of scope here.

### D8: Where the work happens

- **Router.** Validates paginatability/tailability with `query-ir`'s
  validator. Decodes and checks the cursor (checksum, version, TTL,
  fingerprint, `dir` against the re-derived order). Resolves the window,
  frozen for a page and sliding for a tail. Puts a `page` object in the ticket
  payload next to `document` and `now_ns`:
  `{"size", "unit": "rows"|"traces", "order": [...], "after": key?, "window", "tail": {...}?}`.
  Builds the response members from the querier's trailer.
- **Querier.** After the pipeline and **before** `apply_projection` (tie-break
  columns may not be projected), it appends the sort, the keyset predicate,
  and the page cut. The keyset predicate is a lexicographic disjunction
  `(k1 ≷ v1) ∨ (k1 = v1 ∧ k2 ≷ v2) ∨ …`. A leading time key lets it prune
  Iceberg partitions. The cut is a small streaming operator
  (`PageCutExec`): it passes `size` units, then finishes the current tie group
  up to `page_max_tie_rows`, stops at `page_max_bytes`, records the last
  emitted key and whether another row followed (`has_more`), and drops its
  input. Dropping the input cancels upstream work. For `rows`, the sort
  carries a fetch of `size + page_max_tie_rows + 1`, so DataFusion uses a TopK
  instead of a full sort. The trailer report gains
  `page: { last_key, has_more, emitted }`.
- **Trailer type.** The same trailer and JSON object as the `correlate`
  report, with a new optional member. An older router ignores it, but an
  older router never sends a `page` ticket, so the pairing cannot occur.

### D9: Bounds (all `[querier]`, all enforced explicitly)

| Key                                 | Default   | On exceed                                                    |
| ----------------------------------- | --------- | ------------------------------------------------------------ |
| `page_default_size`                 | 1,000     | used when `page.size` is omitted                             |
| `page_max_size`                     | 10,000    | 400 at validation                                            |
| `page_max_bytes`                    | 16 MiB    | the page ends early at a key boundary; `next_cursor` present |
| `page_max_tie_rows`                 | 10,000    | 422 `resource_limit` ("add an `order` key")                  |
| `page_max_walk_rows`                | 1,000,000 | 422 `resource_limit` on the page that would exceed it        |
| `page_cursor_ttl`                   | 15m       | 410                                                          |
| `tail_min_settle`/`tail_max_settle` | 10s / 5m  | clamped; the effective value is echoed back                  |
| `tail_max_lag`                      | 5m        | skip forward and add a `tail_lagged` warning                 |
| `tail_max_span_duration`            | 1h        | longer spans are not tailed (documented)                     |

A single trace (`trace` unit) whose rows exceed `page_max_bytes` gets the
same explicit 422 as an over-bound `match` trace. It is never split.
Existing per-tenant query rate limits (`query-rate-limiting`) apply to every
page and every tail call, and are the real throttle on polling frequency.

### D10: Auth and tenancy

Every page and tail call is an ordinary authenticated request. The same
`document_read_scopes` checks run on each call, including `correlate`
targets, so a key whose scope is revoked mid-walk fails the next call with 403. Tenant and dataset come only from the authenticated request, never from
the cursor. The cursor's fingerprint just has to match them (D4). A cursor
holds no credentials and grants nothing.

### IR result changes (explicit)

- `rows`/`trace` envelopes gain an optional `page: { next_cursor?: string }`,
  present iff the request carried `page`. `next_cursor` is absent on the
  final page.
- `rows`/`trace` envelopes gain an optional
  `tail: { cursor: string, settled_through_ns: i64, settle_ns: i64, caught_up: bool }`,
  present iff the request carried `tail`.
- New warning code `tail_lagged`.
- New error status 410 (`errorType: "gone"`) on `POST /api/v1/query`, only for
  requests that carry a cursor. New `details[].reason` values
  `not_paginatable`, `not_tailable`.
- Ordering: with `page`/`tail`, row order is defined (D2/D7) where it was
  unspecified before. Without them, nothing changes.

## Risks / Trade-offs

- [Each page re-runs the pipeline; a non-time leading key prunes nothing] →
  The default order leads with the partition-aligned time column. Walk and
  page bounds cap the total cost. Docs recommend time-leading `order` keys
  for large walks.
- [Newest-first walks miss rows that arrive late inside the window] →
  Documented (D3). A client that needs a consistent export pages an absolute,
  settled range: one whose `to` is older than the settle floor.
- [The tail is at-most-once past `settle`] → Documented; settle is tunable;
  exact sequence-based tail is a cursor-v2 follow-up (D7).
- [`trace` envelope without `match` needs a full sort on `trace_id`] → It runs
  under the existing memory pool and 422s rather than OOMs. Docs steer users
  to `match` or `rows` for large walks.
- [Tie-group atomicity can make a page much larger than `size`] → Capped by
  `page_max_tie_rows` with an explicit 422.
- [Cursor carries data values] → Body-only, never logged (D4).
- [A large `tail_min_settle` makes "live" feel slow before
  `unflushed-data-visibility`] → Same or better than today's 2s/15s polling,
  which also cannot show uncommitted data. The floor drops automatically
  when that change lands.

## Migration Plan

A stack of PRs (tasks.md), each one leaving `main` working:

1. Pure `query-ir` model and validation (no behaviour reachable: version gate
   still at 12).
2. Cursor codec in `common`.
3. Querier page execution behind the ticket's optional `page` object (no
   router sends it yet).
4. Router + IR v14 + OpenAPI/clients: pagination goes live.
5. CLI/MCP/UI pagination surfaces.
6. Tail in the querier (tail-time, settle, lag).
7. Router + IR v15 + clients: tail goes live.
8. UI live mode + CLI `--follow` move to tail.

Rollback: revert the router PR (4 or 7). Older servers reject `irVersion`
14/15 with the existing unsupported-version error, and clients that never
send `page`/`tail` are unaffected. No persisted state.

## Open Questions

- Default newest-first vs oldest-first for pagination without `order`: this
  design picks newest-first to match the logs/traces views. It is a default
  only, and an `order` stage overrides it.
