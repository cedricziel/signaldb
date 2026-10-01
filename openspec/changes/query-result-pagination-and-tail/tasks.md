# Tasks

A stack of PRs, one per `##` group, each under 500 changed lines (excluding
regenerated clients) and each leaving `main` working. Groups 1–3 add only
code that nothing calls yet, so no behaviour changes until group 4 raises the
IR version to 14; group 7 raises it to 15. Later groups depend on earlier
ones. IR v13 went to the flamegraph `baseline` of
`feat/query-ir-profile-diff`, so `page` is v14 and `tail` v15. Group 9 is an
optional follow-up and outside this change's definition of done.

## 1. IR model: `page`/`tail` fields, order derivation, validation (query-ir)

- [x] 1.1 Write failing tests in `query-ir` (`validate.rs`): `page`
      rejected under irVersion < 14 and `tail` under < 15 (the version gate
      stays at 12 in this PR, so the tests use the validator with an
      explicit max-version parameter); every `not_paginatable` case from
      design D5 (aggregate, topk, bottomk, rank, describe, non-trailing
      `limit`, each non-`rows`/`trace` envelope, `trace` with a non-`trace_id`
      leading `order` key) names its stage or envelope; every `not_tailable`
      case (absolute `range.to`, `order`, `match`, `limit`, `tail` plus
      `page.cursor`); `page.size` above max rejected.
      Verify with `cargo test -p query-ir validate`
- [x] 1.2 Add `Document.page: Option<Page { size, cursor }>` and
      `Document.tail: Option<Tail { cursor, settle }>` (serde-optional, so
      existing documents round-trip unchanged) and the validation from 1.1.
      Add `MAX_IR_VERSION` constants for 14/15 without raising
      `MAX_IR_VERSION`. 1.1 passes
- [x] 1.3 Write failing tests, then implement `pagination_order(doc,
    source) -> Vec<SortKey>`: explicit `order` keys plus appended
      tie-breakers; the per-source default (time desc plus tie-breakers);
      `match` native order; `tail_order` (tail-time asc plus tie-breakers,
      traces → `end_time_unix_nano`). Cover the design D2/D7 tables.
      Verify with `cargo test -p query-ir pagination_order`
      (Implemented in `query_ir::page`. The tie-breakers are logical field
      names, since the planner resolves keys through the field resolver:
      logs `service.name` and profiles `profile.id` stand for the
      `service_name`/`profile_id` columns of design D2. Logs have no per-row
      id, so their order also ends in `observed_timestamp` and a 64-bit hash
      of `body` (`__sdb_body_hash`): only fully identical lines still tie.)

## 2. Cursor codec (common)

- [x] 2.1 Write failing tests in `common::query_cursor`: encode/decode
      round-trip for page and tail cursors with typed key values (i64, string,
      bytes, null); checksum mismatch → `CursorError::Corrupt`; unknown
      version → `Expired`; TTL exceeded → `Expired`; fingerprint mismatch
      (tenant, dataset, document, irVersion) → `Mismatch`; the fingerprint
      ignores `page.cursor`/`tail.cursor` and is stable across JSON key
      order. Verify with `cargo test -p common query_cursor`
- [x] 2.2 Implement the `sdbc1.<payload>.<sum>` codec (design D4) on the
      workspace `base64`/`sha2` crates. Fingerprint over the canonical
      document. 2.1 passes. Run `cargo machete --with-metadata` (no new
      dependency)

## 3. Querier page execution (querier, common trailer)

- [x] 3.1 Write failing tests in `common::flight`: the trailer report
      round-trips an optional `page { last_key, has_more, emitted }`; an
      older trailer without it still parses.
      Verify with `cargo test -p common flight::`
- [x] 3.2 Add the `page` member to the trailer report. 3.1 passes
- [x] 3.3 Write failing `PageCutExec` unit tests in
      `querier::query::page_cut`: passes `size` rows then completes the tie
      group; errors past `page_max_tie_rows`; ends early at `page_max_bytes`
      on a key boundary; `has_more` exact at the boundary (rows == size,
      size + 1); `traces` unit counts distinct `trace_id`s and never splits
      one; reports the last emitted key.
      Verify with `cargo test --profile ci-test -p querier page_cut`
- [x] 3.4 Implement `PageCutExec` plus the lexicographic keyset predicate
      builder (directions, NULLS LAST). 3.3 passes
      (Implemented as `querier::query::page_cut::cut_page`, a function over
      the collected sort output rather than an `ExecutionPlan`: the sort
      carries a fetch, so it consumes its whole input before emitting and a
      streaming cut would stop nothing early. The page byte bound uses the
      batch's average row size. The keyset predicate lands with 3.6.)
- [x] 3.5 Write failing `ir_planner` tests: a ticket payload with `page`
      (order, after, size, window) applies the sort, keyset predicate, and
      cut **before** projection (tie-breakers not in `fields` still work);
      walking a fixture page by page reproduces the unpaged sorted result
      exactly; trailing `limit` caps across pages via `emitted`; the leading
      time key shows partition pruning in `EXPLAIN`.
      Verify with `cargo test --profile ci-test -p querier ir_planner::page`
- [x] 3.6 Read the optional `page` object from the ticket payload in
      `IrService::query`, wire 3.4 in, and fill the trailer report. Without
      `page` the plan is byte-identical to today (assert with an `EXPLAIN`
      snapshot test). 3.5 passes. Add the `[querier]` page keys from design
      D9 to `QuerierConfig`, `signaldb.dist.toml`, and the configuration docs
      (A paged plan drops a trailing `limit`; the router caps it via `ceiling`.)
      (A `rows` page fetches `size + 1` rows and re-reads the boundary tie
      group, bounded by the tie limit, only when it reaches the fetch's end. A
      `trace` page takes the next `size + 1` trace ids by a TopK and their
      spans by a semi-join, each trace capped at `match_max_trace_spans`.)

## 4. Router: pagination goes live (router, query-ir version, API contract)

- [ ] 4.1 Write failing router tests (`endpoints::query`): a v14 `page`
      document yields `page.next_cursor` until the last page; a resubmitted
      cursor continues; the window is frozen from the cursor (a relative
      range does not drift); another tenant's or dataset's cursor → 400; an
      edited document → 400; a corrupt cursor → 400; an expired one → 410
      with `errorType: "gone"`; `not_paginatable` → 400 with `details`; a
      document without `page` yields a byte-identical response.
      Verify with `cargo test -p router endpoints::query`
- [ ] 4.2 Raise `MAX_IR_VERSION` to 14. Validate, decode and check the
      cursor, put `page` into the ticket, build `page.next_cursor` from the
      trailer, and map 410 → `gone` in `api_error.rs`. Add `page` to
      `QueryIrRequest`/`QueryIrResponse` with `utoipa` docs. 4.1 passes
- [ ] 4.3 Regenerate the OpenAPI spec, `signaldb-sdk`, and the TS client
      (`src/ui/src/api/gen`). Verify with `pnpm --filter ./src/ui typecheck`
      and the SDK build
- [ ] 4.4 Write a `tests-integration` test: ingest N logs, walk them with
      `page.size` < N through `POST /api/v1/query`, force a flush plus
      compaction mid-walk, and assert every row exactly once and the
      tenant-bound cursor rejection.
      Verify with `cargo test --profile ci-test -p tests-integration pagination`
- [ ] 4.5 Docs (route via the `docs` skill): a "Pagination" section in
      `docs/users/querying-ir.md` (request/response, default order and
      tie-breakers, what cannot be paginated, consistency, bounds, 410). In
      the Roadmap section, move pagination out of "Still deferred". In the
      `http-api` skill, note that IR cursors travel in the POST body.
      Verify that the docs build passes

## 5. Pagination surfaces: CLI, MCP, UI

- [ ] 5.1 Write a failing CLI test, then implement `signaldb query ir
    --page-size N --all-pages` through `signaldb-sdk`, streaming each page's
      rows as NDJSON and stopping at the last page.
      Verify with `cargo test -p signaldb-cli query`
- [ ] 5.2 Write a failing test, then extend the MCP `query_ir` tool:
      `page`/`cursor` input, and `next_cursor` in the output and its
      description. Verify with `cargo test -p mcp-server query_ir`
- [ ] 5.3 Write failing UI tests (vitest), then add "Load more" to the
      Explore query results table using `page.next_cursor` through the
      generated client only. Verify with `pnpm --filter ./src/ui test &&
    pnpm --filter ./src/ui lint`
- [ ] 5.4 Docs: the CLI reference and MCP tool docs. Verify that the docs
      build passes

## 6. Querier tail execution (querier)

- [ ] 6.1 Write failing `ir_planner` tests for a ticket `page` object with
      `tail`: the tail-time predicate `(cursor, T − settle]`; traces use
      `end_time_unix_nano` with the `start ≥ lower − tail_max_span_duration`
      prune bound; the first call (no `after`) returns the newest `size` rows,
      re-ordered ascending; the trailer reports `caught_up` and the last key;
      an idle window yields an empty page with `caught_up: true`.
      Verify with `cargo test --profile ci-test -p querier ir_planner::tail`
- [ ] 6.2 Implement tail lowering on top of group 3's sort/keyset/cut (newest-N
      via a reversed sort plus fetch, then ascending re-order) and add the
      `tail_*` `[querier]` keys (design D9) to config and
      `signaldb.dist.toml`. 6.1 passes

## 7. Router: live tail goes live (router, query-ir version, API contract)

- [ ] 7.1 Write failing router tests: a v15 `tail` call returns `tail
    { cursor, settled_through_ns, settle_ns, caught_up }`; a follow-up call
      returns only rows after the cursor; settle is clamped and echoed; a
      cursor older than `tail_max_lag` skips forward with a `tail_lagged`
      warning naming the interval; `not_tailable` cases → 400; cross-tenant
      cursor → 400; a revoked key → 401 on the next call.
      Verify with `cargo test -p router endpoints::query`
- [ ] 7.2 Raise `MAX_IR_VERSION` to 15. Implement the tail cursor
      (sliding window, cursor advance to the settle line, lag skip), add
      `tail` to the request/response types, and add `tail_lagged` to the
      `QueryWarning.code` docs. 7.1 passes
- [ ] 7.3 Regenerate the OpenAPI spec, `signaldb-sdk`, and the TS client.
      Verify with `pnpm --filter ./src/ui typecheck`
- [ ] 7.4 Write a `tests-integration` test: start a tail, ingest new logs and
      a long span, and poll. Assert each row is delivered once and in order,
      the long span is delivered by end time, and a row ingested with a
      tail-time behind the cursor is not delivered (documented
      at-most-once). Verify with
      `cargo test --profile ci-test -p tests-integration live_tail`
- [ ] 7.5 Docs: a "Live tail" section in `docs/users/querying-ir.md`
      (protocol, tail-time per source, settle and its floor before/after
      unflushed-data-visibility, late data, lag, what cannot be tailed). In
      the Roadmap section, move live tail out of "Still deferred". Verify
      that the docs build passes

## 8. Live tail surfaces: UI live mode, CLI follow, MCP

- [ ] 8.1 Write failing UI tests, then switch the logs and traces views'
      live mode from `refetchInterval` window re-runs to tail polls through
      the generated client: append new rows, keep the cursor in component
      state, poll immediately while `caught_up` is false, and show
      `tail_lagged` with the existing warning UI. Signals that cannot be
      tailed keep `liveRefetchInterval`. Verify with
      `pnpm --filter ./src/ui test && pnpm --filter ./src/ui typecheck`
- [ ] 8.2 Write a failing CLI test, then implement `signaldb query ir
    --follow [--settle 5s]`, polling with the tail cursor and printing
      NDJSON. Verify with `cargo test -p signaldb-cli query`
- [ ] 8.3 Extend the MCP `query_ir` tool with `tail` input/output; one call
      per tool invocation, with no server-side loop. Verify with
      `cargo test -p mcp-server query_ir`
- [ ] 8.4 Docs: the CLI `--follow` reference, and the UI live-mode behaviour
      in the Explore docs. Update the `frontend-instrumentation` skill only if
      polling spans change name. Verify that the docs build passes

## 9. Follow-ups (not part of this change's definition of done)

- [ ] 9.1 When `unflushed-data-visibility` lands, lower the
      `tail_min_settle` default to 2s in the same PR that enables the
      hot/cold union, and update the Live tail docs
- [ ] 9.2 Evaluate `tail.wait` long-poll and an SSE wrapper (design D6)
- [ ] 9.3 Evaluate a cursor-v2 exact tail over the hot tier's per-writer
      sequence (design D7)
