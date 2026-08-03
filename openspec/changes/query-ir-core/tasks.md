## 1. Type system + versioning foundation (`common`)

- [x] 1.1 Failing unit test (`cargo test -p common`): value-type coercion —
      duration suffixes (`"500ms"`), numeric strings, RFC3339/relative timestamps
      coerce to the registry canonical type; an un-coercible literal is rejected
      at validation, never silently cast.
- [x] 1.2 Failing unit test: absent-value semantics — comparisons on an absent
      field evaluate to `absent` (third truth value), `not(absent)=absent`, and
      the `and`/`or` truth tables propagate it; a `where` emits a row only on
      `true`, so both `field = x` and `not(field = x)` exclude absent rows, while
      `exists`/`not(exists)` observe absence. Independent of engine null
      behaviour.
- [x] 1.3 Failing unit test: relation-type inference — a pipeline's relation type
      (RowSet/Series, source, grain, columns) is inferred stage-by-stage, and a
      stage whose input constraint is unmet fails validation naming the stage.
- [x] 1.4 Failing unit test: IR versioning — a supported `irVersion` parses; an
      out-of-range version is rejected reporting the supported range; a document
      with a new optional field still validates (additive tolerance); a stage
      object with an unknown key is rejected (`deny_unknown_fields`
      physical-addressing guard).
- [x] 1.5 Failing unit test: declared-envelope validation — `rows`⇔
      `RowSet{aggregated=false}`, `table`⇔`RowSet{aggregated=true}`, `series`⇔
      `Series`; declaring `series` over a RowSet terminal (or `rows` over a
      grouped aggregate) is rejected with an envelope-mismatch error.
- [x] 1.6 Failing unit test: extensible-source forward-compat — the `from` source
      is resolved against the source registry; an unregistered source is rejected
      with a clear unknown-source error (not a parse failure), and a previously
      valid document still validates unchanged after a new source is registered
      (document shape is source-independent).
- [x] 1.7 Implement the IR document types, `ValueType`/`RelationType` (incl. the
      `aggregated` discriminator), the versioned operator/function registry,
      coercion, the source registry, and the validator. Make 1.1–1.6 pass.

## 2. Predicate grammar + structured operands (`common`)

- [x] 2.1 Failing unit test: nested `and`/`or`/`not` with all operators parses;
      a leaf naming a physical column or `attributes_json` is rejected
      (logical-namespace guard).
- [x] 2.2 Failing unit test: structured operands — `aggregate`/`order`/`topk`
      operands are structured values (an operand supplied as an expression string
      is rejected); every `Agg` has a unique `as` name, a duplicate name or an
      `AggRef`/`order` reference to an unknown name is rejected, and `topk`/
      `bottomk` `n` must be an integer `> 0`.
- [x] 2.3 Failing unit test: extract field resolution — an `extract` derives
      typed query-local fields usable by later stages with the declared type for
      coercion; a derived name colliding with a registry field or an earlier
      extract is rejected (no silent shadowing).
- [x] 2.4 Implement the `Predicate` enum and structured `Agg` (with `as`)/
      `Order`/`topk`/`bottomk` operands and extract field-scope resolution. Make
      2.1–2.3 pass.

## 3. Attribute-registry resolver interface (`common`)

- [x] 3.1 Failing unit test: resolving a logical field yields either a column ref
      or an `attributes_json` path plus the canonical type; unpromoted → json
      path, promoted → column.
- [x] 3.2 Implement the resolver interface (field → `Column | JsonPath` + type)
      as a **consumer of the attribute-registry epic (#811)** — a query-facing
      view, not a re-implementation; provide a config/in-memory fallback for
      tests. Make 3.1 pass. (If #811's registry API is not yet available, adapt
      the existing slug/registry accessor and leave a TODO referencing #811.)

## 4. IR → LogicalPlan planner, single-signal (`querier`)

- [x] 4.1 Failing unit test (`cargo test -p querier`): `from(logs) + where +
aggregate(step)` lowers to TableScan→Filter→Projection(date_bin)→
      Aggregate→Sort, with an unpromoted-field filter emitted as a JSON
      extraction, satisfying the denotational spec on a fixture.
- [x] 4.2 Failing unit test: promotion invariance — the same IR lowers to a
      column-ref filter when the field is promoted and a json-path filter when
      not, and both execute to the same result over a fixture table.
- [x] 4.3 Failing unit test: `from(traces) + where + topk` (single-signal trace
      query) lowers and executes; `extract` on `traces` is rejected (log-only).
- [x] 4.4 Failing unit test (**execution-level**, `cargo test -p querier`):
      absent-value semantics hold in the _lowered plan_, not just the type layer —
      `not(field = x)` over a fixture where some rows lack `field` excludes those
      rows, proving the result is independent of DataFusion's SQL NULL behaviour
      (the guarantee task 1.2 asserts at the type level).
- [x] 4.5 Failing unit test: curated projection — a `rows` result returns only
      the `fields` set (or the bounded default), never all physical columns
      (`SELECT *`), including for a source with many promoted columns; a `fields`
      entry absent from the terminal relation is rejected.
- [x] 4.6 Failing test: relative-time determinism — with a fixed injected clock,
      a `now-1h` query resolves one absolute `[t0,t1]` at the ticket boundary,
      every stage sees identical bounds, the resolved window is echoed in the
      response, and replaying the echoed absolute window reproduces the result.
- [x] 4.7 Failing test: `regex` safety — a normal pattern matches; an adversarial
      catastrophic-backtracking pattern is bounded by the timeout guard and
      returns an error rather than hanging (predicate `regex` op).
- [x] 4.8 Implement the single-signal planner (from/where/extract/aggregate/
      topk/order/limit) → `LogicalPlan`, carrying resolved absolute time bounds
      through the ticket/plan. `extract` v1 = `json` + `logfmt`; predicate
      `regex` and the deferred `regex` extract parser run behind a bounded,
      timeout-guarded matcher. Make 4.1–4.7 pass.
      DONE: from/where/aggregate/topk/bottomk/order/limit + `extract`
      (json/logfmt via a bounded `ir_extract` scalar UDF), promotion-invariant
      resolution against the real scanned schema, absent-value semantics,
      curated projection, deterministic relative-time, and the bounded
      predicate-`regex` guard.

## 5. Querier Flight ticket (`querier`)

- [x] 5.1 Failing test: a `query_ir:{tenant}:{dataset}:{json}` ticket is
      dispatched, planned, executed, and streams RecordBatches tagged with the
      declared envelope; a malformed ticket returns `invalid_argument`.
- [x] 5.2 Implement the ticket branch in `src/querier/src/flight.rs` alongside
      the existing `query_*` prefixes. Make 5.1 pass.

## 6. Router endpoint + OpenAPI (`router`)

- [x] 6.1 Failing test (`cargo test -p router`): `POST /api/v1/query` validates
      auth/tenant headers, forwards the IR as a `query_ir` ticket, and streams
      the enveloped result; unauthenticated/invalid requests are rejected.
- [x] 6.2 Implement `src/router/src/endpoints/query.rs`; register it in
      `endpoints/mod.rs`. Make 6.1 pass.
- [x] 6.3 Add the endpoint + versioned IR request schema + result-envelope
      schemas to the code-first OpenAPI (`src/router/src/openapi.rs`); assert the
      spec snapshot test covers the new operation.

## 7. Generated clients (SDK parity)

- [x] 7.1 Regenerate the Rust SDK (`src/signaldb-sdk`) from the updated OpenAPI;
      smoke test that the generated `query` operation compiles and round-trips an
      IR request type.
- [x] 7.2 Regenerate the TypeScript client (`src/ui/src/api/gen`); assert the
      generated `query` operation and IR/envelope types are present.

## 8. CLI surface (`signaldb-bin` / SDK consumer)

- [x] 8.1 Failing test: a CLI `query` command reads an IR query (file/stdin),
      submits it via the generated Rust SDK, and prints the enveloped result.
- [x] 8.2 Implement the command against the generated SDK (no hand-written HTTP).
      Make 8.1 pass.

## 9. UI query builder, single-signal (`ui`)

- [x] 9.1 Failing component test (`src/ui`): a builder appends stage objects
      (from(logs) → where(FilterChips) → aggregate) and emits a valid IR document
      via the generated TS client — no dialect-string compilation in the browser.
- [x] 9.2 Failing component test: each envelope renders with the right view —
      `rows`→log/span list, `series`→chart, `table`→topN — chosen from the
      declared envelope before results arrive.
- [x] 9.3 Implement the builder + renderers in `src/ui/src/features/explore`
      (+ a `query-builder` lib), logs and traces, consuming only the generated
      client. Make 9.1–9.2 pass.

## 10. Cross-service integration (`tests-integration`)

- [x] 10.1 Failing E2E: ingest logs+traces; a single-signal logs IR query
      returns results equivalent to the LogQL equivalent.
- [x] 10.2 Failing E2E: a single-signal traces IR query (filter + topk) returns
      the expected spans.
- [x] 10.3 Regression assertion: existing TraceQL/LogQL/PromQL E2E queries still
      pass unchanged (additive, non-regressing).

## 11. Docs + skills

- [x] 11.1 Add user + API documentation for the IR core and `POST /api/v1/query`
      (route via the docs skill; correct audience/frontmatter): the type system,
      versioning policy, stage set, and a worked single-signal example.
- [x] 11.2 Update the `tempo-api` skill (and any query-surface skill) to note the
      native IR surface alongside the compatibility dialects.
- [x] 11.3 Document the change stack + deferred follow-ups (correlate, structural
      traces, metrics model, field discovery; dialects-into-IR; promotion) so the
      dependency graph is discoverable.

## 12. Definition-of-Done gate

- [ ] 12.1 Surface parity confirmed: reachable in the UI, via the CLI, and via
      `POST /api/v1/query`.
- [ ] 12.1a Field-registry gating honoured: a query referencing a field with no
      canonical registry type returns a **defined, tested rejection** (not a
      silent success or an engine error). Full production field coverage is
      explicitly gated on the attribute-registry epic (#811); marking surface
      parity "done" does not imply #811-complete coverage — the rejection path is
      the contract until #811 lands.
- [ ] 12.2 OpenAPI spec updated and both clients (Rust SDK, TS) regenerated from
      it; each consumer uses its generated client, not raw HTTP.
- [ ] 12.3 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`,
      `cargo machete --with-metadata` clean; delta spec synced into
      `openspec/specs/` before archiving.
