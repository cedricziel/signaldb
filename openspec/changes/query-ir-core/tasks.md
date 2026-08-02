## 1. Type system + versioning foundation (`common`)

- [ ] 1.1 Failing unit test (`cargo test -p common`): value-type coercion —
      duration suffixes (`"500ms"`), numeric strings, RFC3339/relative timestamps
      coerce to the registry canonical type; an un-coercible literal is rejected
      at validation, never silently cast.
- [ ] 1.2 Failing unit test: absent-value semantics — `exists` is the only op
      true on an absent field; `not(field = x)` does not match absent rows;
      results are independent of engine null behaviour.
- [ ] 1.3 Failing unit test: relation-type inference — a pipeline's relation type
      (RowSet/Series, source, grain, columns) is inferred stage-by-stage, and a
      stage whose input constraint is unmet fails validation naming the stage.
- [ ] 1.4 Failing unit test: IR versioning — a supported `irVersion` parses; an
      out-of-range version is rejected reporting the supported range; a document
      with a new optional field still validates (additive tolerance); a stage
      object with an unknown key is rejected (`deny_unknown_fields`
      physical-addressing guard).
- [ ] 1.5 Failing unit test: declared-envelope validation — `rows`⇔
      `RowSet{aggregated=false}`, `table`⇔`RowSet{aggregated=true}`, `series`⇔
      `Series`; declaring `series` over a RowSet terminal (or `rows` over a
      grouped aggregate) is rejected with an envelope-mismatch error.
- [ ] 1.6 Failing unit test: extensible-source forward-compat — the `from` source
      is resolved against the source registry; an unregistered source is rejected
      with a clear unknown-source error (not a parse failure), and a previously
      valid document still validates unchanged after a new source is registered
      (document shape is source-independent).
- [ ] 1.7 Implement the IR document types, `ValueType`/`RelationType` (incl. the
      `aggregated` discriminator), the versioned operator/function registry,
      coercion, the source registry, and the validator. Make 1.1–1.6 pass.

## 2. Predicate grammar + structured operands (`common`)

- [ ] 2.1 Failing unit test: nested `and`/`or`/`not` with all operators parses;
      a leaf naming a physical column or `attributes_json` is rejected
      (logical-namespace guard).
- [ ] 2.2 Failing unit test: structured operands — `aggregate`/`order`/`topk`
      operands are structured values; an operand supplied as an expression string
      is rejected.
- [ ] 2.3 Implement the `Predicate` enum and structured `Agg`/`Order`/`topk`
      operands. Make 2.1–2.2 pass.

## 3. Attribute-registry resolver interface (`common`)

- [ ] 3.1 Failing unit test: resolving a logical field yields either a column ref
      or an `attributes_json` path plus the canonical type; unpromoted → json
      path, promoted → column.
- [ ] 3.2 Implement the resolver interface (field → `Column | JsonPath` + type)
      as a **consumer of the attribute-registry epic (#811)** — a query-facing
      view, not a re-implementation; provide a config/in-memory fallback for
      tests. Make 3.1 pass. (If #811's registry API is not yet available, adapt
      the existing slug/registry accessor and leave a TODO referencing #811.)

## 4. IR → LogicalPlan planner, single-signal (`querier`)

- [ ] 4.1 Failing unit test (`cargo test -p querier`): `from(logs) + where +
aggregate(step)` lowers to TableScan→Filter→Projection(date_bin)→
      Aggregate→Sort, with an unpromoted-field filter emitted as a JSON
      extraction, satisfying the denotational spec on a fixture.
- [ ] 4.2 Failing unit test: promotion invariance — the same IR lowers to a
      column-ref filter when the field is promoted and a json-path filter when
      not, and both execute to the same result over a fixture table.
- [ ] 4.3 Failing unit test: `from(traces) + where + topk` (single-signal trace
      query) lowers and executes; `extract` on `traces` is rejected (log-only).
- [ ] 4.4 Failing unit test (**execution-level**, `cargo test -p querier`):
      absent-value semantics hold in the _lowered plan_, not just the type layer —
      `not(field = x)` over a fixture where some rows lack `field` excludes those
      rows, proving the result is independent of DataFusion's SQL NULL behaviour
      (the guarantee task 1.2 asserts at the type level).
- [ ] 4.5 Failing unit test: curated projection — a `rows` result returns only
      the curated/explicit field set, never all physical columns (`SELECT *`),
      including for a source with many promoted columns.
- [ ] 4.6 Implement the single-signal planner (from/where/extract/aggregate/
      topk/order/limit) → `LogicalPlan`. `extract` v1 = `json` + `logfmt`;
      predicate `regex` and the deferred `regex` extract parser run behind a
      bounded, timeout-guarded matcher. Make 4.1–4.5 pass.

## 5. Querier Flight ticket (`querier`)

- [ ] 5.1 Failing test: a `query_ir:{tenant}:{dataset}:{json}` ticket is
      dispatched, planned, executed, and streams RecordBatches tagged with the
      declared envelope; a malformed ticket returns `invalid_argument`.
- [ ] 5.2 Implement the ticket branch in `src/querier/src/flight.rs` alongside
      the existing `query_*` prefixes. Make 5.1 pass.

## 6. Router endpoint + OpenAPI (`router`)

- [ ] 6.1 Failing test (`cargo test -p router`): `POST /api/v1/query` validates
      auth/tenant headers, forwards the IR as a `query_ir` ticket, and streams
      the enveloped result; unauthenticated/invalid requests are rejected.
- [ ] 6.2 Implement `src/router/src/endpoints/query.rs`; register it in
      `endpoints/mod.rs`. Make 6.1 pass.
- [ ] 6.3 Add the endpoint + versioned IR request schema + result-envelope
      schemas to the code-first OpenAPI (`src/router/src/openapi.rs`); assert the
      spec snapshot test covers the new operation.

## 7. Generated clients (SDK parity)

- [ ] 7.1 Regenerate the Rust SDK (`src/signaldb-sdk`) from the updated OpenAPI;
      smoke test that the generated `query` operation compiles and round-trips an
      IR request type.
- [ ] 7.2 Regenerate the TypeScript client (`src/ui/src/api/gen`); assert the
      generated `query` operation and IR/envelope types are present.

## 8. CLI surface (`signaldb-bin` / SDK consumer)

- [ ] 8.1 Failing test: a CLI `query` command reads an IR query (file/stdin),
      submits it via the generated Rust SDK, and prints the enveloped result.
- [ ] 8.2 Implement the command against the generated SDK (no hand-written HTTP).
      Make 8.1 pass.

## 9. UI query builder, single-signal (`ui`)

- [ ] 9.1 Failing component test (`src/ui`): a builder appends stage objects
      (from(logs) → where(FilterChips) → aggregate) and emits a valid IR document
      via the generated TS client — no dialect-string compilation in the browser.
- [ ] 9.2 Failing component test: each envelope renders with the right view —
      `rows`→log/span list, `series`→chart, `table`→topN — chosen from the
      declared envelope before results arrive.
- [ ] 9.3 Implement the builder + renderers in `src/ui/src/features/explore`
      (+ a `query-builder` lib), logs and traces, consuming only the generated
      client. Make 9.1–9.2 pass.

## 10. Cross-service integration (`tests-integration`)

- [ ] 10.1 Failing E2E: ingest logs+traces; a single-signal logs IR query
      returns results equivalent to the LogQL equivalent.
- [ ] 10.2 Failing E2E: a single-signal traces IR query (filter + topk) returns
      the expected spans.
- [ ] 10.3 Regression assertion: existing TraceQL/LogQL/PromQL E2E queries still
      pass unchanged (additive, non-regressing).

## 11. Docs + skills

- [ ] 11.1 Add user + API documentation for the IR core and `POST /api/v1/query`
      (route via the docs skill; correct audience/frontmatter): the type system,
      versioning policy, stage set, and a worked single-signal example.
- [ ] 11.2 Update the `tempo-api` skill (and any query-surface skill) to note the
      native IR surface alongside the compatibility dialects.
- [ ] 11.3 Document the change stack + deferred follow-ups (correlate, structural
      traces, metrics model, field discovery; dialects-into-IR; promotion) so the
      dependency graph is discoverable.

## 12. Definition-of-Done gate

- [ ] 12.1 Surface parity confirmed: reachable in the UI, via the CLI, and via
      `POST /api/v1/query`.
- [ ] 12.2 OpenAPI spec updated and both clients (Rust SDK, TS) regenerated from
      it; each consumer uses its generated client, not raw HTTP.
- [ ] 12.3 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`,
      `cargo machete --with-metadata` clean; delta spec synced into
      `openspec/specs/` before archiving.
