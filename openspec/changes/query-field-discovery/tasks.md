# Tasks: Query Field Discovery

Groups ≈ PRs, sequenced per design "Migration Plan". TDD throughout: the failing
test precedes its implementation. Group 5 is the tier that makes value
suggestions statistics-served; the surface degrades honestly without it.

## 1. IR: `metadata` envelope, `describe` stage, discovery merge (crate `common`)

- [x] 1.1 Tests (`cargo test -p common query_ir`): a `describe` stage or
      `metadata` envelope declared under `irVersion` 1/2/3 is rejected with a
      typed error naming the required version (not coerced, not dropped), and an
      existing v1/v2/v3 document without them keeps validating unchanged; `describe` with a
      non-`metadata` envelope is rejected; `metadata` without a terminal
      `describe` is rejected; a `where` (or any record stage) before `describe`
      is rejected with an error naming the `aggregate`+`topk` equivalent; a
      stage after `describe` is rejected; `fields` on a `metadata` document is
      rejected
- [x] 1.2 Add `ResultEnvelope::Metadata` and `Stage::Describe(Describe { target,
  field, limit, sample })`; bump `MAX_IR_VERSION` to 4; extend the
      version-gating and envelope/terminal checks in `validate()` following the
      `heatmap`/`flamegraph` precedents
- [x] 1.3 Expose `validate_describe(&Document) -> Result<&Describe, IrError>`
      (version, source, range, stage legality — no field resolver needed) so the
      router can validate without a table schema
- [x] 1.4 Tests (`cargo test -p common discovery`): field merge is
      declared ∪ observed (never semconv membership); registry enrichment
      supplies type/brief/enum members for observed keys; coverage =
      present/total; a capped distinct estimate reports as a lower bound; absent
      stats yield absent hints; ordering is declared-first then coverage
      descending then name; truncation sets the flag
- [x] 1.5 Add `common::discovery`: `DiscoveredField`, `DiscoveredValue`,
      `DiscoveredSource`, `FieldOrigin`, `ValueOrigin`, `CardinalityEstimate`,
      `DiscoveryCost`, `MetadataResult`, and the pure merge functions over
      `LogicalSchema`, schema-registry resolutions, and
      `Vec<AttributeStatsRecord>` — no I/O, `serde` + `utoipa::ToSchema`
- [x] 1.6 Querier: an IR ticket carrying a `describe` stage returns a clear
      "not executable here" error rather than falling through the lowering match
      (`cargo test -p querier ir_`)

## 2. Router: the discovery surface (crate `router`)

- [ ] 2.1 Tests (`cargo test -p router discovery`): a `describe: fields`
      document returns declared + observed fields for the authenticated tenant
      only and dispatches no Flight ticket; a request for another tenant is
      rejected; the source's read scope is enforced; missing statistics produce
      the declared set plus a warning and `asOf: null`; `cost.windowScoped` is
      false on the metadata path
- [x] 2.2 `POST /api/v1/query`: when the terminal stage is `describe`, answer
      locally from `LogicalSchema::core()`, the tenant's schema registries, and
      `Catalog::get_attribute_stats` (the `promql::label_stats` pattern) instead
      of building a ticket
- [ ] 2.3 Tests: `describe: values` returns registry/intrinsic enumerations
      exactly with `origin: registry`; an uncovered field returns no values,
      `origin: unavailable`, and the hint naming the equivalent IR query, having
      read no data; `sample: true` returns sampled values with
      `cost.mode: sampled_scan` and a row count; `sample: true` without the
      source read scope is rejected before any read
- [x] 2.4 Implement the values path per design D5, reusing the existing bounded
      label/tag-value tickets for the opt-in sampled branch only
- [x] 2.5 Tests + implementation for `GET /api/v1/query/sources`: registered
      sources with availability from tenant table metadata; a signal with no
      data is available-and-empty, never omitted
- [x] 2.6 Bounds: `max_fields`/`max_values` caps with `truncated`; a `describe`
      response is capped independently of query limits
- [ ] 2.7 OpenAPI (spec regenerated; `cargo xtask generate` still owed): `#[utoipa::path]` for the new route and schemas for the
      metadata envelope; `UPDATE_OPENAPI=1 cargo test -p router
  openapi_spec_is_up_to_date`; `cargo xtask generate` for the Rust SDK and
      the TypeScript client
- [ ] 2.8 Instrumentation: discovery reads carry a boundary span through
      `common::self_monitoring::spans` so the metadata path's cost is visible in
      self-monitoring alongside query reads

## 3. CLI and MCP surfaces

- [ ] 3.1 Tests + implementation: `signaldb discover fields|values|sources`
      against the native surface through the generated SDK, printing name, type,
      origin, coverage, cardinality, and the response's cost line; the
      data-reading path requires an explicit `--sample` flag
- [ ] 3.2 Tests + implementation: MCP `discover_fields`, `discover_field_values`,
      `discover_sources` (read-only hints, cache hints per the MCP tool surface),
      wired through the SDK; the existing `discover_attributes` tool keeps its
      current compat-backed behaviour, unchanged
- [ ] 3.3 MCP tool-surface parity lists and their tests updated for the three new
      tools

## 4. Documentation

- [x] 4.1 `docs/users/querying-ir.md`: the `metadata` envelope, the `describe`
      stage, the tiers, the cost object, and the explicit statement that
      discovery is not window- or predicate-scoped
- [ ] 4.2 A discovery section for the CLI and MCP references; update any skill
      whose described behaviour changed (`tempo-api`, MCP tool surface)
- [ ] 4.3 `scripts/check-doc-freshness.sh "origin/main...HEAD"` clean

## 5. Value sketches: making the statistics tier real (crate `compactor`)

- [ ] 5.1 Tests (`cargo test -p compactor attr_stats`): the accumulator records
      per-key value counts bounded by the existing cardinality cap and yields a
      deterministic top-N with counts; keys above the cap are recorded as
      "too many values to suggest" rather than a misleading partial list
- [ ] 5.2 Extend `AttrStatsAccumulator` from a distinct-value set to counted
      values; emit a bounded top-N per key at flush
- [ ] 5.3 Catalog: `attribute_value_stats (tenant, dataset, signal, attr_key,
  value, count, updated_at)` on SQLite and PostgreSQL, with accessors and
      bounded per-key row replacement; tests on both backends
- [ ] 5.4 Router: values discovery prefers the sketch over the "unavailable"
      answer, reporting `origin: statistics`, approximate, with `asOf`
- [ ] 5.5 Tests: a field covered by a sketch answers from metadata with counts
      and reads no signal data

## 6. Close-out

- [ ] 6.1 `cargo fmt`; targeted clippy per touched crate; `cargo machete
  --with-metadata`
- [ ] 6.2 Integration coverage in `tests-integration` for the end-to-end
      discovery path (declare the new test file in `tests/main.rs`)
- [ ] 6.3 Close #820; comment the handover on #437 (live tail) and cross-link
      #813, #819, #818, #769, #732; file the Explore-UI field-picker swap as a
      follow-up under #769
