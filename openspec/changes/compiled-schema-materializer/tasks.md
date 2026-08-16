## 1. Prerequisites

**Scope correction found while confirming this task**: `unified-table-schema`
landed without the field-level provenance/extraction-rule metadata this
proposal originally planned to build on — that piece was dropped during
its implementation for the same structural reasons (wire/physical
divergence) this change's own OTLP→wire migration would have hit. See
`proposal.md`/`design.md`'s scope-correction notes. This change now
targets only the writer's v1→v2 transform, which needs nothing from
`unified-table-schema` beyond the `SCHEMA_DEFINITIONS`/`ResolvedSchema` API
that predates it.

- [x] 1.1 Confirmed `unified-table-schema` landed (#1241, #1243). Its
      field-level provenance/extraction-rule metadata did not land with it
      (dropped as unworkable) — not a blocker for this change's narrowed
      scope, which resolves extraction rules in Rust rather than from
      `schemas.toml`.
- [x] 1.2 Confirmed the acceptor OTLP-decode benchmark exists
      (`common/benches/ingest_and_wal.rs`'s `bench_ingest_decode`, covering
      `otlp_traces_to_arrow`). No writer v1→v2-transform benchmark existed
      — added one as part of this change (§2) rather than landing
      unmeasured.

## 2. Benchmark baseline

- [x] 2.1 Added `writer/benches/schema_transform_benchmarks.rs`: a
      Criterion benchmark for `transform_trace_v1_to_v2` on a
      representative 1,000-span v1 batch, gated behind the existing
      `benchmarks` feature.
- [x] 2.2 Recorded baseline: ~278µs/batch (~3.6 Melem/s) on the pre-plan
      hand-written implementation.

## 3. Plan and extractor registry (`writer::schema_transform`)

**Incorporates CodeRabbit review findings from PR #1230** (columnar
extractor contract, no first-use panic — see `design.md`'s corresponding
decisions).

- [x] 3.1 Added `build_trace_v1_to_v2_plan_for_errors_on_an_unregistered_field`:
      building a `TraceV1ToV2Plan` from a fabricated `ResolvedSchema`
      containing an unmatched field name returns `Err` naming the field,
      never panics.
- [x] 3.2 Implemented `TraceV1ToV2Plan`/`Extractor` (a boxed
      `Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync` — batch-in,
      array-out only, no per-row alternative) and
      `build_trace_v1_to_v2_plan_for`, selecting each field's extractor via
      the same match `transform_trace_v1_to_v2` used inline before.
- [x] 3.3 Added `trace_v1_to_v2_plan_is_built_once_and_reused`: proves
      `OnceLock` reuse via pointer identity across two calls (a call
      counter would need per-test isolation of the shared global static,
      which a real call counter can't get across the test binary's shared
      process — pointer identity is deterministic regardless of test
      execution order).
- [x] 3.4 Implemented `warm_trace_v1_to_v2_plan()` (public,
      idempotent) populating a `OnceLock`, wired into
      `IcebergTableWriter::new` for the traces table specifically — a bad
      rule reference fails table-writer construction with a clear error
      before the writer serves traffic, not on the first ingested batch.

## 4. Migrate the writer v1→v2 transform

- [x] 4.1 Added `transform_trace_v1_to_v2_produces_the_complete_expected_physical_v3_batch`:
      asserts full output `Schema` equality (all 30 physical-v3 fields:
      names, order, types, nullability) plus representative value checks
      across renamed fields (`span_name`, `service_name`,
      `span_attributes`), not just the 5 numeric fields the pre-existing
      tests already covered. Kept permanently, not deleted.
- [x] 4.2 Implemented plan-based `transform_trace_v1_to_v2`: builds
      `new_columns` by running each extractor in the cached plan, replacing
      the per-batch `match field.name.as_str()`. All pre-existing
      `schema_transform` tests pass unchanged (27/27), plus the new ones.
- [x] 4.3 Benchmark before/after: 278.0µs → 261.0µs per batch, a
      statistically significant ~6% improvement (Criterion: p = 0.00,
      "Performance has improved"). No regression.
- [x] 4.4 Deleted the hand-written per-batch match arm; plan execution is
      the only code path. The golden test from 4.1 stays.

## 5. Docs and specs hygiene

- [x] 5.1 Updated the `flight-schemas` skill: `transform_trace_v1_to_v2`
      described as plan-based, resolved once via `warm_trace_v1_to_v2_plan()`
      at writer startup, never panicking; noted logs/profiles/metrics stay
      hand-written (no v1→v2 split to migrate).
- [x] 5.2 Noted in `docs/architecture/flight-communication.md` that the
      v1→v2 step resolves a materialization plan once per schema version.
- [x] 5.3 `openspec validate --strict compiled-schema-materializer` passes.

## Not implemented in this change

**Explicit scope statement (CodeRabbit review, PR #1230, flagged this
needed stating plainly):** this change covers only traces'
`transform_trace_v1_to_v2`. Logs (`transform_logs_v1_to_iceberg`),
profiles (`transform_profiles_v1_to_iceberg`), and all five metrics
transforms are untouched and stay hand-written — none of them are v1→v2
in the traces sense (logs/profiles/metrics go straight from wire to
physical in one step with no intermediate version), and migrating OTLP→wire
construction for any signal (`conversion_traces.rs`/logs/metrics) is
dropped per the scope correction above, same disposition as
`unified-table-schema`'s dropped wire-schema-generation goal.
