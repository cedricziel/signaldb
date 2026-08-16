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

- [ ] 2.1 Add `writer/benches/schema_transform_benchmarks.rs`: a Criterion
      benchmark for `transform_trace_v1_to_v2` on a representative
      multi-span v1 batch, gated behind the existing `benchmarks` feature
      (same convention as `iceberg_benchmarks.rs`/
      `connection_pool_benchmarks.rs`).
- [ ] 2.2 Record a baseline run on the pre-plan hand-written
      implementation before starting the migration.

## 3. Plan and extractor registry (`writer::schema_transform`)

**Incorporates CodeRabbit review findings from PR #1230** (columnar
extractor contract, no first-use panic — see `design.md`'s corresponding
decisions).

- [ ] 3.1 Failing test: building a `TraceV1ToV2Plan` for the current
      target schema version resolves every non-computed field to a
      registered extractor, and returns a typed `Err` (never panics) on a
      field with no matching extraction rule.
- [ ] 3.2 Implement `TraceV1ToV2Plan`/`ColumnPlan`/`Extractor` (a boxed
      `Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync` — batch-in,
      array-out only, no per-row alternative) and plan construction,
      selecting each field's extractor via the same match
      `transform_trace_v1_to_v2` uses inline today. Test from 3.1 passes.
- [ ] 3.3 Failing test: the plan is built exactly once at writer startup
      (a call-counter on the build path, not timing-based) and every
      subsequent `transform_trace_v1_to_v2` call reads the already-built
      plan without re-resolving it.
- [ ] 3.4 Implement a `OnceLock` populated by an explicit startup call
      (wired into the writer's init path alongside its other startup
      calls), not by first use inside `transform_trace_v1_to_v2` — a bad
      rule reference fails the process before it serves traffic. Test from
      3.3 passes.

## 4. Migrate the writer v1→v2 transform

- [ ] 4.1 Failing test: plan-based `transform_trace_v1_to_v2` produces a
      `RecordBatch` identical to the existing hand-written output — full
      `Schema` equality (field names, order, types, nullability, metadata)
      and full `Array` equality column-for-column, row-for-row, not a
      values/types/nullability subset — on the existing `#1208`-columns
      fixture and the missing-columns-tolerance fixture. This fixture is
      kept permanently as a standing regression guard, not deleted once
      the migration lands.
- [ ] 4.2 Implement plan-based `transform_trace_v1_to_v2`, replacing the
      per-batch `match field.name.as_str()` with plan execution. Tests
      from 4.1 pass, plus the full existing `schema_transform` test suite
      (unchanged behavior).
- [ ] 4.3 Benchmark: writer v1→v2 transform before/after using the bench
      from §2. Require no regression; record the improvement.
- [ ] 4.4 Delete the hand-written per-batch match arm; the plan becomes
      the only code path. (The golden test from 4.1 stays — see its note.)

## 5. Docs and specs hygiene

- [ ] 5.1 Update the `flight-schemas` skill to describe
      `transform_trace_v1_to_v2` as plan-based, resolved once per target
      version and cached, rather than a per-batch field-name match.
- [ ] 5.2 Note in `docs/architecture/flight-communication.md` that the
      v1→v2 step resolves a materialization plan once per schema version.
- [ ] 5.3 Run `openspec validate --strict compiled-schema-materializer` and
      fix any findings before archiving.

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
