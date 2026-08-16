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

- [ ] 3.1 Failing test: building a `TraceV1ToV2Plan` for the current
      target schema version resolves every non-computed field to a
      registered extractor, and panics with a clear message on a field
      with no matching extraction rule.
- [ ] 3.2 Implement `TraceV1ToV2Plan`/`ColumnPlan`/`Extractor` and plan
      construction, selecting each field's extractor via the same match
      `transform_trace_v1_to_v2` uses inline today. Test from 3.1 passes.
- [ ] 3.3 Failing test: the plan for a given target version is built once
      and reused across repeated `transform_trace_v1_to_v2` calls (a
      call-counter on the build path, not timing-based).
- [ ] 3.4 Implement a `OnceLock`-cached plan keyed by target version. Test
      from 3.3 passes.

## 4. Migrate the writer v1→v2 transform

- [ ] 4.1 Failing test: plan-based `transform_trace_v1_to_v2` matches the
      existing hand-written output field-for-field (values, types,
      nullability) on the existing `#1208`-columns fixture and the
      missing-columns-tolerance fixture.
- [ ] 4.2 Implement plan-based `transform_trace_v1_to_v2`, replacing the
      per-batch `match field.name.as_str()` with plan execution. Tests
      from 4.1 pass, plus the full existing `schema_transform` test suite
      (unchanged behavior).
- [ ] 4.3 Benchmark: writer v1→v2 transform before/after using the bench
      from §2. Require no regression; record the improvement.
- [ ] 4.4 Delete the hand-written per-batch match arm; the plan becomes
      the only code path.

## 5. Docs and specs hygiene

- [ ] 5.1 Update the `flight-schemas` skill to describe
      `transform_trace_v1_to_v2` as plan-based, resolved once per target
      version and cached, rather than a per-batch field-name match.
- [ ] 5.2 Note in `docs/architecture/flight-communication.md` that the
      v1→v2 step resolves a materialization plan once per schema version.
- [ ] 5.3 Run `openspec validate --strict compiled-schema-materializer` and
      fix any findings before archiving.

## Not implemented in this change

Migrating OTLP→wire construction (`conversion_traces.rs`/logs/metrics) to
compiled plans — dropped in the scope correction above, same disposition
as `unified-table-schema`'s dropped wire-schema-generation goal. Remains
hand-written.
