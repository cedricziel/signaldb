## 1. Prerequisites

- [ ] 1.1 Confirm `unified-table-schema` has landed and its field-level
      provenance/extraction-rule metadata is available to resolve against.
      If not landed yet, this change is blocked — do not proceed with a
      parallel, divergent metadata model.
- [ ] 1.2 Confirm `performance-benchmarking-suite`'s acceptor-OTLP-decode
      and writer-append benchmarks exist and have a recorded baseline on
      `main`. If not, add the minimum needed baseline first rather than
      landing this change unmeasured.

## 2. Golden references

- [ ] 2.1 Capture representative-fixture golden output (values, types,
      nullability) for `transform_trace_v1_to_v2`'s current hand-written
      behavior.
- [ ] 2.2 Capture the same for `otlp_traces_to_arrow`'s current
      hand-written behavior.

## 3. Plan and extractor registry (`common`)

- [ ] 3.1 Failing test: building a `MaterializationPlan` for a schema
      version resolves every field to its registered extraction rule, and
      fails fast (not silently) on an unregistered rule name.
- [ ] 3.2 Implement `MaterializationPlan`, the extraction-rule registry,
      and plan construction. Test from 3.1 passes.
- [ ] 3.3 Failing test: a plan resolved for a given version pair is
      reused (not rebuilt) across repeated materialization calls (assert
      via a call counter on the resolution path, not by timing).
- [ ] 3.4 Implement the version-keyed plan cache. Test from 3.3 passes.
- [ ] 3.5 Failing test: every field in every current schema version
      (traces, logs, metrics) resolves to a registered rule — catches a
      typo'd or missing rule reference before it reaches runtime.
- [ ] 3.6 Populate the initial extraction-rule set covering every field in
      every current schema version. Test from 3.5 passes.

## 4. Migrate the writer v1→v2 transform (the identified inefficiency)

- [ ] 4.1 Failing test: `transform_trace_v1_to_v2` via plan execution
      matches the golden reference from 2.1.
- [ ] 4.2 Implement plan-based `transform_trace_v1_to_v2`. Test from 4.1
      passes.
- [ ] 4.3 Benchmark: writer v1→v2 transform before/after, using
      `performance-benchmarking-suite`'s harness. Require no regression;
      record the improvement.
- [ ] 4.4 Delete the hand-written `get_column_by_name`-per-field code path
      and the golden reference from 2.1 (job done).

## 5. Migrate OTLP → wire construction

- [ ] 5.1 Failing test: `otlp_traces_to_arrow` via plan execution matches
      the golden reference from 2.2.
- [ ] 5.2 Implement plan-based `otlp_traces_to_arrow`. Test from 5.1
      passes.
- [ ] 5.3 Repeat 5.1/5.2 for the logs and metrics OTLP→wire conversion
      functions.
- [ ] 5.4 Benchmark: acceptor OTLP-decode before/after. Require no
      regression against the baseline from task 1.2.
- [ ] 5.5 Delete the hand-written match-arm code paths and the golden
      reference from 2.2 (job done).

## 6. Docs and specs hygiene

- [ ] 6.1 Update the `flight-schemas` skill to describe materialization as
      plan-based, extractor-registered, resolved once per version.
- [ ] 6.2 Note in `docs/architecture/` (per the `architecture` skill's
      source list) that the write path resolves a materialization plan
      once per schema version rather than dispatching per field per batch.
- [ ] 6.3 Run `openspec validate --strict compiled-schema-materializer` and
      fix any findings before archiving.
