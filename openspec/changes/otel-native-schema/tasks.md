# Tasks

Implemented as a dependent PR stack (see `design.md` — Migration Plan). Each `##`
group is a stack layer; later layers depend on earlier ones. Layer 0 is blocking.

## 0. Spike (blocking — feasibility + benchmark before any layout commit)

- [ ] 0.1 Prototype the warm typed containment index (typed generalization of `attr_tokens`) and prove row-group/file pruning for an unpromoted `key = value` predicate
- [ ] 0.2 Prove the datafusion-iceberg provider can present two attribute layouts (legacy `Map<String,String>` + typed home) under one table scan
- [ ] 0.3 Benchmark on real hive traces/logs: string-map vs typed map vs promoted column vs warm-index, across query classes incl. a **conflicted/off-type key**; measure **files-pruned %** (predict ~0 for the bare typed map), footer/metadata % on realistic **small flush files**, legacy mixed-scan coercion cost, residue parse cost, and **per-attribute registry-lookup cost** (not just builder count)
- [ ] 0.4 Record results; confirm no write-path regression before committing layout. (Variant is out of scope — opaque `Binary`/`unimplemented!` in the fork; no DataFusion type.)

## 1. `extract_value` fidelity fix (prereq for any losslessness claim)

- [ ] 1.1 Write failing tests: `BytesValue` round-trips as bytes (distinct from string); `StringValueStrindex` preserved; duplicate/ordered keys preserved (spec `typed-attribute-storage` — AnyValue-fidelity requirement)
- [ ] 1.2 Fix `conversion_common.rs` `extract_value` (+ the serde_json `Map`/BTreeMap key collapse) so bytes/interned/duplicate-ordered keys survive to the residue
- [ ] 1.3 `cargo test -p common` green; lint/format/machete

## 2. Logical schema + reconciliation of the two schema systems

- [ ] 2.1 Write failing tests: physical column names rejected on every query surface; computed/promoted/partition columns carry no logical meaning; `dropped_*` counts + log severity/flags first-class; arrays/kvlists retrievable-not-filterable; namespace shadowing rule (spec `otel-native-logical-schema`)
- [ ] 2.2 Define the canonical logical schema (resource→scope→signal, dotted OTel names, typed scalar `AnyValue`, `body` as `AnyValue`, record metadata, join keys; SignalDB-defined resource identity flagged non-native) in `common`
- [ ] 2.3 Refactor `schema_parser`/`schemas.toml` so the physical Iceberg schema is the declared realization of the logical schema; mark `computed`/promoted/partition as physical-only
- [ ] 2.4 Split the version clocks: logical (semconv snapshot) vs physical (storage migration); replace the conflated v1/v2 axis
- [ ] 2.5 `cargo test -p common` green; lint/format/machete

## 3. Type authority (one canonical type per tenant+dataset+field)

- [ ] 3.1 Write failing tests: precedence config→semconv-hint→observed-`AnyValue`; canonical type per (tenant,dataset), monotonic (later conflict does not retype); `schema_url` resource/scope-only hint, missing → observed without error; off-type value retained in residue not dropped/multi-homed (spec `attribute-type-authority`)
- [ ] 3.2 Implement the resolver: config override, pinned-semconv-snapshot hint keyed off resource/scope `schema_url`, observed-`AnyValue` default; record resolved type + source; per-(tenant,dataset) scope with cache invalidation on version bump (D9)
- [ ] 3.3 Expose off-type/conflict occurrences as discoverable metadata; wire the config override
- [ ] 3.4 `cargo test -p common` green; lint/format/machete

## 4. Tiered substrate: cold one-home + binary residue + warm index + coexistence

- [ ] 4.1 Write failing tests: canonical-typed value stored+retrieved typed (no cast); off-type/array/kvlist/bytes round-trip via binary residue; warm-index prunes unpromoted equality; unpromoted range = correct unpruned scan; legacy value uncoercible reads null not error (spec `typed-attribute-storage`, `query-ir-core` MODIFIED)
- [ ] 4.2 Add the cold substrate (one canonical typed home per field: per-type maps `attributes_str/_int/_double/_bool`) + self-describing binary residue in `common/iceberg/schemas.rs`, behind the logical→physical realization
- [ ] 4.3 Build the warm derived containment index (per-type tokens + list-leaf bloom); wire pruning into the scan
- [ ] 4.4 Implement registry typed resolution (promoted col | one typed home | residue) returning canonical-typed values by retrieval — no coalesce across homes, de-conflate cast-free from pruned
- [ ] 4.5 Coexistence read-path: legacy `Map<String,String>` safe-cast (null-on-fail) to canonical type; scope the no-cast guarantee to typed-substrate files
- [ ] 4.6 `cargo test -p common -p querier` green; lint/format/machete

## 5. Ingest enforcement (types stored at write, sender value never rewritten)

- [ ] 5.1 Write failing tests: canonical-typed value stored typed; off-type value retained losslessly in residue (never coerced-away or dropped); existing OTLP clients unchanged; conflict/off-type surfaced not silent (spec `ingest-type-enforcement`)
- [ ] 5.2 Route acceptor/writer through the registry to pick the canonical home or residue; replace `writer/src/storage/iceberg.rs` `json_strings_to_map_array` with a real 1→N transform stage (not a find-source-by-name shim); cache the per-attribute lookup
- [ ] 5.3 Keep Flight/WAL as JSON-in-Utf8; assert WAL byte-unchanged this phase
- [ ] 5.4 Surface off-type/conflict as metrics+logs (no silent drop)
- [ ] 5.5 `cargo test -p acceptor -p writer -p common -p tests-integration` (ingest→storage round-trip) green; lint/format/machete

## 6. Promotion as pure perf (budgeted, demotable) + the invariant test

- [ ] 6.1 Write the demote-and-still-correct invariant test: identical result set AND types with promotion off vs on, over canonical-typed fields (specs `typed-attribute-storage`, `query-ir-core` MODIFIED)
- [ ] 6.2 Promotion produces typed columns via **Iceberg field-id evolution** (not create-time `max(id)+1`); driven by `attr_demand`; per-table **budget + LRU demotion** (cold column folds back into the typed map on compaction)
- [ ] 6.3 `cargo test -p querier -p compactor -p tests-integration` green; lint/format/machete

## 7. Compactor rewrite of legacy layout

- [ ] 7.1 Failing test: compactor rewrites a legacy `Map<String,String>` file into the typed home + residue; queries identical before/after
- [ ] 7.2 Implement the rewrite pass; verify bounded Iceberg metadata growth (cf. #895)
- [ ] 7.3 `cargo test -p compactor -p tests-integration compactor` green; lint/format/machete

## 8. Typed metric substrate (replaces the data_json blob)

- [ ] 8.1 Failing tests: metric points/temporality/monotonicity/start_time typed (no blob parse); explicit + exponential histogram buckets typed; exemplar `trace_id`/`span_id` retrievable+joinable; Summary stored+returned as precomputed, `histogram_quantile` over Summary rejected (spec `typed-metric-storage`)
- [ ] 8.2 Add typed metric schemas (one metric model surface; bucket-native histogram/exp-histogram columns; exemplar keys) replacing `data_json`
- [ ] 8.3 Ingest + coexistence + compactor rewrite for metrics, parallel to attributes
- [ ] 8.4 `cargo test -p common -p writer -p tests-integration` green; lint/format/machete

## 9. Metric-native query operators

- [ ] 9.1 Failing tests: instant/range/scalar distinct relation types (mismatch = type error); temporality-aware rate/increase with start_time resets; histogram_quantile over typed explicit + exponential buckets; vector-matching output labels + many-to-many rejection; scalar envelope (spec `metric-native-query`)
- [ ] 9.2 Implement as **custom query-engine operators** (UDWF accumulators for rate/increase, array operators for quantiles, a label-set join + cardinality-validation node for vector matching) over the typed metric substrate — not SQL lowering
- [ ] 9.3 Re-express the PromQL dialect as a projection onto this model; `cargo test -p querier -p common -p tests-integration` green; lint/format/machete

## 10. Cross-signal correlation

- [ ] 10.1 Failing tests: logs-for-selected-traces across differing `trace_id` encodings; correlate on exemplar/resource-identity keys; enrichment fan-out cap deterministic+reported (inner/left only, NOT semi/anti); anti-join truth window-scoped + window widenable for late data; missing/dropped key rejected at validation (spec `cross-signal-correlate`)
- [ ] 10.2 Add the `correlate` stage (DAG/sub-pipeline typing, key validation + survival-through-aggregation, post-join namespacing)
- [ ] 10.3 Bespoke two-phase lowering: materialize the source time envelope, inject it as a literal scan bound on the target (not a free equi-join); wide-side pushdown only when canonical key == stored encoding, else correct-without-pushdown
- [ ] 10.4 Inner/semi/anti/left join kinds; `cargo test -p querier -p common -p tests-integration` green; lint/format/machete

## 11. Structural-trace matching

- [ ] 11.1 Failing tests: descendant matches at any depth OR explicit error (never silent cap); predicate references `events`/`links`; non-trace source rejected; `trace` envelope (spec `structural-trace-query`)
- [ ] 11.2 Implement the **per-trace evaluator** baseline (partition by `trace_id`, in-memory adjacency + closure) — recursive-CTE is not a viable strategy; materialized ancestry (writer+schema+Iceberg migration) is an optional fast-path sub-stack
- [ ] 11.3 `cargo test -p querier -p common -p tests-integration` green; lint/format/machete

## 12. Surface parity + subsumption + docs

- [ ] 12.1 Discovery/introspection over the logical schema + registry (sources, fields as dotted names + canonical type, value suggestions) — subsumes `query-field-discovery` build-side
- [ ] 12.2 Expose the query surfaces via HTTP API + regenerate `signaldb-sdk` (CLI) and the UI TypeScript client; UI/CLI consume only generated clients; update the OpenAPI spec
- [ ] 12.3 Archive superseded changes: `query-metrics-model`, `query-field-discovery`, `query-cross-signal-correlate`, `query-structural-traces`; reframe #811 to point here
- [ ] 12.4 Update docs/skills: `flight-schemas`, `storage-layout`, `adding-new-signal`, `tempo-api`, OTLP-ingestion, multi-tenancy/registry — to the logical/physical model (route via the docs skill)

## 13. Later stack layers (own changes — out of this charter's specs)

- [ ] 13.1 Delivery-side live tail + pagination over the IR — new change
- [ ] 13.2 Typed wire + WAL for full structured/duplicate-key fidelity — new change, **BREAKING**
