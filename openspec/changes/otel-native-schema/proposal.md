## Why

SignalDB claims to be OpenTelemetry-native, but nativeness today holds only at
the ingest door and — via `query-ir-core` — at the query door. The **storage
door is not native**, and the type loss happens at two hops, not one:
`conversion_common.rs` `extract_value` already destroys `BytesValue` (→ UTF-8 or
empty) and `StringValueStrindex` (→ null) and collapses duplicate/ordered keys
(serde_json `Map`) at the OTLP→internal boundary; the writer's
`json_strings_to_map_array` then stringifies the surviving scalars into
`Map<String,String>` (`http.response.status_code=200` → `"200"`). (Int/double/bool
do survive the JSON-in-Utf8 carrier — `serde_json` has native i64/f64, so the
carrier is not the culprit for scalars; the losses are in `extract_value` and the
final map coercion.) Because the physical substrate is untyped, the attribute
registry that `query-ir-core` specifies can only resolve a logical field to _"a
promoted column or an attribute-JSON extraction"_ — i.e. it **reconstructs the
declared type by casting a stringified value at read time** rather than retrieving
a stored type. And ingest never passes through the logical schema at all, so the
registry's canonical type is asserted at plan time but never _enforced at write_.

The result is that a single idea — an OTel-native logical schema, backed by a
typed physical substrate, mediated by one registry — is scattered across five
partial, drifting efforts (`query-metrics-model`, `query-field-discovery`,
`query-cross-signal-correlate`, `query-structural-traces`, and the #811
attribute-registry epic), each rebuilding at query time the types and structure
that were discarded at ingest. This change consolidates them into one canonical
model so the fragments resolve to a single contract instead of five half-native
subsystems.

This is a **charter change**: it establishes the principle, the logical
contract, the type authority, the typed substrate, and the ingest-enforcement
requirement now, and sequences implementation as a dependent PR stack (see
`design.md` / `tasks.md`). It does not attempt to land ingest→storage→query in
one unit.

## What Changes

- **Logical/physical separation becomes an explicit, enforced principle.**
  "OTel-native" is defined as a property of the **logical schema only**. The
  physical schema is an implementation detail (typed maps, promoted columns,
  partitions, hex-ID encodings, per-type metric tables) that never leaks to any
  query or dialect surface. Queries and dialects bind to logical dotted
  OTel-native names; the registry is the sole logical→physical bridge.

- **One canonical OTel-native logical schema** spanning resource → scope →
  signal, shared by ingest, storage, query, and all compatibility dialects:
  typed `AnyValue` scalar attributes, arrays/kvlists retrievable (not necessarily
  filterable), log `body` as `AnyValue` (not string), OTLP record metadata
  (`dropped_*` counts, log severity/flags) first-class, a single metric model
  (type + temporality + monotonicity + points + exemplars, not five leaked
  tables), and cross-signal join keys (`trace_id`, `span_id`, exemplars) — plus a
  **SignalDB-defined** (not OTel-native) resource identity from a configured
  attribute subset.

- **Type authority: the stored value is always `AnyValue`-as-sent; precedence
  selects the canonical typed home, it never rewrites the sender's value.** One
  canonical type per (tenant, dataset, field), chosen by precedence **config →
  semconv hint → observed `AnyValue`**, monotonic (later conflicting data does not
  retype). Semconv is a _hint_ from a pinned snapshot keyed off resource/scope
  `schema_url` (OTLP has no record-level `schema_url`); cross-version semconv
  renaming is out of scope. Values that do not match the canonical type are
  retained losslessly in a residue, not dropped and not multi-homed.

- **A tiered typed physical substrate.** (1) **Cold**: one canonical typed home
  per field (a per-type map or promoted column) + a self-describing **binary**
  residue (CBOR/msgpack, not JSON) for off-type/array/kvlist/bytes values. (2)
  **Warm**: a derived typed containment index (a typed generalization of today's
  `attr_tokens`) — the only thing that prunes before promotion, because Parquet
  keeps no per-key stats inside a map. (3) **Hot**: promoted typed columns
  (stats + bloom). **BREAKING** (on-disk Iceberg layout): new typed columns are
  _added_; legacy `Map<String,String>` files are read via safe coercion and
  rewritten by the compactor (the old map columns persist in old files until
  rewritten). Also fixes `extract_value` so bytes and interned strings survive;
  duplicate-key/order fidelity is deferred (needs acceptor-side residue or phase 2).

- **The promotion-is-only-perf invariant is load-bearing and testable — and now
  actually holds**, because one canonical home means resolution never coalesces
  across competing typed homes. Turn off all promotion → identical results and
  types, only slower. Promotion is demand-driven (`attr_demand`), uses Iceberg
  field-id evolution, and is **bounded by a per-table budget with LRU demotion**
  (cold columns fold back into the typed map on compaction) so live-schema width
  does not grow unbounded as the hot-key set drifts (metadata churn rides #895).
  Two performance properties are kept distinct: **cast-free retrieval** (from any
  typed home) is not the same as **pruning/pushdown** (only from a promoted column
  or the warm index).

- **Ingest enforces the registry at write.** The acceptor/writer path routes
  OTLP `AnyValue` through the registry to pick each value's canonical typed home
  (or the residue), so types are **stored, not reconstructed** — without rewriting
  the sender's value: an off-type value is retained losslessly in the residue,
  never coerced-away or dropped. **BREAKING** (storage layout; ingest now types at
  write). Wire/WAL stay JSON-in-Utf8 in phase 1.

- **A typed metric substrate** so the one metric model is actually queryable:
  bucket-native histograms (explicit bounds/counts; exponential scale/offset),
  typed temporality/monotonicity/start-time, and exemplar `trace_id`/`span_id`
  exposed as join keys — replacing the `data_json` blob. Summary metrics are
  passthrough (precomputed quantiles), not recomputable histograms. **BREAKING**
  (on-disk metric layout).

- **The two schema systems are reconciled.** Today `query_ir` (logical
  namespace + registry) and `schema_parser`/`schemas.toml` (storage schema with
  `computed`/materialized mixed in) are independent. The storage schema becomes
  _the declared physical realization of the logical schema_, with
  `computed`/promoted/partition as physical-only annotations, and two
  independent evolution clocks: logical (semconv/`schema_url`) vs. physical
  (storage migrations).

- **Subsumes and supersedes** `query-metrics-model`, `query-field-discovery`,
  `query-cross-signal-correlate`, `query-structural-traces`, and the #811
  registry epic. The metric-native query model, cross-signal correlation, and
  structural-trace matching are folded in **as requirements here**
  (`metric-native-query`, `cross-signal-correlate`, `structural-trace-query`);
  field discovery/introspection is folded into the logical-schema + type-authority
  surfaces, with its delivery-side tail/pagination named as a later stack layer in
  `design.md`; the standalone stubs are marked for archival.

## Capabilities

### New Capabilities

- `otel-native-logical-schema`: the canonical OTel-native logical contract
  (resource→scope→signal, dotted names, one metric model, `body` as `AnyValue`,
  cross-signal join keys) and the logical/physical separation principle,
  including reconciling the two existing schema systems into one logical truth
  with a physical realization.
- `attribute-type-authority`: one canonical type per (tenant, dataset, field),
  monotonic, chosen by precedence config → semconv-hint → observed `AnyValue`
  (stored value is always as-sent; off-type values go to the residue); semconv is
  a pinned-snapshot hint keyed off resource/scope `schema_url`.
- `typed-attribute-storage`: the tiered substrate — cold one-home typed store +
  binary residue, warm derived containment index (the only pre-promotion pruning),
  hot budgeted+demotable promoted columns — plus the `extract_value` fidelity fix
  and the (now clean) promotion-is-only-perf invariant.
- `typed-metric-storage`: typed OTLP metric substrate — bucket-native histograms,
  typed temporality/monotonicity, first-class exemplar join keys, Summary as
  passthrough — replacing the `data_json` blob (subsumes `query-metrics-model`'s
  storage need).
- `ingest-type-enforcement`: routing OTLP ingest through the logical
  schema/registry so canonical types are asserted and stored at write, not
  reconstructed at read.
- `metric-native-query`: the metric-native query sub-model over the one logical
  metric model — distinct instant/range/scalar relation types, temporality- and
  histogram-aware functions computed over OTLP structure, vector-matching
  arithmetic, and the scalar result envelope (subsumes `query-metrics-model`).
- `cross-signal-correlate`: a `correlate` stage joining the current relation to
  another signal by a shared logical key (incl. exemplar and resource-identity
  keys), with bounded fan-out, time-window-scoped target scans, and an
  inner/semi/anti/left join taxonomy (subsumes `query-cross-signal-correlate`).
- `structural-trace-query`: a `match` stage relating named span-sets by hierarchy
  (child/descendant/ancestor/sibling, incl. `events`/`links`) with descendant
  correctness independent of execution strategy and no silent depth cap (subsumes
  `query-structural-traces`).

### Modified Capabilities

- `query-ir-core`: extend registry-mediated resolution so a logical field
  resolves to a **typed** physical location (promoted column | typed store |
  residue), the canonical type is **enforced at write** (not only coerced at
  plan time), and results carry the canonical type rather than a
  stringified/reconstructed value.

## Impact

- **common** (`schema/`, `iceberg/schemas.rs`, `schema_parser.rs`, `query_ir/`,
  `attr_demand.rs`, `flight/conversion/*`): logical schema definition, typed
  substrate schema, type-authority resolver, reconciliation of the two schema
  systems. **BREAKING** Flight wire schema if typed attributes move onto the
  wire (staged; phase 1 keeps JSON-in-Utf8 as a transitional carrier).
- **acceptor / writer**: ingest-through-registry type enforcement; the
  `json_strings_to_map_array` write hop replaced by type-aware encoding.
  **BREAKING** on-disk Iceberg attribute layout; WAL untouched in phase 1.
- **querier / router / tempo-api**: dialects (TraceQL/LogQL/PromQL) re-expressed
  as projections onto the one logical schema; registry resolves to the typed
  substrate; typed predicates gain pushdown.
- **compactor**: demand-driven, batched promotion of hot keys to typed columns;
  rewrite of legacy `Map<String,String>` files to the typed substrate; bounded
  Iceberg metadata growth (cf. #895).
- **ui / cli / signaldb-sdk / ui client**: explore/discovery surfaces bind to
  logical dotted names and canonical types (subsumes `query-field-discovery`).
- **openspec/changes**: archive `query-metrics-model`, `query-field-discovery`,
  `query-cross-signal-correlate`, `query-structural-traces`; reframe #811.
- **docs / skills**: `flight-schemas`, `storage-layout`, `adding-new-signal`,
  `multi-tenancy` (registry), and OTLP-ingestion docs updated to the
  logical/physical model.
