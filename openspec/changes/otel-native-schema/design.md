## Context

See `proposal.md` — Why. The load-bearing facts that shape the approach:

- `query-ir-core` (merged) already ratifies the logical/physical split at the
  **query door**: queries name logical dotted OTel names, the registry owns each
  field's canonical type, promotion is pure perf, physical column names are
  rejected in queries. The substrate it resolves to, however, is untyped —
  "promoted column **or attribute-JSON extraction**" over `Map<String,String>`.
- Type loss happens at **two** hops, corrected from the first draft. (1)
  `conversion_common.rs` `extract_value` already destroys `BytesValue` (→ UTF-8
  or empty), drops `StringValueStrindex` (→ null), and collapses duplicate/ordered
  keys (serde_json `Map` = BTreeMap) at the OTLP→internal boundary. (2)
  `writer/src/storage/iceberg.rs` `json_strings_to_map_array` then stringifies the
  surviving scalars into `Map<String,String>`. **Int/double/bool and full i64 DO
  survive the JSON-in-Utf8 carrier** (`serde_json` has native i64/u64/f64, no
  `arbitrary_precision`) — the wire is not the culprit for scalars, so phase-1
  typing of scalars from the JSON carrier is feasible; bytes/interned/dup-keys are
  lost earlier and need the `extract_value` fix.
- Two uncoordinated schema systems exist: `query_ir` (logical namespace +
  registry) and `schema_parser`/`schemas.toml` (physical Iceberg schema, with
  `computed`/materialized-label concerns mixed in, versioned v1/v2).
- Parquet keeps **no per-key statistics or bloom filters inside a MAP** (stats/
  blooms are per-leaf: the `key` leaf and the `value` leaf span all keys). So a
  typed map is cast-free but **unprunable**; only a promoted column or a derived
  containment index prunes. The codebase already learned this — `attr_tokens`
  (schema/mod.rs) is a derived tokenized-list column with a bloom, built precisely
  because the map itself does not prune.
- Variant is **not a usable target in this fork**: `PrimitiveType::Variant` exists
  only as a spec enum; it maps to opaque `DataType::Binary`
  (`iceberg-rust-spec/.../arrow/schema.rs`), `Value::try_from_bytes` → `NotSupported`,
  and the shredding/reader/writer suite is `unimplemented!`. DataFusion has no
  Variant type either. Variant is therefore out of scope as a deliverable.
- `attr_demand.rs` already records per-key query demand for the compactor's
  promotion analyzer; #895 bounds Iceberg metadata growth. Both are levers this
  design reuses rather than reinvents.

## Goals / Non-Goals

**Goals:**

- Make "OTel-native" a precise, testable property of the **logical schema**, and
  demote every physical-shape decision (typed maps, promoted columns,
  partitions, ID encodings, per-type metric tables) below the registry so it
  never leaks to a query or dialect surface.
- Give the registry a **typed** physical resolution target so a logical field's
  canonical type is _retrieved_, not _reconstructed by cast_.
- Enforce the canonical type at **write** (ingest through the logical
  schema/registry), so types are stored, not rebuilt at read.
- Establish the invariant tests and the reconciliation of the two schema systems
  as the spine the subsumed fragments hang off.

**Non-Goals:**

- Landing ingest→storage→query in one unit. This is a charter; §Migration Plan
  sequences a dependent PR stack.
- Parquet `Variant` — out of scope as a deliverable (not usable in this fork; see
  Context). The binary residue keeps a future Variant path open.
- Phase-1 fidelity is scoped: the Flight/WAL wire stays JSON-in-Utf8; scalars
  (incl. full i64) and — via the `extract_value` fix — **bytes and interned strings**
  survive it. **Duplicate keys and key order do not** survive a `serde_json::Map`
  round-trip, so that fidelity requires building the binary residue at the acceptor
  _before_ JSON serialization, or the typed-wire phase — it is not delivered by the
  `extract_value` fix alone (corrected per review).
- Cross-version semconv attribute renaming (schema transformation) — hints come
  from one pinned semconv snapshot.
- Redesigning the compaction engine or partition strategy beyond what
  demand-driven typed-column promotion requires.

## Decisions

### D1 — Three doors, one contract: logical schema is the only nativeness surface

Ingest, query, and every dialect bind to one canonical **logical schema**
(resource→scope→signal; dotted OTel names; typed `AnyValue`; log `body` as
`AnyValue`; one metric model; `trace_id`/`span_id`/resource-identity/exemplars
as join keys). The **registry** is the sole logical→physical bridge, consulted
at both write and read. The **physical schema** is free to be arbitrarily
clever/ugly.

_Why:_ the query door already works this way; the failure is that the ingest
door bypasses the contract and the substrate under it is untyped. One contract,
enforced at all three doors, is the whole idea. _Alternative rejected:_
per-dialect logical schemas (status quo) — guarantees drift and re-leaks
physical shape into queries.

### D2 — Type authority: stored value is AnyValue-as-sent; precedence picks the canonical home, never rewrites

The stored value is **always** the `AnyValue` as sent. The registry owns one
canonical type per **(tenant, dataset, field)**, where **`field` is the full
logical identity — signal + attribute level (resource/scope/record) + dotted name**
(so same-named resource and record attributes are distinct fields). This full
identity keys the registry, the resolution cache, and the promoted-column set
alike. Chosen by precedence: (1) config
override; else (2) a **semconv type hint** from a pinned snapshot, selected by the
applicable **resource-/scope-level** `schema_url`; else (3) the observed `AnyValue`
type (first-seen). Precedence only selects which typed home is canonical — it
never coerces/rewrites the sender's value. The canonical type is **monotonic**:
later conflicting data does not retype the field or existing rows.

_Why:_ semconv types are advisory recommendations, not a license for a backend to
irreversibly retype a sender's bytes — coercing-at-write would be data corruption
contradicting the lossless rule (reviewer convergence). OTLP has `schema_url` only
on Resource/Scope and it is usually empty, so tier (3), observed AnyValue, is the
**primary** path in practice, not a fallback. _Alternatives rejected:_ semconv-
coerce-at-write (corrupts data); per-record schema_url typing (no such field
exists; and it makes a field's type a function of each row, which
`query-ir-core`'s single-type literal coercion cannot bind to). Cross-version
semconv **renaming** (schema transformation) is out of scope — hints come from one
pinned snapshot.

### D3 — One canonical home; off-type values go to a lossless binary residue (no multi-home)

A field lives in **exactly one** canonical typed home. A value whose sent type does
not match the canonical type — and every array/kvlist/bytes value — is retained in
a self-describing **binary** residue (CBOR/msgpack), retrievable but not
typed-queryable. A field is **never** scattered across multiple typed homes.

_Why:_ the earlier multi-home + coalesce design was refuted by four reviewers — DF
`coalesce`/`CASE` coerce branches to a common supertype (→ re-stringify) or
`try_cast` back, i.e. exactly the read-time reconstruction this change exists to
abolish, and it made the promotion invariant (D5) false for conflicted keys. One
home makes resolution a pure retrieval and keeps losslessness via the residue.
_Trade-off:_ off-type occurrences are retrievable but not filterable until an
operator repins the type; that is the honest cost of never corrupting the value.

### D4 — Tiered substrate: cold one-home store + binary residue, warm derived index, hot budgeted promotion

- **Cold (lossless).** One canonical typed home per field — a per-type map
  (`attributes_str/_int/_double/_bool`) or a promoted column — plus the binary
  residue for off-type/array/kvlist/bytes.
- **Warm (the only pre-promotion pruning).** A derived typed containment index —
  a typed generalization of `attr_tokens` (per-type `key→value` tokens with a
  bloom on the list leaf). This is what prunes unpromoted equality predicates;
  the typed map itself does not prune (no per-key Parquet stats).
- **Hot (fast).** Demand-driven promotion (`attr_demand`) to typed columns with
  stats + bloom, via **Iceberg field-id evolution**, bounded by a **per-table
  budget with LRU demotion** (a cold column folds back into the typed map on
  compaction), so live-schema width does not grow unbounded as the hot set drifts.

_Why not Variant (was "option B"):_ removed from the decision surface — in this
fork Variant is opaque `Binary` with `unimplemented!` shredding and DataFusion has
no Variant type; making it real is a multi-repo, multi-quarter upstream effort, not
a spike. The binary residue is forward-compatible if Variant ever lands. _Why the
warm index:_ "typed maps gain pushdown" is false at the Parquet layer; without the
index the warm tier is an unpruned (if cast-free) scan.

### D5 — Promotion is only ever performance — and now it actually holds

Because D3 gives each field one canonical home, resolution never coalesces across
competing homes, so promotion (moving that home's key to a top-level column) cannot
change results or types. The **testable invariant**: identical result set AND types
with all promotion off vs. on — scoped to canonical-typed fields (residue values
are retrievable, a separate axis). This is strictly stronger than `query-ir-core`'s
original "same-result" (value equality over a cast) because there is no cast.

### D9 — Registry consistency: monotonic, per tenant+dataset, cache-invalidated on version bump

The registry's canonical type per (tenant, dataset, field) — `field` being the full
signal+level+name identity from D2 — is monotonic (D2) — so
already-written data never disagrees with a later type; new conflicting values go
to the residue rather than flipping the type. Write-path and plan-path read the
same versioned resolution; a config/schema-version bump is the only mutation and it
invalidates cached resolutions. This closes the "mutable derived source of truth
with no invalidation" hole and prevents cross-tenant type contamination.

**Migration rule for a canonical-type change.** When a config/version bump changes a
field's canonical type, existing rows in the old typed home are **not** retyped in
place (monotonicity). They remain readable through the coexistence read-path (the
same machinery that reads legacy `Map<String,String>` files): a value that
safe-casts to the new type reads as the new type, one that does not reads via the
residue. The compactor migrates old-home values forward on its next pass (to the new
home where lossless, else the residue). The one-home invariant is preserved because
the _registry_ names exactly one canonical home at any version; "old home" rows are
a migration artifact the read-path unifies, not a second live home. A type change is
therefore a forward-only, version-gated event, not a free toggle.

### D6 — Ingest enforces at write; wire stays JSON in phase 1

The acceptor/writer path resolves each attribute's canonical type via the
registry (D2) and encodes into the typed substrate (D4), replacing
`json_strings_to_map_array`. The Flight/WAL wire remains JSON-in-Utf8 as a
transitional carrier (it already preserves JSON types), so **WAL format is
untouched in phase 1** — deliberate, given the WAL-corruption history. Typed
wire is a later, explicitly-BREAKING phase for full fidelity.

### D7 — Reconcile the two schema systems: storage schema becomes the logical schema's physical realization

`schemas.toml`/`schema_parser` is refactored so the physical Iceberg schema is
_derived as the realization of_ the logical schema, with `computed`/promoted/
partition marked as physical-only annotations. Logical evolution (semconv/
`schema_url`) and physical evolution (storage migrations) become two independent
version clocks instead of today's conflated v1/v2 axis.

### D8 — Subsumption mapping (what the fragments become)

- `query-field-discovery` → introspection/discovery is a read over the logical
  schema + registry (available sources, queryable fields as dotted names +
  canonical type, value suggestions). Folded into `otel-native-logical-schema` +
  `attribute-type-authority`; delivery-side tail/pagination remains a later
  stack layer.
- `query-metrics-model` → **folded in here** across two capabilities:
  `metric-native-query` (relation types instant/range/scalar, temporality/
  histogram-aware **operators** — not SQL lowering — vector-matching binop, scalar
  envelope) and `typed-metric-storage` (a typed OTLP metric substrate replacing the
  `data_json` blob: bucket-native histograms, typed temporality/monotonicity,
  first-class exemplar `trace_id`/`span_id`, Summary as passthrough). The metric
  layout **is** reshaped — the blob cannot serve bucket-native quantiles or
  exemplar joins, so "read whatever exists" was untenable (reviewer convergence).
- `query-cross-signal-correlate` → **folded in here** as `cross-signal-correlate`:
  the `correlate` stage, its join keys (incl. exemplars, resource-identity),
  bounded fan-out, time-window scoping, and join-kind taxonomy. Key-encoding
  differences resolve through the one logical key; the pushdown-preserving
  canonicalisation (canonicalise the narrow/winners side) is an execution-layer
  detail left to implementation.
- `query-structural-traces` → **folded in here** as `structural-trace-query`: the
  `match` stage, hierarchical relations (incl. `events`/`links`), and the
  no-silent-depth-cap correctness guarantee. The _execution engine_ choice
  (recursive-CTE vs per-trace evaluator vs materialised ancestry) stays a
  spike-gated implementation task — the spec fixes correctness, not strategy.
- #811 registry epic → its "registry as key→physical source of truth" is
  `attribute-type-authority` + the typed resolution target in `query-ir-core`.

## Risks / Trade-offs

- **Warm tier is unpruned without the derived index** → the typed map is cast-free
  but does not prune (no per-key Parquet stats). Mitigation: the warm containment
  index (D4) is in-scope, not optional; the perf story is index-or-promotion, and
  the specs de-conflate "cast-free" from "pruned".
- **Coexistence read-path reintroduces a cast for legacy files** → legacy
  `Map<String,String>` must be safe-cast (null-on-fail, never a hard error) to the
  canonical type; the no-cast guarantee holds only for typed-substrate files, and
  cross-boundary consistency is result-level until compaction rewrites the legacy
  files. Stated as a spec scope, not hidden.
- **Off-type values become unfilterable** → D3 keeps them lossless in the residue
  but not typed-queryable until an operator repins the type; that is the accepted
  cost of never corrupting the sender's value.
- **Promotion churn / unbounded live schema** → promotion via Iceberg field-id
  evolution, per-table budget + LRU demotion (D4); metadata-file retention rides
  #895 (which bounds files, not schema width — the budget bounds width).
- **Write-path cost is the registry lookup, not the builders** → the per-attribute
  registry resolution on the acceptor path (WAL-corruption-sensitive) needs a
  cache; the benchmark must isolate lookup cost, not just builder count.
- **Correlate/structural correctness under perf bounds** → time-window bounds change
  anti/left-join truth (windowed absence) and fan-out caps must not apply to
  semi/anti; structural "no silent cap" is only met by a per-trace evaluator or
  materialized ancestry, not a recursive CTE. Encoded in the specs, not left to
  implementation.
- **`extract_value` is on the critical path for losslessness** → bytes/interned
  fidelity must be fixed there (an early stack layer), else the "lossless residue"
  claim is false regardless of substrate. Duplicate-key/order fidelity is **not**
  achievable in `extract_value` alone (the JSON-in-Utf8 wire's `serde_json::Map`
  collapses it) — it needs acceptor-side binary residue before serialization, or
  the typed-wire phase.

## Migration Plan

Implemented as a dependent PR stack (charter now, stack later):

1. **Spike (blocking) — feasibility, not Variant.** Prototype the warm containment
   index and prove the datafusion-iceberg provider can present two attribute
   layouts (legacy map + typed) under one scan. Benchmark on real hive data: typed
   map vs. string-map vs. promoted column vs. warm-index, across query classes
   including a **conflicted/off-type key**, **files-pruned %** (predicted ~0 for the
   bare typed map), footer/metadata % on realistic **small flush files**, legacy
   mixed-scan coercion cost, residue parse cost, and **per-attribute registry
   lookup cost**. (Variant is out of scope — see Context.)
2. **`extract_value` fidelity fix:** preserve bytes and interned strings at the
   OTLP boundary (prereq for any losslessness claim). Duplicate-key/order fidelity
   is deferred to acceptor-side binary residue or the typed-wire phase (layer 13),
   since `serde_json::Map` on the phase-1 wire collapses it.
3. **Logical schema + reconciliation:** declare the canonical logical schema and
   refactor `schema_parser`/`schemas.toml` so physical is its realization (D1, D7).
4. **Type authority + registry consistency:** one canonical type per (tenant,
   dataset, field) via config→semconv-hint→observed, monotonic, cache-invalidated
   (D2, D9).
5. **Tiered substrate + coexistence read-path:** land cold one-home store + binary
   residue + warm index; legacy files safe-cast on read; new writes typed (D3, D4).
6. **Ingest enforcement:** route ingest through the registry to the canonical home
   or residue, replace `json_strings_to_map_array` (D6).
7. **Promotion as pure-perf + invariant test:** typed promotion via
   `attr_demand`/compactor, Iceberg field-id evolution, per-table budget + LRU
   demotion; assert the demote-and-still-correct invariant (D5).
8. **Compactor rewrite** of legacy attribute layout to the typed substrate.
9. **Typed metric substrate** (`typed-metric-storage`): bucket-native histograms,
   typed temporality, exemplar join keys, Summary passthrough — replacing
   `data_json`. **BREAKING** metric layout.
10. **Metric-native operators** (`metric-native-query`): rate/increase, histogram
    quantiles, vector matching as custom operators over the typed substrate.
11. **Correlate** and **structural `match`** (per-trace evaluator baseline).
12. **Later stack layers** (own changes, out of this charter's specs):
    delivery-side tail/pagination; typed wire + WAL.

Rollback is per-layer and not uniform — the plan distinguishes revertable from
forward-only layers:

- **Revertable (additive), pre-compaction only:** substrate, warm index, and
  promotion layers. The coexistence read-path means a reverted binary still reads
  both the legacy and typed representations, so reverting the reader is safe; new
  files simply stop being written in the typed layout. This holds **only while the
  legacy representation still exists** — i.e. before the compactor rewrite (layer 7)
  runs for a given file.
- **Forward-only (data-shape changes):** the `extract_value` fidelity fix (layer 1),
  the **attribute compactor rewrite (layer 7)** — once it rewrites legacy files to
  the typed layout, the legacy representation is gone, so a pre-typed binary can no
  longer read them — the typed metric substrate (layer 9), and the typed wire/WAL
  (layer 13). A binary from _before_ these layers cannot read data written (or
  rewritten) _after_ them. Reverting requires either keeping the newer reader or a
  compactor backfill to the old shape; these layers ship behind their own flags and
  are called out as forward-only, not claimed as freely revertable. A per-layer
  read/write compatibility matrix and the retention window for legacy
  representations are implementation-task deliverables for each such layer.

## Open Questions

- Exact binary residue encoding (CBOR vs msgpack vs a self-describing internal
  form) — deferrable to the spike; does not change the logical contract or the
  registry's typed-resolution shape.
- The promoted-column budget size and LRU-demotion trigger thresholds — tunable,
  resolvable in the promotion layer without changing the specs.
