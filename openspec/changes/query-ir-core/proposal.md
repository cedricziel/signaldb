## Why

SignalDB's query surfaces are the three compatibility dialects — TraceQL,
LogQL, PromQL. They are correct to keep: they are how Grafana and Tempo/Loki/
Prometheus clients talk to us. But they are a poor foundation for our **own**
UI.

Tracing a query end-to-end exposes the problem. The UI already thinks
structurally — `FilterChips` build `{label, op, value}` objects, `MetricsView`
composes multi-row formulas in a visual builder — then **compiles that structure
down to a dialect string**, ships the string, and the querier parses it **back**
into typed params before planning. It is a `structured → string → structured`
round trip whose string leg exists purely for external compatibility.
Concretely there are today **two** LogQL implementations: a compiler in
TypeScript (`src/ui`) and a parser in Rust (`src/querier`). Building a complex
query in the UI means string surgery in the browser against a grammar that is
re-parsed on the server.

This change establishes the **foundation** that fixes that: a canonical,
structured, versioned query IR with a real type system, executed by lowering to
DataFusion — and it does so at the **smallest honest scope** that proves the
thesis. Cross-signal correlation, structural trace matching, metric range-vectors,
and builder field-discovery are each their own follow-up change (see "Change
stack" below); they depend on the contract this change defines but are separately
designed and separately risky.

## What Changes

- Introduce the **Query IR**: a JSON, **versioned** query document over a
  **registered signal source**, with a defined **type system** (the row-set /
  scalar / series types that flow between stages, null semantics, and value
  coercion) that is specified **independently of the DataFusion plan it lowers
  to**. The plan is an implementation; the types and evaluation semantics are the
  contract.
- Ship the **single-signal core stages**: `from`, `where`, `extract`,
  `aggregate`, `topk`/`bottomk`, `order`, `limit`. All stage operands are
  **structured** — no embedded mini-expression strings (e.g. no
  `topk:{of:"max(duration)"}`; the aggregate reference is a structured value).
- **Lower to a DataFusion `LogicalPlan`**, satisfying the denotational spec. The
  IR document is the public contract; the plan is swappable.
- Resolve every field through the **attribute registry** (dotted OTel-native
  name → physical column **or** `attributes_json` path + canonical type), so
  attribute promotion is invisible to the query contract — pure performance
  upside. This preserves the promotion-invariance property.
- Expose the IR over a native, versioned HTTP endpoint `POST /api/v1/query`
  (router → new `query_ir` Flight ticket → querier), returning a **declared,
  validated result envelope** (`rows | series | table`; `trace`/`scalar` arrive
  with their owning follow-up changes).
- Consume it from **both** first-party surfaces per the surface-parity rule: a
  UI query builder that emits IR stages, and the CLI/SDK — each through its
  **generated** client (TS client / Rust SDK), never hand-written HTTP.

### Scope boundaries (deliberately drawn tight)

- **Signals in core: `logs` and `traces`** — the flat, row-shaped signals. The
  `from` source is a **registered source**, not a hardcoded enum, so `metrics`
  and `profiles` are added by later changes without reshaping the IR. `metrics`
  is _not_ a core source (see `query-metrics-model` — its `data_json` /
  histogram / temporality model needs its own design; folding range-vectors into
  a generic stage is unsound).
- **Single-signal only.** No `correlate`, no `match`, no `binop` in this change.
  The IR type system is defined so those compose onto it as a DAG later.

This is **additive and not a wire-contract change**: OTLP ingest, the Tempo/
LogQL/PromQL surfaces, existing Flight schemas, and the on-disk Iceberg/WAL
layout are unchanged. No existing query regresses. Not BREAKING.

## Change stack

This change is the base of a dependent stack. Each sibling is its own change so
it can be designed and reviewed on its own risk profile:

- **query-ir-core** (this) — IR type system + versioning + single-signal stages
  - native surface + clients + builder. Ship first.
- **query-field-discovery** — build-side introspection (signals/fields/values/
  relationships) the builder needs to make queries easy to _build_, plus
  delivery-side live tail + pagination for results.
- **query-cross-signal-correlate** — `correlate` as a DAG join node.
- **query-structural-traces** — `match`; engine choice prototyped first.
- **query-metrics-model** — temporality/histogram-aware metric sub-model.

Also deferred (own future changes, unchanged by this restructure): dialects
lowering _into_ the IR (one engine; a possible NRQL-style text front-end), and
all-attribute promotion (the existing attribute-registry epic #811, which this
change _consumes_ — see Impact).

## Capabilities

### New Capabilities

- `query-ir-core`: the versioned Query IR contract — its type system and
  denotational evaluation semantics, the shared predicate grammar, the
  single-signal stage set, registry-mediated field resolution, the
  declared/validated result envelope, the extensible signal-source model, and
  the `POST /api/v1/query` surface. Defines the stable contract every sibling
  change and every future front-end lowers into.

### Modified Capabilities

<!-- None. Compatibility dialects unchanged; this is a new native capability
     alongside them. -->

## Impact

- **common** (`src/common/src/`): IR types + **type system / validator** live
  here (shared by router and querier); the **attribute-registry resolver**
  interface (field → column | json-path + canonical type). This resolver is a
  **consumer of the attribute-registry epic (#811)** — this change adds the
  query-facing resolver _view_, it does not re-implement the registry. Uses
  Arrow/Parquet types re-exported by DataFusion (FDAP alignment).
  **Production gating:** core is fully buildable and testable against a
  config/in-memory resolver fallback, but the _canonical field types_ that
  literal coercion depends on have no production source until #811 lands — so
  before #811, core is usable for fields whose type is config-declared or
  registry-known and is otherwise demo/fallback-only. Core does not block on
  #811 for build or test; it depends on #811 for full production field coverage.
- **querier** (`src/querier/src/`): IR→`LogicalPlan` planner for the
  single-signal stages, satisfying the denotational spec; new
  `query_ir:{tenant}:{dataset}:{json}` Flight ticket in `flight.rs`. Reuses
  existing DataFusion/Iceberg registration.
- **router** (`src/router/src/endpoints/`): new `query.rs` endpoint
  `POST /api/v1/query`; registered in `endpoints/mod.rs` and the code-first
  OpenAPI (`openapi.rs`).
- **OpenAPI + generated clients**: endpoint, versioned IR request schema, and
  result-envelope schemas added to the code-first OpenAPI; **Rust SDK**
  (`src/signaldb-sdk`, CLI) and **TypeScript client** (`src/ui/src/api/gen`, UI)
  regenerated in this change.
- **ui** (`src/ui/src/features/explore` + a `query-builder` lib): builder
  emitting IR stages for logs/traces; renders `rows|series|table`; consumes the
  generated TS client.
- **CLI** (`src/signaldb-bin` / SDK consumer): submit an IR query (file/stdin),
  print the enveloped result.
- **tests-integration**: E2E for single-signal logs and traces queries over real
  Iceberg tables; regression assertion that existing dialect queries are
  unchanged.
- No dependency additions beyond DataFusion. No OTLP/Flight/WAL/Iceberg on-disk
  changes. Not BREAKING.
