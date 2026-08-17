## Why

When an operator spots an anomalous region — slow spans in a latency heatmap, one
service's error series, a group row that misbehaves — the next question is
always "what is different about _those_ records?" Today the only way to answer
it in SignalDB is to guess a dimension, `aggregate.by` it, look, and repeat.
With hundreds of attributes per tenant that is a fishing expedition, and the
LLM-facing surfaces (MCP) fare no better: an agent must issue one query per
attribute and rescan the window every time.

The Query IR already carries every primitive the answer needs — a shared
predicate grammar, registry-mediated field resolution over promoted columns and
attribute maps alike, and terminal stages with their own envelopes (`heatmap`,
`flamegraph`). What is missing is a single terminal computation that partitions
the matched records into a **selection** cohort and a **baseline** cohort and
contrasts their value distributions across every attribute at once, ranked so
the distinguishing fields come first. This is the well-studied _data
explanation_ problem: Scorpion (Wu & Madden, VLDB 2013) explains outlier
aggregates by finding predicates over input tuples; MacroBase (Bailis et al.,
SIGMOD 2017) and its relational `DIFF` operator (Abuzaid et al., VLDB 2018)
frame it as comparing an outlier relation against an inlier relation and
returning attribute-value explanations that meet minimum _support_ and _risk
ratio_ thresholds. Shipping it as an IR stage rather than a bespoke endpoint
gives HTTP, CLI, MCP, and SDK parity for free and reuses auth, tenant scoping,
range resolution, and absent-value semantics unchanged.

## What Changes

- **IR v4** adds a terminal `compare` stage and a `comparison` result envelope.
  The stage takes a `selection` predicate (same grammar as `where`); every
  record surviving the preceding pipeline is in the selection cohort if the
  predicate is `true` and in the baseline cohort otherwise. The preceding
  `where` stages scope the whole analysis; a heatmap box, a group-vs-rest
  contrast, and a time sub-window are all just predicates.
- The stage compares the two cohorts across a field set — every registered
  logical field and attribute of the source by default (`"*"`), or an explicit
  list — in **one scan**: attribute maps are unnested into `(key, value)` pairs
  and grouped together with promoted columns, so cost is independent of the
  number of attributes.
- Per field the envelope reports its kind (`dimension` for categorical /
  low-cardinality values, `measure` for numeric/duration), a **participation**
  ratio per cohort (share of records carrying the field — polymorphic spans
  and logs make presence itself a signal), a divergence **score**, and either
  per-value shares (dimensions, capped at `maxValues`, top values drawn from
  _both_ cohorts) or per-bucket shares (measures, server-chosen edges).
- Fields are **ranked** by a documented statistic — Jensen–Shannon divergence
  between the two cohort distributions (Lin, 1991), weighted by selection
  participation — and each dimension value carries its risk ratio and support
  so a client can apply `DIFF`-style thresholds. Fields with zero selection
  participation sink to the bottom rather than being dropped.
- Server-side bounds: `maxValues` per field (default 50, ceiling 200), a
  cardinality cap on the field set, an optional per-cohort `sample` reservoir,
  and the same time-window guards the heatmap stage applies. Retrieval-only
  fields (`body`) and unresolvable names are skipped and listed in the
  response, never errors.
- The Explore UI gains a comparison panel: draw a box on the trace latency
  heatmap or pick "compare to rest" on a grouped row, see the ranked field
  charts, and click a value/bucket to refine the active query (`field = v`,
  `field != v`, `field < x`, `group by field`) — the same drill loop the IR
  already supports.
- CLI (`signaldb query`), MCP (`query_ir`), and the generated TypeScript/Rust
  clients pick up the new stage and envelope through the OpenAPI schema; their
  docs and the `querying-ir.md` reference are updated.

No breaking changes: additive IR version, no ingest, Flight, or on-disk changes.

## Capabilities

### New Capabilities

- `query-ir-compare`: the terminal `compare` stage — cohort partitioning by
  predicate, single-pass distribution comparison across dimensions and
  measures, participation, ranking statistic, per-value/per-bucket payloads,
  bounds and skip rules, and the `comparison` envelope.
- `explore-ui-compare`: the UI surface — invoking a comparison from a heatmap
  selection or a grouped row, rendering ranked field charts, and refining the
  active query from a chart element.

### Modified Capabilities

- `query-ir-core`: the supported stage set and declared-envelope requirements
  gain IR v4's `compare` stage and `comparison` envelope, with the same
  version-gating (rejected under v1–v3) and terminal-stage rules that apply to
  `heatmap` and `flamegraph`.

## Impact

- **common**: `query_ir` — new `Stage::Compare`, `Comparison` relation/envelope
  types, `MAX_IR_VERSION` → 4, validation (terminal, envelope match, field-set
  bounds), OpenAPI/JSON schema types.
- **querier**: `ir_planner` — cohort-flag computed column, map unnest +
  `GROUP BY (key, value, cohort)` for attribute containers, promoted-column
  branch, measure bucketing helper, and a post-aggregation ranking step;
  registry lookup for the `"*"` field set.
- **router**: `POST /api/v1/query` unchanged in shape; OpenAPI regeneration
  (`xtask`) for the new envelope; `querying-ir.md`, CLI and MCP docs.
- **signaldb-cli / signaldb-sdk / MCP**: regenerated clients; MCP `query_ir`
  tool description mentions the stage.
- **ui**: new comparison feature (`src/ui/src/features/compare/`), entry
  points in `TraceVolumeHeatmap` and grouped-table rows, generated types.
- **tests-integration**: end-to-end comparison over seeded traces/logs
  (declared in `tests/main.rs`).
- Research references (design.md cites them): Wu & Madden 2013 (Scorpion);
  Bailis et al. 2017 (MacroBase); Abuzaid et al. 2018 (`DIFF`); Roy & Suciu
  2014 (formal explanations); Lin 1991 (Jensen–Shannon divergence).
