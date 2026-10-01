## Why

First-party reads go through the Query IR, but the IR could not compare two
profile windows: the MCP `compare_profiles` tool (and anything else wanting a
before/after flamegraph) still had to call the Pyroscope-compatible
`/pyroscope/render-diff` endpoint, which exists for Grafana.

## What Changes

- The IR document gains an optional `baseline` range, valid only with
  `result: "flamegraph"` and only at `irVersion` 13 (`MAX_IR_VERSION` becomes 13).
- With a `baseline`, the querier reads the document's `where` stages over both
  windows (each capped at the flamegraph row cap) and merges them with the same
  differential aggregation `/pyroscope/render-diff` uses.
- The two windows run one after the other; each keeps its newest rows under
  the cap (see `query-ir-flamegraph-windows`). An inverted `baseline` is
  rejected like an inverted `range`.
- The flamegraph envelope then carries Pyroscope "double" septuple levels and
  two new totals, `baseline_total` and `comparison_total`. Without a
  `baseline` the response is unchanged.

## Impact

- `query-ir`, `querier` (`ir_planner`), `router` (`POST /api/v1/query`), the
  OpenAPI contract and the generated SDK/UI types.
- Documents at `irVersion` 1-12 keep their exact meaning.
