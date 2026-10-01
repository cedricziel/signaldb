## Why

A truncated IR flamegraph aggregated whichever 1,000 profile rows the scan
produced first, so the same query could return different flamegraphs, unlike
the Pyroscope render path it replaces. An inverted time window returned an
empty result instead of telling the caller the window was wrong.

## What Changes

- A `flamegraph` window orders its rows by `timestamp`, newest first, before
  the row cap.
- A `range` whose `from` is after its `to` is rejected: at validation when both
  bounds are absolute or both relative, otherwise once resolved (400 /
  InvalidInput).

## Impact

- `query-ir` validation, `querier` `ir_planner`, `router` window resolution.
- IR result change: truncated flamegraphs are now the newest rows; inverted
  windows are a 400 instead of an empty result.
