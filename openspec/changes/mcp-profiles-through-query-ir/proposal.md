## Why

The Query IR is the only first-party read path, but the MCP profile tools
still read the Pyroscope-compatible endpoints that exist for Grafana.

## What Changes

- `discover_profile_types` groups the `profiles` source by `sample.type` and
  `sample.unit`; `search_profiles` reads the IR `flamegraph` envelope;
  `compare_profiles` reads it with a `baseline` (`irVersion` 13, see
  `query-ir-flamegraph-baseline`). Each keeps its output shape.
- The Pyroscope selector filters its sample type and `service_name` (`=`,
  `!=`, `=~`, `!~`); any other label or a malformed selector is rejected
  instead of silently ignored.
- Unset ranges default to a bounded window instead of an unbounded scan;
  blank, unparseable or inverted ranges are invalid parameters.
- Flame graph responses carry the IR's `truncated` flag; an empty diff still
  renders as `double` with zero ticks.

## Impact

- `mcp-server`; the parity manifest moves the three Pyroscope operations to
  the reviewed exclusions (the CLI still calls them).
