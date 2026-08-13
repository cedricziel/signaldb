## Why

`query-ir-core` registered `profiles` as a Query IR source and the querier now
returns a flamegraph result envelope for it, but the explore UI's Profiles tab
still had no real implementation — no way to render a flame graph, compare two
windows, jump from a trace span to its profile, or filter by attribute. Large
real-world flame graphs (deep Rust call stacks with heavily monomorphized
generic frames) were also unreadable once rendered: thousands of sub-pixel
frames and hundred-character mangled names.

## What Changes

- A Profiles tab renders flame graphs entirely through Query IR's
  `flamegraph` result envelope — no dependency on the Pyroscope-compatibility
  `render`/`render_diff` endpoints for the UI's own query path.
- A single-range view (service + sample type + optional attribute matchers)
  and a compare view (two independent ranges rendered side by side) are both
  available, plus a single-profile view reachable directly by profile id.
- A trace's span detail links to the profile covering that span's window
  (exact profile-id lookup via the trace's `profiles` summary), completing
  trace→profile navigation.
- Arbitrary attribute-matcher filtering (not just service/sample-type) is
  available on the query form.
- Readability controls: a configurable minimum-width collapse threshold folds
  sub-threshold frames and their subtrees into a synthetic "(other)" node
  instead of rendering unreadably thin bars; a sortable top-functions-by-self
  table is offered as an alternative to the flame graph itself; tick counts
  are formatted with unit-aware humanization (byte units get KiB/MiB/GiB,
  not raw counts).
- Long or generic-heavy symbol names (particularly Rust's
  `<Type as Trait>::method::<Generic>` and `{closure#N}`/`{shim:vtable#N}`
  compiler noise) are simplified to `Type::method` on bar labels and in the
  top-functions table, while a hover tooltip and the accessible name still
  carry the full, unsimplified name plus self/total ticks and percentages.

Not breaking: no OTLP ingest, Tempo/LogQL/PromQL surface, Flight wire schema,
or on-disk Iceberg/WAL change. No backend change at all — this is a UI-only
change consuming the `flamegraph` envelope and other Query IR result shapes
already returned by the querier.

Surfaces explicitly scoped out: this is a web-UI change. Programmatic profile
access already exists via Query IR (HTTP/SDK) and the MCP `get_profile` tool;
neither gains new capability here.

## Capabilities

### New Capabilities

- `explore-ui-profiles`: the Profiles tab renders single-range and compare
  flame graphs via Query IR, links from a trace span to its covering profile,
  supports attribute-matcher filtering, and offers readability controls
  (frame collapsing, a top-functions table, and symbol-name simplification
  with a full-name tooltip) for large or deeply-generic profiles.

### Modified Capabilities

(none — `query-ir-core`'s profiles source and flamegraph envelope already
cover the backend contract this UI consumes)

## Impact

- **src/ui** only: `features/profiles/ProfilesView.tsx`,
  `features/profiles/FlameGraph.tsx`, `lib/flamebearer.ts`,
  `api/profilesIr.ts`, `api/tempo.ts` (trace→profile summary field), and
  associated styles/tests.
- No Rust crates are touched.
- Docs: the explore-UI user guide gains sections on profiles compare,
  attribute filtering, trace links, the collapse threshold, the
  top-functions view, and symbol simplification.
