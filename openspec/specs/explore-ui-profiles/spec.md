# explore-ui-profiles Specification

## Purpose

Defines how the explore UI's Profiles tab renders, compares, filters, and
navigates to CPU/heap profiles, and how it keeps large or deeply-generic
flame graphs readable, all sourced exclusively through Query IR.

## Requirements

### Requirement: Flame graphs render via Query IR

The Profiles tab SHALL render flame graphs using Query IR's `profiles`
source and `flamegraph` result envelope, and SHALL NOT depend on the
Pyroscope-compatibility `render`/`render_diff` endpoints for its own query
path.

#### Scenario: Selecting a service and sample type renders a flame graph

- **WHEN** a user selects a service and sample type on the Profiles tab
- **THEN** the UI issues a Query IR request with `from: "profiles"` and
  `result: "flamegraph"` and renders the returned flamebearer

### Requirement: Single-range and compare views

The Profiles tab SHALL offer a single-range view showing one flame graph
for the selected window, and a compare view showing two independently
selected ranges rendered side by side, each fetched as its own Query IR
flamegraph request.

#### Scenario: Comparing two time ranges

- **WHEN** a user selects a baseline range and a comparison range in the
  compare view
- **THEN** the UI renders two flame graphs, one per range, each reflecting
  only that range's data

### Requirement: Trace-to-profile navigation

A trace span's detail panel SHALL offer a link to the profile covering
that span's time window when the trace's profile summary identifies one,
navigating directly to that profile by id.

#### Scenario: Opening a profile from a trace span

- **WHEN** a user views a span whose trace carries a profile summary and
  clicks the profile link
- **THEN** the Profiles tab opens showing the flame graph for that exact
  profile id, without requiring the user to re-select service/sample
  type/range

### Requirement: Attribute matcher filtering

The Profiles tab SHALL allow filtering the queried profile series by
arbitrary attribute matchers, not solely by service and sample type.

#### Scenario: Filtering by an attribute

- **WHEN** a user adds an attribute matcher (e.g. a label key/value pair)
  to the query
- **THEN** the rendered flame graph reflects only profile data matching
  that attribute

### Requirement: Minimum-width frame collapsing

The flame graph SHALL support a configurable minimum-width collapse
threshold, expressed as a fraction of total ticks, below which a frame and
its entire subtree are folded into a single synthetic "(other)" node
rather than rendered as individually indistinguishable sub-pixel bars.

#### Scenario: Collapsing small frames

- **WHEN** a collapse threshold greater than 0% is selected and the
  profile contains frames narrower than that threshold
- **THEN** those frames and their descendants are replaced by a single
  "(other)" frame carrying their combined self/total ticks

#### Scenario: Threshold set to Off

- **WHEN** the collapse threshold is set to "Off"
- **THEN** every frame in the profile renders individually, uncollapsed

### Requirement: Top-functions table view

The Profiles tab SHALL offer a sortable table of functions ranked by
self time, aggregated by frame name across the whole profile, as an
alternative view to the flame graph itself.

#### Scenario: Switching to the top-functions view

- **WHEN** a user switches the Profiles tab from flame-graph view to
  top-functions view
- **THEN** a table lists each distinct function name with its aggregated
  self ticks, sorted descending by self time by default

### Requirement: Unit-aware tick formatting

Tick values SHALL be formatted according to their sample-type unit: byte
units render with binary humanization (KiB/MiB/GiB), time units render as
durations, and unitless counts render as plain numbers.

#### Scenario: Byte-unit profile renders humanized sizes

- **WHEN** a profile's sample type unit is `bytes`
- **THEN** displayed tick values render as KiB/MiB/GiB rather than raw
  byte counts

### Requirement: Symbol name simplification with full-name access

Bar labels and the top-functions table SHALL display a simplified form of
long or generic-heavy symbol names — reducing a mangled
`<Type as Trait>::method::<Generics>` form to `Type::method` and removing
compiler-generated noise such as `{closure#N}` — while the full,
unsimplified name remains available via the frame's accessible name and a
hover tooltip alongside self/total ticks and percentages.

#### Scenario: A monomorphized generic method is simplified

- **WHEN** a frame's name is a mangled form such as
  `<hashbrown::HashMap<K,V> as Trait>::method::<u64>`
  the flame graph renders `HashMap::method` as the visible bar label
- **THEN** hovering or focusing the frame reveals the full original name
  plus self time, total time, and their percentages of the profile

#### Scenario: An already-short name is unchanged

- **WHEN** a frame's name is a short, already-readable path such as
  `module::function`
- **THEN** the simplified label equals the original name

### Requirement: Pyroscope-compat calls go through the generated client

Wherever the Explore UI calls the Pyroscope-compatible endpoints (profile
types, label names/values, render, render-diff, profiles for a trace) it SHALL
do so through the generated TypeScript client, not a hand-written fetch, so the
UI, CLI, and MCP consume one contract.

#### Scenario: Profile types load through the generated client

- **WHEN** the profiles view loads the available profile types
- **THEN** the request is issued by the generated client's Pyroscope operation
  and the hand-written raw-fetch module for Pyroscope no longer exists
