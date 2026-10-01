## ADDED Requirements

### Requirement: Differential flamegraph over a baseline window

A `profiles` query declaring the `flamegraph` envelope MAY carry a
document-level `baseline` range. The query SHALL then read its `from`/`where`
stages over both the `baseline` window and the document `range`, apply the
flamegraph row cap to each window independently, and merge both into one
differential flamegraph using the same aggregation as the Pyroscope-compatible
render-diff endpoint. Each level SHALL be a sequence of
`[offset_delta_baseline, total_baseline, self_baseline, offset_delta, total,
self, name_index]` septuples, and the response SHALL carry `baseline_total`
and `comparison_total` alongside `total` (their sum). `truncated` SHALL be
`true` when either window exceeded the cap. A `flamegraph` query without a
`baseline` SHALL return exactly the single-window shape and SHALL NOT carry
`baseline_total` or `comparison_total`.

`baseline` SHALL require `irVersion` 13 and SHALL be rejected at validation
on any envelope other than `flamegraph`, when either bound is not a
timestamp literal, and when its `from` is after its `to`. The two windows
SHALL be read one after the other, each keeping its newest rows under the
cap, and the two sides SHALL NOT be normalized for window length.

#### Scenario: Two windows are diffed

- **WHEN** a `profiles` query filters `service.name = checkout`, declares the
  `flamegraph` envelope, `irVersion` 13 and a `baseline` window
- **THEN** the result is a differential flamegraph whose `baseline_total` is
  the baseline window's total and whose `comparison_total` is the `range`
  window's total

#### Scenario: Baseline needs the flamegraph envelope

- **WHEN** a document declares a `baseline` with `result: "rows"`
- **THEN** the query is rejected at validation naming `baseline`

#### Scenario: Baseline below irVersion 13

- **WHEN** a `flamegraph` document declares a `baseline` and `irVersion` 12
- **THEN** the query is rejected naming `irVersion 13`
