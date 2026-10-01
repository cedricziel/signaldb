## ADDED Requirements

### Requirement: The flamegraph cap keeps the newest profiles

When more profile rows match a `flamegraph` query than the row cap, the
aggregated rows SHALL be the newest by `timestamp`, so a truncated flamegraph
is deterministic.

#### Scenario: Truncation keeps the newest rows

- **WHEN** more profiles match a `flamegraph` query than the cap
- **THEN** the result aggregates the newest profiles up to the cap and
  carries `truncated: true`

### Requirement: Inverted windows are rejected

A document whose `range` resolves to a `from` after its `to` SHALL be rejected
as invalid input naming the window, not answered with an empty result.

#### Scenario: Inverted range

- **WHEN** a document's `range` is `{ "from": "now", "to": "now-1h" }`
- **THEN** the request is rejected with a 400 naming `range.from`
