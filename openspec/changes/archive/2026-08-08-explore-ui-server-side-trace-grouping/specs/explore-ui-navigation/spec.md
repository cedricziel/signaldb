## MODIFIED Requirements

### Requirement: Non-signal state stays in the query string

Time range, filters, search text, live-tail mode, trace/group selection,
grouping dimension, grouping grain, PromQL expression, profile type/service
selectors, and tenant/dataset context SHALL remain represented as URL query
parameters, independent of which signal path is active, so a view (including a
specific trace or a specific PromQL query) remains bookmarkable and shareable.

#### Scenario: Query parameters survive a signal switch

- **WHEN** a user on `/logs?tenant=acme&dataset=prod` switches to the
  traces signal
- **THEN** the resulting URL is `/traces?tenant=acme&dataset=prod`

#### Scenario: A shared link reproduces the grouping grain

- **WHEN** a user shares a traces view whose group table counts spans rather
  than traces
- **THEN** opening that link presents the table at the same grain
