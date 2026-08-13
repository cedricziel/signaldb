## Why

`explore-ui-errors` shipped selecting a group by fetching exactly one
arbitrary occurrence (`limit 1`) and showing only its stacktrace and trace
link. This under-serves both what the tab is for: a group can span many
distinct call sites with different stacktraces, and only some occurrences
of a logs-sourced exception may have happened inside an active trace — an
arbitrary single sample hides that variance and, worse, may happen to be
the one occurrence with no trace to link when others in the same group do
have one. It's also inconsistent with the group→instances drill-in pattern
already established for trace groups (`api/traceGroupMembers.ts`,
`MemberTable`) and catalog entities: selecting an aggregate elsewhere in
this UI always reveals its individual members, not a single sample.

## What Changes

- Selecting an exception group now fetches its individual occurrences (up
  to 25, newest first) instead of one arbitrary example, mirroring the
  group→instances pattern used everywhere else in the explore UI.
- Each occurrence lists its own timestamp and, independently, its own
  trace link — a group is no longer represented by one sampled trace id;
  some occurrences may link to a trace while others (same group) do not.
- Clicking an occurrence expands its own stacktrace inline; clicking its
  trace link (when present) navigates directly without also expanding the
  row.

Not breaking: no backend or query surface change — `buildErrorOccurrencesDoc`
reuses the same field/pin mechanics `buildErrorExampleDoc` already used,
just with `limit: 25` instead of `limit: 1` and an explicit newest-first
order.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `explore-ui-errors`: replaces the single-example drill-in with a
  per-group occurrence list, each instance independently linkable to its
  own trace.

## Impact

- **src/ui** only: `api/errors.ts` (`fetchErrorOccurrences` replaces
  `fetchErrorExample`), `features/errors/ErrorsView.tsx`,
  `features/errors/errors.css`.
