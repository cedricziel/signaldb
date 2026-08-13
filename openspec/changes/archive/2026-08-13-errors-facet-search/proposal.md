## Why

The Errors tab (`explore-ui-errors`) shipped with no way to narrow its
group list — a busy window with many distinct exceptions had to be
scanned row by row. Live testing of the same tab also surfaced a
stacktrace-readability defect: a flat `<pre>` block let a wrapped long
line (a URL inside a JS stack frame) reset to the left edge instead of
staying indented under its frame, reading as broken indentation at
narrower widths, with no visual way to tell the caller's own code apart
from framework/dependency noise.

## What Changes

- A facet sidebar (Type/Service/Source) over the exception group list,
  mirroring the traces tab's facets: each facet shows its distinct values
  with a summed occurrence count, selecting a value narrows the list via a
  removable filter chip, and a facet's own active filter does not narrow
  its own counts (so its alternatives stay switchable). Unlike the traces
  facets, this needs no extra Query IR round trip — the Errors tab's group
  list is already a small, fully materialized aggregate, so faceting is a
  plain client-side fold over data already in memory.
- Stacktrace rendering: each line renders in its own box with its own
  hanging indent, so a wrapped long line stays aligned under its frame
  rather than resetting to the left edge; the header line is visually
  distinguished from frame lines; a frame whose location falls in a common
  dependency directory (`node_modules`, `.vite/deps`, `site-packages`,
  `.cargo/registry`, `vendor/`) is de-emphasized. Deliberately not a full
  per-language stack-frame parser — `exception.stacktrace` is opaque,
  unstandardized text across languages.

Not breaking: no backend or query surface change — both are client-side
refinements of the tab shipped in `explore-ui-errors`.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `explore-ui-errors`: adds a faceted-narrowing requirement for the group
  list.

## Impact

- **src/ui** only: `lib/errorFacets.ts`, `features/errors/ErrorFacets.tsx`,
  `lib/stacktrace.ts`, and wiring/styling in `ErrorsView.tsx`/`errors.css`/
  `explore.css`.
