## Why

A layout analysis of common issue/error-tracking tools surfaced two gaps
against `explore-ui-errors`, both cheap to close because the underlying
data or query mechanism already existed:

- Such tools typically lead their issue detail view with a
  **count-over-time chart** above the sample/occurrence list; the Errors
  tab had none.
- They commonly facet on handled-vs-unhandled state; SignalDB already
  resolves the equivalent OTel attribute, `exception.escaped`, as part of
  the exception-event resolution added for this capability — it just
  wasn't grouped, faceted, or displayed anywhere.

Sort options ("Newest" alongside "Count", also a common pattern) were a
third, unrelated-but-free addition: purely a client-side re-sort of the
already-fetched group list.

## What Changes

- Groups now include a 4th dimension, `exception.escaped`, decoded as
  `ErrorGroup.escaped`. A new "Handled" facet and table column display it
  as Unhandled/Handled/— (raw `"true"`/`"false"`/absent), the common
  handled-vs-unhandled facet pattern. Occurrence and count-over-time
  queries pin on it like the other three dimensions, so a group with mixed
  handling states no longer silently merges.
- The group table's Count and Last seen columns are now sortable (client-
  side re-sort over the already-fetched, already-filtered list); Count
  desc remains the default.
- Selecting a group now also fetches and renders a compact count-over-time
  sparkline above its occurrence list — a single-series, axis-free bar
  chart (deliberately not the full multi-series traces volume chart) built
  from a step-bucketed Query IR `series` aggregate pinned to the exact
  group, reusing `bucketizeSeries`/`padBuckets` from the shared histogram
  module.

Not breaking: no backend change — `exception.escaped` resolution already
existed; this only groups, facets, and queries by a field that was already
resolvable. No new HTTP endpoint, OpenAPI operation, or MCP tool.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `explore-ui-errors`: groups additionally split by `exception.escaped`
  (Handled facet/column), the group list is sortable by count or recency,
  and a selected group shows a count-over-time sparkline.

## Impact

- **src/ui** only: `api/errors.ts` (4th group dimension, occurrence/volume
  pin, new `buildErrorGroupVolumeDoc`/`fetchErrorGroupVolume`),
  `lib/errorFacets.ts` (`escaped` facet field + value-label mapping),
  `features/errors/ErrorFacets.tsx`, `features/errors/ErrorSparkline.tsx`
  (new), `features/errors/ErrorsView.tsx`, `features/errors/errors.css`.
