## 1. Handled facet and grouping

- [x] 1.1 Write failing tests for the 4th group dimension
      (`exception.escaped`), decoding, and pinning it through
      occurrence/volume queries.
- [x] 1.2 Implement in `api/errors.ts`.
- [x] 1.3 Write failing tests for the `escaped` facet field and its
      Unhandled/Handled value-label mapping in `lib/errorFacets.ts`.
- [x] 1.4 Implement; wire the friendly label into `ErrorFacets.tsx` and a
      Handled column in `ErrorsView.tsx`'s group table.

## 2. Sortable group list

- [x] 2.1 Write failing tests that clicking the Last seen header re-sorts
      the group list by recency.
- [x] 2.2 Wire the shared `useSort`/`SortTh`/`sortRows` helpers into the
      group table's Count and Last seen columns.

## 3. Count-over-time sparkline

- [x] 3.1 Write failing tests for `buildErrorGroupVolumeDoc`/
      `fetchErrorGroupVolume`: pinned to the exact group, step-bucketed
      count series.
- [x] 3.2 Implement in `api/errors.ts`, reusing `VolumeSeries`.
- [x] 3.3 Write failing tests for `ErrorSparkline`: one bar per padded
      bucket, an empty state when there's no data in range.
- [x] 3.4 Implement `features/errors/ErrorSparkline.tsx`; wire it above the
      occurrence list in `ErrorsView.tsx`.

## 4. Verification

- [x] 4.1 `pnpm run typecheck && pnpm run lint && pnpm vitest run`.
- [x] 4.2 Live-verify against a real deployment: the Handled column/facet
      render correctly, sorting by Last seen reorders the list, and the
      sparkline renders a real occurrence-count spike for a selected group.
