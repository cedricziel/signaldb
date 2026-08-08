/**
 * Loading placeholders shaped like the content they stand in for.
 *
 * A table that collapses to "Loading…" and then springs back to full height
 * shifts everything below it on every refetch — and the group table refetches
 * on every sort, since sorting is a server-side `order` stage. Holding the
 * rows' shape keeps the layout still.
 *
 * The placeholder is `aria-hidden` and the region carries `aria-busy`, so a
 * screen reader hears "busy" rather than a run of empty cells.
 */

/** One shimmering bar. `width` accepts any CSS length or percentage. */
export function SkeletonBar({ width = "100%" }: { width?: string }) {
  return <span className="skeleton-bar" style={{ width }} aria-hidden="true" />;
}

/**
 * Placeholder rows for a table, matching its column count so the header's
 * widths do not jump when real rows arrive.
 *
 * `numericFrom` marks the first right-aligned column; cells at or after it get
 * the narrow, right-aligned treatment the measure columns use.
 */
export function SkeletonRows({
  rows,
  columns,
  numericFrom = columns,
}: {
  rows: number;
  columns: number;
  numericFrom?: number;
}) {
  // Varying the widths a little reads as content rather than as a progress
  // bar; the cycle is deterministic so rows do not reshuffle between renders.
  const widths = ["70%", "45%", "60%", "35%", "55%"];
  return (
    <>
      {Array.from({ length: rows }, (_, r) => (
        <tr key={r} className="skeleton-row">
          {Array.from({ length: columns }, (_, c) => (
            <td key={c} className={c >= numericFrom ? "num" : undefined}>
              <SkeletonBar
                width={
                  c >= numericFrom ? "3em" : widths[(r + c) % widths.length]
                }
              />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

/** Placeholder lines for a non-tabular list. */
export function SkeletonLines({ lines }: { lines: number }) {
  const widths = ["92%", "78%", "85%", "64%", "88%"];
  return (
    <div className="skeleton-lines" aria-hidden="true">
      {Array.from({ length: lines }, (_, i) => (
        <SkeletonBar key={i} width={widths[i % widths.length]} />
      ))}
    </div>
  );
}
