// One row's headline metric, at cell size.
//
// A line, not bars: the column charts a level (CPU utilization, memory in
// use), and bars drawn from zero make two very different levels look alike
// while implying the metric counts occurrences. `ErrorSparkline` is the bar
// version, for the counts it is right for.
//
// No axes and no legend — the cell has no room and the entity's own page has
// the readable chart. It does read through the shared `VizTooltip`, though:
// without it the cell is a picture rather than data, and every other
// visualization in the UI is interrogable the same way.
import { useRef, useState } from "react";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { formatTimeBucket, formatValue } from "../../lib/vizFormat";

interface Props {
  /** Series as the IR envelope carries them — `[timestampNs, value]` pairs. */
  series: { points: unknown[][] }[];
  label: string;
}

const WIDTH = 80;
const HEIGHT = 18;

export function EntitySparkline({ series, label }: Props) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const [active, setActive] = useState<number | null>(null);

  // The query groups by the entity's identity alone, so a row is one series.
  // Concatenating several would draw a line through unrelated measurements.
  const points = (series[0]?.points ?? [])
    .map((p) => ({ tMs: Number(p[0]) / 1_000_000, v: Number(p[1]) }))
    .filter((p) => Number.isFinite(p.v));

  // One point has no shape, and none at all is not a zero line: the row's
  // other columns are real measurements and this must not look like one.
  if (points.length < 2) return null;

  const values = points.map((p) => p.v);
  const min = Math.min(...values);
  const max = Math.max(...values);
  // A flat series is still a fact — draw it mid-cell rather than dividing by
  // a zero range.
  const span = max - min || 1;
  const step = WIDTH / (points.length - 1);
  const stepMs = points.length > 1 ? points[1]!.tMs - points[0]!.tMs : 0;
  const line = points
    .map((p, i) => `${i * step},${HEIGHT - ((p.v - min) / span) * HEIGHT}`)
    .join(" ");

  const hit = points[active ?? -1];

  return (
    <div className="entity-sparkline-host viz-host" ref={hostRef}>
      <svg
        className="entity-sparkline"
        width={WIDTH}
        height={HEIGHT}
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`${label} over the selected window`}
        preserveAspectRatio="none"
      >
        <polyline points={line} fill="none" strokeWidth="1" />
        {points.map((p, i) => (
          // Invisible hit bands, one per point: the line itself is a pixel
          // wide and impossible to hover.
          <rect
            key={p.tMs}
            className="entity-sparkline-hit"
            x={i * step - step / 2}
            y={0}
            width={step}
            height={HEIGHT}
            role="button"
            tabIndex={-1}
            aria-label={`${label} at ${formatTimeBucket(p.tMs, stepMs)}`}
            onPointerEnter={(e) => {
              setActive(i);
              pointer.track(e);
            }}
            onPointerMove={pointer.track}
            onPointerLeave={() => {
              setActive((a) => (a === i ? null : a));
              pointer.clear();
            }}
            onFocus={(e) => {
              setActive(i);
              pointer.anchorTo(e.currentTarget);
            }}
            onBlur={() => {
              setActive((a) => (a === i ? null : a));
              pointer.clear();
            }}
          />
        ))}
      </svg>
      {hit && pointer.anchor && (
        <VizTooltip
          anchor={pointer.anchor}
          host={pointer.host}
          title={formatTimeBucket(hit.tMs, stepMs)}
          rows={[
            {
              swatch: "var(--accent)",
              label,
              value: formatValue(hit.v),
            },
          ]}
          valueWidthCh={formatValue(max).length}
        />
      )}
    </div>
  );
}
