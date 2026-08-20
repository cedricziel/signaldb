// One row's headline metric, at cell size.
//
// A line, not bars: the column charts a level (CPU utilization, memory in
// use), and bars drawn from zero make two very different levels look alike
// while implying the metric counts occurrences. `ErrorSparkline` is the bar
// version, for the counts it is right for.
//
// Presentational only — it reports what the pointer is over and draws no
// tooltip itself. A table cell sets `overflow: hidden` for its ellipsis, so a
// tooltip rendered in here is clipped to 80x18; the table owns one instead,
// anchored somewhere with room (see `EntityTable`).
import type { PointerEvent as ReactPointerEvent } from "react";

/** What the pointer is over, in terms the tooltip can render. */
export interface SparklinePoint {
  tMs: number;
  v: number;
  stepMs: number;
  /** The series' peak, so the tooltip can reserve a stable value width. */
  max: number;
}

interface Props {
  /** Series as the IR envelope carries them — `[timestampNs, value]` pairs. */
  series: { points: unknown[][] }[];
  label: string;
  /** Called with the hovered point, or null when the pointer leaves. */
  onHover?: (
    point: SparklinePoint | null,
    event?: ReactPointerEvent<SVGElement>,
  ) => void;
}

const WIDTH = 80;
const HEIGHT = 18;

export function EntitySparkline({ series, label, onHover }: Props) {
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
  const stepMs = points[1]!.tMs - points[0]!.tMs;
  const line = points
    .map((p, i) => `${i * step},${HEIGHT - ((p.v - min) / span) * HEIGHT}`)
    .join(" ");

  return (
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
        // Hit bands, one per point: the line is a pixel wide and impossible
        // to hover. They carry no fill — an SVG rect defaults to opaque
        // black, which paints over the whole cell.
        <rect
          key={p.tMs}
          className="entity-sparkline-hit"
          x={i * step - step / 2}
          y={0}
          width={step}
          height={HEIGHT}
          onPointerEnter={(e) => onHover?.({ ...p, stepMs, max }, e)}
          onPointerMove={(e) => onHover?.({ ...p, stepMs, max }, e)}
          onPointerLeave={() => onHover?.(null)}
        />
      ))}
    </svg>
  );
}
