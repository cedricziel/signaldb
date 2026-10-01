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
// anchored somewhere with room (see `EntityTable`). A thin wrapper over the
// shared `Sparkline`, in its non-tooltip reporting mode.
import type { PointerEvent as ReactPointerEvent } from "react";
import { Sparkline } from "../../components/Sparkline";

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
    .map((p) => ({ x: Number(p[0]) / 1_000_000, v: Number(p[1]) }))
    .filter((p) => Number.isFinite(p.v));

  // One point has no shape, and none at all is not a zero line: the row's
  // other columns are real measurements and this must not look like one.
  if (points.length < 2) return null;

  return (
    <Sparkline
      points={points}
      width={WIDTH}
      height={HEIGHT}
      tone="accent"
      ariaLabel={`${label} over the selected window`}
      showTooltip={false}
      onHover={(p, e) =>
        p
          ? onHover?.({ tMs: p.x, v: p.v, stepMs: p.stepMs, max: p.max }, e)
          : onHover?.(null)
      }
    />
  );
}
