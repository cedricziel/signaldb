import {
  bucketizeSeries,
  padBuckets,
  type VolumeSeries,
} from "../explore/SignalHistogram";
import { axisLabelFormatter } from "../../lib/time";

interface Props {
  series: VolumeSeries[];
  order: string[];
  colors: Record<string, string>;
  rangeMs: { fromMs: number; toMs: number };
  stepMs: number;
  unit: string;
  label: string;
}

const WIDTH = 720;
const HEIGHT = 64;
const PADDING = { top: 3, right: 8, bottom: 14, left: 48 };
const number = new Intl.NumberFormat();

export function TraceVolumeHeatmap({
  series,
  order,
  colors,
  rangeMs,
  stepMs,
  unit,
  label,
}: Props) {
  let buckets = bucketizeSeries(series);
  if (buckets.length > 0) {
    buckets = padBuckets(buckets, rangeMs.fromMs, rangeMs.toMs, stepMs);
  }
  if (buckets.length === 0 || buckets.every((bucket) => bucket.total === 0)) {
    return <div className="trace-heatmap-empty">No volume in range</div>;
  }

  // One shared maximum makes equal intensity mean equal count across statuses.
  const max = Math.max(
    ...order.flatMap((key) =>
      buckets.map((bucket) => bucket.counts[key] ?? 0),
    ),
  );
  const formatAxis = axisLabelFormatter(
    buckets[0]!.tMs,
    buckets[buckets.length - 1]!.tMs,
  );
  const plotWidth = WIDTH - PADDING.left - PADDING.right;
  const plotHeight = HEIGHT - PADDING.top - PADDING.bottom;
  const cellWidth = plotWidth / buckets.length;
  const cellHeight = plotHeight / order.length;
  const summary = `${label} heatmap. Rows are status. Columns are time buckets. Color identifies status and intensity represents each count relative to the maximum of ${number.format(max)} ${unit}.`;

  return (
    <div
      className="trace-heatmap"
      data-testid="trace-volume-heatmap"
      role="group"
      aria-label={`${label} heatmap`}
      aria-describedby="trace-volume-heatmap-summary"
    >
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} preserveAspectRatio="none">
        {order.map((key, row) => (
          <g key={key}>
            <text
              className="trace-heatmap-ylabel"
              x={PADDING.left - 5}
              y={PADDING.top + cellHeight * (row + 0.5) + 3}
            >
              {key}
            </text>
            {buckets.map((bucket, column) => {
              const count = bucket.counts[key] ?? 0;
              const intensity = max === 0 ? 0 : count / max;
              return (
                <rect
                  key={bucket.tMs}
                  className="trace-heatmap-cell"
                  data-testid="trace-volume-heatmap-cell"
                  data-status={key}
                  data-count={count}
                  data-intensity={intensity.toFixed(3)}
                  x={PADDING.left + cellWidth * column}
                  y={PADDING.top + cellHeight * row}
                  width={cellWidth}
                  height={cellHeight}
                  fill={colors[key]}
                  fillOpacity={0.12 + intensity * 0.88}
                  aria-label={`${key}, ${formatAxis(bucket.tMs)}: ${number.format(count)} ${unit}`}
                />
              );
            })}
          </g>
        ))}
        <text className="trace-heatmap-xlabel" x={PADDING.left} y={HEIGHT - 3}>
          {formatAxis(buckets[0]!.tMs)}
        </text>
        <text
          className="trace-heatmap-xlabel"
          x={WIDTH - PADDING.right}
          y={HEIGHT - 3}
          textAnchor="end"
        >
          {formatAxis(buckets[buckets.length - 1]!.tMs)}
        </text>
      </svg>
      <div className="trace-heatmap-summary" id="trace-volume-heatmap-summary">
        {summary}
      </div>
    </div>
  );
}
