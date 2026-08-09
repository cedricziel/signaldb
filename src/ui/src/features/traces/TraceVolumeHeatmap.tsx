import {
  bucketizeSeries,
  padBuckets,
  type VolumeSeries,
} from "../explore/SignalHistogram";
import { axisLabelFormatter } from "../../lib/time";
import { formatDurationMs } from "../../lib/waterfall";

interface Props {
  series: VolumeSeries[];
  latency: VolumeSeries[];
  order: string[];
  colors: Record<string, string>;
  rangeMs: { fromMs: number; toMs: number };
  stepMs: number;
  label: string;
}

const WIDTH = 720;
const HEIGHT = 64;
const PADDING = { top: 3, right: 8, bottom: 14, left: 48 };
export function TraceVolumeHeatmap({
  series,
  latency,
  order,
  colors,
  rangeMs,
  stepMs,
  label,
}: Props) {
  let buckets = bucketizeSeries(series);
  if (buckets.length > 0) {
    buckets = padBuckets(buckets, rangeMs.fromMs, rangeMs.toMs, stepMs);
  }
  if (buckets.length === 0 || buckets.every((bucket) => bucket.total === 0)) {
    return <div className="trace-heatmap-empty">No volume in range</div>;
  }

  const latencyByStatusAndTime = new Map(
    latency.flatMap((s) =>
      s.points.map(([tMs, value]) => [`${s.key}|${tMs}`, value] as const),
    ),
  );
  // One shared maximum makes equal intensity mean equal latency across statuses.
  const max = Math.max(
    0,
    ...order.flatMap((key) =>
      buckets.flatMap((bucket) => {
        const value = latencyByStatusAndTime.get(`${key}|${bucket.tMs}`);
        return (bucket.counts[key] ?? 0) > 0 && value !== undefined
          ? [value]
          : [];
      }),
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
  const summary = `${label} heatmap. Rows are status. Columns are time buckets. Color identifies status and intensity represents average latency relative to the maximum of ${formatDurationMs(max)}. Empty cells have no spans.`;

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
              const latencyMs = latencyByStatusAndTime.get(
                `${key}|${bucket.tMs}`,
              );
              const hasLatency = count > 0 && latencyMs !== undefined;
              const intensity = !hasLatency || max === 0 ? 0 : latencyMs / max;
              return (
                <rect
                  key={bucket.tMs}
                  className="trace-heatmap-cell"
                  data-testid="trace-volume-heatmap-cell"
                  data-status={key}
                  data-count={count}
                  data-latency={latencyMs}
                  data-intensity={intensity.toFixed(3)}
                  x={PADDING.left + cellWidth * column}
                  y={PADDING.top + cellHeight * row}
                  width={cellWidth}
                  height={cellHeight}
                  fill={colors[key]}
                  fillOpacity={hasLatency ? 0.12 + intensity * 0.88 : 0}
                  aria-label={
                    hasLatency
                      ? `${key}, ${formatAxis(bucket.tMs)}: average latency ${formatDurationMs(latencyMs ?? 0)}`
                      : `${key}, ${formatAxis(bucket.tMs)}: no data`
                  }
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
