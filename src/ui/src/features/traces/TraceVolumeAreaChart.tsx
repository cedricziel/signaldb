import {
  bucketizeSeries,
  compactCount,
  padBuckets,
  type VolumeBucket,
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
const PADDING = { top: 4, right: 8, bottom: 16, left: 42 };

function areaPath(
  buckets: VolumeBucket[],
  key: string,
  preceding: string[],
  max: number,
): string {
  const plotWidth = WIDTH - PADDING.left - PADDING.right;
  const plotHeight = HEIGHT - PADDING.top - PADDING.bottom;
  const x = (index: number) =>
    PADDING.left + (plotWidth * index) / Math.max(1, buckets.length - 1);
  const y = (value: number) => PADDING.top + plotHeight * (1 - value / max);
  const lower = (bucket: VolumeBucket) =>
    preceding.reduce(
      (sum, previous) => sum + (bucket.counts[previous] ?? 0),
      0,
    );
  const upper = (bucket: VolumeBucket) =>
    lower(bucket) + (bucket.counts[key] ?? 0);

  const top = buckets.map(
    (bucket, index) => `${x(index)},${y(upper(bucket))}`,
  );
  const bottom = buckets
    .map((bucket, index) => `${x(index)},${y(lower(bucket))}`)
    .reverse();
  return `M ${top.join(" L ")} L ${bottom.join(" L ")} Z`;
}

export function TraceVolumeAreaChart({
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
    return <div className="trace-area-empty">No volume in range</div>;
  }

  const max = Math.max(...buckets.map((bucket) => bucket.total));
  const keys = order.filter((key) =>
    buckets.some((bucket) => (bucket.counts[key] ?? 0) > 0),
  );
  const formatAxis = axisLabelFormatter(
    buckets[0]!.tMs,
    buckets[buckets.length - 1]!.tMs,
  );
  const plotWidth = WIDTH - PADDING.left - PADDING.right;
  const plotHeight = HEIGHT - PADDING.top - PADDING.bottom;
  const y = (fraction: number) => PADDING.top + plotHeight * (1 - fraction);

  return (
    <div className="trace-area" data-testid="trace-volume-area">
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`${label} area chart`}
        preserveAspectRatio="none"
      >
        {[0, 0.5, 1].map((fraction) => (
          <g key={fraction}>
            <line
              className="trace-area-grid"
              x1={PADDING.left}
              x2={WIDTH - PADDING.right}
              y1={y(fraction)}
              y2={y(fraction)}
            />
            <text
              className="trace-area-ylabel"
              x={PADDING.left - 5}
              y={y(fraction) + 3}
            >
              {compactCount(Math.round(max * fraction))}
            </text>
          </g>
        ))}
        {keys.map((key, index) => (
          <path
            key={key}
            className="trace-area-series"
            d={areaPath(buckets, key, keys.slice(0, index), max)}
            fill={colors[key]}
            stroke={colors[key]}
          />
        ))}
        <text className="trace-area-xlabel" x={PADDING.left} y={HEIGHT - 5}>
          {formatAxis(buckets[0]!.tMs)}
        </text>
        <text
          className="trace-area-xlabel"
          x={PADDING.left + plotWidth}
          y={HEIGHT - 5}
          textAnchor="end"
        >
          {formatAxis(buckets[buckets.length - 1]!.tMs)}
        </text>
      </svg>
      <div className="trace-area-legend" aria-label="Status series">
        {keys.map((key) => (
          <span key={key}>
            <i style={{ background: colors[key] }} />
            {key}
          </span>
        ))}
        <span className="trace-area-unit">{unit}</span>
      </div>
    </div>
  );
}
