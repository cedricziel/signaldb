import { useRef, useState } from "react";
import {
  bucketizeSeries,
  padBuckets,
  type VolumeBucket,
  type VolumeSeries,
} from "../explore/SignalHistogram";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { axisLabelFormatter } from "../../lib/time";
import {
  compactCount,
  formatTimeBucket,
  formatValue,
} from "../../lib/vizFormat";

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

  const top = buckets.map((bucket, index) => `${x(index)},${y(upper(bucket))}`);
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
  const rootRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(rootRef);
  const [active, setActive] = useState<number | null>(null);

  let buckets = bucketizeSeries(series);
  if (buckets.length > 0) {
    buckets = padBuckets(buckets, rangeMs.fromMs, rangeMs.toMs, stepMs);
  }
  if (buckets.length === 0 || buckets.every((bucket) => bucket.total === 0)) {
    return <div className="trace-area-empty">No volume in this window</div>;
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
  // Each bucket owns the half-step either side of its x position, matching
  // where the area's vertices sit; the ends are clamped to the plot.
  const stepPx = plotWidth / Math.max(1, buckets.length - 1);
  const hitX = (index: number) =>
    Math.max(PADDING.left, PADDING.left + stepPx * (index - 0.5));
  const hitWidth = (index: number) =>
    Math.min(WIDTH - PADDING.right, PADDING.left + stepPx * (index + 0.5)) -
    hitX(index);
  const bucketLabel = (bucket: VolumeBucket) =>
    `${formatTimeBucket(bucket.tMs, stepMs)}: ${formatValue(bucket.total, unit)}`;
  // A padded, empty bucket has no data under the pointer: no tooltip.
  const activeBucket =
    active === null || (buckets[active]?.total ?? 0) === 0
      ? null
      : buckets[active]!;

  return (
    <div
      className="trace-area viz-host"
      data-testid="trace-volume-area"
      ref={rootRef}
    >
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
        {buckets.map((bucket, index) => (
          <rect
            key={bucket.tMs}
            className="trace-area-hit"
            data-testid="trace-area-bucket"
            x={hitX(index)}
            y={PADDING.top}
            width={hitWidth(index)}
            height={plotHeight}
            tabIndex={0}
            aria-label={bucketLabel(bucket)}
            aria-describedby={
              activeBucket === bucket ? "trace-area-tip" : undefined
            }
            onPointerMove={(e) => {
              setActive(index);
              pointer.track(e);
            }}
            onPointerLeave={() => {
              setActive((a) => (a === index ? null : a));
              pointer.clear();
            }}
            onFocus={(e) => {
              setActive(index);
              pointer.anchorTo(e.currentTarget);
            }}
            onBlur={() => {
              setActive((a) => (a === index ? null : a));
              pointer.clear();
            }}
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
      {activeBucket && pointer.anchor && (
        <VizTooltip
          id="trace-area-tip"
          anchor={pointer.anchor}
          host={pointer.host}
          title={formatTimeBucket(activeBucket.tMs, stepMs)}
          rows={keys
            .filter((key) => (activeBucket.counts[key] ?? 0) > 0)
            .map((key) => ({
              swatch: colors[key],
              label: key,
              value: formatValue(activeBucket.counts[key] ?? 0, unit),
            }))}
          footer={{
            label: "total",
            value: formatValue(activeBucket.total, unit),
          }}
          valueWidthCh={formatValue(max, unit).length}
        />
      )}
    </div>
  );
}
