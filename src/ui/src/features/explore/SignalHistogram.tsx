/**
 * Stacked volume chart shared by the explore tabs.
 *
 * The component is deliberately signal-agnostic: it takes series already
 * keyed and normalised to milliseconds, plus the caller's stacking order and
 * colours, and knows nothing about log levels or span statuses. Logs and
 * traces are two thin adapters over it rather than two copies of the same
 * rendering maths.
 */
import { useState } from "react";
import { axisLabelFormatter } from "../../lib/time";
import {
  barHeight,
  splitSegments,
  valueAtFraction,
  type Scale,
} from "./scale";

/** A series of `[timestampMs, value]` points, ascending or not. */
export interface VolumeSeries {
  key: string;
  points: [number, number][];
}

export interface VolumeBucket {
  tMs: number;
  counts: Record<string, number>;
  total: number;
}

export function bucketizeSeries(series: VolumeSeries[]): VolumeBucket[] {
  const byTime = new Map<number, Record<string, number>>();
  for (const s of series) {
    for (const [tMs, value] of s.points) {
      const bucket = byTime.get(tMs) ?? {};
      bucket[s.key] = (bucket[s.key] ?? 0) + value;
      byTime.set(tMs, bucket);
    }
  }
  return [...byTime.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([tMs, counts]) => ({
      tMs,
      counts,
      total: Object.values(counts).reduce((a, b) => a + b, 0),
    }));
}

/**
 * Fill the selected range with empty buckets so sparse data keeps its true
 * position on the time axis. Bucket alignment follows the backend's
 * epoch-aligned `date_bin` grid.
 */
export function padBuckets(
  buckets: VolumeBucket[],
  fromMs: number,
  toMs: number,
  stepMs: number,
): VolumeBucket[] {
  if (stepMs <= 0 || toMs <= fromMs) return buckets;
  const byTime = new Map(buckets.map((b) => [b.tMs, b]));
  const start = Math.floor(fromMs / stepMs) * stepMs;
  const out: VolumeBucket[] = [];
  for (let t = start; t <= toMs; t += stepMs) {
    out.push(byTime.get(t) ?? { tMs: t, counts: {}, total: 0 });
  }
  // Keep any buckets that fall outside the aligned grid (defensive).
  for (const b of buckets) {
    if (!out.some((o) => o.tMs === b.tMs)) out.push(b);
  }
  return out.sort((a, b) => a.tMs - b.tMs);
}

const NUM = new Intl.NumberFormat();

interface Props {
  series: VolumeSeries[];
  /** Stacking order, bottom to top. Series outside it are not drawn. */
  order: string[];
  colors: Record<string, string>;
  rangeMs: { fromMs: number; toMs: number };
  stepMs: number;
  scale: Scale;
  /** Noun for the tooltip total, e.g. "lines" or "traces". */
  unit: string;
  /** Accessible name for the plot. */
  label: string;
  height?: number;
  /** Supplied when the caller persists the scale; omit to hide the control. */
  onScaleChange?: (scale: Scale) => void;
}

export function SignalHistogram({
  series,
  order,
  colors,
  rangeMs,
  stepMs,
  scale,
  unit,
  label,
  height = 84,
  onScaleChange,
}: Props) {
  const [active, setActive] = useState<number | null>(null);

  let buckets = bucketizeSeries(series);
  if (buckets.length > 0) {
    buckets = padBuckets(buckets, rangeMs.fromMs, rangeMs.toMs, stepMs);
  }

  if (buckets.length === 0 || buckets.every((b) => b.total === 0)) {
    return <div className="svol svol-empty">No volume in range</div>;
  }

  const max = Math.max(...buckets.map((b) => b.total));
  const fmtAxis = axisLabelFormatter(
    buckets[0]!.tMs,
    buckets[buckets.length - 1]!.tMs,
  );
  const activeBucket = active === null ? null : buckets[active];

  return (
    <div className="svol">
      <div className="svol-plot" style={{ height }}>
        <div className="svol-yaxis" aria-hidden="true">
          <span className="svol-ymax" data-testid="svol-ymax">
            {NUM.format(max)}
          </span>
          <span className="svol-ymid">
            {NUM.format(Math.round(valueAtFraction(0.5, max, scale)))}
          </span>
          <span className="svol-yzero">0</span>
        </div>
        <div className="svol-bars" role="img" aria-label={label}>
          {buckets.map((b, i) => {
            const px = barHeight(b.total, max, height, scale);
            return (
              <button
                type="button"
                className="svol-col"
                data-testid="svol-col"
                key={b.tMs}
                // The hit target spans the plot, not the drawn bar: a bucket
                // at the 1px floor must be as easy to interrogate as the peak.
                style={{ height: "100%" }}
                aria-label={`${fmtAxis(b.tMs)}: ${NUM.format(b.total)} ${unit}`}
                aria-describedby={active === i ? "svol-tip" : undefined}
                onMouseEnter={() => setActive(i)}
                onMouseLeave={() => setActive((a) => (a === i ? null : a))}
                onFocus={() => setActive(i)}
                onBlur={() => setActive((a) => (a === i ? null : a))}
              >
                <span
                  className="svol-bar"
                  data-testid="svol-bar"
                  style={{ height: `${px}px` }}
                >
                  {splitSegments(b.counts, order, px).map((seg) => (
                    <i
                      key={seg.key}
                      data-testid="svol-seg"
                      data-series-key={seg.key}
                      style={{
                        height: `${seg.px}px`,
                        background: colors[seg.key],
                      }}
                    />
                  ))}
                </span>
              </button>
            );
          })}
        </div>
      </div>
      <div className="svol-xaxis" data-testid="svol-xaxis">
        <span role="presentation">{fmtAxis(buckets[0]!.tMs)}</span>
        {onScaleChange && (
          <button
            type="button"
            className="svol-scale"
            aria-pressed={scale === "log"}
            title="Compress the vertical range so low buckets stay readable next to a spike"
            onClick={() => onScaleChange(scale === "log" ? "linear" : "log")}
          >
            log scale
          </button>
        )}
        <span role="presentation">
          {fmtAxis(buckets[buckets.length - 1]!.tMs)}
        </span>
      </div>
      {activeBucket && (
        <div className="svol-tip" id="svol-tip" role="status">
          <div className="svol-tip-t">{fmtAxis(activeBucket.tMs)}</div>
          {order
            .filter((key) => (activeBucket.counts[key] ?? 0) > 0)
            .map((key) => (
              <div className="svol-tip-row" key={key}>
                <i style={{ background: colors[key] }} />
                <span>{key}</span>
                <b>{NUM.format(activeBucket.counts[key] ?? 0)}</b>
              </div>
            ))}
          <div className="svol-tip-total">
            <span>total</span>
            <b>
              {NUM.format(activeBucket.total)} {unit}
            </b>
          </div>
        </div>
      )}
    </div>
  );
}
