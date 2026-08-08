/**
 * Stacked volume chart shared by the explore tabs.
 *
 * The component is deliberately signal-agnostic: it takes series already
 * keyed and normalised to milliseconds, plus the caller's stacking order and
 * colours, and knows nothing about log levels or span statuses. Logs and
 * traces are two thin adapters over it rather than two copies of the same
 * rendering maths.
 */
import { useRef, useState } from "react";
import { axisLabelFormatter } from "../../lib/time";
import { barHeight, splitSegments, valueAtFraction, type Scale } from "./scale";

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

/**
 * Abbreviate a count for an axis gridline, where width is scarce.
 *
 * Deliberately not `Intl`'s `notation: "compact"`: that is locale-dependent
 * and in some locales (`de`, for one) does not abbreviate at all, which both
 * overflows the axis and makes the result untestable. One decimal below ten of
 * a unit (`1.5K`), none above it (`373K`).
 */
export function compactCount(n: number): string {
  const units: [number, string][] = [
    [1e9, "B"],
    [1e6, "M"],
    [1e3, "K"],
  ];
  for (const [scale, suffix] of units) {
    if (n >= scale) {
      const v = n / scale;
      return `${v < 10 ? Math.round(v * 10) / 10 : Math.round(v)}${suffix}`;
    }
  }
  return String(Math.round(n));
}

/**
 * Every number the chart renders carries its unit — an axis gridline or a
 * tooltip row read on its own must not be ambiguous about what it counts.
 */
const withUnit = (n: number, unit: string) => `${NUM.format(n)} ${unit}`;
const compactWithUnit = (n: number, unit: string) =>
  `${compactCount(n)} ${unit}`;

/** Where the tooltip sits, in coordinates relative to the chart root. */
interface TipPos {
  x: number;
  y: number;
  /** Past the horizontal midline the tooltip flips to the pointer's left. */
  flipX: boolean;
}

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
  const [tip, setTip] = useState<TipPos | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);

  /** Track the pointer so the tooltip reads next to what it describes. */
  const trackPointer = (e: { clientX: number; clientY: number }) => {
    const rect = rootRef.current?.getBoundingClientRect();
    if (!rect) return;
    const x = e.clientX - rect.left;
    setTip({ x, y: e.clientY - rect.top, flipX: x > rect.width / 2 });
  };

  /** Keyboard focus has no pointer: anchor under the column instead. */
  const trackElement = (el: HTMLElement) => {
    const rect = rootRef.current?.getBoundingClientRect();
    if (!rect) return;
    const box = el.getBoundingClientRect();
    const x = box.left - rect.left + box.width / 2;
    setTip({ x, y: box.bottom - rect.top, flipX: x > rect.width / 2 });
  };

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
    <div className="svol" ref={rootRef}>
      <div className="svol-plot" style={{ height }}>
        <div className="svol-yaxis" aria-hidden="true">
          <span className="svol-ymax" data-testid="svol-ymax">
            {compactWithUnit(max, unit)}
          </span>
          <span className="svol-ymid">
            {compactWithUnit(
              Math.round(valueAtFraction(0.5, max, scale)),
              unit,
            )}
          </span>
          <span className="svol-yzero">{compactWithUnit(0, unit)}</span>
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
                aria-label={`${fmtAxis(b.tMs)}: ${withUnit(b.total, unit)}`}
                aria-describedby={active === i ? "svol-tip" : undefined}
                onMouseEnter={(e) => {
                  setActive(i);
                  trackPointer(e);
                }}
                onMouseMove={trackPointer}
                onMouseLeave={() => setActive((a) => (a === i ? null : a))}
                onFocus={(e) => {
                  setActive(i);
                  trackElement(e.currentTarget);
                }}
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
      {activeBucket && tip && (
        <div
          className="svol-tip"
          id="svol-tip"
          role="status"
          style={{
            left: `${tip.x}px`,
            top: `${tip.y}px`,
            transform: tip.flipX
              ? "translate(calc(-100% - 14px), 12px)"
              : "translate(14px, 12px)",
          }}
        >
          <div className="svol-tip-t">{fmtAxis(activeBucket.tMs)}</div>
          {order
            .filter((key) => (activeBucket.counts[key] ?? 0) > 0)
            .map((key) => (
              <div className="svol-tip-row" key={key}>
                <i style={{ background: colors[key] }} />
                <span>{key}</span>
                <b>{withUnit(activeBucket.counts[key] ?? 0, unit)}</b>
              </div>
            ))}
          <div className="svol-tip-total">
            <span>total</span>
            <b>{withUnit(activeBucket.total, unit)}</b>
          </div>
        </div>
      )}
    </div>
  );
}
