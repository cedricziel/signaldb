import { useEffect, useRef, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { seriesName, type PromSeries } from "../../api/prom";
import { VizTooltip, type VizTooltipRow } from "../../components/VizTooltip";
import { alignSeries, seriesColorVar } from "../../lib/promSeries";
import { formatTimestamp, formatValue } from "../../lib/vizFormat";

interface Props {
  series: PromSeries[];
  height?: number;
  /** Unit appended to every value in the tooltip, e.g. `req/s`. */
  unit?: string;
  /** Series label for the tooltip; defaults to the Prometheus series name. */
  labelOf?: (s: PromSeries) => string;
}

/**
 * The slice of a uPlot instance the tooltip reads — narrowed so the pure
 * resolver is testable without a canvas.
 */
export interface CursorPlot {
  data: ArrayLike<ArrayLike<number | null | undefined>>;
  series: { label?: string; stroke?: unknown }[];
  cursor: { idx?: number | null; left?: number; top?: number };
  over: HTMLElement;
}

/**
 * Resolve the cursor's x-aligned index into tooltip rows: the timestamp at
 * the panel's resolution, and one row per series with its swatch and value —
 * a muted `–` where the series has no sample rather than dropping the row.
 */
export function rowsForCursorIndex(
  u: CursorPlot,
  idx: number,
  unit = "",
): { title: string; rows: VizTooltipRow[] } {
  const xs = u.data[0] ?? [];
  const t = Number(xs[idx]);
  const resolution =
    xs.length > 1 ? Math.abs(Number(xs[1]) - Number(xs[0])) : 60_000;
  const rows: VizTooltipRow[] = [];
  for (let i = 1; i < u.series.length; i++) {
    const v = u.data[i]?.[idx];
    const missing = v === null || v === undefined || Number.isNaN(v);
    const stroke = u.series[i]?.stroke;
    rows.push({
      swatch: typeof stroke === "string" ? stroke : undefined,
      label: u.series[i]?.label ?? `series ${i}`,
      value: missing ? "–" : formatValue(v, unit),
      muted: missing,
    });
  }
  return { title: formatTimestamp(t, resolution), rows };
}

function cssColor(varExpr: string, el: HTMLElement): string {
  const name = /var\((--[a-z0-9-]+)\)/i.exec(varExpr)?.[1];
  if (!name) return varExpr;
  return getComputedStyle(el).getPropertyValue(name).trim() || "#888";
}

interface Tip {
  anchor: { x: number; y: number };
  host: { width: number; height: number };
  title: string;
  rows: VizTooltipRow[];
}

/** Hoisted so a stable default keeps the chart effect from re-running. */
const promLabel = (s: PromSeries) => seriesName(s.labels);

export function MetricsChart({
  series,
  height = 260,
  unit = "",
  labelOf = promLabel,
}: Props) {
  const hostRef = useRef<HTMLDivElement>(null);
  const [tip, setTip] = useState<Tip | null>(null);

  useEffect(() => {
    const host = hostRef.current;
    if (!host || series.length === 0) return;

    const data = alignSeries(series) as uPlot.AlignedData;
    // Read the cursor's index on every move and lift the rows into React
    // state; uPlot's own legend stays hidden.
    const onCursor = (u: CursorPlot) => {
      const idx = u.cursor.idx;
      if (idx === null || idx === undefined || idx < 0) {
        setTip(null);
        return;
      }
      const hostRect = host.getBoundingClientRect();
      const overRect = u.over.getBoundingClientRect();
      const { title, rows } = rowsForCursorIndex(u, idx, unit);
      setTip({
        anchor: {
          x: (u.cursor.left ?? 0) + overRect.left - hostRect.left,
          y: (u.cursor.top ?? 0) + overRect.top - hostRect.top,
        },
        host: { width: hostRect.width, height: hostRect.height },
        title,
        rows,
      });
    };
    const make = () =>
      new uPlot(
        {
          width: host.clientWidth || 800,
          height,
          // Timestamps are already in ms.
          ms: 1,
          series: [
            {},
            ...series.map((s, i) => ({
              label: labelOf(s),
              stroke: cssColor(seriesColorVar(i), host),
              width: 1.5,
              points: { show: false },
            })),
          ],
          axes: [
            { stroke: cssColor("var(--dim)", host) },
            { stroke: cssColor("var(--dim)", host) },
          ],
          legend: { show: false },
          hooks: {
            setCursor: [(u) => onCursor(u as unknown as CursorPlot)],
          },
        },
        data,
        host,
      );

    let plot = make();
    const onResize = () => {
      plot.destroy();
      plot = make();
    };
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      plot.destroy();
      setTip(null);
    };
  }, [series, height, unit, labelOf]);

  return (
    <div ref={hostRef} className="viz-host" data-testid="metrics-chart">
      {tip && (
        <VizTooltip
          anchor={tip.anchor}
          host={tip.host}
          title={tip.title}
          rows={tip.rows}
        />
      )}
    </div>
  );
}
