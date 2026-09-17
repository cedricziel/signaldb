import { useEffect, useRef, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { seriesName, type PromSeries } from "../../api/prom";
import { VizTooltip, type VizTooltipRow } from "../../components/VizTooltip";
import { alignSeries, seriesColorVar } from "../../lib/promSeries";
import { subscribeTheme } from "../../lib/theme";
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

/** Rows shown at once; past this, a chart with many series (a busy metric
 * builder formula, a high-cardinality group-by) would otherwise grow a
 * tooltip taller than the chart itself. */
const MAX_TOOLTIP_ROWS = 10;

/**
 * Resolve the cursor's x-aligned index into tooltip rows: the timestamp at
 * the panel's resolution, and one row per series with its swatch and value —
 * a muted `–` where the series has no sample rather than dropping the row.
 * Past {@link MAX_TOOLTIP_ROWS}, only the largest-magnitude values are shown
 * (a missing sample sorts last), with the rest summarized in `footer`.
 */
export function rowsForCursorIndex(
  u: CursorPlot,
  idx: number,
  unit = "",
): { title: string; rows: VizTooltipRow[]; footer?: string } {
  const xs = u.data[0] ?? [];
  const t = Number(xs[idx]);
  const resolution =
    xs.length > 1 ? Math.abs(Number(xs[1]) - Number(xs[0])) : 60_000;
  const all = [];
  for (let i = 1; i < u.series.length; i++) {
    const v = u.data[i]?.[idx];
    const missing = v === null || v === undefined || Number.isNaN(v);
    const stroke = u.series[i]?.stroke;
    all.push({
      // -1 (rather than 0) keeps a missing sample ranked below even a
      // genuine zero-valued series, not just below every nonzero one.
      magnitude: missing ? -1 : Math.abs(Number(v)),
      row: {
        swatch: typeof stroke === "string" ? stroke : undefined,
        label: u.series[i]?.label ?? `series ${i}`,
        value: missing ? "–" : formatValue(v, unit),
        muted: missing,
      } satisfies VizTooltipRow,
    });
  }
  const title = formatTimestamp(t, resolution);
  if (all.length <= MAX_TOOLTIP_ROWS) {
    return { title, rows: all.map((r) => r.row) };
  }
  const shown = [...all]
    .sort((a, b) => b.magnitude - a.magnitude)
    .slice(0, MAX_TOOLTIP_ROWS)
    .map((r) => r.row);
  return {
    title,
    rows: shown,
    footer: `+${all.length - MAX_TOOLTIP_ROWS} more`,
  };
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
  footer?: string;
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
  // Bumped whenever the effective theme may have changed (a toggle, or a
  // system-level prefers-color-scheme flip) so the effect below re-runs and
  // rebuilds the chart with freshly resolved CSS variables — a chart drawn
  // once at mount otherwise keeps the colours (grid, ticks, series strokes)
  // of whichever theme was active then.
  const [themeTick, setThemeTick] = useState(0);
  useEffect(() => subscribeTheme(() => setThemeTick((t) => t + 1)), []);

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
      const { title, rows, footer } = rowsForCursorIndex(u, idx, unit);
      setTip({
        anchor: {
          x: (u.cursor.left ?? 0) + overRect.left - hostRect.left,
          y: (u.cursor.top ?? 0) + overRect.top - hostRect.top,
        },
        host: { width: hostRect.width, height: hostRect.height },
        title,
        rows,
        footer,
      });
    };
    const initialWidth = host.clientWidth || 800;
    const gridStroke = cssColor("var(--border)", host);
    const tickStroke = cssColor("var(--dim)", host);
    const font = getComputedStyle(host).getPropertyValue("--ui").trim();
    // Both axes read the same theme colours; built once and shared rather
    // than repeating the same four-key object per axis.
    const axis: uPlot.Axis = {
      stroke: tickStroke,
      ticks: { stroke: tickStroke },
      grid: { stroke: gridStroke },
      font: font ? `12px ${font}` : undefined,
    };
    const make = () =>
      new uPlot(
        {
          width: initialWidth,
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
          axes: [axis, axis],
          legend: { show: false },
          hooks: {
            setCursor: [(u) => onCursor(u as unknown as CursorPlot)],
          },
        },
        data,
        host,
      );

    const plot = make();
    let lastWidth = initialWidth;
    // Resize in place (`setSize`, not destroy+recreate) and react to the
    // chart's own container rather than only `window` — a sidebar drag or
    // any container-only reflow never fires a window resize event.
    // Debounced so a continuous drag doesn't thrash the canvas.
    let resizeTimer: ReturnType<typeof setTimeout> | null = null;
    const observer = new ResizeObserver((entries) => {
      const width = entries[0]?.contentRect.width;
      if (resizeTimer !== null) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        resizeTimer = null;
        if (!width || width === lastWidth) return;
        lastWidth = width;
        plot.setSize({ width, height });
      }, 120);
    });
    observer.observe(host);

    return () => {
      if (resizeTimer !== null) clearTimeout(resizeTimer);
      observer.disconnect();
      plot.destroy();
      setTip(null);
    };
  }, [series, height, unit, labelOf, themeTick]);

  return (
    <div ref={hostRef} className="viz-host" data-testid="metrics-chart">
      {tip && (
        <VizTooltip
          anchor={tip.anchor}
          host={tip.host}
          title={tip.title}
          rows={tip.rows}
          footer={tip.footer}
        />
      )}
    </div>
  );
}
