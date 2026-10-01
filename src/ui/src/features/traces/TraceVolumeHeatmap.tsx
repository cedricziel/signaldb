import { useMemo, useRef, useState } from "react";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { useContainerWidth } from "../../hooks/useContainerWidth";
import { useRovingFocus } from "../../hooks/useRovingFocus";
import { axisLabelFormatter } from "../../lib/time";
import { formatShare, formatTimeBucket } from "../../lib/vizFormat";
import { formatDurationMs } from "../../lib/waterfall";
import type { TraceLatencyHeatmap } from "../../api/traceVolume";

interface Props {
  heatmap: TraceLatencyHeatmap;
  label: string;
}

// A fallback for the one frame before the ResizeObserver below reports the
// container's real pixel width — arbitrary but matches the old fixed value.
const DEFAULT_WIDTH = 720;
const HEIGHT = 220;
// The left gutter widens to fit whatever the longest y-axis label turns out
// to be (see `leftGutter` below); this is only the floor for a narrow label
// set.
const MIN_LEFT_PADDING = 48;
// Roughly a monospace-ish character width at the label's 10px font size —
// exact glyph metrics aren't worth measuring for an axis gutter.
const LABEL_CHAR_WIDTH = 6;

export function TraceVolumeHeatmap({ heatmap, label }: Props) {
  const rootRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(rootRef);
  const [active, setActive] = useState<{ column: number; row: number } | null>(
    null,
  );
  // The viewBox tracks the container's real pixel width so `preserveAspectRatio="none"`
  // never has to stretch it — otherwise text (and cell strokes) render
  // non-uniformly, shrinking to unreadable sizes on a phone and clipping the
  // gutter on a wide desktop panel.
  const width = useContainerWidth(rootRef, DEFAULT_WIDTH);

  const start =
    Math.floor(heatmap.window.start_ns / heatmap.x.step_ns) * heatmap.x.step_ns;
  const times: number[] = [];
  for (let t = start; t < heatmap.window.end_ns; t += heatmap.x.step_ns)
    times.push(t);
  const rows = heatmap.y.bounds.length + 1;
  // Epoch nanoseconds exceed JavaScript's safe integer range, so use relative
  // bucket positions rather than independently-rounded absolute coordinates.
  const columnOf = (time: number) =>
    Math.round((time - start) / heatmap.x.step_ns);
  const cells = new Map(
    heatmap.cells.map((cell) => [
      `${columnOf(cell.time_bucket_ns)}|${cell.duration_bucket}`,
      cell.count,
    ]),
  );
  const columnTotals = new Map<number, number>();
  for (const cell of heatmap.cells) {
    const column = columnOf(cell.time_bucket_ns);
    columnTotals.set(column, (columnTotals.get(column) ?? 0) + cell.count);
  }
  const max =
    heatmap.cells.length > 0
      ? Math.max(...heatmap.cells.map((cell) => cell.count))
      : 0;
  // Only populated cells are tab stops (a heatmap has hundreds of empty
  // ones); rows outer, columns inner, matching render order below.
  const populated = useMemo(() => {
    const out: { column: number; row: number }[] = [];
    for (let row = 0; row < rows; row++) {
      for (let column = 0; column < times.length; column++) {
        if ((cells.get(`${column}|${row}`) ?? 0) > 0) {
          out.push({ column, row });
        }
      }
    }
    return out;
  }, [cells, rows, times.length]);
  const populatedIndex = useMemo(() => {
    const m = new Map<string, number>();
    populated.forEach((p, i) => m.set(`${p.column}|${p.row}`, i));
    return m;
  }, [populated]);
  const byColumn = useMemo(() => {
    const m = new Map<number, number[]>();
    for (const p of populated) {
      const list = m.get(p.column) ?? [];
      list.push(p.row);
      m.set(p.column, list);
    }
    return m;
  }, [populated]);
  const roving = useRovingFocus(populated.length, {
    // Left/right step through populated cells in the same row only.
    horizontal: (index, direction) => {
      const cell = populated[index];
      if (!cell) return null;
      for (
        let column = cell.column + direction;
        column >= 0 && column < times.length;
        column += direction
      ) {
        const next = populatedIndex.get(`${column}|${cell.row}`);
        if (next !== undefined) return next;
      }
      return null;
    },
    // Up/down step through populated cells in the same column only.
    vertical: (index, direction) => {
      const cell = populated[index];
      if (!cell) return null;
      const rowsInColumn = byColumn.get(cell.column) ?? [];
      const at = rowsInColumn.indexOf(cell.row);
      const targetRow = rowsInColumn[at + direction];
      return targetRow === undefined
        ? null
        : (populatedIndex.get(`${cell.column}|${targetRow}`) ?? null);
    },
  });
  if (heatmap.cells.length === 0)
    return <div className="trace-heatmap-empty">No spans in this window</div>;
  const formatAxis = axisLabelFormatter(
    times[0]! / 1e6,
    times[times.length - 1]! / 1e6,
  );
  const stepMs = heatmap.x.step_ns / 1e6;
  const bucketBounds = (row: number) => {
    const lower = row === 0 ? 0 : heatmap.y.bounds[row - 1]! / 1e6;
    const upper =
      heatmap.y.bounds[row] === undefined
        ? undefined
        : heatmap.y.bounds[row]! / 1e6;
    return { lower, upper };
  };
  const bucketLabel = (row: number) => {
    const { lower, upper } = bucketBounds(row);
    return upper === undefined
      ? `${formatDurationMs(lower)}+`
      : `${formatDurationMs(lower)}-${formatDurationMs(upper)}`;
  };
  const longestLabel = Math.max(
    ...Array.from({ length: rows }, (_, row) => bucketLabel(row).length),
  );
  const PADDING = {
    top: 3,
    right: 8,
    bottom: 14,
    left: Math.max(MIN_LEFT_PADDING, longestLabel * LABEL_CHAR_WIDTH + 14),
  };
  const plotWidth = width - PADDING.left - PADDING.right;
  const plotHeight = HEIGHT - PADDING.top - PADDING.bottom;
  const cellWidth = plotWidth / times.length;
  const cellHeight = plotHeight / rows;
  /** The tooltip's latency range: `lo – hi`, or `lo+` for the overflow row. */
  const bucketRange = (row: number) => {
    const { lower, upper } = bucketBounds(row);
    return upper === undefined
      ? `${formatDurationMs(lower)}+`
      : `${formatDurationMs(lower)} – ${formatDurationMs(upper)}`;
  };
  const summary = `${label} heatmap. Rows are latency buckets. Columns are time buckets. Color intensity represents span count. Empty cells have no spans.`;

  const activeCount = active
    ? (cells.get(`${active.column}|${active.row}`) ?? 0)
    : 0;
  const activeCell = active && activeCount > 0 ? active : null;

  return (
    <div
      className="trace-heatmap viz-host"
      data-testid="trace-volume-heatmap"
      role="group"
      aria-label={`${label} heatmap`}
      aria-describedby="trace-volume-heatmap-summary"
      ref={rootRef}
    >
      <svg viewBox={`0 0 ${width} ${HEIGHT}`} preserveAspectRatio="none">
        {Array.from({ length: rows }, (_, row) => (
          <g key={row}>
            <text
              className="trace-heatmap-ylabel"
              x={PADDING.left - 5}
              y={PADDING.top + cellHeight * (row + 0.5) + 3}
            >
              {bucketLabel(row)}
            </text>
            {times.map((time, column) => {
              const count = cells.get(`${column}|${row}`) ?? 0;
              const intensity = count / max;
              const isActive =
                activeCell?.column === column && activeCell.row === row;
              const leave = () => {
                setActive((a) =>
                  a?.column === column && a.row === row ? null : a,
                );
                pointer.clear();
              };
              // Only populated cells are tab stops: an empty cell has
              // nothing to announce, and a heatmap has hundreds of them.
              const index = populatedIndex.get(`${column}|${row}`);
              const item = index === undefined ? null : roving.itemProps(index);
              return (
                <rect
                  key={time}
                  className="trace-heatmap-cell"
                  data-testid="trace-volume-heatmap-cell"
                  data-count={count}
                  data-intensity={intensity.toFixed(3)}
                  x={PADDING.left + cellWidth * column}
                  y={PADDING.top + cellHeight * row}
                  width={cellWidth}
                  height={cellHeight}
                  fill="var(--info-bar)"
                  fillOpacity={count > 0 ? 0.12 + intensity * 0.88 : 0}
                  tabIndex={item?.tabIndex}
                  ref={item?.ref}
                  onKeyDown={item?.onKeyDown}
                  aria-label={
                    count > 0
                      ? `${formatAxis(time / 1e6)}, ${bucketLabel(row)}: ${count} spans`
                      : `${formatAxis(time / 1e6)}, ${bucketLabel(row)}: no spans`
                  }
                  aria-describedby={isActive ? "trace-heatmap-tip" : undefined}
                  onPointerMove={(e) => {
                    setActive({ column, row });
                    if (index !== undefined) roving.setActiveIndex(index);
                    pointer.track(e);
                  }}
                  onPointerLeave={leave}
                  onFocus={(e) => {
                    item?.onFocus();
                    setActive({ column, row });
                    pointer.anchorTo(e.currentTarget);
                  }}
                  onBlur={leave}
                />
              );
            })}
          </g>
        ))}
        <text className="trace-heatmap-xlabel" x={PADDING.left} y={HEIGHT - 3}>
          {formatAxis(times[0]! / 1e6)}
        </text>
        <text
          className="trace-heatmap-xlabel"
          x={width - PADDING.right}
          y={HEIGHT - 3}
          textAnchor="end"
        >
          {formatAxis(times[times.length - 1]! / 1e6)}
        </text>
      </svg>
      <div className="trace-heatmap-summary" id="trace-volume-heatmap-summary">
        {summary}
      </div>
      {activeCell && pointer.anchor && (
        <VizTooltip
          id="trace-heatmap-tip"
          anchor={pointer.anchor}
          host={pointer.host}
          title={formatTimeBucket(times[activeCell.column]! / 1e6, stepMs)}
          rows={[
            { label: "latency", value: bucketRange(activeCell.row) },
            { label: "spans", value: String(activeCount) },
            {
              label: "share of column",
              value: formatShare(
                activeCount,
                columnTotals.get(activeCell.column) ?? 0,
              ),
            },
          ]}
        />
      )}
    </div>
  );
}
