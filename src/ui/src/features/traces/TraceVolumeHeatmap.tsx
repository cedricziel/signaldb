import { axisLabelFormatter } from "../../lib/time";
import { formatDurationMs } from "../../lib/waterfall";
import type { TraceLatencyHeatmap } from "../../api/traceVolume";

interface Props {
  heatmap: TraceLatencyHeatmap;
  label: string;
}

const WIDTH = 720;
const HEIGHT = 220;
const PADDING = { top: 3, right: 8, bottom: 14, left: 78 };
export function TraceVolumeHeatmap({
  heatmap,
  label,
}: Props) {
  const start = Math.floor(heatmap.window.start_ns / heatmap.x.step_ns) * heatmap.x.step_ns;
  const times: number[] = [];
  for (let t = start; t < heatmap.window.end_ns; t += heatmap.x.step_ns) times.push(t);
  const rows = heatmap.y.bounds.length + 1;
  if (heatmap.cells.length === 0) return <div className="trace-heatmap-empty">No spans in range</div>;
  const cells = new Map(heatmap.cells.map((cell) => [`${cell.time_bucket_ns}|${cell.duration_bucket}`, cell.count]));
  const max = Math.max(...heatmap.cells.map((cell) => cell.count));
  const formatAxis = axisLabelFormatter(
    times[0]! / 1e6,
    times[times.length - 1]! / 1e6,
  );
  const plotWidth = WIDTH - PADDING.left - PADDING.right;
  const plotHeight = HEIGHT - PADDING.top - PADDING.bottom;
  const cellWidth = plotWidth / times.length;
  const cellHeight = plotHeight / rows;
  const bucketLabel = (row: number) => {
    const lower = row === 0 ? 0 : heatmap.y.bounds[row - 1]! / 1e6;
    const upper = heatmap.y.bounds[row] === undefined ? undefined : heatmap.y.bounds[row]! / 1e6;
    return upper === undefined ? `${formatDurationMs(lower)}+` : `${formatDurationMs(lower)}-${formatDurationMs(upper)}`;
  };
  const summary = `${label} heatmap. Rows are latency buckets. Columns are time buckets. Color intensity represents span count. Empty cells have no spans.`;

  return (
    <div
      className="trace-heatmap"
      data-testid="trace-volume-heatmap"
      role="group"
      aria-label={`${label} heatmap`}
      aria-describedby="trace-volume-heatmap-summary"
    >
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} preserveAspectRatio="none">
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
              const count = cells.get(`${time}|${row}`) ?? 0;
              const intensity = count / max;
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
                  aria-label={
                    count > 0
                      ? `${formatAxis(time / 1e6)}, ${bucketLabel(row)}: ${count} spans`
                      : `${formatAxis(time / 1e6)}, ${bucketLabel(row)}: no spans`
                  }
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
          x={WIDTH - PADDING.right}
          y={HEIGHT - 3}
          textAnchor="end"
        >
          {formatAxis(times[times.length - 1]! / 1e6)}
        </text>
      </svg>
      <div className="trace-heatmap-summary" id="trace-volume-heatmap-summary">
        {summary}
      </div>
    </div>
  );
}
