import { useMemo, useState } from "react";
import type { RenderResponse } from "../../api/pyroscope";
import {
  type FlameFrame,
  type FlameView,
  ancestorPath,
  colorBucket,
  decodeFlamebearer,
  formatPct,
  formatTicks,
  frameView,
  placeFrames,
} from "../../lib/flamebearer";

const PALETTE = ["--svc-a", "--svc-b", "--svc-c", "--svc-d", "--svc-e"];

interface FlamePaneProps {
  levels: FlameFrame[][];
  /** Root width in ticks — the denominator for percentage formatting. */
  totalTicks: number;
  /** Unit of the selected profile type, e.g. "nanoseconds", for formatting. */
  unit: string;
  /** Shown above the toolbar; omitted for the single-flamegraph view. */
  title?: string;
}

/**
 * One interactive flame graph: search-highlight, zoom-with-breadcrumb, and a
 * hover/focus detail line. Shared by the single-profile view and each side
 * of the diff view (independently fetched and zoomed, just placed side by
 * side), which only differ in their frame data.
 */
export function FlamePane({ levels, totalTicks, unit, title }: FlamePaneProps) {
  const [zoomStack, setZoomStack] = useState<FlameFrame[]>([]);
  const [hovered, setHovered] = useState<FlameFrame | null>(null);
  const [highlight, setHighlight] = useState("");

  const focused = zoomStack[zoomStack.length - 1] ?? null;
  const view: FlameView = focused
    ? frameView(focused)
    : { x: 0, total: Math.max(totalTicks, 1), level: 0 };
  const placed = useMemo(() => placeFrames(levels, view), [levels, view]);

  // Case-insensitive substring match, and the self-time share it covers —
  // the quick answer to "how much of this is my code?".
  const needle = highlight.trim().toLowerCase();
  const matchedSelf = useMemo(() => {
    if (!needle) return 0;
    let sum = 0;
    for (const level of levels) {
      for (const f of level) {
        if (f.name.toLowerCase().includes(needle)) sum += f.self;
      }
    }
    return sum;
  }, [levels, needle]);

  const detail = hovered ?? focused ?? levels[0]?.[0] ?? null;

  function zoomTo(frame: FlameFrame) {
    if (frame.level === 0) {
      setZoomStack([]);
      return;
    }
    setZoomStack(ancestorPath(levels, frame));
  }

  return (
    <div className="flamegraph">
      {title && <div className="flame-title">{title}</div>}
      <div className="flame-toolbar">
        <input
          className="flame-search"
          aria-label="Highlight frames"
          placeholder="Highlight frames… (e.g. querier, common::)"
          value={highlight}
          onChange={(e) => setHighlight(e.target.value)}
        />
        {needle && (
          <span className="flame-matched">
            {formatPct(matchedSelf, totalTicks)} matched
          </span>
        )}
        {zoomStack.length > 0 && (
          <div className="flame-breadcrumb" aria-label="Zoom path">
            <button
              type="button"
              className="flame-crumb"
              onClick={() => setZoomStack([])}
            >
              root
            </button>
            {/* zoomStack[0] is always the root frame itself — the "root"
                button above already covers it, so start one past it. */}
            {zoomStack.slice(1).map((f, i) => (
              <span key={`${f.level}-${f.x}`}>
                <span className="flame-crumb-sep">›</span>
                <button
                  type="button"
                  className="flame-crumb"
                  onClick={() => setZoomStack(zoomStack.slice(0, i + 2))}
                >
                  {f.name}
                </button>
              </span>
            ))}
          </div>
        )}
      </div>

      <div className="flame-rows" onMouseLeave={() => setHovered(null)}>
        {placed.map((row, depth) =>
          row.length === 0 ? null : (
            <div className="flame-row" key={depth}>
              {row.map(({ frame, leftPct, widthPct }) => {
                const isRoot = frame.level === 0;
                const color = isRoot
                  ? "--accent"
                  : PALETTE[colorBucket(frame.name, PALETTE.length)];
                const dim =
                  needle !== "" && !frame.name.toLowerCase().includes(needle);
                return (
                  <button
                    key={`${frame.level}-${frame.x}`}
                    type="button"
                    className={`flame-frame${dim ? " dim" : ""}`}
                    style={{
                      left: `${leftPct}%`,
                      width: `${widthPct}%`,
                      background: `var(${color}-soft, var(${color}))`,
                      borderColor: `var(${color})`,
                    }}
                    title={`${frame.name} — ${formatTicks(frame.total, unit)} (${formatPct(frame.total, totalTicks)})`}
                    onMouseEnter={() => setHovered(frame)}
                    onFocus={() => setHovered(frame)}
                    onClick={() => zoomTo(frame)}
                  >
                    <span className="flame-label">{frame.name}</span>
                  </button>
                );
              })}
            </div>
          ),
        )}
      </div>

      {detail && (
        <div className="flame-detail" aria-live="polite">
          <span className="flame-detail-name">{detail.name}</span>
          <span className="flame-detail-meta">
            self {formatTicks(detail.self, unit)} (
            {formatPct(detail.self, totalTicks)}) · total{" "}
            {formatTicks(detail.total, unit)} (
            {formatPct(detail.total, totalTicks)})
          </span>
        </div>
      )}
    </div>
  );
}

interface Props {
  render: RenderResponse;
  /** Unit of the selected profile type, e.g. "nanoseconds", for formatting. */
  unit: string;
}

export function FlameGraph({ render, unit }: Props) {
  const fb = render.flamebearer;
  const levels = useMemo(() => decodeFlamebearer(fb), [fb]);
  return <FlamePane levels={levels} totalTicks={fb.numTicks} unit={unit} />;
}
