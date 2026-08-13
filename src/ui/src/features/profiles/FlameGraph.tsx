import { useMemo, useState } from "react";
import type { RenderResponse } from "../../api/pyroscope";
import {
  type FlameFrame,
  type FlameView,
  type FunctionTotal,
  ancestorPath,
  collapseSmallFrames,
  colorBucket,
  decodeFlamebearer,
  formatPct,
  formatTicks,
  frameView,
  OTHER_FRAME_NAME,
  placeFrames,
  topFunctionsBySelf,
} from "../../lib/flamebearer";
import { type SortValue, SortTh, sortRows, useSort } from "../../lib/sortTable";

const PALETTE = ["--svc-a", "--svc-b", "--svc-c", "--svc-d", "--svc-e"];

/** Fraction-of-root thresholds offered by the "Collapse" selector. */
const COLLAPSE_PRESETS = [
  { label: "Off", value: 0 },
  { label: "0.5%", value: 0.005 },
  { label: "1%", value: 0.01 },
  { label: "2%", value: 0.02 },
  { label: "5%", value: 0.05 },
];

type ViewMode = "flame" | "top";

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
  const [viewMode, setViewMode] = useState<ViewMode>("flame");
  const [collapseThreshold, setCollapseThreshold] = useState(
    COLLAPSE_PRESETS[1]!.value, // 0.5% by default — "big and noisy" is the common case
  );
  const [topSort, toggleTopSort] = useSort("self", "desc");

  // Below-threshold frames (and their subtrees) fold into "(other)" bars so
  // a wide, noisy profile isn't dominated by hairline slivers. Computed
  // against the root total so it stays stable across zoom.
  const effectiveLevels = useMemo(
    () => collapseSmallFrames(levels, totalTicks, collapseThreshold),
    [levels, totalTicks, collapseThreshold],
  );

  function setCollapseThresholdAndReset(value: number) {
    setCollapseThreshold(value);
    // The focused frame may not exist in the newly-collapsed tree.
    setZoomStack([]);
  }

  const focused = zoomStack[zoomStack.length - 1] ?? null;
  const view: FlameView = focused
    ? frameView(focused)
    : { x: 0, total: Math.max(totalTicks, 1), level: 0 };
  const placed = useMemo(
    () => placeFrames(effectiveLevels, view),
    [effectiveLevels, view],
  );

  // Case-insensitive substring match, and the self-time share it covers —
  // the quick answer to "how much of this is my code?". Computed against
  // the collapsed tree so it matches what can actually be highlighted.
  const needle = highlight.trim().toLowerCase();
  const matchedSelf = useMemo(() => {
    if (!needle) return 0;
    let sum = 0;
    for (const level of effectiveLevels) {
      for (const f of level) {
        if (f.name.toLowerCase().includes(needle)) sum += f.self;
      }
    }
    return sum;
  }, [effectiveLevels, needle]);

  const detail = hovered ?? focused ?? effectiveLevels[0]?.[0] ?? null;

  function zoomTo(frame: FlameFrame) {
    if (frame.level === 0) {
      setZoomStack([]);
      return;
    }
    setZoomStack(ancestorPath(effectiveLevels, frame));
  }

  // The flat "top functions" table aggregates over the original,
  // uncollapsed tree — ranking is already a form of noise reduction, so it
  // shouldn't also lose small-but-real functions to the collapse threshold.
  const topFunctions = useMemo(() => topFunctionsBySelf(levels), [levels]);
  const topFiltered = needle
    ? topFunctions.filter((f) => f.name.toLowerCase().includes(needle))
    : topFunctions;
  const TOP_CAP = 50;
  const topRows = sortRows(
    topFiltered.slice(0, TOP_CAP),
    topSort,
    functionSortValue,
  );
  const topTruncated = Math.max(0, topFiltered.length - TOP_CAP);

  function selectFunction(name: string) {
    setHighlight(name);
    setViewMode("flame");
  }

  return (
    <div className="flamegraph">
      {title && <div className="flame-title">{title}</div>}
      <div className="flame-toolbar">
        <div className="flame-view-toggle" role="tablist" aria-label="View">
          <button
            type="button"
            role="tab"
            aria-selected={viewMode === "flame"}
            className={viewMode === "flame" ? "active" : undefined}
            onClick={() => setViewMode("flame")}
          >
            Flame graph
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={viewMode === "top"}
            className={viewMode === "top" ? "active" : undefined}
            onClick={() => setViewMode("top")}
          >
            Top functions
          </button>
        </div>
        {viewMode === "flame" && (
          <label className="flame-collapse">
            Collapse
            <select
              aria-label="Collapse small frames"
              value={collapseThreshold}
              onChange={(e) =>
                setCollapseThresholdAndReset(Number(e.target.value))
              }
            >
              {COLLAPSE_PRESETS.map((p) => (
                <option key={p.value} value={p.value}>
                  {p.label}
                </option>
              ))}
            </select>
          </label>
        )}
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
        {viewMode === "flame" && zoomStack.length > 0 && (
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

      {viewMode === "flame" ? (
        <>
          <div className="flame-rows" onMouseLeave={() => setHovered(null)}>
            {placed.map((row, depth) =>
              row.length === 0 ? null : (
                <div className="flame-row" key={depth}>
                  {row.map(({ frame, leftPct, widthPct }) => {
                    const isRoot = frame.level === 0;
                    const isOther = frame.name === OTHER_FRAME_NAME;
                    const color = isRoot
                      ? "--accent"
                      : PALETTE[colorBucket(frame.name, PALETTE.length)];
                    const dim =
                      needle !== "" &&
                      !frame.name.toLowerCase().includes(needle);
                    return (
                      <button
                        key={`${frame.level}-${frame.x}`}
                        type="button"
                        className={`flame-frame${dim ? " dim" : ""}${isOther ? " other" : ""}`}
                        style={
                          isOther
                            ? { left: `${leftPct}%`, width: `${widthPct}%` }
                            : {
                                left: `${leftPct}%`,
                                width: `${widthPct}%`,
                                background: `var(${color}-soft, var(${color}))`,
                                borderColor: `var(${color})`,
                              }
                        }
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
        </>
      ) : (
        <div className="flame-top">
          <table className="flame-top-table">
            <thead>
              <tr>
                <SortTh
                  label="Function"
                  sortKey="name"
                  sort={topSort}
                  toggle={toggleTopSort}
                />
                <SortTh
                  label="Self"
                  sortKey="self"
                  sort={topSort}
                  toggle={toggleTopSort}
                  numeric
                />
                <th className="num">Self %</th>
                <SortTh
                  label="Total"
                  sortKey="total"
                  sort={topSort}
                  toggle={toggleTopSort}
                  numeric
                />
                <th className="num">Total %</th>
              </tr>
            </thead>
            <tbody>
              {topRows.map((f) => (
                <tr key={f.name} onClick={() => selectFunction(f.name)}>
                  <td>
                    <button className="flame-top-open">{f.name}</button>
                    {f.count > 1 && (
                      <span className="flame-top-count"> ×{f.count}</span>
                    )}
                  </td>
                  <td className="num">{formatTicks(f.self, unit)}</td>
                  <td className="num">{formatPct(f.self, totalTicks)}</td>
                  <td className="num">{formatTicks(f.total, unit)}</td>
                  <td className="num">{formatPct(f.total, totalTicks)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {topRows.length === 0 && (
            <div className="profiles-note">No functions match.</div>
          )}
          {topTruncated > 0 && (
            <div className="profiles-note">
              Showing the top {TOP_CAP} of {topFiltered.length} functions by
              self time.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function functionSortValue(f: FunctionTotal, key: string): SortValue {
  switch (key) {
    case "name":
      return f.name;
    case "total":
      return f.total;
    default:
      return f.self;
  }
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
