import {
  memo,
  useCallback,
  useId,
  useMemo,
  useRef,
  useState,
  type FocusEvent,
  type PointerEvent,
} from "react";
import type { RenderResponse } from "../../api/profileTypes";
import type { FrameLocation } from "../../api/gen";
import { SourceSnippet } from "../../components/SourceSnippet";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import {
  useRovingFocus,
  type RovingFocusItemProps,
} from "../../hooks/useRovingFocus";
import {
  type FlameFrame,
  type FlameView,
  type FramePlacement,
  type FunctionTotal,
  ancestorPath,
  collapseSmallFrames,
  colorBucket,
  decodeFlamebearer,
  formatPct,
  formatTicks,
  frameContains,
  frameView,
  OTHER_FRAME_NAME,
  placeFrames,
  simplifyFrameName,
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

/** What the pointer is over — a flame frame or a top-functions row. */
interface HoverInfo {
  name: string;
  self: number;
  total: number;
  /** CSS colour of the frame's bar; top-table rows have none. */
  swatch?: string;
  /** The frame's source location, when known — shown as `file:line` in the
   * tooltip footer. No fetch happens on hover; this only reads the
   * pre-fetched flamegraph-envelope `locations`. */
  location?: FrameLocation;
}

/** Colour of a frame's bar, keyed by name; the root uses the accent. */
function frameColor(frame: FlameFrame): string {
  return frame.level === 0
    ? "--accent"
    : PALETTE[colorBucket(frame.name, PALETTE.length)]!;
}

/** One style object per colour, built once: the render path visits
 * thousands of frames and would otherwise allocate a fresh object (and two
 * identical strings) for each on every hover or search change. */
const FRAME_STYLE = new Map(
  ["--accent", ...PALETTE].map((color) => [
    color,
    {
      // A soft fill under a solid edge of the same hue; the mix resolves
      // against the current theme's surface, so it holds in dark mode too.
      background: `color-mix(in srgb, var(${color}) 38%, var(--surface))`,
      borderColor: `var(${color})`,
    },
  ]),
);

function frameHoverInfo(
  frame: FlameFrame,
  nameLocation?: Map<string, FrameLocation>,
): HoverInfo {
  const isOther = frame.name === OTHER_FRAME_NAME;
  return {
    name: frame.name,
    self: frame.self,
    total: frame.total,
    swatch: isOther ? undefined : `var(${frameColor(frame)})`,
    location: nameLocation?.get(frame.name),
  };
}

/** `file:line` for a tooltip footer, or `undefined` when the location is
 * unknown (line 0 per the flamegraph envelope's convention). */
function locationFooter(location: FrameLocation | undefined): string | undefined {
  return location && location.line > 0
    ? `${location.file}:${location.line}`
    : undefined;
}

interface FlameRowsProps {
  placed: ReturnType<typeof placeFrames>;
  needle: string;
  /** The frame the tooltip currently describes, for `aria-describedby`. */
  hovered: FlameFrame | null;
  tipId: string;
  /** Roving-tabindex props for a frame — one tab stop for the whole pane. */
  getItemProps: (frame: FlameFrame) => RovingFocusItemProps;
  onHover: (frame: FlameFrame, e: PointerEvent<HTMLElement>) => void;
  onFocus: (frame: FlameFrame, e: FocusEvent<HTMLElement>) => void;
  onLeave: () => void;
  onZoom: (frame: FlameFrame) => void;
}

/**
 * The frame bars. Memoized so that pointer tracking for the tooltip — a
 * state update on every mousemove — re-renders only the tooltip, not the
 * thousands of frames a real profile can have; the rows re-render only when
 * the layout, highlight, or hovered frame changes.
 */
const FlameRows = memo(function FlameRows({
  placed,
  needle,
  hovered,
  tipId,
  getItemProps,
  onHover,
  onFocus,
  onLeave,
  onZoom,
}: FlameRowsProps) {
  return (
    <div
      className="flame-rows"
      role="group"
      aria-label="Flame graph frames"
      onPointerLeave={onLeave}
    >
      {placed.map((row, depth) =>
        row.length === 0 ? null : (
          <div className="flame-row" key={depth}>
            {row.map(({ frame, leftPct, widthPct }) => {
              const isOther = frame.name === OTHER_FRAME_NAME;
              const color = frameColor(frame);
              const dim =
                needle !== "" && !frame.name.toLowerCase().includes(needle);
              const item = getItemProps(frame);
              return (
                <div
                  key={`${frame.level}-${frame.x}`}
                  className="flame-frame-wrap"
                  style={{ left: `${leftPct}%`, width: `${widthPct}%` }}
                >
                  <button
                    type="button"
                    className={`flame-frame${dim ? " dim" : ""}${isOther ? " other" : ""}`}
                    style={isOther ? undefined : FRAME_STYLE.get(color)}
                    aria-label={frame.name}
                    aria-describedby={hovered === frame ? tipId : undefined}
                    tabIndex={item.tabIndex}
                    ref={item.ref}
                    onKeyDown={item.onKeyDown}
                    onPointerMove={(e) => onHover(frame, e)}
                    onFocus={(e) => {
                      item.onFocus();
                      onFocus(frame, e);
                    }}
                    onBlur={onLeave}
                    onClick={() => onZoom(frame)}
                  >
                    <span className="flame-label">
                      {simplifyFrameName(frame.name)}
                    </span>
                  </button>
                </div>
              );
            })}
          </div>
        ),
      )}
    </div>
  );
});

/** Crumbs collapse to past this many levels deep. */
const BREADCRUMB_COLLAPSE_DEPTH = 4;

/**
 * The zoom path, `root › a › b › current`. Deeper than
 * {@link BREADCRUMB_COLLAPSE_DEPTH} levels, the middle collapses to a single
 * "…" crumb — `root › … › parent › current` — so a deep zoom path stays
 * readable instead of clipping inside the toolbar; clicking it reveals the
 * full path. `.flame-breadcrumb` also allows horizontal scroll as a
 * fallback, for when even the collapsed form (or a caller with unusually
 * long frame names) still overflows.
 */
function Breadcrumb({
  zoomStack,
  onNavigate,
}: {
  zoomStack: FlameFrame[];
  onNavigate: (next: FlameFrame[]) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const crumbs = [
    { key: "root", label: "root", onClick: () => onNavigate([]) },
    // zoomStack[0] is always the root frame itself — the "root" crumb above
    // already covers it, so start one past it.
    ...zoomStack.slice(1).map((f, i) => ({
      key: `${f.level}-${f.x}`,
      label: f.name,
      onClick: () => onNavigate(zoomStack.slice(0, i + 2)),
    })),
  ];
  const collapsed = !expanded && crumbs.length > BREADCRUMB_COLLAPSE_DEPTH;
  const shown = collapsed
    ? [crumbs[0]!, crumbs[crumbs.length - 2]!, crumbs[crumbs.length - 1]!]
    : crumbs;

  return (
    <div className="flame-breadcrumb" aria-label="Zoom path">
      <button
        type="button"
        className="flame-crumb"
        onClick={crumbs[0]!.onClick}
      >
        {crumbs[0]!.label}
      </button>
      {collapsed && (
        <span>
          <span className="flame-crumb-sep">›</span>
          <button
            type="button"
            className="flame-crumb flame-crumb-ellipsis"
            aria-label="Show full zoom path"
            onClick={() => setExpanded(true)}
          >
            …
          </button>
        </span>
      )}
      {shown.slice(1).map((crumb) => (
        <span key={crumb.key}>
          <span className="flame-crumb-sep">›</span>
          <button
            type="button"
            className="flame-crumb"
            onClick={crumb.onClick}
          >
            {crumb.label}
          </button>
        </span>
      ))}
    </div>
  );
}

interface FlamePaneProps {
  levels: FlameFrame[][];
  /** Root width in ticks — the denominator for percentage formatting. */
  totalTicks: number;
  /** Unit of the selected profile type, e.g. "nanoseconds", for formatting. */
  unit: string;
  /** Shown above the toolbar; omitted for the single-flamegraph view. */
  title?: string;
  /** Set when the caller's tenant can be served source context (see
   * `useSourceContextEnabled`); enables the Top-functions Source column.
   * `SourceSnippet` still decides per row, on top of a frame actually
   * having a location. */
  tenant?: string;
  /** Function name → source location, built once by `FlameGraph`. */
  nameLocation?: Map<string, FrameLocation>;
}

/**
 * One interactive flame graph: search-highlight, zoom-with-breadcrumb, and a
 * hover/focus detail line. Shared by the single-profile view and each side
 * of the diff view (independently fetched and zoomed, just placed side by
 * side), which only differ in their frame data.
 */
export function FlamePane({
  levels,
  totalTicks,
  unit,
  title,
  tenant,
  nameLocation,
}: FlamePaneProps) {
  const [zoomStack, setZoomStack] = useState<FlameFrame[]>([]);
  const [hovered, setHovered] = useState<FlameFrame | null>(null);
  const [highlight, setHighlight] = useState("");
  const [viewMode, setViewMode] = useState<ViewMode>("flame");
  const [collapseThreshold, setCollapseThreshold] = useState(
    COLLAPSE_PRESETS[1]!.value, // 0.5% by default — "big and noisy" is the common case
  );
  const [topSort, toggleTopSort] = useSort("self", "desc");
  // One pointer-following VizTooltip for the whole pane (flame graph and
  // top-functions table alike); the pane root is its positioning host.
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const tipId = useId();
  const clearHover = useCallback(() => {
    setHovered(null);
    setHoverInfo(null);
    pointer.clear();
  }, [pointer.clear]);
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

  // Roving tabindex over every visible frame: one tab stop for the pane,
  // arrow keys move within a level (left/right) or to a covering
  // parent/child frame in the level above/below (up/down). Kept alongside
  // `placed` (not `effectiveLevels`) so it only sees frames actually drawn
  // in the current zoom.
  const flatFrames = useMemo(() => {
    const out: FramePlacement[] = [];
    for (const row of placed) if (row.length > 0) out.push(...row);
    return out;
  }, [placed]);
  const frameIndex = useMemo(() => {
    const m = new Map<FlameFrame, number>();
    flatFrames.forEach((p, i) => m.set(p.frame, i));
    return m;
  }, [flatFrames]);
  // Contiguous [start, end) index ranges of `flatFrames` per drawn level, in
  // the same order rows render — a frame's siblings on its own level.
  const levelRanges = useMemo(() => {
    const ranges: { level: number; start: number; end: number }[] = [];
    let cursor = 0;
    for (const row of placed) {
      if (row.length === 0) continue;
      ranges.push({
        level: row[0]!.frame.level,
        start: cursor,
        end: cursor + row.length,
      });
      cursor += row.length;
    }
    return ranges;
  }, [placed]);
  const stepWithinLevel = useCallback(
    (index: number, direction: -1 | 1) => {
      const range = levelRanges.find((r) => index >= r.start && index < r.end);
      if (!range) return null;
      const next = index + direction;
      return next >= range.start && next < range.end ? next : null;
    },
    [levelRanges],
  );
  const stepAcrossLevel = useCallback(
    (index: number, direction: -1 | 1) => {
      const frame = flatFrames[index]?.frame;
      if (!frame) return null;
      const targetLevel = frame.level + direction;
      const range = levelRanges.find((r) => r.level === targetLevel);
      if (!range) return null;
      if (direction === -1) {
        // Up: the unique parent-level frame whose interval contains this one.
        for (let i = range.start; i < range.end; i++) {
          if (frameContains(flatFrames[i]!.frame, frame)) return i;
        }
        return null;
      }
      // Down: the first (leftmost) child whose interval falls within this
      // frame's.
      for (let i = range.start; i < range.end; i++) {
        const f = flatFrames[i]!.frame;
        if (f.x >= frame.x && f.x < frame.x + frame.total) return i;
      }
      return null;
    },
    [flatFrames, levelRanges],
  );
  const roving = useRovingFocus(flatFrames.length, {
    horizontal: stepWithinLevel,
    vertical: stepAcrossLevel,
  });
  const getFrameItemProps = useCallback(
    (frame: FlameFrame) => roving.itemProps(frameIndex.get(frame) ?? -1),
    [roving.itemProps, frameIndex],
  );

  // Pointer/focus over a frame also moves the pane's roving tab stop
  // (`frameIndex`/`roving` above), so Tab picks up keyboard navigation from
  // wherever the pointer last landed, not just where an arrow key left it.
  const hoverFrame = useCallback(
    (frame: FlameFrame, e: PointerEvent<HTMLElement>) => {
      setHovered(frame);
      setHoverInfo(frameHoverInfo(frame, nameLocation));
      const index = frameIndex.get(frame);
      if (index !== undefined) roving.setActiveIndex(index);
      pointer.track(e);
    },
    [pointer.track, frameIndex, roving.setActiveIndex, nameLocation],
  );
  const focusFrame = useCallback(
    (frame: FlameFrame, e: FocusEvent<HTMLElement>) => {
      setHovered(frame);
      setHoverInfo(frameHoverInfo(frame, nameLocation));
      pointer.anchorTo(e.currentTarget);
    },
    [pointer.anchorTo, nameLocation],
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

  const zoomTo = useCallback(
    (frame: FlameFrame) => {
      if (frame.level === 0) {
        setZoomStack([]);
        return;
      }
      setZoomStack(ancestorPath(effectiveLevels, frame));
    },
    [effectiveLevels],
  );

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
    <div className="flamegraph viz-host" ref={hostRef}>
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
          <Breadcrumb zoomStack={zoomStack} onNavigate={setZoomStack} />
        )}
      </div>

      {viewMode === "flame" ? (
        <>
          <FlameRows
            placed={placed}
            needle={needle}
            hovered={hovered}
            tipId={tipId}
            getItemProps={getFrameItemProps}
            onHover={hoverFrame}
            onFocus={focusFrame}
            onLeave={clearHover}
            onZoom={zoomTo}
          />

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
                {tenant && <th>Source</th>}
              </tr>
            </thead>
            <tbody>
              {topRows.map((f) => {
                const location = nameLocation?.get(f.name);
                return (
                  <tr
                    key={f.name}
                    onClick={() => selectFunction(f.name)}
                    onPointerMove={(e) => {
                      setHoverInfo({
                        name: f.name,
                        self: f.self,
                        total: f.total,
                        location,
                      });
                      pointer.track(e);
                    }}
                    onPointerLeave={clearHover}
                  >
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
                    {tenant && (
                      <td
                        className="flame-top-source"
                        onClick={(e) => e.stopPropagation()}
                      >
                        {location && location.line > 0 && (
                          <SourceSnippet
                            tenant={tenant}
                            path={location.file}
                            line={location.line}
                          />
                        )}
                      </td>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
          {topRows.length === 0 && (
            <div className="view-note">No functions match.</div>
          )}
          {topTruncated > 0 && (
            <div className="view-note">
              Showing the top {TOP_CAP} of {topFiltered.length} functions by
              self time.
            </div>
          )}
        </div>
      )}
      {hoverInfo && pointer.anchor && (
        <VizTooltip
          id={tipId}
          anchor={pointer.anchor}
          host={pointer.host}
          title={hoverInfo.name}
          rows={[
            {
              swatch: hoverInfo.swatch,
              label: "self",
              value: `${formatTicks(hoverInfo.self, unit)} (${formatPct(hoverInfo.self, totalTicks)})`,
            },
            {
              swatch: hoverInfo.swatch,
              label: "total",
              value: `${formatTicks(hoverInfo.total, unit)} (${formatPct(hoverInfo.total, totalTicks)})`,
            },
          ]}
          footer={locationFooter(hoverInfo.location)}
        />
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
  /** Set when the caller's tenant can be served source context; enables
   * the Top-functions table's Source column. `SourceSnippet` still decides
   * per row. */
  tenant?: string;
  /** Per-name source location, parallel to `render.flamebearer.names` (see
   * `FlamegraphFetch.locations`). */
  locations?: Array<FrameLocation | null>;
}

export function FlameGraph({ render, unit, tenant, locations }: Props) {
  const fb = render.flamebearer;
  const levels = useMemo(() => decodeFlamebearer(fb), [fb]);
  // Built once per (names, locations) pair — `names`/`locations` are
  // index-aligned and names are already unique (the flamegraph envelope
  // itself dedupes), so no need to guard against overwriting an entry.
  const nameLocation = useMemo(() => {
    const map = new Map<string, FrameLocation>();
    if (!locations) return map;
    fb.names.forEach((name, i) => {
      const location = locations[i];
      if (location) map.set(name, location);
    });
    return map;
  }, [fb.names, locations]);
  return (
    <FlamePane
      levels={levels}
      totalTicks={fb.numTicks}
      unit={unit}
      tenant={tenant}
      nameLocation={nameLocation}
    />
  );
}
