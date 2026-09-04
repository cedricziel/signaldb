import { useQuery } from "@tanstack/react-query";
import {
  Fragment,
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import {
  tempoSearchTags,
  type ProfileSummaryView,
  type SpanEventView,
  type TempoSpan,
} from "../../api/tempo";
import { ApiError } from "../../api/http";
import { fetchTraceDetail } from "../../api/traceDetail";
import {
  STATUS_COLORS,
  STATUS_ORDER,
  fetchTraceLatencyHeatmap,
  fetchTraceVolume,
} from "../../api/traceVolume";
import { SignalHistogram } from "../explore/SignalHistogram";
import { AttributeKeyInput } from "../../components/AttributeKeyInput";
import { AttributeValue } from "../../components/AttributeValue";
import {
  MobileFiltersToggle,
  MobileSidebarDrawer,
} from "../../components/MobileSidebarDrawer";
import { SemanticKey } from "../../components/SemanticKey";
import {
  useVizPointer,
  VizTooltip,
  type VizTooltipRow,
} from "../../components/VizTooltip";
import { useSemantics } from "../../hooks/useSemantics";
import { useMobileSidebar } from "../../hooks/useMobileSidebar";
import { groupBySemanticTitle } from "../../lib/semantics";
import { TraceFacets } from "./TraceFacets";
import { TraceVolumeAreaChart } from "./TraceVolumeAreaChart";
import { TraceVolumeHeatmap } from "./TraceVolumeHeatmap";
import {
  KIND_VALUES,
  compileTraceQL,
  facetField,
  removeTraceFilter,
  upsertTraceFilter,
  withDefaultTraceFilters,
  type TraceFilter,
} from "../../lib/traceFilters";
import {
  durationToSeconds,
  formatTimestamp,
  nanosToMs,
  rangeScopeKey,
  resolveRange,
  resolveStep,
  stepOptionsForRange,
  type ResolvedRange,
} from "../../lib/time";
import {
  BUILTIN_DIMENSIONS,
  compositeKey,
  formatRate,
  groupLabel,
  KEY_SEP,
  NOT_SET,
  parseCompositeKey,
  parseGroupBy,
} from "../../lib/traceGroups";
import {
  DEFAULT_GROUP_SORT,
  GROUP_BUDGET,
  fetchTraceGroups,
  type GroupGrain,
  type GroupSort,
} from "../../api/traceGroups";
import { fetchTraceGroupMembers } from "../../api/traceGroupMembers";
import { SkeletonLines, SkeletonRows } from "../explore/Skeleton";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { buildWaterfall, formatDurationMs } from "../../lib/waterfall";
import { fetchWindowTotal, looksUnresolved } from "./unresolvedGroup";
import { describeService, groupSpanAttributes } from "./spanAttributes";
import { SortTh, useSort } from "../../lib/sortTable";
import { MemberTable } from "./MemberTable";
import "./traces.css";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

/** OTel span kind -> a stable slot in the shared --svc-a..e palette (the
 * same one FlameGraph uses), so a kind's color means the same thing across
 * the app rather than being hash-bucketed per span name. */
const KIND_CLASS: Record<string, string> = {
  SERVER: "kind-server",
  CLIENT: "kind-client",
  INTERNAL: "kind-internal",
  PRODUCER: "kind-producer",
  CONSUMER: "kind-consumer",
};

function kindClass(kind: string | undefined): string {
  if (!kind) return "";
  return KIND_CLASS[kind.toUpperCase()] ?? "";
}

/** The kind's palette colour (what `.span-bar.kind-*` and the legend use). */
const KIND_SWATCH: Record<string, string> = {
  SERVER: "var(--svc-a)",
  CLIENT: "var(--svc-b)",
  INTERNAL: "var(--svc-c)",
  PRODUCER: "var(--svc-d)",
  CONSUMER: "var(--svc-e)",
};

/**
 * Rows of the waterfall hover tooltip: who ran the span (service, namespace,
 * version from the resource), its kind, duration, and status. Absent
 * namespace/version/kind stay listed, muted, so the layout is stable while
 * the pointer moves between spans.
 */
function spanTooltipRows(
  span: TempoSpan,
  durationMs: number,
  kind: string | undefined,
): VizTooltipRow[] {
  const optional = (label: string, value: unknown): VizTooltipRow =>
    value === undefined || value === ""
      ? { label, value: "–", muted: true }
      : { label, value: String(value) };
  return [
    { label: "service", value: span.serviceName },
    optional("namespace", span.attributes["resource.service.namespace"]),
    optional("version", span.attributes["resource.service.version"]),
    kind
      ? { swatch: KIND_SWATCH[kind.toUpperCase()], label: "kind", value: kind }
      : { label: "kind", value: "–", muted: true },
    { label: "duration", value: formatDurationMs(durationMs) },
    { label: "status", value: span.status },
  ];
}

function plural(n: number, noun: string): string {
  return `${n} ${noun}${n === 1 ? "" : "s"}`;
}

export function TracesView({ state, update }: Props) {
  if (state.trace !== "") {
    return <TraceDetail state={state} update={update} />;
  }
  return <TraceSearch state={state} update={update} />;
}

function TraceSearch({ state, update }: Props) {
  const [volumeView, setVolumeView] = useState<
    "histogram" | "area" | "heatmap"
  >("histogram");
  const mobileSidebar = useMobileSidebar();
  const rangeKey = rangeScopeKey(state);
  // The state may carry no kind filter (a fresh URL, a link from another
  // view): the default kinds apply on read, and every update writes the
  // full, explicit set back.
  const filters = withDefaultTraceFilters(state.traceFilters);
  const traceql = compileTraceQL(filters);
  const dims = parseGroupBy(state.groupBy);

  const resolvedForStep = resolveRange(state.range, Date.now());
  const step = resolveStep(resolvedForStep, state.step);
  // Deliberately keyed without `state.limit`: the volume aggregate covers the
  // whole window and must not move when the trace list's limit changes. It
  // does follow the filters, so the chart describes what the table shows.
  const volume = useQuery({
    queryKey: ["trace-volume", rangeKey, step, traceql],
    queryFn: () =>
      fetchTraceVolume(resolveRange(state.range, Date.now()), step, filters),
  });
  const latencyHeatmap = useQuery({
    queryKey: ["trace-latency", rangeKey, step, traceql],
    queryFn: () =>
      fetchTraceLatencyHeatmap(
        resolveRange(state.range, Date.now()),
        step,
        filters,
      ),
    enabled: volumeView === "heatmap",
  });

  const addFilter = (f: TraceFilter) =>
    update({ traceFilters: upsertTraceFilter(filters, f), group: "" });
  const removeFilter = (f: TraceFilter) =>
    update({ traceFilters: removeTraceFilter(filters, f), group: "" });
  // Multi-value facets (kind) show their selection in the sidebar checkboxes;
  // as chips they would be permanent clutter (a default set is always on).
  const chips = filters.filter((f) => !facetField(f.field)?.multi);

  return (
    <div className="traces-search">
      <div className="histo-wrap">
        <div className="trace-volume-head">
          <span>Span volume</span>
          <div
            className="trace-volume-mode"
            role="group"
            aria-label="Trace volume visualization"
          >
            <button
              type="button"
              aria-pressed={volumeView === "histogram"}
              onClick={() => setVolumeView("histogram")}
            >
              Histogram
            </button>
            <button
              type="button"
              aria-pressed={volumeView === "area"}
              onClick={() => setVolumeView("area")}
            >
              Area
            </button>
            <button
              type="button"
              aria-pressed={volumeView === "heatmap"}
              onClick={() => setVolumeView("heatmap")}
            >
              Heatmap
            </button>
          </div>
        </div>
        {volumeView === "heatmap" ? (
          latencyHeatmap.data ? (
            <TraceVolumeHeatmap
              heatmap={latencyHeatmap.data}
              label="Span latency"
            />
          ) : latencyHeatmap.isPending ? (
            <div className="trace-heatmap-empty">Loading latency...</div>
          ) : latencyHeatmap.isError ? (
            <div className="trace-heatmap-empty" role="alert">
              Latency query failed: {(latencyHeatmap.error as Error).message}
            </div>
          ) : null
        ) : volume.data && volumeView === "histogram" ? (
          <SignalHistogram
            series={volume.data}
            order={STATUS_ORDER}
            colors={STATUS_COLORS}
            rangeMs={resolvedForStep}
            stepMs={(durationToSeconds(step) ?? 60) * 1000}
            scale={state.scale}
            unit="spans"
            label="Span volume over time by status"
            onScaleChange={(scale) => update({ scale })}
            step={state.step}
            stepOptions={stepOptionsForRange(resolvedForStep)}
            onStepChange={(step) => update({ step })}
          />
        ) : volume.data && volumeView === "area" ? (
          <TraceVolumeAreaChart
            series={volume.data}
            order={STATUS_ORDER}
            colors={STATUS_COLORS}
            rangeMs={resolvedForStep}
            stepMs={(durationToSeconds(step) ?? 60) * 1000}
            unit="spans"
            label="Span volume"
          />
        ) : null}
      </div>
      {chips.length > 0 && (
        <div className="filter-chips" aria-label="Active filters">
          {chips.map((f) => (
            <button
              className="filter-chip"
              key={`${f.field}|${f.value}`}
              aria-label={`Remove filter ${f.field} = ${f.value}`}
              onClick={() => removeFilter(f)}
            >
              <span className="filter-chip-k">{f.field}</span>
              <span className="filter-chip-v">{f.value}</span>
              <span className="filter-chip-x">×</span>
            </button>
          ))}
        </div>
      )}
      <MobileFiltersToggle
        open={mobileSidebar.open}
        onToggle={mobileSidebar.toggle}
      />

      <div className="traces-body">
        <MobileSidebarDrawer
          open={mobileSidebar.open}
          onClose={mobileSidebar.close}
        >
          <TraceFacets
            range={resolvedForStep}
            rangeKey={rangeKey}
            filters={filters}
            onAddFilter={addFilter}
            onRemoveFilter={removeFilter}
          />
        </MobileSidebarDrawer>
        <div className="traces-main">
          <div className="traces-toolbar">
            <form
              className="trace-id-form"
              onSubmit={(e) => {
                e.preventDefault();
                const id = new FormData(e.currentTarget).get("traceId");
                if (typeof id === "string" && id.trim() !== "") {
                  update({ trace: id.trim() }, { push: true });
                }
              }}
            >
              <input
                name="traceId"
                aria-label="Trace ID"
                placeholder="Open trace by ID…"
              />
              <button type="submit">Open</button>
            </form>
            {state.group === "" && (
              <>
                <DimensionPickers
                  groupBy={state.groupBy}
                  update={update}
                  range={resolvedForStep}
                  rangeKey={rangeKey}
                />
                <GrainToggle grain={state.grain} update={update} />
              </>
            )}
          </div>

          {state.group === "" ? (
            <GroupList
              dims={dims}
              filters={filters}
              traceql={traceql}
              range={resolvedForStep}
              rangeKey={rangeKey}
              grain={state.grain}
              rangeSeconds={rangeSeconds(state)}
              update={update}
            />
          ) : (
            <GroupDetail state={state} update={update} />
          )}
        </div>
      </div>
    </div>
  );
}

function rangeSeconds(state: ExploreState): number {
  const r = resolveRange(state.range, Date.now());
  return Math.max(1, (r.toMs - r.fromMs) / 1000);
}

function DimensionPickers({
  groupBy,
  update,
  range,
  rangeKey,
}: {
  groupBy: string;
  update: UpdateFn;
  range: ResolvedRange;
  rangeKey: string;
}) {
  const dims = parseGroupBy(groupBy);
  const primary = dims[0] ?? "";
  const secondary = dims[1] ?? "";
  const setDims = (next: string[]) =>
    update({ groupBy: next.filter((d) => d !== "").join(","), group: "" });
  return (
    <div className="group-pickers">
      <label className="group-by">
        Group by
        <select
          aria-label="Group by"
          value={primary}
          onChange={(e) => {
            const v = e.currentTarget.value;
            setDims([v, secondary === v ? "" : secondary]);
          }}
        >
          {dimensionOptions(primary).map((d) => (
            <option key={d} value={d}>
              {d}
            </option>
          ))}
        </select>
      </label>
      <label className="group-by">
        Then by
        <select
          aria-label="Then by"
          value={secondary}
          onChange={(e) => setDims([primary, e.currentTarget.value])}
        >
          <option value="">—</option>
          {dimensionOptions(secondary)
            .filter((d) => d !== primary && d !== "")
            .map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
        </select>
      </label>
      <CustomDimensionInput
        range={range}
        rangeKey={rangeKey}
        onSubmit={(dim) => setDims([dim, secondary])}
      />
    </div>
  );
}

/**
 * Free-text "group by attribute" input: the group table is a server-side
 * aggregate with no trace sample to derive attribute names from, so this is
 * how a user reaches a field the built-in dimensions don't cover. Suggests
 * the attribute keys actually observed in the window (`/api/search/tags`,
 * #1073) merged with schema-registry hits, the same combobox as the logs
 * tab's filter-key input (`FilterChips`) — see `AttributeKeyInput`.
 */
// Stable empty list so a pending tag query does not hand the combobox a
// fresh array (and a fresh suggestion memo) on every render.
const NO_TAGS: string[] = [];

function CustomDimensionInput({
  range,
  rangeKey,
  onSubmit,
}: {
  range: ResolvedRange;
  rangeKey: string;
  onSubmit: (dim: string) => void;
}) {
  const [value, setValue] = useState("");
  const tags = useQuery({
    queryKey: ["trace-tag-names", rangeKey],
    queryFn: () => tempoSearchTags(range),
    staleTime: 60_000,
  });

  const submit = (dim: string) => {
    const trimmed = dim.trim();
    if (trimmed === "") return;
    onSubmit(trimmed);
    setValue("");
  };

  return (
    <form
      className="group-custom"
      onSubmit={(e) => {
        e.preventDefault();
        submit(value);
      }}
    >
      <AttributeKeyInput
        value={value}
        onChange={setValue}
        onPick={submit}
        observed={tags.data ?? NO_TAGS}
        ariaLabel="Custom dimension"
        placeholder="Group by attribute…"
      />
    </form>
  );
}

/**
 * Options offered by a dimension picker: the built-ins, plus the currently
 * picked dimension if it isn't one of them — otherwise switching this picker
 * away and back would lose a dimension typed into the custom field.
 */
function dimensionOptions(picked: string): string[] {
  return picked === "" || (BUILTIN_DIMENSIONS as string[]).includes(picked)
    ? BUILTIN_DIMENSIONS
    : [...BUILTIN_DIMENSIONS, picked].sort();
}

function GrainToggle({
  grain,
  update,
}: {
  grain: GroupGrain;
  update: UpdateFn;
}) {
  return (
    <label className="group-by">
      Grain
      <select
        aria-label="Grain"
        value={grain}
        onChange={(e) =>
          update({ grain: e.currentTarget.value as GroupGrain, group: "" })
        }
      >
        <option value="traces">Traces</option>
        <option value="spans">Spans</option>
      </select>
    </label>
  );
}

function GroupList({
  dims,
  filters,
  traceql,
  range,
  rangeKey,
  grain,
  rangeSeconds,
  update,
}: {
  dims: string[];
  filters: TraceFilter[];
  traceql: string;
  range: ResolvedRange;
  rangeKey: string;
  grain: GroupGrain;
  rangeSeconds: number;
  update: UpdateFn;
}) {
  // Sorting is a server-side `order` stage (see api/traceGroups), so a new
  // sort must refetch rather than re-rank the fetched page.
  const [sort, toggle] = useSort(
    DEFAULT_GROUP_SORT.key,
    DEFAULT_GROUP_SORT.dir,
  );
  const result = useQuery({
    queryKey: [
      "trace-groups",
      rangeKey,
      dims.join(","),
      grain,
      traceql,
      sort.key,
      sort.dir,
    ],
    queryFn: () =>
      fetchTraceGroups(dims, range, filters, grain, sort as GroupSort),
  });

  // #1070: an unresolvable dimension answers 200 with a single null-labelled
  // group holding the window total rather than erroring. A dimension that
  // resolves but that every remaining record simply lacks produces the same
  // *shape* legitimately, so the shape alone (`looksUnresolved`) only makes
  // it a suspect — confirming it needs the window total under the same
  // scope, fetched only when suspect.
  const suspect = result.data ? looksUnresolved(result.data.groups) : false;
  const windowTotal = useQuery({
    queryKey: ["trace-window-total", rangeKey, grain, traceql],
    queryFn: () => fetchWindowTotal(range, filters, grain),
    enabled: suspect,
  });
  const unresolved =
    suspect &&
    windowTotal.data !== undefined &&
    windowTotal.data === result.data!.groups[0]!.count;

  const pending = result.isPending || (suspect && windowTotal.isPending);
  const groups = unresolved ? [] : (result.data?.groups ?? []);
  const columns = dims.length + 6;
  const countLabel = grain === "spans" ? "Spans" : "Traces";
  const done = !pending && result.data !== undefined;
  // Trace grain scopes the aggregate to root spans only; a filter on a field
  // that only ever appears on a child span legitimately matches nothing.
  const rootGrainOnly =
    grain === "traces" && filters.some((f) => !facetField(f.field)?.multi);
  // Kind is a selection that is always on (Server/Client/Producer/Consumer
  // by default): when it hides everything, say so rather than "no groups".
  const kindsNarrowed =
    filters.filter((f) => f.field === "kind").length < KIND_VALUES.length;

  return (
    <>
      {result.isError && (
        <div className="query-error" role="alert">
          Groups failed: {(result.error as Error).message}
        </div>
      )}
      <table className="trace-table" aria-busy={pending}>
        <thead>
          <tr>
            {dims.map((d, i) => (
              <SortTh
                key={d}
                label={d}
                sortKey={`dim:${i}`}
                sort={sort}
                toggle={toggle}
              />
            ))}
            <SortTh
              label={countLabel}
              sortKey="n"
              sort={sort}
              toggle={toggle}
              numeric
            />
            {/* Rate is count / a fixed window — strictly increasing in count,
                so it sorts identically to n; no separate sort key needed. */}
            <SortTh
              label="Rate"
              sortKey="n"
              sort={sort}
              toggle={toggle}
              numeric
            />
            <SortTh
              label="Errors"
              sortKey="errors"
              sort={sort}
              toggle={toggle}
              numeric
            />
            <SortTh
              label="P50"
              sortKey="p50"
              sort={sort}
              toggle={toggle}
              numeric
            />
            <SortTh
              label="P95"
              sortKey="p95"
              sort={sort}
              toggle={toggle}
              numeric
            />
            <SortTh
              label="Last seen"
              sortKey="last"
              sort={sort}
              toggle={toggle}
              firstDir="desc"
            />
          </tr>
        </thead>
        <tbody>
          {pending ? (
            <SkeletonRows
              rows={8}
              columns={columns}
              numericFrom={dims.length}
            />
          ) : (
            groups.map((g) => {
              const key = compositeKey(g.values);
              return (
                <tr
                  key={key}
                  onClick={() => update({ group: key }, { push: true })}
                >
                  <td>
                    <button className="trace-open">
                      {g.values[0] ?? NOT_SET}
                    </button>
                  </td>
                  {g.values.slice(1).map((v, i) => (
                    <td key={dims[i + 1]}>{v ?? NOT_SET}</td>
                  ))}
                  <td className="num">{g.count}</td>
                  <td className="num">{formatRate(g.count, rangeSeconds)}</td>
                  <td className={`num${g.errors > 0 ? " err-rate" : ""}`}>
                    {g.errors > 0
                      ? `${Math.round((100 * g.errors) / g.count)}%`
                      : "–"}
                  </td>
                  <td className="num">{formatDurationMs(g.p50Ms)}</td>
                  <td className="num">{formatDurationMs(g.p95Ms)}</td>
                  <td>{formatTimestamp(nanosToMs(g.lastNs))}</td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
      {unresolved && (
        <div className="view-note">
          &ldquo;{dims.join(", ")}&rdquo; isn&rsquo;t queryable for this tenant
          or window right now — pick a different dimension.
        </div>
      )}
      {done && !unresolved && groups.length === 0 && rootGrainOnly && (
        <div className="view-note">
          No groups: trace grain only inspects each trace's root span, and one
          of the active filters is on a field that only appears on a child span.
          Switch to span grain to see it.
        </div>
      )}
      {done && !unresolved && groups.length === 0 && !rootGrainOnly && (
        <div className="view-note">
          No groups in this time range.
          {kindsNarrowed &&
            " Only the selected span kinds are included — Internal spans are off by default; adjust span.kind in the sidebar."}
        </div>
      )}
      {result.data?.truncated && (
        <div className="view-note">
          Showing the top {GROUP_BUDGET} groups by the current sort — narrow the
          time range or filters to see the rest.
        </div>
      )}
    </>
  );
}

function GroupDetail({
  state,
  update,
}: {
  state: ExploreState;
  update: UpdateFn;
}) {
  const rangeKey = rangeScopeKey(state);
  const range = resolveRange(state.range, Date.now());
  const traceql = compileTraceQL(state.traceFilters);
  const dims = parseGroupBy(state.groupBy);
  const values = parseCompositeKey(state.group, dims);

  // The dimension pins are server-side now (see api/traceGroupMembers) — this
  // returns exactly the group's members, newest first, bounded by the same
  // limit the rest of the traces tab uses.
  const membersQuery = useQuery({
    queryKey: [
      "trace-group-members",
      rangeKey,
      dims.join(","),
      values.join(KEY_SEP),
      state.grain,
      traceql,
      state.limit,
    ],
    queryFn: () =>
      fetchTraceGroupMembers(
        dims,
        values,
        range,
        state.traceFilters,
        state.grain,
        state.limit,
      ),
  });

  // At trace grain the root-span predicate makes every row a whole trace,
  // shown by its root span. At span grain a row is whatever span matched —
  // not necessarily the root — so labeling it "Root" would assert something
  // the data doesn't guarantee (exactly the misleading-label class #1070
  // exists to avoid).
  const isSpanGrain = state.grain === "spans";
  const memberNoun = isSpanGrain ? "span" : "trace";

  return (
    <>
      <div className="trace-head">
        <button className="backbtn" onClick={() => update({ group: "" })}>
          ← groups
        </button>
        <h3>{groupLabel(state.group)}</h3>
        <span className="tmeta">{dims.join(", ")}</span>
      </div>
      <MemberTable
        members={membersQuery.data}
        isError={membersQuery.isError}
        errorMessage={
          membersQuery.isError
            ? `Search failed: ${(membersQuery.error as Error).message}`
            : undefined
        }
        identityLabel={isSpanGrain ? "Span" : "Root"}
        emptyMessage={`No ${memberNoun}s for this group in this time range.`}
        // Always true (the query always applies a limit) — states the bound
        // rather than claiming truncation we can't detect here.
        footnote={`Showing up to ${plural(state.limit, memberNoun)}, newest first.`}
        onOpenTrace={(traceId) => update({ trace: traceId }, { push: true })}
      />
    </>
  );
}

const SIDEBAR_MIN_PX = 260;
const SIDEBAR_MAX_PX = 640;
const SIDEBAR_DEFAULT_PX = 320;
const SIDEBAR_WIDTH_KEY = "signaldb.trace.sidebarWidth";

function clampSidebarWidth(px: number): number {
  return Math.min(SIDEBAR_MAX_PX, Math.max(SIDEBAR_MIN_PX, px));
}

/** Drag-to-resize the span-detail sidebar; width persists across sessions. */
function useSidebarWidth() {
  const [width, setWidth] = useState(() => {
    const stored = Number(localStorage.getItem(SIDEBAR_WIDTH_KEY));
    return stored > 0 ? clampSidebarWidth(stored) : SIDEBAR_DEFAULT_PX;
  });
  const dragRef = useRef<{ startX: number; startWidth: number } | null>(null);

  useEffect(() => {
    localStorage.setItem(SIDEBAR_WIDTH_KEY, String(width));
  }, [width]);

  const onPointerMove = useCallback((e: MouseEvent) => {
    const drag = dragRef.current;
    if (!drag) return;
    // The sidebar sits right of the resizer, so dragging left widens it.
    setWidth(clampSidebarWidth(drag.startWidth - (e.clientX - drag.startX)));
  }, []);

  const onPointerUp = useCallback(() => {
    dragRef.current = null;
    window.removeEventListener("mousemove", onPointerMove);
    window.removeEventListener("mouseup", onPointerUp);
  }, [onPointerMove]);

  const startDrag = useCallback(
    (e: { preventDefault: () => void; clientX: number }) => {
      e.preventDefault();
      dragRef.current = { startX: e.clientX, startWidth: width };
      window.addEventListener("mousemove", onPointerMove);
      window.addEventListener("mouseup", onPointerUp);
    },
    [width, onPointerMove, onPointerUp],
  );

  return { width, startDrag };
}

function TraceDetail({ state, update }: Props) {
  const [selected, setSelected] = useState<string | null>(null);
  const { width: sidebarWidth, startDrag } = useSidebarWidth();
  // Waterfall hover tooltip: the shared VizTooltip, hosted on the (non-
  // scrolling) trace body so it can overlap the detail pane and isn't
  // affected by the waterfall's own scroll offset.
  const bodyRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(bodyRef);
  const [hoveredSpanId, setHoveredSpanId] = useState<string | null>(null);
  const tipId = useId();
  const clearHover = () => {
    setHoveredSpanId(null);
    pointer.clear();
  };
  // One Query IR read for the whole trace: spans with kind, status,
  // attribute containers, and events, plus the profiles captured during it.
  // The viewer's range is tried first; a trace opened by ID that lies
  // outside it is retried over a wide window (see fetchTraceDetail).
  const trace = useQuery({
    queryKey: ["trace-detail", state.trace, state.tenant, state.dataset],
    queryFn: () =>
      fetchTraceDetail(state.trace, resolveRange(state.range, Date.now())),
  });
  const spanKinds = useMemo(() => {
    const kinds: Record<string, string> = {};
    for (const s of trace.data?.spans ?? []) {
      if (s.kind) kinds[s.spanId] = s.kind;
    }
    return kinds;
  }, [trace.data]);
  const waterfall = useMemo(
    () => (trace.data ? buildWaterfall(trace.data.spans) : undefined),
    [trace.data],
  );

  if (trace.isError || (trace.isSuccess && trace.data === null)) {
    if (
      !trace.isError ||
      (trace.error instanceof ApiError && trace.error.status === 404)
    ) {
      return (
        <div className="trace-not-found" role="alert">
          <button className="backbtn" onClick={() => update({ trace: "" })}>
            ← traces
          </button>
          <h3>Trace not found</h3>
          <p>
            <code>{state.trace}</code> isn&rsquo;t in the selected time window.
            Trace storage is scoped by time — if you know roughly when it
            happened, widen the range and try again.
          </p>
        </div>
      );
    }
    return (
      <div className="query-error" role="alert">
        Trace lookup failed: {(trace.error as Error).message}
      </div>
    );
  }
  if (trace.isPending) {
    return (
      <div className="traceview" aria-busy="true">
        <SkeletonLines lines={10} />
      </div>
    );
  }

  if (!waterfall || !trace.data) return null;
  const traceData = trace.data;

  const hoveredRow = hoveredSpanId
    ? waterfall.rows.find((r) => r.span.spanId === hoveredSpanId)
    : undefined;
  const selectedRow =
    waterfall.rows.find((r) => r.span.spanId === selected) ??
    // An error span with no recorded exception (e.g. a root span that only
    // propagates a child's failure) would open to an empty Events section,
    // hiding the exception on whichever span actually recorded it.
    waterfall.rows.find(
      (r) =>
        r.span.status === "error" &&
        r.span.events.some((e) => e.name === "exception"),
    ) ??
    waterfall.rows.find((r) => r.span.status === "error") ??
    waterfall.rows[0];

  return (
    <div className="traceview">
      <div className="trace-head">
        <button className="backbtn" onClick={() => update({ trace: "" })}>
          ← traces
        </button>
        <h3>{traceData.rootTraceName}</h3>
        <span className="tmeta">
          {formatDurationMs(
            // The search API truncates durationMs; span extents are exact.
            traceData.durationMs > 0
              ? traceData.durationMs
              : Number(waterfall.traceDurationNs) / 1e6,
          )}{" "}
          · {plural(waterfall.rows.length, "span")} ·{" "}
          {plural(waterfall.services.length, "service")}
          {waterfall.errorCount > 0 && (
            <em className="tmeta-err">
              {" "}
              · {plural(waterfall.errorCount, "error")}
            </em>
          )}
        </span>
        <span className="trace-id">{traceData.traceId}</span>
      </div>
      {Object.keys(spanKinds).length > 0 && (
        <div className="span-kind-legend" aria-label="Span kind legend">
          {Array.from(new Set(Object.values(spanKinds)))
            .sort()
            .map((kind) => (
              <span key={kind} className={`legend-chip ${kindClass(kind)}`}>
                {kind}
              </span>
            ))}
        </div>
      )}
      <div
        className="trace-body viz-host"
        ref={bodyRef}
        style={{ "--span-detail-w": `${sidebarWidth}px` } as CSSProperties}
      >
        <div
          className="waterfall"
          role="list"
          aria-label="Spans"
          onPointerLeave={clearHover}
        >
          {waterfall.rows.map((row) => (
            <button
              key={row.span.spanId}
              role="listitem"
              className="span-row"
              aria-selected={selectedRow?.span.spanId === row.span.spanId}
              aria-describedby={
                hoveredSpanId === row.span.spanId ? tipId : undefined
              }
              onClick={() => setSelected(row.span.spanId)}
              onPointerMove={(e) => {
                setHoveredSpanId(row.span.spanId);
                pointer.track(e);
              }}
              onFocus={(e) => {
                setHoveredSpanId(row.span.spanId);
                pointer.anchorTo(e.currentTarget);
              }}
              onBlur={clearHover}
            >
              <span
                className="span-label"
                style={{ paddingLeft: row.depth * 16 }}
              >
                <span className="span-svc">{row.span.serviceName}</span>
                <span className="span-name">{row.span.name}</span>
              </span>
              <span className="span-track">
                <span
                  className={`span-bar ${kindClass(spanKinds[row.span.spanId])}${row.span.status === "error" ? " error" : ""}${row.extentInferred ? " inferred" : ""}`}
                  title={
                    row.extentInferred
                      ? "No duration recorded; drawn over its child spans"
                      : undefined
                  }
                  style={{
                    left: `${row.leftPct}%`,
                    width: `${row.widthPct}%`,
                  }}
                />
              </span>
              <span
                className={`span-dur${row.span.status === "error" ? " error" : ""}`}
              >
                {formatDurationMs(row.durationMs)}
              </span>
            </button>
          ))}
        </div>
        {selectedRow && (
          <>
            <div
              className="trace-resizer"
              role="separator"
              aria-orientation="vertical"
              aria-label="Resize span details"
              onMouseDown={startDrag}
            />
            <SpanDetail
              span={selectedRow.span}
              traceId={traceData.traceId}
              profiles={traceData.profiles}
              kind={spanKinds[selectedRow.span.spanId]}
              update={update}
            />
          </>
        )}
        {hoveredRow && pointer.anchor && (
          <VizTooltip
            id={tipId}
            anchor={pointer.anchor}
            host={pointer.host}
            title={hoveredRow.span.name}
            rows={spanTooltipRows(
              hoveredRow.span,
              hoveredRow.durationMs,
              spanKinds[hoveredRow.span.spanId],
            )}
          />
        )}
      </div>
    </div>
  );
}

function SpanDetail({
  span,
  traceId,
  profiles,
  kind,
  update,
}: {
  span: TempoSpan;
  traceId: string;
  profiles: ProfileSummaryView[];
  /** OTel span kind, when the IR row carried one. */
  kind: string | undefined;
  update: UpdateFn;
}) {
  const groups = useMemo(
    () => groupSpanAttributes(span.attributes),
    [span.attributes],
  );
  const attributeKeys = useMemo(
    () => groups.flatMap((g) => g.entries.map(([k]) => k)),
    [groups],
  );
  const semantics = useSemantics(attributeKeys);
  const spanProfiles = profiles.filter((p) => p.spanId === span.spanId);
  return (
    <aside className="span-detail" aria-label="Span details">
      <h4>{span.name}</h4>
      <div className="span-detail-sub">
        {kind && <span className={`kind-chip ${kindClass(kind)}`}>{kind}</span>}
        {describeService(span.serviceName, span.attributes)}
        {span.status === "error" && <em className="tmeta-err"> · error</em>}
      </div>
      <button
        className="act act-primary"
        onClick={() =>
          update({
            signal: "logs",
            trace: "",
            raw: "",
            filters: [{ label: "trace_id", op: "=", value: traceId }],
          })
        }
      >
        Logs for this trace →
      </button>
      {spanProfiles.map((p) => (
        <button
          key={p.profileId}
          className="act"
          onClick={() =>
            update(
              { signal: "profiles", trace: "", profileId: p.profileId },
              { push: true },
            )
          }
        >
          Profile: {p.sampleType} →
        </button>
      ))}
      {span.events.length > 0 && (
        <>
          <div className="span-detail-sec">Events</div>
          <ul className="span-events">
            {span.events.map((event, i) => (
              <SpanEventItem key={i} event={event} spanStartNs={span.startNs} />
            ))}
          </ul>
        </>
      )}
      {groups.length === 0 && (
        <>
          <div className="span-detail-sec">Attributes</div>
          <div className="view-note">No attributes recorded.</div>
        </>
      )}
      {groups.map((group) => (
        <div key={group.label}>
          <div className="span-detail-sec">{group.label}</div>
          {groupBySemanticTitle(group.entries, semantics).map((sub) => (
            <Fragment key={sub.title ?? ""}>
              {sub.title && (
                <div className="span-detail-subsec">{sub.title}</div>
              )}
              <dl className="span-attrs">
                {sub.entries.map(([k, v]) => (
                  <div key={k}>
                    <dt>
                      <SemanticKey name={k} semantics={semantics.get(k)} />
                    </dt>
                    <dd>
                      <AttributeValue
                        value={String(v)}
                        label={`value for ${k}`}
                      />
                    </dd>
                  </div>
                ))}
              </dl>
            </Fragment>
          ))}
        </div>
      ))}
    </aside>
  );
}

/** Time from span start to the event, e.g. "+15 ms"; null when the event
 * carries no timestamp (or one that doesn't parse) rather than showing NaN. */
function eventOffsetLabel(
  spanStartNs: string,
  eventTimeNs: string,
): string | null {
  if (!eventTimeNs) return null;
  try {
    const offsetMs = nanosToMs(eventTimeNs) - nanosToMs(spanStartNs);
    const sign = offsetMs < 0 ? "-" : "+";
    return `${sign}${formatDurationMs(Math.abs(offsetMs))}`;
  } catch {
    return null;
  }
}

/** When an event happened: wall-clock time (ms precision, local zone) with
 * the exact nanosecond timestamp on hover, and the offset from the span's
 * start. Timing marks such as the browser's `fetchStart`/`responseEnd`
 * carry nothing but this. */
function EventTime({
  spanStartNs,
  eventTimeNs,
}: {
  spanStartNs: string;
  eventTimeNs: string;
}) {
  const offset = eventOffsetLabel(spanStartNs, eventTimeNs);
  if (!eventTimeNs) return null;
  let clock: string | null;
  try {
    clock = formatTimestamp(nanosToMs(eventTimeNs));
  } catch {
    clock = null;
  }
  return (
    <span className="span-event-time">
      {clock && (
        <span
          data-testid="span-event-clock"
          title={`${eventTimeNs} ns since the epoch`}
        >
          {clock}
        </span>
      )}
      {clock && offset && " · "}
      {offset && <span>{offset}</span>}
    </span>
  );
}

/** One span event. Exceptions (name === "exception") get an error treatment
 * with message/type promoted and the stacktrace shown as preformatted text. */
function SpanEventItem({
  event,
  spanStartNs,
}: {
  event: SpanEventView;
  spanStartNs: string;
}) {
  const isException = event.name === "exception";
  if (isException) {
    const message = event.attributes["exception.message"];
    const type = event.attributes["exception.type"];
    const stacktrace = event.attributes["exception.stacktrace"];
    const shown = new Set([
      "exception.message",
      "exception.type",
      "exception.stacktrace",
    ]);
    const rest = Object.entries(event.attributes).filter(
      ([k]) => !shown.has(k),
    );
    return (
      <li className="span-event span-event-err">
        <div className="span-event-head">
          <span className="span-event-name">exception</span>
          <EventTime
            spanStartNs={spanStartNs}
            eventTimeNs={event.timeUnixNano}
          />
          {type !== undefined && (
            <span className="span-event-type">
              <AttributeValue
                value={String(type)}
                label="value for exception.type"
              />
            </span>
          )}
        </div>
        {message !== undefined && (
          <div className="span-event-msg">
            <AttributeValue
              value={String(message)}
              label="value for exception.message"
            />
          </div>
        )}
        {stacktrace !== undefined && String(stacktrace) !== "" && (
          <div className="span-event-trace">
            <AttributeValue
              value={String(stacktrace)}
              label="value for exception.stacktrace"
            />
          </div>
        )}
        {rest.map(([k, v]) => (
          <div className="span-event-attr" key={k}>
            <span>{k}</span>
            <AttributeValue value={String(v)} label={`value for ${k}`} />
          </div>
        ))}
      </li>
    );
  }
  return (
    <li className="span-event">
      <div className="span-event-head">
        <span className="span-event-name">{event.name}</span>
        <EventTime spanStartNs={spanStartNs} eventTimeNs={event.timeUnixNano} />
      </div>
      {Object.entries(event.attributes).map(([k, v]) => (
        <div className="span-event-attr" key={k}>
          <span>{k}</span>
          <AttributeValue value={String(v)} label={`value for ${k}`} />
        </div>
      ))}
    </li>
  );
}
