import { useQuery } from "@tanstack/react-query";
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import {
  tempoGetTrace,
  type SpanEventView,
  type TempoSpan,
} from "../../api/tempo";
import {
  STATUS_COLORS,
  STATUS_ORDER,
  fetchTraceLatencyHeatmap,
  fetchTraceVolume,
} from "../../api/traceVolume";
import { SignalHistogram } from "../explore/SignalHistogram";
import { AttributeValue } from "../../components/AttributeValue";
import { TraceFacets } from "./TraceFacets";
import { TraceVolumeAreaChart } from "./TraceVolumeAreaChart";
import { TraceVolumeHeatmap } from "./TraceVolumeHeatmap";
import {
  compileTraceQL,
  upsertTraceFilter,
  type TraceFilter,
} from "../../lib/traceFilters";
import {
  durationToSeconds,
  formatTimestamp,
  nanosToMs,
  rangeToParam,
  resolveRange,
  resolveStep,
  stepOptionsForRange,
  type ResolvedRange,
} from "../../lib/time";
import {
  BUILTIN_DIMENSIONS,
  formatRate,
  groupLabel,
  KEY_SEP,
  NOT_SET,
  parseGroupBy,
} from "../../lib/traceGroups";
import {
  DEFAULT_GROUP_SORT,
  GROUP_BUDGET,
  fetchTraceGroups,
  type GroupGrain,
  type GroupSort,
  type TraceGroup,
} from "../../api/traceGroups";
import {
  fetchTraceGroupMembers,
  type TraceGroupMember,
} from "../../api/traceGroupMembers";
import { SkeletonLines, SkeletonRows } from "../explore/Skeleton";
import type { ExploreState } from "../../lib/urlState";
import { buildWaterfall, formatDurationMs } from "../../lib/waterfall";
import { fetchWindowTotal, looksUnresolved } from "./unresolvedGroup";
import { describeService, groupSpanAttributes } from "./spanAttributes";
import "./traces.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

function plural(n: number, noun: string): string {
  return `${n} ${noun}${n === 1 ? "" : "s"}`;
}

/* ---------- column sorting ---------- */

type SortDir = "asc" | "desc";
interface SortSpec {
  key: string;
  dir: SortDir;
}

function useSort(defaultKey: string, defaultDir: SortDir) {
  const [sort, setSort] = useState<SortSpec>({
    key: defaultKey,
    dir: defaultDir,
  });
  const toggle = (key: string, firstDir: SortDir) =>
    setSort((s) =>
      s.key === key
        ? { key, dir: s.dir === "asc" ? "desc" : "asc" }
        : { key, dir: firstDir },
    );
  return [sort, toggle] as const;
}

type SortValue = string | number | bigint;

function compareValues(a: SortValue, b: SortValue): number {
  if (typeof a === "string" || typeof b === "string") {
    return String(a).localeCompare(String(b));
  }
  return a < b ? -1 : a > b ? 1 : 0;
}

function sortRows<T>(
  rows: T[],
  sort: SortSpec,
  value: (row: T, key: string) => SortValue,
): T[] {
  const sign = sort.dir === "asc" ? 1 : -1;
  return [...rows].sort(
    (a, b) => sign * compareValues(value(a, sort.key), value(b, sort.key)),
  );
}

function SortTh({
  label,
  sortKey,
  sort,
  toggle,
  numeric = false,
  firstDir,
}: {
  label: string;
  sortKey: string;
  sort: SortSpec;
  toggle: (key: string, firstDir: SortDir) => void;
  /** Right-aligned metric column; sorts descending on first click. */
  numeric?: boolean;
  /** Overrides the first-click direction (e.g. timestamps: newest first). */
  firstDir?: SortDir;
}) {
  const active = sort.key === sortKey;
  return (
    <th
      className={numeric ? "num" : undefined}
      aria-sort={
        active ? (sort.dir === "asc" ? "ascending" : "descending") : undefined
      }
    >
      <button
        className="th-sort"
        onClick={() => toggle(sortKey, firstDir ?? (numeric ? "desc" : "asc"))}
      >
        {label}
      </button>
    </th>
  );
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
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  const filters = state.traceFilters;
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
    update({
      traceFilters: filters.filter(
        (x) => !(x.field === f.field && x.value === f.value),
      ),
      group: "",
    });

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
      {filters.length > 0 && (
        <div className="trace-chips" aria-label="Active filters">
          {filters.map((f) => (
            <button
              className="chip"
              key={`${f.field}|${f.value}`}
              aria-label={`Remove filter ${f.field} = ${f.value}`}
              onClick={() => removeFilter(f)}
            >
              <span className="chip-k">{f.field}</span>
              <span className="chip-v">{f.value}</span>
              <span className="chip-x">×</span>
            </button>
          ))}
        </div>
      )}
      <div className="traces-body">
        <TraceFacets
          range={resolvedForStep}
          rangeKey={rangeKey}
          filters={filters}
          onAddFilter={addFilter}
          onRemoveFilter={removeFilter}
        />
        <div className="traces-main">
          <div className="traces-toolbar">
            <form
              className="trace-id-form"
              onSubmit={(e) => {
                e.preventDefault();
                const id = new FormData(e.currentTarget).get("traceId");
                if (typeof id === "string" && id.trim() !== "") {
                  update({ trace: id.trim() });
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
                <DimensionPickers groupBy={state.groupBy} update={update} />
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
}: {
  groupBy: string;
  update: (patch: Partial<ExploreState>) => void;
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
      {/* There's no trace sample to derive attribute names from any more
          (the group table is a server-side aggregate) — typing one in is how
          a user reaches an attribute the built-ins don't cover. */}
      <form
        className="group-custom"
        onSubmit={(e) => {
          e.preventDefault();
          const value = new FormData(e.currentTarget).get("dim");
          if (typeof value === "string" && value.trim() !== "") {
            setDims([value.trim(), secondary]);
            e.currentTarget.reset();
          }
        }}
      >
        <input
          name="dim"
          aria-label="Custom dimension"
          placeholder="Group by attribute…"
        />
      </form>
    </div>
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
  update: (patch: Partial<ExploreState>) => void;
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

/**
 * Builds the same key `groupKey()` would build for a `TraceSummary`, from a
 * server-computed group's `values` instead — so a row click drills in exactly
 * as it did when grouping was client-side, and `GroupDetail`'s
 * `groupKey(t, dims) === state.group` match keeps working unmodified.
 */
function groupRowKey(values: TraceGroup["values"]): string {
  return values.map((v) => v ?? NOT_SET).join(KEY_SEP);
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
  update: (patch: Partial<ExploreState>) => void;
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
  const rootGrainOnly = grain === "traces" && filters.length > 0;

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
              const key = groupRowKey(g.values);
              return (
                <tr key={key} onClick={() => update({ group: key })}>
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
        <div className="traces-note">
          &ldquo;{dims.join(", ")}&rdquo; isn&rsquo;t queryable for this tenant
          or window right now — pick a different dimension.
        </div>
      )}
      {done && !unresolved && groups.length === 0 && rootGrainOnly && (
        <div className="traces-note">
          No groups: trace grain only inspects each trace's root span, and one
          of the active filters is on a field that only appears on a child span.
          Switch to span grain to see it.
        </div>
      )}
      {done && !unresolved && groups.length === 0 && !rootGrainOnly && (
        <div className="traces-note">No groups in this time range.</div>
      )}
      {result.data?.truncated && (
        <div className="traces-note">
          Showing the top {GROUP_BUDGET} groups by the current sort — narrow the
          time range or filters to see the rest.
        </div>
      )}
    </>
  );
}

function memberSortValue(m: TraceGroupMember, key: string): SortValue {
  switch (key) {
    case "root":
      return m.spanName;
    case "service":
      return m.serviceName;
    case "duration":
      return BigInt(m.durationNanos);
    case "id":
      return m.traceId;
    default:
      return BigInt(m.startNs);
  }
}

/**
 * Reverses `groupRowKey`: splits the URL-stored composite key back into one
 * value per dimension. The URL already collapsed "no value for this
 * dimension" to the NOT_SET sentinel, so a value equal to it is treated as
 * null here too — the displayed "(not set)" row means the field is absent,
 * so drilling into it must query "not exists", not `eq "(not set)"`.
 */
function parseGroupValues(key: string, dims: string[]): (string | null)[] {
  const parts = key.split(KEY_SEP);
  return dims.map((_, i) => {
    const v = parts[i];
    return v === undefined || v === NOT_SET ? null : v;
  });
}

function GroupDetail({
  state,
  update,
}: {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}) {
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  const range = resolveRange(state.range, Date.now());
  const traceql = compileTraceQL(state.traceFilters);
  const dims = parseGroupBy(state.groupBy);
  const values = parseGroupValues(state.group, dims);

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

  const [sort, toggle] = useSort("time", "desc");
  const rows = membersQuery.data
    ? sortRows(membersQuery.data, sort, memberSortValue)
    : [];

  // At trace grain the root-span predicate makes every row a whole trace,
  // shown by its root span. At span grain a row is whatever span matched —
  // not necessarily the root — so labeling it "Root" would assert something
  // the data doesn't guarantee (exactly the misleading-label class #1070
  // exists to avoid).
  const isSpanGrain = state.grain === "spans";
  const identityLabel = isSpanGrain ? "Span" : "Root";
  const memberNoun = isSpanGrain ? "span" : "trace";

  const header = (
    <tr>
      <SortTh
        label={identityLabel}
        sortKey="root"
        sort={sort}
        toggle={toggle}
      />
      <SortTh label="Service" sortKey="service" sort={sort} toggle={toggle} />
      <SortTh
        label="Time"
        sortKey="time"
        sort={sort}
        toggle={toggle}
        firstDir="desc"
      />
      <SortTh
        label="Duration"
        sortKey="duration"
        sort={sort}
        toggle={toggle}
        numeric
      />
      <SortTh label="Trace ID" sortKey="id" sort={sort} toggle={toggle} />
    </tr>
  );

  return (
    <>
      <div className="trace-head">
        <button className="backbtn" onClick={() => update({ group: "" })}>
          ← groups
        </button>
        <h3>{groupLabel(state.group)}</h3>
        <span className="tmeta">{dims.join(", ")}</span>
      </div>
      {membersQuery.isError ? (
        <div className="query-error" role="alert">
          Search failed: {(membersQuery.error as Error).message}
        </div>
      ) : membersQuery.isPending ? (
        <table className="trace-table" aria-busy="true">
          <thead>{header}</thead>
          <tbody>
            <SkeletonRows rows={8} columns={5} numericFrom={3} />
          </tbody>
        </table>
      ) : rows.length === 0 ? (
        <div className="traces-note">
          No {memberNoun}s for this group in this time range.
        </div>
      ) : (
        <>
          <table className="trace-table">
            <thead>{header}</thead>
            <tbody>
              {rows.map((m) => (
                <tr
                  key={`${m.traceId}-${m.spanId}`}
                  onClick={() => update({ trace: m.traceId })}
                >
                  <td>
                    <button className="trace-open">{m.spanName}</button>
                  </td>
                  <td>{m.serviceName}</td>
                  <td>{formatTimestamp(nanosToMs(m.startNs))}</td>
                  <td className="num">
                    {formatDurationMs(nanosToMs(m.durationNanos))}
                  </td>
                  <td className="trace-id">{m.traceId}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {/* Always true (the query always applies a limit) — states the
              bound rather than claiming truncation we can't detect here. */}
          <div className="traces-note">
            Showing up to {plural(state.limit, memberNoun)}, newest first.
          </div>
        </>
      )}
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
  const trace = useQuery({
    queryKey: ["tempo-trace", state.trace, state.tenant, state.dataset],
    queryFn: () => tempoGetTrace(state.trace),
  });

  if (trace.isError) {
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

  const waterfall = buildWaterfall(trace.data.spans);
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
        <h3>{trace.data.rootTraceName}</h3>
        <span className="tmeta">
          {formatDurationMs(
            // The search API truncates durationMs; span extents are exact.
            trace.data.durationMs > 0
              ? trace.data.durationMs
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
        <span className="trace-id">{trace.data.traceId}</span>
      </div>
      <div
        className="trace-body"
        style={{ "--span-detail-w": `${sidebarWidth}px` } as CSSProperties}
      >
        <div className="waterfall" role="list" aria-label="Spans">
          {waterfall.rows.map((row) => (
            <button
              key={row.span.spanId}
              role="listitem"
              className="span-row"
              aria-selected={selectedRow?.span.spanId === row.span.spanId}
              onClick={() => setSelected(row.span.spanId)}
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
                  className={`span-bar${row.span.status === "error" ? " error" : ""}`}
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
              traceId={trace.data.traceId}
              update={update}
            />
          </>
        )}
      </div>
    </div>
  );
}

function SpanDetail({
  span,
  traceId,
  update,
}: {
  span: TempoSpan;
  traceId: string;
  update: (patch: Partial<ExploreState>) => void;
}) {
  const groups = groupSpanAttributes(span.attributes);
  return (
    <aside className="span-detail" aria-label="Span details">
      <h4>{span.name}</h4>
      <div className="span-detail-sub">
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
          <div className="traces-note">No attributes recorded.</div>
        </>
      )}
      {groups.map((group) => (
        <div key={group.label}>
          <div className="span-detail-sec">{group.label}</div>
          <dl className="span-attrs">
            {group.entries.map(([k, v]) => (
              <div key={k}>
                <dt>{k}</dt>
                <dd>
                  <AttributeValue value={String(v)} label={`value for ${k}`} />
                </dd>
              </div>
            ))}
          </dl>
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
  const offset = eventOffsetLabel(spanStartNs, event.timeUnixNano);
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
          {offset && <span className="span-event-time">{offset}</span>}
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
        {offset && <span className="span-event-time">{offset}</span>}
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
