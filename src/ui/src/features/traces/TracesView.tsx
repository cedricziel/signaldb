import { useQuery } from "@tanstack/react-query";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import {
  tempoGetTrace,
  type ProfileSummaryView,
  type SpanEventView,
  type TempoSpan,
} from "../../api/tempo";
import { ApiError } from "../../api/http";
import { fetchSpanKinds } from "../../api/spanKinds";
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
  const rangeKey = rangeScopeKey(state);
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
  update: UpdateFn;
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
  const trace = useQuery({
    queryKey: ["tempo-trace", state.trace, state.tenant, state.dataset],
    queryFn: () => tempoGetTrace(state.trace),
  });

  // Span kind isn't part of the Tempo-compat wire format — fetched
  // separately over Query IR once the trace's own time extent is known
  // (the viewer's selected range may not cover a trace opened by ID).
  const traceStartMs = trace.data ? nanosToMs(trace.data.startNs) : 0;
  const traceDurationMs = trace.data?.durationMs ?? 0;
  const kindsQuery = useQuery({
    queryKey: ["span-kinds", state.trace, state.tenant, state.dataset],
    queryFn: () =>
      fetchSpanKinds(
        state.trace,
        traceStartMs - 60_000,
        traceStartMs + traceDurationMs + 60_000,
      ),
    enabled: trace.data !== undefined,
  });
  const spanKinds = kindsQuery.data ?? {};
  const waterfall = useMemo(
    () => (trace.data ? buildWaterfall(trace.data.spans) : undefined),
    [trace.data],
  );

  if (trace.isError) {
    if (trace.error instanceof ApiError && trace.error.status === 404) {
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

  if (!waterfall) return null;

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
                  className={`span-bar ${kindClass(spanKinds[row.span.spanId])}${row.span.status === "error" ? " error" : ""}`}
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
              profiles={trace.data.profiles}
              kind={spanKinds[selectedRow.span.spanId]}
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
  profiles,
  kind,
  update,
}: {
  span: TempoSpan;
  traceId: string;
  profiles: ProfileSummaryView[];
  /** Not part of the Tempo wire format — fetched separately over Query IR
   * (see fetchSpanKinds); undefined while that query is still loading. */
  kind: string | undefined;
  update: UpdateFn;
}) {
  const groups = useMemo(
    () => groupSpanAttributes(span.attributes),
    [span.attributes],
  );
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
