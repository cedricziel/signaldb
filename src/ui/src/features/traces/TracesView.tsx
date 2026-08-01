import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import {
  tempoGetTrace,
  tempoSearch,
  type SpanEventView,
  type TempoSpan,
  type TraceSummary,
} from "../../api/tempo";
import {
  formatTimestamp,
  nanosToMs,
  rangeToParam,
  resolveRange,
} from "../../lib/time";
import {
  formatRate,
  groupDimensions,
  groupKey,
  groupLabel,
  groupTraces,
  parseGroupBy,
  type TraceGroup,
} from "../../lib/traceGroups";
import type { ExploreState } from "../../lib/urlState";
import { buildWaterfall, formatDurationMs } from "../../lib/waterfall";
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
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  const search = useQuery({
    queryKey: ["tempo-search", rangeKey, state.limit],
    queryFn: () =>
      tempoSearch(resolveRange(state.range, Date.now()), state.limit),
  });

  return (
    <div className="traces-search">
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
          <DimensionPickers
            traces={search.data ?? []}
            groupBy={state.groupBy}
            update={update}
          />
        )}
      </div>

      {search.isError && (
        <div className="query-error" role="alert">
          Search failed: {(search.error as Error).message}
        </div>
      )}
      {search.isPending && <div className="traces-note">Loading…</div>}
      {search.data &&
        (state.group === "" ? (
          <GroupList
            traces={search.data}
            dims={parseGroupBy(state.groupBy)}
            rangeSeconds={rangeSeconds(state)}
            update={update}
          />
        ) : (
          <GroupDetail state={state} traces={search.data} update={update} />
        ))}
    </div>
  );
}

function rangeSeconds(state: ExploreState): number {
  const r = resolveRange(state.range, Date.now());
  return Math.max(1, (r.toMs - r.fromMs) / 1000);
}

function DimensionPickers({
  traces,
  groupBy,
  update,
}: {
  traces: TraceSummary[];
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
          {dimensionOptions(traces, primary).map((d) => (
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
          {dimensionOptions(traces, secondary)
            .filter((d) => d !== primary && d !== "")
            .map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
        </select>
      </label>
    </div>
  );
}

/** The picked dimension stays selectable even if absent from this range. */
function dimensionOptions(traces: TraceSummary[], picked: string): string[] {
  const dims = groupDimensions(traces);
  return picked === "" || dims.includes(picked)
    ? dims
    : [...dims, picked].sort();
}

function GroupList({
  traces,
  dims,
  rangeSeconds,
  update,
}: {
  traces: TraceSummary[];
  dims: string[];
  rangeSeconds: number;
  update: (patch: Partial<ExploreState>) => void;
}) {
  const [sort, toggle] = useSort("traces", "desc");
  if (traces.length === 0) {
    return <div className="traces-note">No traces in this time range.</div>;
  }
  const groups = sortRows(groupTraces(traces, dims), sort, groupSortValue);
  const showServices = !dims.includes("service.name");
  return (
    <table className="trace-table">
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
          {showServices && (
            <SortTh
              label="Services"
              sortKey="services"
              sort={sort}
              toggle={toggle}
            />
          )}
          <SortTh
            label="Traces"
            sortKey="traces"
            sort={sort}
            toggle={toggle}
            numeric
          />
          <SortTh
            label="Rate"
            sortKey="rate"
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
        {groups.map((g) => (
          <tr key={g.key} onClick={() => update({ group: g.key })}>
            <td>
              <button className="trace-open">{g.values[0]}</button>
            </td>
            {g.values.slice(1).map((v, i) => (
              <td key={dims[i + 1]}>{v}</td>
            ))}
            {showServices && <td>{formatServices(g)}</td>}
            <td className="num">{g.traces.length}</td>
            <td className="num">{formatRate(g.traces.length, rangeSeconds)}</td>
            <td className={`num${g.errorCount > 0 ? " err-rate" : ""}`}>
              {g.errorCount > 0
                ? `${Math.round((100 * g.errorCount) / g.traces.length)}%`
                : "–"}
            </td>
            <td className="num">{formatDurationMs(g.p50Ms)}</td>
            <td className="num">{formatDurationMs(g.p95Ms)}</td>
            <td>{formatTimestamp(nanosToMs(g.lastStartNs))}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function formatServices(g: TraceGroup): string {
  return g.services.length > 2
    ? `${plural(g.services.length, "service")}`
    : g.services.join(", ");
}

function groupSortValue(g: TraceGroup, key: string): SortValue {
  if (key.startsWith("dim:")) return g.values[Number(key.slice(4))] ?? "";
  switch (key) {
    case "services":
      return g.services.join(", ");
    case "errors":
      return g.errorCount / g.traces.length;
    case "p50":
      return g.p50Ms;
    case "p95":
      return g.p95Ms;
    case "last":
      return BigInt(g.lastStartNs);
    default:
      return g.traces.length;
  }
}

function traceSortValue(t: TraceSummary, key: string): SortValue {
  switch (key) {
    case "root":
      return t.rootTraceName;
    case "service":
      return t.rootServiceName;
    case "duration":
      return t.durationMs;
    case "id":
      return t.traceId;
    default:
      return BigInt(t.startNs);
  }
}

function GroupDetail({
  state,
  traces,
  update,
}: {
  state: ExploreState;
  traces: TraceSummary[];
  update: (patch: Partial<ExploreState>) => void;
}) {
  const [sort, toggle] = useSort("time", "desc");
  const dims = parseGroupBy(state.groupBy);
  const members = sortRows(
    traces.filter((t) => groupKey(t, dims) === state.group),
    sort,
    traceSortValue,
  );
  return (
    <>
      <div className="trace-head">
        <button className="backbtn" onClick={() => update({ group: "" })}>
          ← groups
        </button>
        <h3>{groupLabel(state.group)}</h3>
        <span className="tmeta">
          {dims.join(", ")} · {plural(members.length, "trace")}
        </span>
      </div>
      {members.length === 0 ? (
        <div className="traces-note">
          No traces for this group in this time range.
        </div>
      ) : (
        <table className="trace-table">
          <thead>
            <tr>
              <SortTh label="Root" sortKey="root" sort={sort} toggle={toggle} />
              <SortTh
                label="Service"
                sortKey="service"
                sort={sort}
                toggle={toggle}
              />
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
              <SortTh
                label="Trace ID"
                sortKey="id"
                sort={sort}
                toggle={toggle}
              />
            </tr>
          </thead>
          <tbody>
            {members.map((t) => (
              <tr key={t.traceId} onClick={() => update({ trace: t.traceId })}>
                <td>
                  <button className="trace-open">{t.rootTraceName}</button>
                </td>
                <td>{t.rootServiceName}</td>
                <td>{formatTimestamp(nanosToMs(t.startNs))}</td>
                <td className="num">{formatDurationMs(t.durationMs)}</td>
                <td className="trace-id">{t.traceId}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

function TraceDetail({ state, update }: Props) {
  const [selected, setSelected] = useState<string | null>(null);
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
  if (trace.isPending) return <div className="traces-note">Loading…</div>;

  const waterfall = buildWaterfall(trace.data.spans);
  const selectedRow =
    waterfall.rows.find((r) => r.span.spanId === selected) ??
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
      <div className="trace-body">
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
          <SpanDetail
            span={selectedRow.span}
            traceId={trace.data.traceId}
            update={update}
          />
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
  const attrs = Object.entries(span.attributes);
  return (
    <aside className="span-detail" aria-label="Span details">
      <h4>{span.name}</h4>
      <div className="span-detail-sub">
        {span.serviceName}
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
              <SpanEventItem key={i} event={event} />
            ))}
          </ul>
        </>
      )}
      <div className="span-detail-sec">Attributes</div>
      {attrs.length === 0 && (
        <div className="traces-note">No attributes recorded.</div>
      )}
      <dl className="span-attrs">
        {attrs.map(([k, v]) => (
          <div key={k}>
            <dt>{k}</dt>
            <dd>{String(v)}</dd>
          </div>
        ))}
      </dl>
    </aside>
  );
}

/** One span event. Exceptions (name === "exception") get an error treatment
 * with message/type promoted and the stacktrace shown as preformatted text. */
function SpanEventItem({ event }: { event: SpanEventView }) {
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
          {type !== undefined && (
            <span className="span-event-type">{String(type)}</span>
          )}
        </div>
        {message !== undefined && (
          <div className="span-event-msg">{String(message)}</div>
        )}
        {stacktrace !== undefined && String(stacktrace) !== "" && (
          <pre className="span-event-trace">{String(stacktrace)}</pre>
        )}
        {rest.map(([k, v]) => (
          <div className="span-event-attr" key={k}>
            <span>{k}</span>
            <span>{String(v)}</span>
          </div>
        ))}
      </li>
    );
  }
  return (
    <li className="span-event">
      <div className="span-event-head">
        <span className="span-event-name">{event.name}</span>
      </div>
      {Object.entries(event.attributes).map(([k, v]) => (
        <div className="span-event-attr" key={k}>
          <span>{k}</span>
          <span>{String(v)}</span>
        </div>
      ))}
    </li>
  );
}
