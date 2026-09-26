// The System Overview (`/overview`): is anything broken right now, what is
// in the system, and how much is it ingesting — tenant-wide, scoped to one
// environment and the selected window. Every row links into an existing
// view; nothing filters in place.

import { useEffect, useMemo, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { fetchErrorGroupVolume, type ErrorGroup } from "../../api/errors";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { ServiceGraph } from "../../components/ServiceGraph";
import {
  SignalHistogram,
  type VolumeSeries,
} from "../../components/SignalHistogram";
import { Sparkline, type SparklineMarker } from "../../components/Sparkline";
import { TimeRangePicker } from "../../components/TimeRangePicker";
import type { Scale } from "../explore/scale";
import { groupKey } from "../errors/ErrorsView";
import { useSourceContextEnabled } from "../../lib/useSourceContextEnabled";
import { useWhoami } from "../../lib/useWhoami";
import {
  formatRangeLabel,
  rangeScopeKey,
  resolveRange,
  type ResolvedRange,
} from "../../lib/time";
import { compactCount } from "../../lib/vizFormat";
import { formatDurationMs } from "../../lib/waterfall";
import { formatRatePerSec } from "../../lib/traceGroups";
import { crossSignalSearch, type ExploreState } from "../../lib/urlState";
import type { SeriesPoint } from "../../api/entityDetailStats";
import type { Deploy, Endpoint, VersionSighting } from "../../api/overview";
import {
  healthCounts,
  kpiFigures,
  lastDeployLabel,
  serviceRows,
  setupSteps,
  type Health,
  type KpiFigure,
  type ServiceRow,
} from "./overviewModel";
import {
  overviewStep,
  useDeploys,
  useEnvironments,
  useIngestVolume,
  useMemberCount,
  useServiceActivity,
  useServiceMap,
  useServices,
  useSlowestEndpoints,
  useSystemKpis,
  useTopErrorGroups,
  type OverviewScope,
} from "./useOverviewData";
import { DeploysLane } from "./DeploysLane";
import { SetupButton, SetupDialog } from "./SetupChecklist";
import { ZoomPan } from "./ZoomPan";
import { serviceHref, viewHref } from "./links";
import "../catalog/catalog.css";
import "./overview.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

const SIGNAL_ORDER = ["logs", "traces", "metrics", "profiles"];
const SIGNAL_COLORS: Record<string, string> = {
  logs: "var(--svc-a)",
  traces: "var(--svc-c)",
  metrics: "var(--svc-f)",
  profiles: "var(--svc-i)",
};

export function OverviewView({ state, update }: Props) {
  // Resolved per render, as CatalogView does: queries are keyed by
  // `rangeKey`, so a refetch reads the window as of when it runs.
  const rangeKey = rangeScopeKey(state);
  const range = resolveRange(state.range, Date.now());
  const scope: OverviewScope = { range, rangeKey, env: state.env };
  const rangeSeconds = (range.toMs - range.fromMs) / 1000;

  const kpis = useSystemKpis(scope);
  const services = useServices(scope);
  const activity = useServiceActivity(scope);
  const deploysQ = useDeploys(scope);
  const map = useServiceMap(scope);
  const ingest = useIngestVolume(scope);
  const errors = useTopErrorGroups(scope);
  const slowest = useSlowestEndpoints(scope);
  const envs = useEnvironments(scope);
  const { canManage } = useWhoami(state);
  const members = useMemberCount(state.tenant, canManage);
  const githubLinked = useSourceContextEnabled(state.tenant);

  const rows = useMemo(
    () => serviceRows(services.data?.entities ?? [], rangeSeconds),
    [services.data, rangeSeconds],
  );
  const deploys = deploysQ.data?.deploys ?? [];
  const markers: SparklineMarker[] = deploys.map((d) => ({
    x: d.atMs,
    label: `${d.service} ${d.version}`,
  }));
  const figures = kpiFigures(kpis.data, rows, ingest.data);
  const steps = setupSteps({
    rows,
    githubLinked: state.tenant ? githubLinked : undefined,
    memberCount: canManage ? members.data : undefined,
    canManage,
  });

  // `?setup` (the palette's "Open setup checklist") opens the dialog on load.
  const location = useLocation();
  const navigate = useNavigate();
  const [setupOpen, setSetupOpen] = useState(false);
  useEffect(() => {
    const params = new URLSearchParams(location.search);
    if (!params.has("setup")) return;
    setSetupOpen(true);
    params.delete("setup");
    const rest = params.toString();
    navigate(`${location.pathname}${rest ? `?${rest}` : ""}`, {
      replace: true,
    });
  }, [location.search, location.pathname, navigate]);

  const envOptions = [
    ...new Set([...(envs.data ?? []), ...(state.env ? [state.env] : [])]),
  ].sort();
  const external = map.data?.external ?? 0;
  const allDone = steps.every((s) => s.done);

  return (
    <div className="catalog overview">
      <div className="catalog-main">
        <div className="entity-detail overview-detail">
          <div className="catalog-headline overview-headline">
            <span className="overview-title-group">
              <span className="catalog-title">Overview</span>
              <span className="catalog-sub">
                {rows.length} services · {external} external ·{" "}
                {state.env || "all environments"}
              </span>
            </span>
            <div className="overview-controls">
              <label className="overview-env">
                env
                <select
                  aria-label="Environment"
                  value={state.env}
                  onChange={(e) => update({ env: e.target.value })}
                >
                  <option value="">all</option>
                  {envOptions.map((e) => (
                    <option key={e} value={e}>
                      {e}
                    </option>
                  ))}
                </select>
              </label>
              <TimeRangePicker
                range={state.range}
                onChange={(r) => update({ range: r })}
              />
              {!allDone && (
                <SetupButton steps={steps} onOpen={() => setSetupOpen(true)} />
              )}
            </div>
          </div>

          {setupOpen && (
            <SetupDialog
              steps={steps}
              onClose={() => setSetupOpen(false)}
              linkSearch={crossSignalSearch(state)}
            />
          )}

          <div className="overview-kpis">
            {figures.map((k) => (
              <KpiSparkCard
                key={k.label}
                figure={k}
                markers={markers}
                loading={
                  k.label === "Ingest" ? ingest.isPending : kpis.isPending
                }
              />
            ))}
            <ServicesCard rows={rows} external={external} state={state} />
          </div>
          {kpis.isError && <QueryError what="system KPIs" error={kpis.error} />}

          <DeploysLane
            deploys={deploys}
            range={range}
            title={`Deploys · ${formatRangeLabel(state.range).toLowerCase()}`}
          />

          <div className="overview-body">
            <div className="overview-main-col">
              <section className="overview-card">
                <div className="catalog-headline">
                  <span className="overview-title-group">
                    <span className="catalog-title">Service map</span>
                    <span className="catalog-sub">
                      p95 per service · edge width is call volume
                    </span>
                  </span>
                  <Link
                    className="overview-link"
                    to={viewHref("/catalog", state, { catalogView: "map" })}
                  >
                    Open in Catalog
                  </Link>
                </div>
                <div className="overview-map">
                  <ZoomPan>
                    <ServiceGraph
                      nodes={map.data?.nodes ?? []}
                      edges={map.data?.edges ?? []}
                      loading={map.isPending}
                      error={map.isError ? String(map.error) : undefined}
                      emptyMessage="No service calls in this window"
                      capped={
                        map.data?.dropped
                          ? {
                              shown: map.data.total - map.data.dropped,
                              total: map.data.total,
                            }
                          : undefined
                      }
                      onNodeClick={(id) => {
                        const row = rows.find((r) => r.name === id);
                        navigate(
                          row
                            ? serviceHref(row.key, state)
                            : viewHref("/catalog", state),
                        );
                      }}
                    />
                  </ZoomPan>
                </div>
              </section>

              <section className="overview-card">
                <div className="catalog-headline">
                  <span className="overview-title-group">
                    <span className="catalog-title">Services</span>
                    <span className="catalog-sub">worst health first</span>
                  </span>
                  <Link
                    className="overview-link"
                    to={viewHref("/catalog", state)}
                  >
                    All services
                  </Link>
                </div>
                {services.isError ? (
                  <QueryError what="services" error={services.error} />
                ) : services.isPending ? (
                  <div className="overview-placeholder">Loading services…</div>
                ) : rows.length === 0 ? (
                  <EmptyState title="No services in this window">
                    Services appear here once they send spans, logs or metrics.
                  </EmptyState>
                ) : (
                  <ServicesTable
                    rows={rows}
                    series={activity.data}
                    deploys={deploys}
                    latest={deploysQ.data?.latest}
                    range={range}
                    state={state}
                  />
                )}
              </section>

              <IngestCard
                series={ingest.data}
                pending={ingest.isPending}
                error={ingest.isError ? ingest.error : undefined}
                range={range}
                state={state}
              />
            </div>

            <div className="overview-rail">
              <ErrorGroupsCard
                groups={errors.data?.groups}
                pending={errors.isPending}
                error={errors.isError ? errors.error : undefined}
                range={range}
                rangeKey={rangeKey}
                state={state}
              />
              <SlowestCard
                endpoints={slowest.data}
                pending={slowest.isPending}
                error={slowest.isError ? slowest.error : undefined}
                state={state}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function clockTime(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function nsClock(ns: string): string {
  const ms = Number(ns) / 1e6;
  if (!ms) return "–";
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function toSpark(series: SeriesPoint[]) {
  return series.map((p) => ({ x: p.tMs, v: p.value }));
}

function KpiSparkCard({
  figure,
  markers,
  loading,
}: {
  figure: KpiFigure;
  markers: SparklineMarker[];
  loading: boolean;
}) {
  return (
    <div className="overview-kpi">
      <div className="overview-kpi-top">
        <span className="overview-label overview-kpi-label">
          {figure.label}
        </span>
        {figure.change && (
          <span className={`overview-kpi-change ${figure.change.tone}`}>
            {figure.change.text}
          </span>
        )}
      </div>
      <div className="overview-kpi-value-row">
        <span
          className={`overview-kpi-value${figure.valueTone === "error" ? " error" : ""}`}
        >
          {loading ? "–" : figure.value}
        </span>
        <span className="overview-kpi-unit">{figure.unit}</span>
      </div>
      <div className="overview-kpi-detail">
        {loading ? "loading…" : figure.detail}
      </div>
      <div className="overview-kpi-spark">
        <Sparkline
          points={toSpark(figure.series)}
          markers={markers}
          tone={figure.seriesTone}
          width="100%"
          height={36}
          strokeWidth={1.5}
          formatValue={figure.formatPoint}
          formatLabel={clockTime}
          valueLabel={figure.label.toLowerCase()}
          ariaLabel={`${figure.label} over time`}
        />
      </div>
    </div>
  );
}

const HEALTH_VAR: Record<Health, string> = {
  healthy: "var(--ok)",
  degraded: "var(--warn)",
  critical: "var(--err)",
};

function ServicesCard({
  rows,
  external,
  state,
}: {
  rows: ServiceRow[];
  external: number;
  state: ExploreState;
}) {
  const counts = healthCounts(rows);
  const total = rows.length || 1;
  return (
    <div className="overview-kpi">
      <div className="overview-kpi-top">
        <span className="overview-label">Services</span>
        <Link className="overview-kpi-link" to={viewHref("/catalog", state)}>
          Catalog
        </Link>
      </div>
      <div className="overview-kpi-value-row">
        <span className="overview-kpi-value">{rows.length}</span>
        <span className="overview-kpi-unit">reporting</span>
      </div>
      <div className="overview-kpi-detail">
        {external} external dependencies
      </div>
      <div className="overview-health-bar" aria-hidden="true">
        {(["healthy", "degraded", "critical"] as Health[]).map((h) => (
          <span
            key={h}
            style={{
              width: `${(counts[h] / total) * 100}%`,
              background: HEALTH_VAR[h],
            }}
          />
        ))}
      </div>
      <div className="overview-health-legend">
        <span>
          <i style={{ background: HEALTH_VAR.healthy }} />
          {counts.healthy} healthy
        </span>
        <span>
          <i style={{ background: HEALTH_VAR.degraded }} />
          {counts.degraded} degraded
        </span>
        <span className="critical">
          <i style={{ background: HEALTH_VAR.critical }} />
          {counts.critical} critical
        </span>
      </div>
    </div>
  );
}

function ServicesTable({
  rows,
  series,
  deploys,
  latest,
  range,
  state,
}: {
  rows: ServiceRow[];
  series: Map<string, SeriesPoint[]> | undefined;
  deploys: Deploy[];
  latest: Map<string, VersionSighting> | undefined;
  range: ResolvedRange;
  state: ExploreState;
}) {
  const navigate = useNavigate();
  const { stepSeconds } = overviewStep(range);
  return (
    <div className="table-scroll">
      <table className="trace-table overview-services" aria-label="Services">
        <thead>
          <tr>
            <th aria-label="Health" className="overview-health-col" />
            <th>service.name</th>
            <th>Trend</th>
            <th className="num">Rate</th>
            <th className="num">Errors</th>
            <th className="num">P95</th>
            <th>Last deploy</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const href = serviceHref(r.key, state);
            const traced = r.sources.has("traces");
            const own = deploys
              .filter((d) => d.service === r.name)
              .slice(-1)
              .map((d) => ({ x: d.atMs, label: d.version }));
            return (
              <tr
                key={r.key}
                className="catalog-row-drillable"
                onClick={(e) => {
                  if ((e.target as HTMLElement).closest("a")) return;
                  navigate(href);
                }}
              >
                <td className="overview-health-col">
                  <span
                    className="overview-health-dot"
                    role="img"
                    aria-label={r.health}
                    title={r.health}
                    style={{ background: HEALTH_VAR[r.health] }}
                  />
                </td>
                <td>
                  <Link className="overview-service-link" to={href}>
                    {r.name}
                  </Link>
                </td>
                <td className="entity-sparkline-cell">
                  <Sparkline
                    points={toSpark(series?.get(r.key) ?? [])}
                    markers={own}
                    width={88}
                    height={18}
                    strokeWidth={1.25}
                    formatValue={(v) => formatRatePerSec(v / stepSeconds)}
                    formatLabel={clockTime}
                    valueLabel="rate"
                    ariaLabel={`${r.name} request rate over time`}
                  />
                </td>
                <td className="num">
                  {traced ? formatRatePerSec(r.ratePerSec) : "–"}
                </td>
                <td className={`num${r.errorRate >= 0.005 ? " err" : ""}`}>
                  {traced
                    ? r.errorRate < 0.001
                      ? r.errorRate === 0
                        ? "0%"
                        : "<0.1%"
                      : `${(r.errorRate * 100).toFixed(1)}%`
                    : "–"}
                </td>
                <td className="num">
                  {traced ? formatDurationMs(r.p95Ms) : "–"}
                </td>
                <td className="overview-deploy-cell">
                  {latest
                    ? lastDeployLabel(r.name, deploys, latest, range.toMs)
                    : "…"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function IngestCard({
  series,
  pending,
  error,
  range,
  state,
}: {
  series: VolumeSeries[] | undefined;
  pending: boolean;
  error: unknown;
  range: ResolvedRange;
  state: ExploreState;
}) {
  const [scale, setScale] = useState<Scale>("linear");
  const { stepSeconds } = overviewStep(range);
  const totals = SIGNAL_ORDER.map((key) => ({
    key,
    total:
      (series ?? [])
        .find((s) => s.key === key)
        ?.points.reduce((a, [, v]) => a + v, 0) ?? 0,
  }));
  const sum = totals.reduce((a, t) => a + t.total, 0);
  return (
    <section className="overview-card">
      <div className="catalog-headline">
        <span className="overview-title-group">
          <span className="catalog-title">Ingest volume</span>
          <span className="catalog-sub">records accepted per signal</span>
        </span>
        <span className="overview-total">{compactCount(sum)} total</span>
      </div>
      <div className="overview-ingest-legend">
        {totals.map((t) => (
          <Link
            key={t.key}
            to={viewHref(`/${t.key}`, state)}
            className="overview-ingest-item"
          >
            <span
              className="overview-swatch"
              style={{ background: SIGNAL_COLORS[t.key] }}
            />
            {t.key}
            <span className="overview-ingest-total">
              {compactCount(t.total)}
            </span>
            <span className="overview-ingest-share">
              {sum > 0 ? `${Math.round((t.total / sum) * 100)}%` : "–"}
            </span>
          </Link>
        ))}
      </div>
      {error ? (
        <QueryError what="ingest volume" error={error} />
      ) : pending ? (
        <div className="overview-placeholder">Loading ingest volume…</div>
      ) : (
        <SignalHistogram
          series={series ?? []}
          order={SIGNAL_ORDER}
          colors={SIGNAL_COLORS}
          rangeMs={{ fromMs: range.fromMs, toMs: range.toMs }}
          stepMs={stepSeconds * 1000}
          scale={scale}
          onScaleChange={setScale}
          unit="records"
          label="Ingest volume over time by signal"
          height={96}
        />
      )}
    </section>
  );
}

/** One error group's occurrences over the window — its own query, like
 * the Errors tab's per-row sparkline. */
function ErrorGroupSpark({
  group,
  range,
  rangeKey,
}: {
  group: ErrorGroup;
  range: ResolvedRange;
  rangeKey: string;
}) {
  const { step } = overviewStep(range);
  const { data } = useQuery({
    queryKey: ["overview-error-volume", rangeKey, step, groupKey(group)],
    queryFn: () => fetchErrorGroupVolume(group, range, step),
    staleTime: 30_000,
  });
  const points = (data?.[0]?.points ?? []).map(([x, v]) => ({ x, v }));
  return (
    <span className="overview-error-spark">
      <Sparkline
        points={points}
        tone="error"
        width={72}
        height={18}
        strokeWidth={1.25}
        showTooltip={false}
        ariaLabel={`${group.exceptionType ?? "error"} occurrences over time`}
      />
    </span>
  );
}

function ErrorGroupsCard({
  groups,
  pending,
  error,
  range,
  rangeKey,
  state,
}: {
  groups: ErrorGroup[] | undefined;
  pending: boolean;
  error: unknown;
  range: ResolvedRange;
  rangeKey: string;
  state: ExploreState;
}) {
  const top = (groups ?? []).slice(0, 5);
  return (
    <section className="overview-card rail">
      <div className="catalog-headline">
        <span className="catalog-title">Top error groups</span>
        <Link className="overview-link" to={viewHref("/errors", state)}>
          All errors
        </Link>
      </div>
      {error ? (
        <QueryError what="error groups" error={error} />
      ) : pending ? (
        <div className="overview-placeholder">Loading error groups…</div>
      ) : top.length === 0 ? (
        <EmptyState title="No errors in this window" />
      ) : (
        <div className="overview-rows">
          {top.map((g) => (
            <Link
              key={groupKey(g)}
              className="overview-row overview-error-row"
              to={viewHref("/errors", state, { group: groupKey(g) })}
            >
              <ErrorGroupSpark group={g} range={range} rangeKey={rangeKey} />
              <span className="overview-row-text">
                <span className="overview-row-primary">
                  {g.exceptionType ?? "(no type)"}
                </span>
                <span className="overview-row-secondary">
                  {g.exceptionMessage ?? ""}
                </span>
                <span className="overview-row-meta">
                  {g.serviceName ?? "(no service)"} · {g.source} ·{" "}
                  {nsClock(g.lastNs)}
                </span>
              </span>
              <span className="overview-row-num">
                {g.count.toLocaleString("en-US")}
              </span>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}

function SlowestCard({
  endpoints,
  pending,
  error,
  state,
}: {
  endpoints: Endpoint[] | undefined;
  pending: boolean;
  error: unknown;
  state: ExploreState;
}) {
  return (
    <section className="overview-card rail">
      <div className="catalog-headline">
        <span className="catalog-title">Slowest endpoints</span>
        <Link className="overview-link" to={viewHref("/traces", state)}>
          Open in Traces
        </Link>
      </div>
      {error ? (
        <QueryError what="slowest endpoints" error={error} />
      ) : pending ? (
        <div className="overview-placeholder">Loading endpoints…</div>
      ) : !endpoints?.length ? (
        <EmptyState title="No server spans in this window" />
      ) : (
        <div
          className="overview-slowest"
          role="table"
          aria-label="Slowest endpoints"
        >
          <div className="overview-slowest-head" role="row">
            <span role="columnheader" className="overview-label">
              Endpoint
            </span>
            <span role="columnheader" className="overview-label num">
              P95 ↓
            </span>
            <span role="columnheader" className="overview-label num">
              P99
            </span>
          </div>
          {endpoints.map((e) => (
            <Link
              key={`${e.service}:${e.name}`}
              role="row"
              className="overview-row overview-slowest-row"
              to={viewHref("/traces", state, {
                traceFilters: [
                  ...(e.service
                    ? [{ field: "service.name", value: e.service }]
                    : []),
                  { field: "name", value: e.name },
                ],
              })}
            >
              <span role="cell" className="overview-row-text">
                <span className="overview-row-primary">{e.name}</span>
                <span className="overview-row-meta">
                  {e.service ?? "(no service)"}
                </span>
              </span>
              <span role="cell" className="overview-row-num">
                {formatDurationMs(e.p95Ms)}
              </span>
              <span role="cell" className="overview-row-num dim">
                {formatDurationMs(e.p99Ms)}
              </span>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}
