// The Real users page's Overview tab: the KPI strip, Core Web Vitals,
// sessions over time, top errors, and browser/device breakdowns. Layout and
// component shapes mirror the design prototype (rum/overview.jsx, rum.css)
// as closely as the shipped data allows.
import {
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
import { KpiCard } from "../../components/KpiCard";
import { Sparkline } from "../../components/Sparkline";
import { QueryError } from "../../components/QueryError";
import { EmptyState } from "../../components/EmptyState";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { Panel } from "./Panel";
import { compactCount, formatShare } from "../../lib/vizFormat";
import { relChange } from "../../lib/relChange";
import {
  ratingLabel,
  ratingSwatchColorVar,
  ratingTextColorVar,
  vitalFigure,
  vitalThresholdBound,
  VITAL_LABELS,
  VITAL_NAMES,
  VITAL_TITLES,
  type VitalFigure,
  type VitalName,
} from "./rumModel";
import { formatDurationMs } from "../../lib/waterfall";
import {
  routedPages,
  routePoorShare,
  sortPagesByPoorShare,
  type RumPageRow,
  type RumRequestRow,
} from "../../api/rum";
import { errorGroupKey } from "../../api/rumErrorGroups";
import { SplitBar } from "./NetworkTab";
import {
  useRumBreakdown,
  useRumErrorGroups,
  useRumKpis,
  useRumNetworkRequests,
  useRumPages,
  useRumSessionsOverTime,
  useRumTracedShare,
  useRumVitals,
  type RumScope,
} from "./useRumData";

interface Props {
  scope: RumScope;
  /** The app's current `service.version`, for the same "new in release"
   * comparison the Errors tab makes — `null` when unknown. */
  currentVersion: string | null;
  onOpenSetup: () => void;
  onOpenNetwork: () => void;
  onOpenPages: (route: string) => void;
  onOpenErrors: (groupKey: string) => void;
}

export function OverviewTab({
  scope,
  currentVersion,
  onOpenNetwork,
  onOpenPages,
  onOpenErrors,
}: Props) {
  const kpis = useRumKpis(scope);
  const sessions = kpis.data?.sessions;
  const users = kpis.data?.users;
  const errors = kpis.data?.sessionsWithErrors;
  const pageViews = kpis.data?.pageViews;
  const tracedShare = useRumTracedShare(scope);
  const vitals = useRumVitals(scope);
  const sessionsOverTime = useRumSessionsOverTime(scope);
  const topErrors = useRumErrorGroups(scope, currentVersion);
  const network = useRumNetworkRequests(scope);
  const pages = useRumPages(scope);
  const browser = useRumBreakdown(scope, "resource.browser.brands", {
    requireField: true,
    limit: 8,
  });
  const device = useRumBreakdown(scope, "resource.browser.mobile");

  const sessionsShare =
    sessions && errors && sessions.value > 0
      ? errors.value / sessions.value
      : undefined;
  const prevShare =
    sessions?.previous && errors?.previous && sessions.previous > 0
      ? errors.previous / sessions.previous
      : undefined;
  const shareSeries = shareOverTime(
    sessions?.series ?? [],
    errors?.series ?? [],
  );

  return (
    <div className="rum-stack">
      <div className="rum-kpis">
        <KpiCard
          label="Sessions"
          value={kpis.isPending ? "–" : compactCount(sessions?.value ?? 0)}
          change={
            sessions?.previous !== undefined
              ? relChange(sessions.value, sessions.previous, false)
              : undefined
          }
          detail="distinct session.id"
        >
          <Sparkline
            points={(sessions?.series ?? []).map((p) => ({
              x: p.tMs,
              v: p.value,
            }))}
            width="100%"
            height={28}
            showTooltip={false}
          />
        </KpiCard>
        <KpiCard
          label="Users"
          value={kpis.isPending ? "–" : compactCount(users?.value ?? 0)}
          change={
            users?.previous !== undefined
              ? relChange(users.value, users.previous, false)
              : undefined
          }
          detail="distinct user.id"
        >
          <Sparkline
            points={(users?.series ?? []).map((p) => ({
              x: p.tMs,
              v: p.value,
            }))}
            width="100%"
            height={28}
            showTooltip={false}
          />
        </KpiCard>
        <KpiCard
          label="Sessions with errors"
          value={
            sessionsShare === undefined
              ? "–"
              : formatShare(errors!.value, sessions!.value)
          }
          change={
            prevShare !== undefined
              ? relChange(sessionsShare ?? 0, prevShare, true)
              : undefined
          }
          detail="≥1 exception"
        >
          <Sparkline
            points={shareSeries.map((p) => ({ x: p.tMs, v: p.value }))}
            tone="error"
            width="100%"
            height={28}
            showTooltip={false}
          />
        </KpiCard>
        <KpiCard
          label="Page views"
          value={kpis.isPending ? "–" : compactCount(pageViews?.value ?? 0)}
          change={
            pageViews?.previous !== undefined
              ? relChange(pageViews.value, pageViews.previous, false)
              : undefined
          }
          detail="browser.navigation"
        >
          <Sparkline
            points={(pageViews?.series ?? []).map((p) => ({
              x: p.tMs,
              v: p.value,
            }))}
            width="100%"
            height={28}
            showTooltip={false}
          />
        </KpiCard>
        <KpiCard
          label="Traced requests"
          value={
            tracedShare.isPending || !tracedShare.data?.hasData
              ? "–"
              : `${Math.round(tracedShare.data.value * 100)}%`
          }
          change={
            tracedShare.data?.hasData && tracedShare.data.previous !== undefined
              ? relChange(
                  tracedShare.data.value,
                  tracedShare.data.previous,
                  false,
                )
              : undefined
          }
          detail="client spans with a server child"
        >
          <Sparkline
            points={(tracedShare.data?.series ?? []).map((p) => ({
              x: p.tMs,
              v: p.value,
            }))}
            width="100%"
            height={28}
            showTooltip={false}
          />
        </KpiCard>
      </div>
      {kpis.isError && <QueryError what="RUM KPIs" error={kpis.error} />}
      {tracedShare.isError && (
        <QueryError what="traced-request share" error={tracedShare.error} />
      )}

      <div className="rum-grid-2-1">
        <Panel
          title="Core Web Vitals"
          meta="p75 · share of page views good / needs improvement / poor"
        >
          {vitals.isError ? (
            <QueryError what="Web Vitals" error={vitals.error} />
          ) : (
            <div className="rum-vitals">
              {VITAL_NAMES.map((name) => {
                const data = vitals.data?.get(name);
                const figure = vitalFigure(name, data?.p75, data?.counts ?? {});
                return <VitalCell key={name} name={name} figure={figure} />;
              })}
            </div>
          )}
        </Panel>
        <Panel title="Sessions" meta={rangeLabelFor(scope)}>
          {sessionsOverTime.isError ? (
            <QueryError
              what="sessions over time"
              error={sessionsOverTime.error}
            />
          ) : (
            <StackedSessions data={sessionsOverTime} />
          )}
        </Panel>
      </div>

      <Panel
        title="Slowest pages"
        meta="top 5 routes by worst Web Vital's poor share"
      >
        {pages.isError ? (
          <QueryError what="pages" error={pages.error} />
        ) : pages.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : (pages.data?.length ?? 0) === 0 ? (
          <EmptyState title="No page views in this window" />
        ) : (
          <SlowestPagesList rows={pages.data!} onSelect={onOpenPages} />
        )}
      </Panel>

      <div className="rum-grid-3">
        <Panel title="Top errors">
          {topErrors.isError ? (
            <QueryError what="top errors" error={topErrors.error} />
          ) : topErrors.isPending ? (
            <div className="rum-placeholder">Loading…</div>
          ) : (topErrors.data?.length ?? 0) === 0 ? (
            <EmptyState title="No errors in this window" />
          ) : (
            <div className="rum-error-list">
              {topErrors.data!.slice(0, 6).map((g) => (
                <button
                  key={errorGroupKey(g)}
                  type="button"
                  className="rum-row"
                  onClick={() => onOpenErrors(errorGroupKey(g))}
                >
                  <span className="rum-row-main">
                    <span className="ell rum-row-title">
                      <span style={{ color: "var(--err)" }}>
                        {g.exceptionType ?? "Error"}
                      </span>{" "}
                      <span className="rum-row-message">
                        {g.exceptionMessage ?? ""}
                      </span>
                    </span>
                  </span>
                  {g.backendCause && (
                    <span className="rum-pill warn">backend cause</span>
                  )}
                  <span className="mono dim rum-row-count">
                    {compactCount(g.count)} occurrences
                  </span>
                </button>
              ))}
            </div>
          )}
        </Panel>

        <Panel
          title="Frontend → backend"
          meta="top requests · client+network vs backend p75"
          actions={
            <button type="button" className="btn-ghost" onClick={onOpenNetwork}>
              View all
            </button>
          }
        >
          {network.isError ? (
            <QueryError
              what="frontend → backend requests"
              error={network.error}
            />
          ) : network.isPending ? (
            <div className="rum-placeholder">Loading…</div>
          ) : (network.data?.length ?? 0) === 0 ? (
            <EmptyState title="No client HTTP spans in this window" />
          ) : (
            <FrontendBackendList rows={network.data!} />
          )}
        </Panel>

        <Panel title="Sessions by browser">
          <MiniBars
            rows={browser.data}
            pending={browser.isPending}
            emptyHint="Upgrade the SDK to capture browser.brands"
          />
          <span className="rum-eyebrow rum-device-eyebrow">Device</span>
          <MiniBars
            rows={device.data}
            pending={device.isPending}
            labelFor={(v) =>
              v === "true"
                ? "Mobile"
                : v === "false"
                  ? "Desktop"
                  : (v ?? "Unknown")
            }
          />
        </Panel>
      </div>
    </div>
  );
}

/** The window label a `Panel`'s `meta` shows next to "Sessions" — mirrors
 * the prototype's static "last 24 h" with the page's own selected range. */
function rangeLabelFor(scope: RumScope): string {
  const hours = (scope.range.toMs - scope.range.fromMs) / 3_600_000;
  return hours < 48
    ? `last ${Math.round(hours)} h`
    : `last ${Math.round(hours / 24)} d`;
}

/** Zips two bucketed series sharing the same `tMs` grid into a per-bucket
 * error share (0–1) — a bucket the error series has no point for (no
 * errors that step) reads as 0, not "missing", mirroring
 * `entityDetailStats.ts`'s `errorRateSeries`. */
function shareOverTime(
  total: { tMs: number; value: number }[],
  errors: { tMs: number; value: number }[],
): { tMs: number; value: number }[] {
  const errByT = new Map(errors.map((p) => [p.tMs, p.value]));
  return total.map((p) => ({
    tMs: p.tMs,
    value: p.value > 0 ? (errByT.get(p.tMs) ?? 0) / p.value : 0,
  }));
}

/** One vital's card — p75, rating and distribution bar — shared by the
 * Overview's Core Web Vitals panel and the Pages tab's route detail. */
export function VitalCell({
  name,
  figure,
}: {
  name: VitalName;
  figure: VitalFigure;
}) {
  const good = figure.shares.find((s) => s.rating === "good");
  const goodPct = good ? Math.round(good.share * 100) : 0;
  return (
    <div className="rum-vital">
      <div className="rum-vital-head">
        <b className="mono rum-vital-key">{VITAL_LABELS[name]}</b>
        <span className="dim ell rum-vital-name">{VITAL_TITLES[name]}</span>
      </div>
      <div className="rum-vital-head">
        <span className="mono rum-vital-value">{figure.formatted}</span>
        {figure.rating && (
          <span
            className="rum-vital-rating"
            style={{ color: ratingTextColorVar(figure.rating) }}
          >
            {ratingLabel(figure.rating)}
          </span>
        )}
      </div>
      <div className="rum-vital-dist-wrap">
        <VitalDist name={name} figure={figure} />
        <div className="mono faint rum-vital-goodpct">{goodPct}% good</div>
      </div>
    </div>
  );
}

/** The good/needs-improvement/poor bar for one vital, with the shared
 * `VizTooltip` on hover and keyboard focus. */
function VitalDist({ name, figure }: { name: VitalName; figure: VitalFigure }) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const total = figure.shares.reduce((s, r) => s + r.count, 0);
  if (total === 0) {
    return <DistBar shares={figure.shares} />;
  }
  return (
    <div
      ref={hostRef}
      tabIndex={0}
      aria-label={`${VITAL_TITLES[name]} thresholds`}
      onPointerMove={pointer.track}
      onPointerLeave={pointer.clear}
      onFocus={(e) => pointer.anchorTo(e.currentTarget)}
      onBlur={pointer.clear}
      className="rum-vital-dist"
    >
      <DistBar shares={figure.shares} />
      {pointer.anchor && (
        <VizTooltip
          anchor={pointer.anchor}
          host={pointer.host}
          title={`${VITAL_TITLES[name]} · p75 thresholds`}
          rows={figure.shares.map((s) => ({
            swatch: ratingSwatchColorVar(s.rating),
            label:
              s.rating === "good"
                ? `Good ≤ ${vitalThresholdBound(name, "good")}`
                : s.rating === "poor"
                  ? `Poor > ${vitalThresholdBound(name, "poor")}`
                  : ratingLabel(s.rating),
            value: formatShare(s.count, total),
          }))}
          footer="share of page views"
        />
      )}
    </div>
  );
}

function DistBar({ shares }: { shares: VitalFigure["shares"] }) {
  return (
    <div className="rum-distbar">
      {shares.map((s) => (
        <span
          key={s.rating}
          title={`${ratingLabel(s.rating)} ${Math.round(s.share * 100)}%`}
          style={{
            width: `${s.share * 100}%`,
            background: ratingSwatchColorVar(s.rating),
          }}
        />
      ))}
    </div>
  );
}

/** Stacked bars of sessions with/without errors, with a hover `VizTooltip`
 * and start/middle/"now" time labels — mirrors the prototype's
 * `StackedSessions`. The deploy marker and version-keyed error-share note
 * are omitted: neither is cheap to derive from real data (see design.md's
 * "one bounded IR read" principle) without a dedicated per-version read,
 * which is out of this group's scope. */
function StackedSessions({
  data,
}: {
  data: ReturnType<typeof useRumSessionsOverTime>;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  if (data.isPending) return <div className="rum-placeholder">Loading…</div>;
  const total = data.data?.total ?? [];
  const withErrors = data.data?.withErrors ?? [];
  if (total.length === 0) {
    return <EmptyState title="No sessions in this window" />;
  }
  const errByT = new Map(withErrors.map((p) => [p.tMs, p.value]));
  const bars = total.map((p) => {
    const totalV = Math.max(0, Math.round(p.value));
    const err = Math.min(totalV, Math.round(errByT.get(p.tMs) ?? 0));
    return { tMs: p.tMs, total: totalV, err, ok: totalV - err };
  });
  const max = Math.max(1, ...bars.map((b) => b.total));

  function onMove(e: ReactPointerEvent<HTMLDivElement>) {
    const rect = hostRef.current?.getBoundingClientRect();
    if (!rect) return;
    const i = Math.max(
      0,
      Math.min(
        bars.length - 1,
        Math.floor(((e.clientX - rect.left) / rect.width) * bars.length),
      ),
    );
    setHoverIdx(i);
    pointer.track(e);
  }
  function onLeave() {
    setHoverIdx(null);
    pointer.clear();
  }

  const hb = hoverIdx !== null ? bars[hoverIdx] : undefined;
  const timeLabel = (tMs: number) =>
    `${new Date(tMs).toISOString().slice(11, 16)} UTC`;

  return (
    <div className="rum-stacked-sessions">
      <div className="rum-stacked-legend">
        <span className="rum-key">
          <i style={{ background: "var(--dim)" }} />
          without errors
        </span>
        <span className="rum-key">
          <i style={{ background: "var(--err-bar)" }} />
          with errors
        </span>
      </div>
      <div
        ref={hostRef}
        onPointerMove={onMove}
        onPointerLeave={onLeave}
        className="rum-stacked-bars"
      >
        {bars.map((b, i) => (
          <div
            key={i}
            className="rum-stacked-bar"
            style={{
              height: `${(b.total / max) * 100}%`,
              opacity: hoverIdx !== null && hoverIdx !== i ? 0.55 : 1,
            }}
          >
            <span
              className="rum-stacked-bar-ok"
              style={{ flex: b.ok || 0.0001 }}
            />
            <span
              className="rum-stacked-bar-err"
              style={{
                flex: `0 0 ${b.total > 0 ? Math.max(2, (b.err / b.total) * 100) : 0}%`,
              }}
            />
          </div>
        ))}
        {hb && pointer.anchor && (
          <VizTooltip
            anchor={pointer.anchor}
            host={pointer.host}
            title={timeLabel(hb.tMs)}
            rows={[
              {
                label: "without errors",
                value: compactCount(hb.ok),
                swatch: "var(--dim)",
              },
              {
                label: "with errors",
                value: compactCount(hb.err),
                swatch: "var(--err-bar)",
              },
            ]}
            footer={{
              label: "error share",
              value: formatShare(hb.err, hb.total),
            }}
          />
        )}
      </div>
      <div className="mono faint rum-stacked-axis">
        <span>{timeLabel(bars[0]!.tMs)}</span>
        <span>{timeLabel(bars[Math.floor(bars.length / 2)]!.tMs)}</span>
        <span>now</span>
      </div>
    </div>
  );
}

function MiniBars({
  rows,
  pending,
  emptyHint,
  labelFor,
}: {
  rows: { value: string | null; count: number }[] | undefined;
  pending: boolean;
  emptyHint?: string;
  labelFor?: (value: string | null) => string;
}) {
  if (pending) return <div className="rum-placeholder">Loading…</div>;
  const list = rows ?? [];
  if (list.length === 0) {
    return <EmptyState title="No breakdown yet">{emptyHint}</EmptyState>;
  }
  const total = list.reduce((s, r) => s + r.count, 0) || 1;
  const pcts = list
    .map((r) => ({
      label: labelFor ? labelFor(r.value) : (r.value ?? "Unknown"),
      pct: Math.round((r.count / total) * 100),
    }))
    .slice(0, 6);
  const max = Math.max(1, ...pcts.map((r) => r.pct));
  return (
    <div className="rum-minibars">
      {pcts.map((r, i) => (
        <div className="rum-minibars-row" key={i}>
          <span className="mono rum-minibars-label">{r.label}</span>
          <span className="rum-minibars-track">
            <span
              className="rum-minibars-fill"
              style={{ width: `${(r.pct / max) * 100}%` }}
            />
          </span>
          <span className="mono dim rum-minibars-value">{r.pct}%</span>
        </div>
      ))}
    </div>
  );
}

/** The Overview panel's top 5 requests by call volume, reusing the Network
 * tab's own split bar — the row list stays compact (no header, no origin
 * column); "View all" opens the full table. */
function FrontendBackendList({ rows }: { rows: RumRequestRow[] }) {
  const top = rows.slice(0, 5);
  const maxP75 = Math.max(1, ...top.map((r) => r.totalP75Ms ?? 0));
  return (
    <div className="rum-fe-be-list">
      {top.map((r) => (
        <div
          key={`${r.method}\u0000${r.origin}\u0000${r.template}`}
          className="rum-fe-be-row"
        >
          <span className="mono ell rum-fe-be-name">
            <span className="dim">{r.method}</span> {r.template}
          </span>
          <SplitBar row={r} maxP75={maxP75} />
          <span className="mono num dim">
            {r.totalP75Ms !== null ? formatDurationMs(r.totalP75Ms) : "—"}
          </span>
        </div>
      ))}
    </div>
  );
}

/** Routes with no explicit `url.template` are excluded: they'd open the
 * Pages tab to nothing selectable. */
function SlowestPagesList({
  rows,
  onSelect,
}: {
  rows: RumPageRow[];
  onSelect: (route: string) => void;
}) {
  const top = sortPagesByPoorShare(routedPages(rows)).slice(0, 5);
  if (top.length === 0) {
    return <EmptyState title="No routed page views in this window" />;
  }
  return (
    <div className="rum-fe-be-list">
      {top.map((r) => (
        <button
          key={r.route}
          type="button"
          className="rum-slowest-page-row"
          onClick={() => onSelect(r.route!)}
        >
          <span className="mono ell rum-fe-be-name">{r.route}</span>
          <span className="mono dim">{compactCount(r.views)} views</span>
          <span
            className="mono num"
            style={{
              color: routePoorShare(r) > 0 ? "var(--err)" : "var(--ok-text)",
            }}
          >
            {Math.round(routePoorShare(r) * 100)}% poor
          </span>
        </button>
      ))}
    </div>
  );
}
