// The Real users page's Pages tab: the route list (sorted by worst Web
// Vital's poor share) and, once a route is selected via `?route=`, its
// detail panel — vitals, load breakdown and backend calls. Layout mirrors
// the design prototype's `PagesTab`/`LoadWaterfall` (perf.jsx), adapted onto
// `Panel`/`VizTooltip` and this build's own `RumPageRow` shape.
import { useRef } from "react";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { compactCount } from "../../lib/vizFormat";
import { formatDurationMs } from "../../lib/waterfall";
import { Panel } from "./Panel";
import { VitalCell } from "./OverviewTab";
import {
  joinBackendCallsToNetworkService,
  routedPages,
  routePoorShare,
  sortPagesByPoorShare,
  type RumBackendCallRow,
  type RumPageRow,
  type RoutedPageRow,
} from "../../api/rum";
import {
  loadBreakdownPhases,
  vitalFigure,
  VITAL_LABELS,
  VITAL_NAMES,
} from "./rumModel";
import {
  useRumBackendCalls,
  useRumLoadBreakdown,
  useRumNetworkRequests,
  useRumPages,
  type RumScope,
} from "./useRumData";

interface Props {
  scope: RumScope;
  route: string;
  onSelectRoute: (route: string) => void;
  onOpenSetup: () => void;
  onOpenNetwork: () => void;
}

export function PagesTab({
  scope,
  route,
  onSelectRoute,
  onOpenSetup,
  onOpenNetwork,
}: Props) {
  const pages = useRumPages(scope);
  const rows = pages.data ?? [];
  const routed = sortPagesByPoorShare(routedPages(rows)) as RoutedPageRow[];
  const missing = rows.find((r) => r.route === null);
  const selected = routed.find((r) => r.route === route);

  return (
    <div className="rum-stack">
      {missing && missing.views > 0 && (
        <MissingRouteCallout row={missing} onOpenSetup={onOpenSetup} />
      )}

      <Panel
        title="Pages"
        meta="browser.navigation · sorted by worst Web Vital's poor share · error share = exceptions carrying this route's url.template ÷ views"
      >
        {pages.isError ? (
          <QueryError what="pages" error={pages.error} />
        ) : pages.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : routed.length === 0 ? (
          <EmptyState title="No routed page views in this window" />
        ) : (
          <PagesTable rows={routed} selected={route} onSelect={onSelectRoute} />
        )}
      </Panel>

      {selected && (
        <RouteDetail
          scope={scope}
          row={selected}
          onOpenNetwork={onOpenNetwork}
        />
      )}
    </div>
  );
}

const TABLE_VITALS = ["lcp", "inp", "cls", "ttfb"] as const;

function MissingRouteCallout({
  row,
  onOpenSetup,
}: {
  row: RumPageRow;
  onOpenSetup: () => void;
}) {
  return (
    <div className="rum-callout">
      <span>
        {compactCount(row.views)} page views carry no{" "}
        <code className="mono">url.template</code> and don't parse into a route
        from <code className="mono">url.full</code>, so they're not in the list
        below. Set <code className="mono">url.template</code> when starting a
        navigation span to route them.
      </span>
      <button type="button" className="btn" onClick={onOpenSetup}>
        Show setup
      </button>
    </div>
  );
}

function PagesTable({
  rows,
  selected,
  onSelect,
}: {
  rows: RoutedPageRow[];
  selected: string;
  onSelect: (route: string) => void;
}) {
  return (
    <div className="rum-pages-table">
      <div className="rum-pages-head">
        <span>Route</span>
        <span className="num">Views</span>
        {TABLE_VITALS.map((v) => (
          <span key={v} className="num">
            {VITAL_LABELS[v]}
          </span>
        ))}
        <span className="num">Errors</span>
      </div>
      {rows.map((r) => (
        <button
          key={r.route}
          type="button"
          className={
            r.route === selected ? "rum-pages-row on" : "rum-pages-row"
          }
          aria-current={r.route === selected ? "true" : undefined}
          onClick={() => onSelect(r.route)}
        >
          <span className="mono ell">{r.route}</span>
          <span className="mono num dim">{compactCount(r.views)}</span>
          {TABLE_VITALS.map((v) => {
            const data = r.vitals.get(v);
            const figure = vitalFigure(v, data?.p75, data?.counts ?? {});
            return (
              <span key={v} className="mono num">
                {figure.formatted}
              </span>
            );
          })}
          <span
            className="mono num"
            style={{
              color:
                r.errorShare !== null && r.errorShare > 0
                  ? "var(--err)"
                  : "var(--dim)",
            }}
          >
            {r.errorShare !== null ? `${Math.round(r.errorShare * 100)}%` : "—"}
          </span>
        </button>
      ))}
    </div>
  );
}

function RouteDetail({
  scope,
  row,
  onOpenNetwork,
}: {
  scope: RumScope;
  row: RoutedPageRow;
  onOpenNetwork: () => void;
}) {
  const loadBreakdown = useRumLoadBreakdown(scope, row.route);
  const backendCalls = useRumBackendCalls(scope, row.route);
  const network = useRumNetworkRequests(scope);
  const joined = joinBackendCallsToNetworkService(
    backendCalls.data ?? [],
    network.data ?? [],
  );

  return (
    <div className="rum-stack">
      <Panel
        title={row.route}
        meta={`${compactCount(row.views)} views · ${Math.round(routePoorShare(row) * 100)}% poor on its worst vital`}
      >
        <div className="rum-vitals">
          {VITAL_NAMES.map((name) => {
            const data = row.vitals.get(name);
            const figure = vitalFigure(name, data?.p75, data?.counts ?? {});
            return <VitalCell key={name} name={name} figure={figure} />;
          })}
        </div>
      </Panel>

      <Panel
        title="Load breakdown"
        meta="p75 of each navigation phase, from fetchStart"
      >
        {loadBreakdown.isError ? (
          <QueryError what="load breakdown" error={loadBreakdown.error} />
        ) : loadBreakdown.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : !loadBreakdown.data ? (
          <EmptyState title="No navigation timing recorded for this route" />
        ) : (
          <LoadWaterfall phases={loadBreakdownPhases(loadBreakdown.data)} />
        )}
      </Panel>

      <Panel
        title="Backend calls"
        meta="browser.resource_timing · fetch/xhr from this route"
        actions={
          <button type="button" className="btn-ghost" onClick={onOpenNetwork}>
            View in Network
          </button>
        }
      >
        {backendCalls.isError ? (
          <QueryError what="backend calls" error={backendCalls.error} />
        ) : backendCalls.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : joined.length === 0 ? (
          <EmptyState title="No backend calls recorded for this route" />
        ) : (
          <BackendCallsList rows={joined} />
        )}
      </Panel>
    </div>
  );
}

const PHASE_COLORS = [
  "var(--accent)",
  "var(--ok)",
  "var(--warn)",
  "var(--err)",
  "var(--dim)",
  "var(--ok-text)",
  "var(--warn-banner-text)",
];

function LoadWaterfall({
  phases,
}: {
  phases: ReturnType<typeof loadBreakdownPhases>;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const total = Math.max(
    1,
    phases.reduce((s, p) => s + p.ms, 0),
  );
  return (
    <div className="rum-load-wrap">
      <div
        ref={hostRef}
        tabIndex={0}
        className="rum-load-bar"
        onPointerMove={pointer.track}
        onPointerLeave={pointer.clear}
        onFocus={(e) => pointer.anchorTo(e.currentTarget)}
        onBlur={pointer.clear}
      >
        {phases.map((p, i) => (
          <span
            key={p.label}
            style={{
              flex: p.ms || 0.0001,
              background: PHASE_COLORS[i % PHASE_COLORS.length],
            }}
          />
        ))}
      </div>
      {pointer.anchor && (
        <VizTooltip
          anchor={pointer.anchor}
          host={pointer.host}
          title="Load breakdown"
          rows={phases.map((p, i) => ({
            label: p.label,
            value: formatDurationMs(p.ms),
            swatch: PHASE_COLORS[i % PHASE_COLORS.length],
          }))}
          footer={{ label: "total", value: formatDurationMs(total) }}
        />
      )}
      <div className="rum-load-legend">
        {phases.map((p, i) => (
          <span key={p.label}>
            <i style={{ background: PHASE_COLORS[i % PHASE_COLORS.length] }} />
            {p.label} · {formatDurationMs(p.ms)}
          </span>
        ))}
      </div>
    </div>
  );
}

function BackendCallsList({ rows }: { rows: RumBackendCallRow[] }) {
  return (
    <div className="rum-backend-list">
      {rows.map((r) => (
        <div key={`${r.origin}\u0000${r.template}`} className="rum-backend-row">
          <span className="mono ell">{r.template}</span>
          <span className="mono num dim">{compactCount(r.calls)}</span>
          <span className="mono num">
            {r.p75Ms !== null ? formatDurationMs(r.p75Ms) : "—"}
          </span>
          <span className="mono dim ell">{r.backendService ?? "no trace"}</span>
        </div>
      ))}
    </div>
  );
}
