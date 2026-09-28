// The Real users page's Pages tab: the route list, sorted by worst Web
// Vital's poor share. Selecting a route writes `?route=`; its detail panel
// (vitals, load breakdown, backend calls) ships in the next commit. Layout
// mirrors the design prototype's `PagesTab` (perf.jsx), adapted onto
// `Panel`/`VizTooltip` and this build's own `RumPageRow` shape.
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { compactCount } from "../../lib/vizFormat";
import { Panel } from "./Panel";
import {
  routedPages,
  sortPagesByPoorShare,
  type RumPageRow,
  type RoutedPageRow,
} from "../../api/rum";
import { vitalFigure, VITAL_LABELS } from "./rumModel";
import { useRumPages, type RumScope } from "./useRumData";

interface Props {
  scope: RumScope;
  route: string;
  onSelectRoute: (route: string) => void;
  onOpenSetup: () => void;
}

export function PagesTab({ scope, route, onSelectRoute, onOpenSetup }: Props) {
  const pages = useRumPages(scope);
  const rows = pages.data ?? [];
  const routed = sortPagesByPoorShare(routedPages(rows)) as RoutedPageRow[];
  const missing = rows.find((r) => r.route === null);

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
