// The Real users page's Interactions tab: clicks by target, joined to the
// page's own INP p75 (design.md decision 3a — one read for clicks, no
// second read for the vital: it's already in `useRumPages`'s cache). Each
// row links toward the Sessions tab — not shipped until tasks.md group 3,
// so the link currently redirects to Overview via `RealUsersRoute`'s
// unknown-tab fallback; kept as a real link rather than a dead button so it
// starts working the moment Sessions ships.
import { useRef } from "react";
import { Link } from "react-router";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { compactCount } from "../../lib/vizFormat";
import { Panel } from "./Panel";
import type { RumInteractionRow } from "../../api/rum";
import { cssSelectorLabel, formatVitalValue } from "./rumModel";
import { useRumInteractions, useRumPages, type RumScope } from "./useRumData";

interface Props {
  scope: RumScope;
}

interface InteractionWithInp extends RumInteractionRow {
  pageInpP75?: number;
}

export function InteractionsTab({ scope }: Props) {
  const interactions = useRumInteractions(scope);
  const pages = useRumPages(scope);

  const inpByRoute = new Map<string, number>();
  for (const p of pages.data ?? []) {
    const inp = p.vitals.get("inp")?.p75;
    if (p.route !== null && inp !== undefined) inpByRoute.set(p.route, inp);
  }
  const rows: InteractionWithInp[] = (interactions.data ?? []).map((r) => ({
    ...r,
    pageInpP75: r.route !== null ? inpByRoute.get(r.route) : undefined,
  }));

  return (
    <div className="rum-stack">
      <Panel
        title="Interactions"
        meta="browser.user_action.click · by target and page · INP is the page's own p75"
      >
        {interactions.isError ? (
          <QueryError what="interactions" error={interactions.error} />
        ) : interactions.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : rows.length === 0 ? (
          <EmptyState title="No clicks recorded in this window" />
        ) : (
          <InteractionsTable scope={scope} rows={rows} />
        )}
      </Panel>
    </div>
  );
}

function InteractionsTable({
  scope,
  rows,
}: {
  scope: RumScope;
  rows: InteractionWithInp[];
}) {
  const maxInp = Math.max(1, ...rows.map((r) => r.pageInpP75 ?? 0));
  return (
    <div className="rum-network-table">
      <div className="rum-network-head rum-interactions-head">
        <span>Target</span>
        <span>Page</span>
        <span className="num">Clicks</span>
        <span>Page INP p75</span>
        <span></span>
      </div>
      {rows.map((r, i) => (
        <InteractionRow key={i} scope={scope} row={r} maxInp={maxInp} />
      ))}
    </div>
  );
}

function InteractionRow({
  scope,
  row,
  maxInp,
}: {
  scope: RumScope;
  row: InteractionWithInp;
  maxInp: number;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const widthPct = ((row.pageInpP75 ?? 0) / maxInp) * 100;
  return (
    <div className="rum-network-row rum-interactions-row">
      <span className="mono ell" title={row.target}>
        {cssSelectorLabel(row.target)}
      </span>
      <span className="mono dim ell">{row.route ?? "—"}</span>
      <span className="mono num dim">{compactCount(row.clicks)}</span>
      <div
        ref={hostRef}
        tabIndex={0}
        className="rum-splitbar-wrap"
        onPointerMove={pointer.track}
        onPointerLeave={pointer.clear}
        onFocus={(e) => pointer.anchorTo(e.currentTarget)}
        onBlur={pointer.clear}
      >
        {row.pageInpP75 !== undefined ? (
          <span
            className="rum-splitbar-track"
            style={{ width: `${widthPct}%` }}
          >
            <span className="rum-splitbar-client" style={{ flex: 1 }} />
          </span>
        ) : (
          <span className="dim">—</span>
        )}
        {pointer.anchor && row.pageInpP75 !== undefined && (
          <VizTooltip
            anchor={pointer.anchor}
            host={pointer.host}
            title={row.route ?? row.target}
            rows={[
              {
                label: "INP p75",
                value: formatVitalValue("inp", row.pageInpP75),
                swatch: "var(--accent)",
              },
            ]}
          />
        )}
      </div>
      <Link
        className="btn-ghost"
        to={`/rum/sessions?app=${encodeURIComponent(scope.app)}`}
      >
        Sessions
      </Link>
    </div>
  );
}
