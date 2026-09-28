// The Real users page's Network tab: client HTTP spans grouped by method
// and URL template, split into client+network and backend time via
// `correlate`, the untraced-origin callout, and resources by initiator
// type. Layout mirrors the design prototype's `NetworkTab` (perf.jsx),
// adapted onto `Panel`/`VizTooltip` rather than the prototype's own markup.
import { useRef } from "react";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { compactCount, formatShare } from "../../lib/vizFormat";
import { formatDurationMs } from "../../lib/waterfall";
import { Panel } from "./Panel";
import type { RumRequestRow, RumResourceRow } from "../../api/rum";
import { isMobilePlatform, type RumPlatform } from "./rumModel";
import {
  useRumNetworkRequests,
  useRumResources,
  type RumScope,
} from "./useRumData";

interface Props {
  scope: RumScope;
  /** The selected app's platform — hides the (browser-only) Resources panel
   * on mobile (`rum-explore-tabs`'s "Platform-aware labels" requirement). */
  platform: RumPlatform;
  onOpenSetup: () => void;
}

export function NetworkTab({ scope, platform, onOpenSetup }: Props) {
  const requests = useRumNetworkRequests(scope);
  const resources = useRumResources(scope);
  const rows = requests.data ?? [];
  const untraced = untracedOrigins(rows);
  const mobile = isMobilePlatform(platform);

  return (
    <div className="rum-stack">
      {untraced.length > 0 && (
        <UntracedCallout origins={untraced} onOpenSetup={onOpenSetup} />
      )}

      <Panel
        title="Requests"
        meta="grouped by method and URL template · p75 split into client+network and backend time"
      >
        {requests.isError ? (
          <QueryError what="network requests" error={requests.error} />
        ) : requests.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : rows.length === 0 ? (
          <EmptyState title="No client HTTP spans in this window" />
        ) : (
          <RequestsTable rows={rows} />
        )}
      </Panel>

      {!mobile && (
        <Panel
          title="Resources"
          meta="browser.resource_timing · by initiator type · p75"
        >
          {resources.isError ? (
            <QueryError what="resources" error={resources.error} />
          ) : resources.isPending ? (
            <div className="rum-placeholder">Loading…</div>
          ) : (resources.data?.length ?? 0) === 0 ? (
            <EmptyState title="No resource timing recorded in this window" />
          ) : (
            <ResourcesTable rows={resources.data!} />
          )}
        </Panel>
      )}
    </div>
  );
}

interface UntracedOrigin {
  origin: string;
  calls: number;
}

/** Origins whose requests never joined a backend trace — excludes SDK
 * export endpoints, which are expected to have no server child
 * (`explore-ui-rum`'s "Network tab" requirement). */
function untracedOrigins(rows: RumRequestRow[]): UntracedOrigin[] {
  const tracedOrigins = new Set(
    rows
      .filter((r) => r.tracedCalls > 0 || !r.tracedKnown)
      .map((r) => r.origin),
  );
  const byOrigin = new Map<string, UntracedOrigin>();
  for (const r of rows) {
    if (r.isSdkExport || r.calls === 0 || tracedOrigins.has(r.origin)) {
      continue;
    }
    const entry = byOrigin.get(r.origin) ?? { origin: r.origin, calls: 0 };
    entry.calls += r.calls;
    byOrigin.set(r.origin, entry);
  }
  return Array.from(byOrigin.values());
}

function UntracedCallout({
  origins,
  onOpenSetup,
}: {
  origins: UntracedOrigin[];
  onOpenSetup: () => void;
}) {
  const names = origins.map((o) => o.origin).join(", ");
  const total = origins.reduce((s, o) => s + o.calls, 0);
  return (
    <div className="rum-callout">
      <span>
        {compactCount(total)} requests to <b>{names}</b> couldn't be joined to a
        backend trace. Check that the SDK propagates{" "}
        <code className="mono">traceparent</code> to that origin, that its CORS
        policy allows the header, and that its backend is instrumented.
      </span>
      <button type="button" className="btn" onClick={onOpenSetup}>
        Show setup
      </button>
    </div>
  );
}

function RequestsTable({ rows }: { rows: RumRequestRow[] }) {
  const maxP75 = Math.max(1, ...rows.map((r) => r.totalP75Ms ?? 0));
  return (
    <div className="rum-network-table">
      <div className="rum-network-head">
        <span>Request</span>
        <span>Origin</span>
        <span className="num">Calls</span>
        <span>Client · backend</span>
        <span className="num">p75</span>
        <span className="num">Err</span>
        <span className="num">Traced</span>
        <span>Backend service</span>
      </div>
      {rows.map((r) => (
        <RequestRow
          key={`${r.method}\u0000${r.origin}\u0000${r.template}`}
          row={r}
          maxP75={maxP75}
        />
      ))}
    </div>
  );
}

function RequestRow({ row, maxP75 }: { row: RumRequestRow; maxP75: number }) {
  const tracedShare = row.calls > 0 ? row.tracedCalls / row.calls : 0;
  return (
    <div className="rum-network-row">
      <span className="mono ell">
        <span className="dim">{row.method}</span> {row.template}
      </span>
      <span className="mono dim ell">{row.origin}</span>
      <span className="mono num dim">{compactCount(row.calls)}</span>
      <SplitBar row={row} maxP75={maxP75} />
      <span className="mono num">
        {row.totalP75Ms !== null ? formatDurationMs(row.totalP75Ms) : "—"}
      </span>
      <span
        className="mono num"
        style={{ color: row.errorCalls > 0 ? "var(--err)" : "var(--dim)" }}
      >
        {formatShare(row.errorCalls, row.calls)}
      </span>
      <span
        className="mono num"
        style={{
          color: row.isSdkExport
            ? "var(--dim)"
            : tracedShare > 0
              ? "var(--ok-text)"
              : "var(--warn-banner-text)",
        }}
      >
        {row.isSdkExport || !row.tracedKnown
          ? "—"
          : formatShare(row.tracedCalls, row.calls)}
      </span>
      <span className="mono dim ell">
        {row.isSdkExport
          ? "SDK export"
          : row.backendService
            ? row.backendService
            : row.tracedKnown
              ? "no trace"
              : "—"}
      </span>
    </div>
  );
}

/** The client+network vs backend split bar, with a `VizTooltip` breakdown —
 * backend segment only when the group has at least one traced call. Shared
 * with the Overview "Frontend → backend" panel. */
export function SplitBar({
  row,
  maxP75,
}: {
  row: RumRequestRow;
  maxP75: number;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const total = row.totalP75Ms ?? 0;
  const backend =
    row.backendP75Ms !== undefined ? Math.min(row.backendP75Ms, total) : 0;
  const client = Math.max(0, total - backend);
  const widthPct = (total / maxP75) * 100;

  return (
    <div
      ref={hostRef}
      tabIndex={0}
      className="rum-splitbar-wrap"
      onPointerMove={pointer.track}
      onPointerLeave={pointer.clear}
      onFocus={(e) => pointer.anchorTo(e.currentTarget)}
      onBlur={pointer.clear}
    >
      <span className="rum-splitbar-track" style={{ width: `${widthPct}%` }}>
        <span
          className="rum-splitbar-client"
          style={{ flex: client || 0.0001 }}
        />
        {backend > 0 && (
          <span className="rum-splitbar-backend" style={{ flex: backend }} />
        )}
      </span>
      {pointer.anchor && (
        <VizTooltip
          anchor={pointer.anchor}
          host={pointer.host}
          title={`${row.method} ${row.template}`}
          rows={[
            {
              label: "client + network (est.)",
              value: formatDurationMs(client),
              swatch: "var(--accent)",
            },
            ...(backend > 0
              ? [
                  {
                    label: "backend",
                    value: formatDurationMs(backend),
                    swatch: "var(--dim)",
                  },
                ]
              : []),
          ]}
          footer={{ label: "p75", value: formatDurationMs(total) }}
        />
      )}
    </div>
  );
}

function ResourcesTable({ rows }: { rows: RumResourceRow[] }) {
  return (
    <div className="rum-resources-table">
      <div className="rum-network-head rum-resources-head">
        <span>Type</span>
        <span className="num">Count</span>
        <span className="num">Transfer</span>
        <span className="num">p75</span>
        <span className="num">Largest</span>
      </div>
      {rows.map((r) => (
        <div
          key={r.initiatorType}
          className="rum-network-row rum-resources-row"
        >
          <span className="mono">{r.initiatorType}</span>
          <span className="mono num dim">{compactCount(r.count)}</span>
          <span className="mono num">
            {compactCount(r.transferBytes, "By")}
          </span>
          <span className="mono num">{formatDurationMs(r.p75Ms)}</span>
          <span className="mono num">
            {compactCount(r.maxTransferBytes, "By")}
          </span>
        </div>
      ))}
    </div>
  );
}
