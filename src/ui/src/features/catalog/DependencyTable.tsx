// The per-dependency detail table under the "Time by dependency" bar: one
// row per downstream target (see api/dependencyTargets.ts for how a target
// is derived) plus a synthetic "(self)" row for in-process time. Kept as a
// separate query/component from `DependencyBreakdown` so a slow per-target
// breakdown never blocks the bar it sits under from painting.
import { useRef, useState, type CSSProperties } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  fetchDependencyTargets,
  type DependencyKind,
  type DependencyTargetRow,
} from "../../api/dependencyTargets";
import { QueryError } from "../../components/QueryError";
import { ShareBar } from "../../components/ShareBar";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import type { ResolvedRange } from "../../lib/time";
import { formatShare, formatValue } from "../../lib/vizFormat";
import { formatDurationMs } from "../../lib/waterfall";
import { DEP_COLORS } from "./DependencyBreakdown";
import { SkeletonLines } from "../explore/Skeleton";
import "./DependencyTable.css";

const KIND_LABELS: Record<DependencyKind | "self", string> = {
  database: "Database",
  http: "HTTP",
  rpc: "RPC",
  messaging: "Messaging",
  self: "Self",
};

interface Row {
  key: string;
  kind: DependencyKind | "self";
  target: string;
  operation: string;
  durationNs: number;
  /** `null` for the self row — it has no call count of its own. */
  count: number | null;
  p95Ns: number | null;
}

function toRows(
  targets: DependencyTargetRow[],
  selfDurationNs: number,
  serviceName: string,
): Row[] {
  const rows: Row[] = targets.map((t) => ({
    key: t.key,
    kind: t.kind,
    target: t.target,
    operation: t.operation,
    durationNs: t.durationNs,
    count: t.count,
    p95Ns: t.p95Ns,
  }));
  if (selfDurationNs > 0) {
    rows.push({
      key: "self",
      kind: "self",
      target: `${serviceName} (self)`,
      operation: "",
      durationNs: selfDurationNs,
      count: null,
      p95Ns: null,
    });
  }
  return rows.sort((a, b) => b.durationNs - a.durationNs);
}

/** Calls per request: target call count over the service's own request
 * (root/SERVER span) count. `null` (self, or no requests observed) renders
 * as an em dash — there is nothing to divide by. */
function formatCallsPerReq(count: number | null, requestCount: number) {
  if (count === null || requestCount <= 0) return "–";
  return (count / requestCount).toFixed(2);
}

export function DependencyTable({
  serviceName,
  range,
  rangeKey,
}: {
  serviceName: string;
  range: ResolvedRange;
  rangeKey: string;
}) {
  const query = useQuery({
    queryKey: ["catalog-dependency-targets", serviceName, rangeKey],
    queryFn: () => fetchDependencyTargets(serviceName, range),
  });
  const rootRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(rootRef);
  const [active, setActive] = useState<string | null>(null);

  if (query.isPending) {
    return <SkeletonLines lines={3} />;
  }
  if (query.isError) {
    return <QueryError what="dependency targets" error={query.error} />;
  }

  const {
    rows: targetRows,
    selfDurationNs,
    requestDurationNs,
    requestCount,
  } = query.data;
  const rows = toRows(targetRows, selfDurationNs, serviceName);
  if (rows.length === 0) {
    return null;
  }
  const activeRow = rows.find((r) => r.key === active) ?? null;

  return (
    <div className="dep-table-scroll viz-host" ref={rootRef}>
      <table className="dep-table">
        <thead>
          <tr>
            <th>Dependency</th>
            <th>Kind</th>
            <th>Share of request time ↓</th>
            <th className="dep-table-num">P95</th>
            <th className="dep-table-num">Calls/req</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const share =
              requestDurationNs > 0 ? r.durationNs / requestDurationNs : 0;
            const color = DEP_COLORS[r.kind] ?? "var(--faint)";
            return (
              <tr key={r.key}>
                <td>
                  <div className="dep-table-target">{r.target}</div>
                  {r.operation && (
                    <div className="dep-table-op">{r.operation}</div>
                  )}
                </td>
                <td>
                  <span
                    className="dep-table-kind"
                    style={
                      { "--kind-color": color } as CSSProperties & {
                        "--kind-color": string;
                      }
                    }
                  >
                    {KIND_LABELS[r.kind]}
                  </span>
                </td>
                <td>
                  <div
                    className="dep-table-share"
                    data-testid="dep-table-share-bar"
                    tabIndex={0}
                    aria-label={`${r.target}: ${formatShare(r.durationNs, requestDurationNs)}`}
                    aria-describedby={
                      active === r.key ? "dep-table-tip" : undefined
                    }
                    onPointerMove={(e) => {
                      setActive(r.key);
                      pointer.track(e);
                    }}
                    onPointerLeave={() => {
                      setActive((a) => (a === r.key ? null : a));
                      pointer.clear();
                    }}
                    onFocus={(e) => {
                      setActive(r.key);
                      pointer.anchorTo(e.currentTarget);
                    }}
                    onBlur={() => {
                      setActive((a) => (a === r.key ? null : a));
                      pointer.clear();
                    }}
                  >
                    <ShareBar fraction={share} fillColor={color} />
                    <span className="dep-table-share-pct">
                      {formatShare(r.durationNs, requestDurationNs)}
                    </span>
                  </div>
                </td>
                <td className="dep-table-num">
                  {r.p95Ns === null ? "–" : formatDurationMs(r.p95Ns / 1e6)}
                </td>
                <td className="dep-table-num">
                  {formatCallsPerReq(r.count, requestCount)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {activeRow && pointer.anchor && (
        <VizTooltip
          id="dep-table-tip"
          anchor={pointer.anchor}
          host={pointer.host}
          title={activeRow.target}
          rows={[
            {
              swatch: DEP_COLORS[activeRow.kind],
              label: "time",
              value: formatDurationMs(activeRow.durationNs / 1e6),
            },
            {
              label: "share",
              value: formatShare(activeRow.durationNs, requestDurationNs),
            },
            {
              label: "calls",
              value:
                activeRow.count === null ? "–" : formatValue(activeRow.count),
            },
          ]}
        />
      )}
    </div>
  );
}
