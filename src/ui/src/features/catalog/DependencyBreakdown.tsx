// "Where does this service's outbound time go" — a proportional stacked
// bar plus exact figures per dependency category. See
// api/dependencyBreakdown.ts for how the numbers are derived (five
// sum(duration) queries combined client-side; no dedicated backend
// aggregation exists for a derived category like this).
import { useQuery } from "@tanstack/react-query";
import { fetchDependencyBreakdown } from "../../api/dependencyBreakdown";
import type { ResolvedRange } from "../../lib/time";
import { formatDurationMs } from "../../lib/waterfall";

function plural(n: number, noun: string): string {
  return `${n.toLocaleString()} ${noun}${n === 1 ? "" : "s"}`;
}

export function DependencyBreakdown({
  serviceName,
  range,
  rangeKey,
}: {
  serviceName: string;
  range: ResolvedRange;
  rangeKey: string;
}) {
  const query = useQuery({
    queryKey: ["catalog-dependency-breakdown", serviceName, rangeKey],
    queryFn: () => fetchDependencyBreakdown(serviceName, range),
  });

  if (query.isPending) {
    return <div className="traces-note">Loading…</div>;
  }
  if (query.isError) {
    return (
      <div className="query-error" role="alert">
        Failed to load: {(query.error as Error).message}
      </div>
    );
  }

  const categories = query.data;
  const total = categories.reduce((sum, c) => sum + c.durationNs, 0);
  if (total === 0) {
    return (
      <div className="traces-note">
        No database, HTTP, RPC, or messaging calls observed for this service in
        this window.
      </div>
    );
  }

  return (
    <div className="dep-breakdown">
      <div
        className="dep-bar"
        role="img"
        aria-label={`Time spent by dependency type: ${categories
          .map((c) => `${c.label} ${formatShare(c.durationNs, total)}`)
          .join(", ")}`}
      >
        {categories.map((c) => (
          <span
            key={c.key}
            className={`dep-seg dep-${c.key}`}
            style={{ width: `${(c.durationNs / total) * 100}%` }}
          />
        ))}
      </div>
      <dl className="dep-legend">
        {categories.map((c) => (
          <div key={c.key} className="dep-legend-item">
            <dt className={`dep-swatch dep-${c.key}`}>{c.label}</dt>
            <dd>
              {formatDurationMs(c.durationNs / 1e6)} ·{" "}
              {formatShare(c.durationNs, total)} · {plural(c.count, "call")}
            </dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

function formatShare(part: number, total: number): string {
  if (total <= 0) return "0%";
  return `${((part / total) * 100).toFixed(1)}%`;
}
