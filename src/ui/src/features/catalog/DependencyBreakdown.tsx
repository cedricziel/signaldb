// "Where does this service's outbound time go" — a proportional stacked
// bar plus exact figures per dependency category. See
// api/dependencyBreakdown.ts for how the numbers are derived (five
// sum(duration) queries combined client-side; no dedicated backend
// aggregation exists for a derived category like this).
import { useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchDependencyBreakdown } from "../../api/dependencyBreakdown";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import type { ResolvedRange } from "../../lib/time";
import { formatShare, formatValue } from "../../lib/vizFormat";
import { formatDurationMs } from "../../lib/waterfall";
import { SkeletonLines } from "../explore/Skeleton";

function plural(n: number, noun: string): string {
  return `${n.toLocaleString()} ${noun}${n === 1 ? "" : "s"}`;
}

/** Tooltip swatch per category — mirrors the `.dep-*` rules in catalog.css. */
const DEP_COLORS: Record<string, string> = {
  database: "var(--svc-a)",
  http: "var(--svc-b)",
  rpc: "var(--svc-c)",
  messaging: "var(--svc-d)",
  other: "var(--faint)",
};

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
  const rootRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(rootRef);
  const [active, setActive] = useState<string | null>(null);

  if (query.isPending) {
    return <SkeletonLines lines={5} />;
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
      <div className="view-note">
        No database, HTTP, RPC, or messaging calls observed for this service in
        this window.
      </div>
    );
  }
  const activeCategory = categories.find((c) => c.key === active) ?? null;

  return (
    <div className="dep-breakdown viz-host" ref={rootRef}>
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
            data-testid="dep-seg"
            style={{ width: `${(c.durationNs / total) * 100}%` }}
            tabIndex={0}
            aria-label={`${c.label}: ${formatDurationMs(c.durationNs / 1e6)}, ${formatShare(c.durationNs, total)}`}
            aria-describedby={active === c.key ? "dep-tip" : undefined}
            onPointerMove={(e) => {
              setActive(c.key);
              pointer.track(e);
            }}
            onPointerLeave={() => {
              setActive((a) => (a === c.key ? null : a));
              pointer.clear();
            }}
            onFocus={(e) => {
              setActive(c.key);
              pointer.anchorTo(e.currentTarget);
            }}
            onBlur={() => {
              setActive((a) => (a === c.key ? null : a));
              pointer.clear();
            }}
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
      {activeCategory && pointer.anchor && (
        <VizTooltip
          id="dep-tip"
          anchor={pointer.anchor}
          host={pointer.host}
          title={activeCategory.label}
          rows={[
            {
              swatch: DEP_COLORS[activeCategory.key],
              label: "time",
              value: formatDurationMs(activeCategory.durationNs / 1e6),
            },
            {
              label: "share",
              value: formatShare(activeCategory.durationNs, total),
            },
            { label: "calls", value: formatValue(activeCategory.count) },
          ]}
        />
      )}
    </div>
  );
}
