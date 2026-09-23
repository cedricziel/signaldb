/**
 * The entity detail page's "Operations" section — the breakdown table
 * (`EntityDetail.tsx`'s `breakdownEntity`, e.g. a service's `span.name`
 * operations), specialized beyond the generic `EntityTable` it used to
 * share with the top-values table: a per-row "last hour" sparkline, a
 * substring filter, and a top-8-by-rate/show-all toggle so a service with
 * hundreds of operations doesn't dump its whole breakdown on the page at
 * once.
 *
 * Reuses `EntityTable`'s query (`fetchCatalogEntities`, `entityQueryKey`)
 * and formatting (`red.tsx`) rather than re-deriving them — only the
 * layout and the filter/top-N behavior are new.
 */
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchCatalogEntities, type EntityPin } from "../../api/catalog";
import type { GroupSort } from "../../api/traceGroups";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { Sparkline } from "../../components/Sparkline";
import { SkeletonRows } from "../explore/Skeleton";
import { SortTh, useSort } from "../../lib/sortTable";
import { NOT_SET } from "../../lib/traceGroups";
import {
  formatTimestampForRange,
  nanosToMs,
  type ResolvedRange,
} from "../../lib/time";
import { formatTimestamp } from "../../lib/vizFormat";
import { entityQueryKey } from "./CatalogView";
import { redDuration, redErrorClass, redErrorRate, redRate } from "./red";
import { useOperationSeries } from "./useOperationSeries";
import type { EntityTypeDef } from "./entityTypes";
import "./OperationsTable.css";

/** Above this count, the table opens collapsed to the busiest operations
 * rather than dumping every row on the page at once. */
const TOP_N = 8;

const sparklineLabel = (x: number) => formatTimestamp(x, 60_000);

export function OperationsTable({
  entity,
  range,
  rangeKey,
  rangeSeconds,
  pinned,
  onRowClick,
}: {
  entity: EntityTypeDef;
  range: ResolvedRange;
  rangeKey: string;
  rangeSeconds: number;
  pinned: EntityPin[];
  onRowClick: (values: (string | null)[]) => void;
}) {
  const breakdownField = entity.identity[0]!;
  const [sort, toggle] = useSort("n", "desc");
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState(false);

  const result = useQuery({
    queryKey: entityQueryKey(entity.id, rangeKey, sort as GroupSort, pinned),
    queryFn: () =>
      fetchCatalogEntities(entity, range, sort as GroupSort, pinned),
  });
  const seriesQuery = useOperationSeries(
    entity,
    breakdownField,
    range,
    rangeKey,
    pinned,
  );
  const series = seriesQuery.data;

  const pending = result.isPending;
  const rows = result.data?.entities ?? [];
  const label = entity.label.toLowerCase();

  const query = filter.trim().toLowerCase();
  const isFiltering = query !== "";
  const filtered = isFiltering
    ? rows.filter((r) => (r.values[0] ?? "").toLowerCase().includes(query))
    : rows;

  const canCollapse = !isFiltering && rows.length > TOP_N;
  const topRateNames = canCollapse
    ? new Set(
        [...rows]
          .sort((a, b) => (b.red?.traces ?? 0) - (a.red?.traces ?? 0))
          .slice(0, TOP_N)
          .map((r) => r.values[0]),
      )
    : null;
  const displayed =
    canCollapse && !expanded
      ? filtered.filter((r) => topRateNames!.has(r.values[0]))
      : filtered;

  const subtitle = isFiltering
    ? `${filtered.length} of ${rows.length} match`
    : canCollapse && !expanded
      ? `top ${TOP_N} of ${rows.length} by rate`
      : `${rows.length} ${label}, by rate`;

  return (
    <div className="catalog-main operations-table">
      <div className="catalog-headline operations-headline">
        <div className="operations-title-group">
          <span className="catalog-title">{entity.label}</span>
          <span className="catalog-sub">{subtitle}</span>
        </div>
        <input
          type="search"
          className="search-input"
          placeholder={`Filter ${label}…`}
          aria-label={`Filter ${label}`}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
      </div>
      {result.isError && <QueryError what={label} error={result.error} />}
      <div className="operations-scroll">
        <table className="trace-table operations-grid" aria-busy={pending}>
          <thead>
            <tr>
              <th>{breakdownField}</th>
              <th>Last hour</th>
              <SortTh
                label="Rate"
                sortKey="n"
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
            {pending ? (
              <SkeletonRows rows={8} columns={7} numericFrom={2} />
            ) : (
              displayed.map((g) => {
                const name = g.values[0];
                const points = (series?.get(name ?? "") ?? []).map((p) => ({
                  x: p.tMs,
                  v: p.value,
                }));
                return (
                  <tr
                    key={name ?? NOT_SET}
                    className="catalog-row-drillable"
                    onClick={() => onRowClick(g.values)}
                  >
                    <td title={name ?? undefined}>
                      <button type="button" className="trace-open">
                        {name ?? NOT_SET}
                      </button>
                    </td>
                    <td className="operations-sparkline-cell">
                      <Sparkline
                        points={points}
                        width={80}
                        height={18}
                        tone="accent"
                        valueLabel="count"
                        formatLabel={sparklineLabel}
                        ariaLabel={`${name ?? NOT_SET} over the last hour`}
                      />
                    </td>
                    <td className="num">{redRate(g.red, rangeSeconds)}</td>
                    <td
                      className={`num${redErrorClass(g.red) ? " err-rate" : ""}`}
                    >
                      {redErrorRate(g.red)}
                    </td>
                    <td className="num">{redDuration(g.red, "p50Ms")}</td>
                    <td className="num">{redDuration(g.red, "p95Ms")}</td>
                    <td>
                      {formatTimestampForRange(nanosToMs(g.lastNs), range)}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
      {!pending && isFiltering && filtered.length === 0 && (
        <EmptyState title={`No ${label} match “${filter}”.`} />
      )}
      {canCollapse && (
        <div className="operations-toggle">
          <button
            type="button"
            className="btn-ghost"
            onClick={() => setExpanded((v) => !v)}
          >
            {expanded
              ? `Show top ${TOP_N}`
              : `Show all ${rows.length} ${label}`}
          </button>
        </div>
      )}
    </div>
  );
}
