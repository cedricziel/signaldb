/**
 * Cross-signal pivot actions for an attribute row.
 *
 * Correlation is a property of the attribute, not of the view it happens to
 * be shown in: where the schema registry says a key *identifies* an entity
 * (an `entity_roles` role of `"identifying"`, not merely `"descriptive"`),
 * the row offers actions that carry that identity into another signal or
 * the catalog, alongside the existing filter/group-by actions. A key with
 * no identifying role offers nothing here — see the span panel
 * (`features/traces/TracesView.tsx`) and the log detail
 * (`features/logs/LogList.tsx`), which append these to their own row
 * actions.
 */
import type { AttributeRowAction } from "../components/AttributeTable";
import { entityType, type EntityTypeDef } from "../features/catalog/entityTypes";
import { toLokiLabel } from "./labelSuggestions";
import type { AttributeSemantics } from "./semantics";
import { facetableField } from "./traceFilters";
import { compositeKey } from "./traceGroups";
import type { ExploreState, UpdateFn } from "./urlState";

export interface PivotTarget {
  kind: "logs" | "traces" | "catalog";
  label: string;
  ariaLabel: string;
  /** State patch a caller applies via `update(patch, { push: true })` to
   * land on the pivot target. */
  patch: Partial<ExploreState>;
}

/** Every pivot target's patch clears the same view-scoped params — the
 * current trace/search/group selection, and the other two signals' own
 * filter state — so none of them ride along into a view that doesn't
 * understand them (a stale trace filter silently narrowing /logs, a stale
 * search silently narrowing /traces, ...). Each pivot below spreads this in
 * and adds only what actually identifies its own target. */
const CROSS_SIGNAL_RESET: Partial<ExploreState> = {
  trace: "",
  search: "",
  group: "",
  raw: "",
  traceFilters: [],
  filters: [],
};

/**
 * Pivot actions for one attribute row.
 *
 * `bag` is every attribute the row renders alongside — the span panel's
 * merged span+resource values (`resource.` prefix stripped) for a span row,
 * or the log line's labels+metadata for a log row — since a catalog pivot
 * needs an entity's *full* identity, not just the one key/value the row
 * itself carries.
 */
export function attributePivots(
  key: string,
  value: string,
  sem: AttributeSemantics | undefined,
  bag: ReadonlyMap<string, string>,
  from: "logs" | "traces",
): PivotTarget[] {
  const identifying = (sem?.primary.entity_roles ?? []).filter(
    (role) => role.role === "identifying",
  );
  if (identifying.length === 0) return [];

  const pivots: PivotTarget[] = [];

  if (from === "traces") {
    pivots.push({
      kind: "logs",
      label: "logs ↗",
      ariaLabel: `Logs with ${key} = ${value}`,
      patch: {
        ...CROSS_SIGNAL_RESET,
        signal: "logs",
        filters: [{ label: toLokiLabel(key), op: "=", value }],
      },
    });
  }

  if (from === "logs") {
    const field = facetableField(key);
    if (field) {
      pivots.push({
        kind: "traces",
        label: "traces ↗",
        ariaLabel: `Traces with ${key} = ${value}`,
        patch: {
          ...CROSS_SIGNAL_RESET,
          signal: "traces",
          traceFilters: [{ field, value }],
        },
      });
    }
  }

  // Only the entity's *primary* identity key needs to be in the bag — a row
  // still deserves a catalog link once it names the entity at all, even
  // when a secondary identity dimension (e.g. k8s.namespace.name alongside
  // k8s.pod.name) never arrived on this particular row/span. The catalog
  // renders a missing secondary as "(not set)" and drills on it from there.
  const catalogPivots: { type: EntityTypeDef; pivot: PivotTarget }[] = [];
  for (const role of identifying) {
    const id = toLokiLabel(role.entity);
    const type = entityType(id);
    // Registry-only (non-curated) entity types have no catalog page to open.
    if (!type) continue;
    if (!bag.has(type.identity[0]!)) continue;
    const primaryValue = bag.get(type.identity[0]!)!;
    catalogPivots.push({
      type,
      pivot: {
        kind: "catalog",
        label: "catalog ↗",
        ariaLabel: `Open ${type.singular} ${primaryValue} in the catalog`,
        patch: {
          ...CROSS_SIGNAL_RESET,
          signal: "catalog",
          catalogEntity: id,
          catalogPrimary: compositeKey(
            type.identity.map((k) => bag.get(k) ?? null),
          ),
          catalogSecondary: "",
        },
      },
    });
  }
  // A key naming more than one catalog-backed entity (rare, but the roles
  // are a list) needs the entity spelled out in the label — otherwise two
  // "catalog ↗" buttons on the same row would be indistinguishable.
  const disambiguate = catalogPivots.length > 1;
  for (const { type, pivot } of catalogPivots) {
    pivots.push(
      disambiguate
        ? { ...pivot, label: `catalog: ${type.singular} ↗` }
        : pivot,
    );
  }

  return pivots;
}

/**
 * `attributePivots`'s results as row actions, landing on
 * `update(patch, { push: true })` — the one bit of glue both adopters (the
 * span panel and the log detail) would otherwise duplicate.
 */
export function pivotRowActions(
  key: string,
  value: string,
  sem: AttributeSemantics | undefined,
  bag: ReadonlyMap<string, string>,
  from: "logs" | "traces",
  update: UpdateFn,
): AttributeRowAction[] {
  return attributePivots(key, value, sem, bag, from).map((pivot) => ({
    label: pivot.label,
    ariaLabel: pivot.ariaLabel,
    onClick: () => update(pivot.patch, { push: true }),
  }));
}
