import {
  fireEvent,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { resolveRange } from "../../lib/time";
import { renderWithClient } from "../../test/render";
import {
  CatalogView,
  drillFilters,
  EntityTable,
  isDrillable,
} from "./CatalogView";
import { compositeKey } from "../../lib/traceGroups";
import { ENTITY_TYPES, type EntityTypeDef } from "./entityTypes";
import * as catalogApi from "../../api/catalog";
import * as sourceFieldsApi from "../../api/sourceFields";
import * as membersApi from "../../api/traceGroupMembers";
import * as entityTypesHook from "./useEntityTypes";
import * as sparklineApi from "../../api/entitySparkline";
import * as entityMetricsApi from "../../api/entityMetrics";
import type { CatalogEntity, EntityObservation } from "../../api/catalog";

// The entity table is a server-side aggregate (see api/catalog) — mocked at
// the module boundary, the same way TracesView.test.tsx mocks
// fetchTraceGroups. `importOriginal` keeps the real `buildEntityDoc` (covered
// by api/catalog.test.ts) and swaps only the network-touching entry point.
vi.mock("../../api/catalog", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/catalog")>();
  return { ...actual, fetchCatalogEntities: vi.fn() };
});
vi.mock("../../api/sourceFields", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/sourceFields")>();
  return { ...actual, fetchFieldValueSketch: vi.fn() };
});
vi.mock("../../api/traceGroupMembers", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/traceGroupMembers")>();
  return { ...actual, fetchTraceGroupMembers: vi.fn() };
});
// The observed set is two metadata fetches away (see useEntityTypes); what
// this view owes is resolving the URL's entity type against whatever that
// hook reports, so the hook is the boundary to control here.
vi.mock("../../api/entitySparkline", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/entitySparkline")>();
  return {
    ...actual,
    fetchEntitySparklines: vi.fn(),
    fetchEntityActivity: vi.fn(),
  };
});
vi.mock("../../api/entityMetrics", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/entityMetrics")>();
  return {
    ...actual,
    discoverObservedMetricNames: vi.fn(),
    fetchEntityMetricNames: vi.fn(),
    fetchMetricDefinitions: vi.fn(),
  };
});
vi.mock("./useEntityTypes", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./useEntityTypes")>();
  return { ...actual, useCatalogEntityTypes: vi.fn() };
});

const fetchCatalogEntities = vi.mocked(catalogApi.fetchCatalogEntities);
const fetchFieldValueSketch = vi.mocked(sourceFieldsApi.fetchFieldValueSketch);
const fetchTraceGroupMembers = vi.mocked(membersApi.fetchTraceGroupMembers);
const useCatalogEntityTypes = vi.mocked(entityTypesHook.useCatalogEntityTypes);
const fetchEntitySparklines = vi.mocked(sparklineApi.fetchEntitySparklines);
const fetchEntityActivity = vi.mocked(sparklineApi.fetchEntityActivity);
const discoverObservedMetricNames = vi.mocked(
  entityMetricsApi.discoverObservedMetricNames,
);
const fetchEntityMetricNames = vi.mocked(
  entityMetricsApi.fetchEntityMetricNames,
);
const fetchMetricDefinitions = vi.mocked(
  entityMetricsApi.fetchMetricDefinitions,
);

/** A registry metric definition, as the association lookup returns it. */
function metricDef(name: string) {
  return {
    name,
    brief: "",
    group_id: `metric.${name}`,
    instrument: "gauge",
    unit: "1",
    attributes: [],
    entity_associations: ["host"],
    namespace: "otel",
    source: "bundled",
    version: "1.43.0",
  } as never;
}

afterEach(() => {
  vi.restoreAllMocks();
});

beforeEach(() => {
  fetchCatalogEntities.mockReset();
  fetchCatalogEntities.mockResolvedValue({ entities: [], truncated: false });
  fetchFieldValueSketch.mockReset();
  fetchFieldValueSketch.mockResolvedValue(undefined);
  fetchTraceGroupMembers.mockReset();
  fetchTraceGroupMembers.mockResolvedValue([]);
  fetchEntitySparklines.mockReset();
  fetchEntitySparklines.mockResolvedValue(new Map());
  fetchEntityActivity.mockReset();
  fetchEntityActivity.mockResolvedValue(new Map());
  discoverObservedMetricNames.mockReset();
  fetchEntityMetricNames.mockReset();
  fetchMetricDefinitions.mockReset();
  discoverObservedMetricNames.mockResolvedValue([]);
  fetchEntityMetricNames.mockResolvedValue([]);
  fetchMetricDefinitions.mockResolvedValue([]);
  useCatalogEntityTypes.mockReset();
  // The curated list, unanalyzed: what a deployment reports before any field
  // metadata has landed.
  useCatalogEntityTypes.mockReturnValue({
    types: ENTITY_TYPES,
    isPending: false,
    analyzed: false,
  });
});

/** An entity traces observed, optionally also seen in other signals. */
function group(
  values: (string | null)[],
  traces: number,
  errors: number,
  p50Ms: number,
  p95Ms: number,
  lastNs: string,
  alsoSeenIn: EntityObservation[] = [],
): CatalogEntity {
  return {
    values,
    observations: [{ source: "traces", count: traces }, ...alsoSeenIn],
    lastNs,
    red: { traces, errors, p50Ms, p95Ms },
  };
}

/** An entity no trace ever carried — discovered through another signal only,
 * so it has no RED at all. */
function unmeasured(
  values: (string | null)[],
  observations: EntityObservation[],
  lastNs: string,
): CatalogEntity {
  return { values, observations, lastNs };
}

function renderView(state: Partial<ExploreState> = {}) {
  const update = vi.fn();
  renderWithClient(
    <CatalogView
      state={{ ...DEFAULT_STATE, signal: "catalog", ...state }}
      update={update}
    />,
  );
  return update;
}

describe("CatalogView", () => {
  it("fetches the selected entity type once, not once per consumer", async () => {
    // The nav needs a count for every entity type and the table needs the
    // rows for the selected one — the same aggregate, at the same sort, over
    // the same window. They must share a cache entry: keyed differently, the
    // selected type is fetched twice on every paint, doubling the cost of the
    // one query most likely to be expensive.
    renderView();
    await screen.findByRole("complementary", { name: "Entity types" });
    await waitFor(() => expect(fetchCatalogEntities).toHaveBeenCalled());

    const forSelected = fetchCatalogEntities.mock.calls.filter(
      ([entity]) => entity.id === "service",
    );
    expect(forSelected).toHaveLength(1);
  });

  it("lists every registered entity type in the nav, Services selected by default", async () => {
    renderView();
    const nav = screen.getByRole("complementary", { name: "Entity types" });
    expect(
      within(nav).getByRole("button", { name: /Services/ }),
    ).toHaveAttribute("aria-pressed", "true");
    expect(within(nav).getByText("Databases")).toBeInTheDocument();
    expect(within(nav).getByText("Hosts")).toBeInTheDocument();
  });

  it("switches the selected entity type through the URL state", async () => {
    const update = renderView();
    const user = userEvent.setup();
    const nav = screen.getByRole("complementary", { name: "Entity types" });
    await user.click(within(nav).getByRole("button", { name: /Databases/ }));
    expect(update).toHaveBeenCalledWith({ catalogEntity: "database" });
  });

  it("renders discovered entities with RED metrics", async () => {
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        group(["gateway", "edge"], 1240, 5, 12, 48, "1700000000000000000"),
      ],
      truncated: false,
    });
    renderView();
    expect(await screen.findByText("gateway")).toBeInTheDocument();
    expect(screen.getByText("edge")).toBeInTheDocument();
    expect(screen.getByText("12 ms")).toBeInTheDocument();
    expect(screen.getByText("48 ms")).toBeInTheDocument();
  });

  it("dispatches to the entity detail page once catalogPrimary is set", async () => {
    renderView({ catalogPrimary: compositeKey(["gateway", "edge"]) });
    expect(
      await screen.findByRole("navigation", { name: "Breadcrumb" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("complementary", { name: "Entity types" }),
    ).not.toBeInTheDocument();
  });

  it("shows an honest empty state naming the missing identity attribute", async () => {
    renderView({ catalogEntity: "host" });
    const note = await screen.findByText(/No hosts observed in this window/);
    expect(within(note).getByText("host.name")).toBeInTheDocument();
  });

  it("opens a service row's own detail page rather than jumping to Traces", async () => {
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        group(["gateway", "edge"], 1240, 5, 12, 48, "1700000000000000000"),
      ],
      truncated: false,
    });
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("gateway"));
    expect(update).toHaveBeenCalledWith(
      { catalogPrimary: compositeKey(["gateway", "edge"]) },
      { push: true },
    );
  });

  it("rates errors against the trace count, not total volume across sources", async () => {
    // 1 error among 10 traces, plus 90 unrelated log lines for the same
    // entity: the merged count is 100, but the error is a rate of the 10
    // traces it actually happened among — 10%, not 1%.
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        group(["gateway", "edge"], 10, 1, 12, 48, "1700000000000000000", [
          { source: "logs", count: 90 },
        ]),
      ],
      truncated: false,
    });
    renderView();
    expect(await screen.findByText("10%")).toBeInTheDocument();
    expect(screen.queryByText("1%")).not.toBeInTheDocument();
  });

  it("lists entities without any sample counts", async () => {
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        group(["gateway", "edge"], 400, 0, 12, 48, "1700000000000000000", [
          { source: "logs", count: 2000 },
        ]),
      ],
      truncated: false,
    });
    renderView();
    const row = (await screen.findByText("gateway")).closest("tr")!;
    // How many spans or log lines back an entity is a fact about our storage,
    // not about the service. The list answers "which entities are there",
    // and 400 + 2000 is not "2400 requests" either.
    expect(within(row).queryByText("400")).not.toBeInTheDocument();
    expect(within(row).queryByText("2000")).not.toBeInTheDocument();
    expect(within(row).queryByText("2400")).not.toBeInTheDocument();
  });

  it("reports measurements as unavailable for an entity no trace carried", async () => {
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        unmeasured(
          ["batch-worker", "jobs"],
          [{ source: "metrics", count: 833 }],
          "1700000000000000000",
        ),
      ],
      truncated: false,
    });
    renderView();
    const row = (await screen.findByText("batch-worker")).closest("tr")!;
    // Rate, Errors, P50 and P95 are all trace-derived: an entity seen only
    // in metrics has no span status and no span duration, so each reads as
    // unavailable. A zeroed row would report it as a flawless, instant
    // service.
    expect(within(row).getAllByText("–")).toHaveLength(4);
    expect(within(row).getByText("batch-worker")).toBeInTheDocument();
    expect(within(row).queryByText("0%")).not.toBeInTheDocument();
    expect(within(row).queryByText("0 ms")).not.toBeInTheDocument();
  });
});

describe("the empty state", () => {
  it("separates 'none in this window' from 'none ever'", async () => {
    // The value sketch is not window-scoped, so it cannot list what is here —
    // but it can say the attribute has values elsewhere, which turns a dead
    // end into "widen the range".
    fetchFieldValueSketch.mockResolvedValue({
      distinct: 3,
      examples: ["ix-signaldb-mcp-1"],
      asOf: "2026-08-19 07:13:57",
    });
    renderView({ catalogEntity: "host" });

    // The note renders as soon as the window comes back empty; the sketch is
    // a second, later fetch that appends to it — so this waits for the
    // sketch's own sentence rather than reading the note the moment it exists.
    const note = await screen.findByText(/No hosts observed in this window/);
    await waitFor(() =>
      expect(note).toHaveTextContent("3 values have been seen outside it"),
    );
    expect(note).toHaveTextContent("2026-08-19 07:13:57");
    expect(note).toHaveTextContent("Try a wider time range");
  });

  it("claims nothing when no statistics cover the attribute", async () => {
    // The common case. Silence here is the honest answer: an uncompacted
    // deployment knows nothing about what exists outside the window, and
    // saying "none have ever been seen" would be a claim we cannot support.
    fetchFieldValueSketch.mockResolvedValue(undefined);
    renderView({ catalogEntity: "host" });

    const note = await screen.findByText(/No hosts observed in this window/);
    expect(note).not.toHaveTextContent("seen outside it");
    expect(note).not.toHaveTextContent("wider time range");
  });

  it("reads naturally when the sketch holds a single value", async () => {
    fetchFieldValueSketch.mockResolvedValue({
      distinct: 1,
      examples: ["db-01"],
    });
    renderView({ catalogEntity: "host" });

    const note = await screen.findByText(/No hosts observed in this window/);
    await waitFor(() =>
      expect(note).toHaveTextContent("One value has been seen outside it"),
    );
  });
});

describe("registry-derived entity types", () => {
  // A type the schema registry contributed, with no curated presentation and
  // no FACET_FIELDS entry for its identity attribute — the shape every
  // registry-only entity type has.
  const serviceInstance: EntityTypeDef = {
    id: "service_instance",
    label: "Service instances",
    singular: "service instance",
    identity: ["service.instance.id"],
    sources: ["metrics"],
  };

  it("opens its rows into a detail page like any curated type", async () => {
    // Row navigation goes through catalogPrimary, which is independent of
    // FACET_FIELDS — so a registry-derived type is browsable even though
    // nothing maps its identity onto a trace facet.
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        unmeasured(
          ["7f3c-instance"],
          [{ source: "metrics", count: 12 }],
          "1700000000000000000",
        ),
      ],
      truncated: false,
    });
    const update = renderView({ catalogEntity: "service_instance" });
    const user = userEvent.setup();
    await user.click(await screen.findByText("7f3c-instance"));
    expect(update).toHaveBeenCalledWith(
      { catalogPrimary: compositeKey(["7f3c-instance"]) },
      { push: true },
    );
  });

  it("renders its detail page from its own definition, not a curated one", async () => {
    // The URL is the only carrier of the selected type, and a derived type
    // exists only in the observed set — so this deep link is the case
    // `resolveEntityType` exists for.
    useCatalogEntityTypes.mockReturnValue({
      types: [...ENTITY_TYPES, serviceInstance],
      isPending: false,
      analyzed: true,
    });
    renderView({
      catalogEntity: "service_instance",
      catalogPrimary: compositeKey(["7f3c-instance"]),
    });

    const crumb = screen.getByRole("navigation", { name: "Breadcrumb" });
    expect(within(crumb).getByText("Service instances")).toBeInTheDocument();
    await waitFor(() => expect(fetchCatalogEntities).toHaveBeenCalled());
    const [entity, , , pinned] = fetchCatalogEntities.mock.calls[0]!;
    expect(entity.id).toBe("service_instance");
    expect(pinned).toEqual([
      { field: "service.instance.id", value: "7f3c-instance" },
    ]);
  });

  it("waits for the observed set rather than guessing the type", async () => {
    // Mid-fetch a derived type is not resolvable yet. Falling back to the
    // curated default there paints a whole dashboard of another entity's
    // numbers — and fetches them — before correcting itself.
    useCatalogEntityTypes.mockReturnValue({
      types: ENTITY_TYPES,
      isPending: true,
      analyzed: false,
    });
    renderView({
      catalogEntity: "service_instance",
      catalogPrimary: compositeKey(["7f3c-instance"]),
    });

    expect(fetchCatalogEntities).not.toHaveBeenCalled();
    // Asserted positively: absence alone is also satisfied by a blank page,
    // which is the regression this test exists to catch.
    expect(screen.getByText("Loading entity types…")).toBeInTheDocument();
    expect(
      screen.queryByRole("navigation", { name: "Breadcrumb" }),
    ).not.toBeInTheDocument();
  });

  it("has no trace escape hatch, because nothing maps its identity", () => {
    // Deliberate and worth pinning: `drillFilters` compiles a Traces filter
    // from FACET_FIELDS, which covers the curated identity attributes only.
    // A registry-derived type therefore cannot offer "View matching traces",
    // and must render no button rather than one that navigates to an
    // unfiltered trace list.
    expect(isDrillable(serviceInstance)).toBe(false);
    expect(drillFilters(serviceInstance, ["7f3c-instance"])).toEqual([]);
  });
});

describe("the entity list's sparkline column", () => {
  it("charts the entity type's headline metric, named in the header", async () => {
    // An entity type no trace ever carried reads as entirely unmeasured
    // otherwise — four dashes and nothing else.
    // The observed set carries the registry name, which is the join key the
    // association lookup needs (see `withRegistryNames`).
    useCatalogEntityTypes.mockReturnValue({
      types: ENTITY_TYPES.map((e) =>
        e.id === "host" ? { ...e, registryEntity: "host" } : e,
      ),
      isPending: false,
      analyzed: false,
    });
    discoverObservedMetricNames.mockResolvedValue(["system.cpu.utilization"]);
    fetchEntityMetricNames.mockResolvedValue(["system.cpu.utilization"]);
    fetchMetricDefinitions.mockResolvedValue([
      metricDef("system.cpu.utilization"),
    ]);
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        unmeasured(
          ["db-01"],
          [{ source: "metrics", count: 12 }],
          "1700000000000000000",
        ),
      ],
      truncated: false,
    });
    fetchEntitySparklines.mockResolvedValue(
      new Map([["db-01", [{ labels: { host_name: "db-01" }, points: [] }]]]),
    );

    renderView({ catalogEntity: "host" });

    expect(
      await screen.findByText("system.cpu.utilization"),
    ).toBeInTheDocument();
  });

  it("stays out of a table that did not ask for it", async () => {
    // EntityTable is shared with the breakdown and top-values tables, whose
    // entity types are synthetic. Today those happen to carry no registry
    // name — but that is a coincidence in another file, not a guarantee, so
    // the column is opt-in and the detail page's tables do not opt in.
    discoverObservedMetricNames.mockResolvedValue(["system.cpu.utilization"]);
    fetchEntityMetricNames.mockResolvedValue(["system.cpu.utilization"]);
    fetchMetricDefinitions.mockResolvedValue([
      metricDef("system.cpu.utilization"),
    ]);
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        unmeasured(
          ["db-01"],
          [{ source: "metrics", count: 12 }],
          "1700000000000000000",
        ),
      ],
      truncated: false,
    });

    renderWithClient(
      <EntityTable
        entity={{
          id: "host::span.name",
          label: "Operations",
          singular: "Operations",
          identity: ["span.name"],
          // Deliberately set: the guard must be the caller's opt-in, not the
          // absence of this field.
          registryEntity: "host",
        }}
        range={resolveRange(DEFAULT_STATE.range, Date.now())}
        rangeKey="1h|acme|prod"
        rangeSeconds={3600}
      />,
    );

    await screen.findByText("db-01");
    expect(fetchEntitySparklines).not.toHaveBeenCalled();
  });

  it("renders the sparkline tooltip outside the cell that would clip it", async () => {
    // A table cell sets `overflow: hidden` for its ellipsis, so a tooltip
    // rendered inside one is trapped in the 80x18 column. The table owns it.
    fetchEntityMetricNames.mockResolvedValue([]);
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        group(["gateway", "edge"], 10, 0, 1, 2, "1700000000000000000"),
      ],
      truncated: false,
    });
    fetchEntityActivity.mockResolvedValue(
      new Map([
        [
          compositeKey(["gateway", "edge"]),
          [
            {
              labels: {},
              points: [
                ["1700000000000000000", 4],
                ["1700000060000000000", 9],
              ],
            },
          ],
        ],
      ]),
    );

    renderView();
    await screen.findByText("spans");

    const bands = await waitFor(() => {
      const found = document.querySelectorAll(".entity-sparkline-hit");
      expect(found.length).toBeGreaterThan(0);
      return found;
    });
    fireEvent.pointerEnter(bands[1]!, { clientX: 10, clientY: 10 });

    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("spans");
    expect(tip.closest("td")).toBeNull();
  });

  it("charts the entity's activity when the registry names no metric", async () => {
    // Services are the case this exists for: OTel associates no metric with
    // the `service` entity at all, so a registry-only column would be
    // permanently empty on the catalog's most-used page. The header says what
    // is drawn, so a count is never read as the metric it is not.
    fetchEntityMetricNames.mockResolvedValue([]);
    fetchCatalogEntities.mockResolvedValue({
      entities: [
        group(["gateway", "edge"], 10, 0, 1, 2, "1700000000000000000"),
      ],
      truncated: false,
    });

    renderView();

    await screen.findByText("gateway");
    await waitFor(() => expect(fetchEntityActivity).toHaveBeenCalled());
    expect(screen.getByText("spans")).toBeInTheDocument();
    expect(fetchEntitySparklines).not.toHaveBeenCalled();
  });
});

describe("isDrillable / drillFilters", () => {
  const unmapped: EntityTypeDef = {
    id: "queue_depth",
    label: "Queue depths",
    singular: "queue",
    // Neither dimension has a FACET_FIELDS entry, unlike every currently
    // registered entity type.
    identity: ["queue.name", "queue.region"],
  };

  it("is not drillable when no identity dimension has a facet mapping", () => {
    expect(isDrillable(unmapped)).toBe(false);
    expect(drillFilters(unmapped, ["orders", "eu"])).toEqual([]);
  });

  it("is drillable using only the identity dimensions that have a mapping", () => {
    const service: EntityTypeDef = {
      id: "service",
      label: "Services",
      singular: "service",
      identity: ["service.name", "service.namespace"],
      spanKindScope: "Server",
    };
    expect(isDrillable(service)).toBe(true);
    expect(drillFilters(service, ["gateway", "edge"])).toEqual([
      { field: "service.name", value: "gateway" },
    ]);
  });
});
