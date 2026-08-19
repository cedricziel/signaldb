import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { CatalogView, drillFilters, isDrillable } from "./CatalogView";
import { compositeKey } from "../../lib/traceGroups";
import type { EntityTypeDef } from "./entityTypes";
import * as catalogApi from "../../api/catalog";
import * as sourceFieldsApi from "../../api/sourceFields";
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

const fetchCatalogEntities = vi.mocked(catalogApi.fetchCatalogEntities);
const fetchFieldValueSketch = vi.mocked(sourceFieldsApi.fetchFieldValueSketch);

afterEach(() => {
  vi.restoreAllMocks();
});

beforeEach(() => {
  fetchCatalogEntities.mockReset();
  fetchCatalogEntities.mockResolvedValue({ entities: [], truncated: false });
  fetchFieldValueSketch.mockReset();
  fetchFieldValueSketch.mockResolvedValue(undefined);
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

    const note = await screen.findByText(/No hosts observed in this window/);
    expect(note).toHaveTextContent("3 values have been seen outside it");
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
    expect(note).toHaveTextContent("One value has been seen outside it");
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
