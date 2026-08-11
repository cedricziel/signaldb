import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { CatalogView, drillFilters, isDrillable } from "./CatalogView";
import type { EntityTypeDef } from "./entityTypes";
import * as catalogApi from "../../api/catalog";
import type { TraceGroup } from "../../api/traceGroups";

// The entity table is a server-side aggregate (see api/catalog) — mocked at
// the module boundary, the same way TracesView.test.tsx mocks
// fetchTraceGroups. `importOriginal` keeps the real `buildEntityDoc` (covered
// by api/catalog.test.ts) and swaps only the network-touching entry point.
vi.mock("../../api/catalog", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/catalog")>();
  return { ...actual, fetchCatalogEntities: vi.fn() };
});

const fetchCatalogEntities = vi.mocked(catalogApi.fetchCatalogEntities);

afterEach(() => {
  vi.restoreAllMocks();
});

beforeEach(() => {
  fetchCatalogEntities.mockReset();
  fetchCatalogEntities.mockResolvedValue({ groups: [], truncated: false });
});

function group(
  values: (string | null)[],
  count: number,
  errors: number,
  p50Ms: number,
  p95Ms: number,
  lastNs: string,
): TraceGroup {
  return { values, count, errors, p50Ms, p95Ms, lastNs };
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
      groups: [
        group(["gateway", "edge"], 1240, 5, 12, 48, "1700000000000000000"),
      ],
      truncated: false,
    });
    renderView();
    expect(await screen.findByText("gateway")).toBeInTheDocument();
    expect(screen.getByText("edge")).toBeInTheDocument();
    expect(screen.getByText("1240")).toBeInTheDocument();
  });

  it("shows an honest empty state naming the missing identity attribute", async () => {
    renderView({ catalogEntity: "host" });
    const note = await screen.findByText(/No hosts observed in this window/);
    expect(within(note).getByText("host.name")).toBeInTheDocument();
  });

  it("drills a service row into Traces filtered by service.name", async () => {
    fetchCatalogEntities.mockResolvedValue({
      groups: [
        group(["gateway", "edge"], 1240, 5, 12, 48, "1700000000000000000"),
      ],
      truncated: false,
    });
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("gateway"));
    expect(update).toHaveBeenCalledWith(
      {
        signal: "traces",
        traceFilters: [{ field: "service.name", value: "gateway" }],
      },
      { push: true },
    );
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
