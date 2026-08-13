import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { compositeKey } from "../../lib/traceGroups";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { EntityDetail } from "./EntityDetail";
import * as catalogApi from "../../api/catalog";
import * as membersApi from "../../api/traceGroupMembers";
import * as dependencyBreakdownApi from "../../api/dependencyBreakdown";
import type { TraceGroup } from "../../api/traceGroups";
import type { TraceGroupMember } from "../../api/traceGroupMembers";

vi.mock("../../api/catalog", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/catalog")>();
  return { ...actual, fetchCatalogEntities: vi.fn() };
});
vi.mock("../../api/traceGroupMembers", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/traceGroupMembers")>();
  return { ...actual, fetchTraceGroupMembers: vi.fn() };
});
vi.mock("../../api/dependencyBreakdown", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/dependencyBreakdown")>();
  return { ...actual, fetchDependencyBreakdown: vi.fn() };
});

const fetchCatalogEntities = vi.mocked(catalogApi.fetchCatalogEntities);
const fetchTraceGroupMembers = vi.mocked(membersApi.fetchTraceGroupMembers);
const fetchDependencyBreakdown = vi.mocked(
  dependencyBreakdownApi.fetchDependencyBreakdown,
);

afterEach(() => {
  vi.restoreAllMocks();
});

beforeEach(() => {
  fetchCatalogEntities.mockReset();
  fetchTraceGroupMembers.mockReset();
  fetchDependencyBreakdown.mockReset();
  fetchCatalogEntities.mockResolvedValue({ groups: [], truncated: false });
  fetchTraceGroupMembers.mockResolvedValue([]);
  fetchDependencyBreakdown.mockResolvedValue([]);
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

function member(
  traceId: string,
  spanId: string,
  spanName: string,
  serviceName: string,
): TraceGroupMember {
  return {
    traceId,
    spanId,
    parentSpanId: null,
    spanName,
    serviceName,
    startNs: "1700000000000000000",
    durationNanos: "5000000",
    statusCode: "Ok",
  };
}

function renderView(state: Partial<ExploreState> = {}) {
  const update = vi.fn();
  renderWithClient(
    <EntityDetail
      state={{
        ...DEFAULT_STATE,
        signal: "catalog",
        catalogEntity: "service",
        catalogPrimary: compositeKey(["gateway", "edge"]),
        ...state,
      }}
      update={update}
    />,
  );
  return update;
}

describe("EntityDetail", () => {
  it("shows a breadcrumb naming the entity type and the drilled-into entity", async () => {
    renderView();
    const crumb = screen.getByRole("navigation", { name: "Breadcrumb" });
    expect(within(crumb).getByText("catalog")).toBeInTheDocument();
    expect(within(crumb).getByText("Services")).toBeInTheDocument();
    expect(within(crumb).getByText("gateway · edge")).toBeInTheDocument();
  });

  it("clicking catalog or the entity-type crumb returns to the list", async () => {
    const update = renderView();
    const user = userEvent.setup();
    const crumb = screen.getByRole("navigation", { name: "Breadcrumb" });
    await user.click(within(crumb).getByText("catalog"));
    expect(update).toHaveBeenCalledWith({
      catalogPrimary: "",
      catalogSecondary: "",
    });
  });

  it("shows the entity's own RED numbers, pinned to its identity values", async () => {
    fetchCatalogEntities.mockImplementation(async (entityType) => {
      if (entityType.id === "service") {
        return {
          groups: [
            group(["gateway", "edge"], 1240, 5, 12, 48, "1700000000000000000"),
          ],
          truncated: false,
        };
      }
      return { groups: [], truncated: false };
    });
    renderView();
    expect(await screen.findByText("1240")).toBeInTheDocument();
  });

  it("shows the breakdown table for an entity type that defines one", async () => {
    fetchCatalogEntities.mockImplementation(async (entityType) => {
      if (entityType.identity[0] === "span.name") {
        return {
          groups: [group(["GET /health"], 400, 0, 5, 9, "1700000000000000000")],
          truncated: false,
        };
      }
      return { groups: [], truncated: false };
    });
    renderView();
    expect(await screen.findByText("Operations")).toBeInTheDocument();
    expect(await screen.findByText("GET /health")).toBeInTheDocument();
  });

  it("drills a breakdown row into the secondary pin", async () => {
    fetchCatalogEntities.mockImplementation(async (entityType) => {
      if (entityType.identity[0] === "span.name") {
        return {
          groups: [group(["GET /health"], 400, 0, 5, 9, "1700000000000000000")],
          truncated: false,
        };
      }
      return { groups: [], truncated: false };
    });
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("GET /health"));
    expect(update).toHaveBeenCalledWith(
      { catalogSecondary: "GET /health" },
      { push: true },
    );
  });

  it("hides the breakdown table at the secondary pin depth", async () => {
    renderView({ catalogSecondary: "GET /health" });
    expect(screen.queryByText("Operations")).not.toBeInTheDocument();
  });

  it("shows recent matching spans and opens one into the trace waterfall", async () => {
    // Regression: this used to only set `trace`, which is meaningless
    // within the catalog signal (only TracesView renders a waterfall for
    // it) — the row silently did nothing visible. It must also switch to
    // the traces signal, same as the "View matching traces" escape hatch.
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1", "s1", "GET /health", "gateway"),
    ]);
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("GET /health"));
    expect(update).toHaveBeenCalledWith(
      { signal: "traces", trace: "t1" },
      { push: true },
    );
  });

  it('offers a "view matching traces" escape hatch that filters Traces', async () => {
    const update = renderView();
    const user = userEvent.setup();
    await user.click(
      await screen.findByRole("button", { name: /View matching traces/ }),
    );
    expect(update).toHaveBeenCalledWith(
      {
        signal: "traces",
        traceFilters: [{ field: "service.name", value: "gateway" }],
      },
      { push: true },
    );
  });

  it("shows a read-only top-values table for an entity type that defines one", async () => {
    fetchCatalogEntities.mockImplementation(async (entityType) => {
      if (entityType.identity[0] === "db.query.text") {
        return {
          groups: [
            group(
              ["SELECT * FROM users WHERE id = ?"],
              300,
              0,
              4,
              8,
              "1700000000000000000",
            ),
          ],
          truncated: false,
        };
      }
      return { groups: [], truncated: false };
    });
    const update = renderView({
      catalogEntity: "database",
      catalogPrimary: compositeKey(["prod", "postgres"]),
    });

    expect(await screen.findByText("Top statements")).toBeInTheDocument();
    const cell = await screen.findByText("SELECT * FROM users WHERE id = ?");
    expect(cell).toBeInTheDocument();

    // Read-only: clicking a row doesn't drill in or otherwise call update.
    const user = userEvent.setup();
    await user.click(cell);
    expect(update).not.toHaveBeenCalled();
  });

  describe("dependency-type breakdown", () => {
    it("shows a proportional bar and legend for a service's own page", async () => {
      fetchDependencyBreakdown.mockResolvedValue([
        { key: "database", label: "Database", durationNs: 300, count: 3 },
        { key: "http", label: "HTTP", durationNs: 100, count: 1 },
      ]);
      renderView(); // default state: catalogEntity "service", primary "gateway · edge"

      expect(screen.getByText("Time by dependency")).toBeInTheDocument();
      // The section headline renders immediately; its content is a
      // separate async query, so wait for that to settle too.
      expect(await screen.findByText("Database")).toBeInTheDocument();
      expect(screen.getByText(/75\.0%/)).toBeInTheDocument();
      expect(screen.getByText("HTTP")).toBeInTheDocument();
      expect(screen.getByText(/25\.0%/)).toBeInTheDocument();
      expect(fetchDependencyBreakdown).toHaveBeenCalledWith(
        "gateway",
        expect.anything(),
      );
    });

    it("shows an empty note rather than an empty bar when there's no dependency traffic", async () => {
      fetchDependencyBreakdown.mockResolvedValue([]);
      renderView();

      expect(
        await screen.findByText(/No database, HTTP, RPC, or messaging calls/),
      ).toBeInTheDocument();
    });

    it("is not shown for non-service entity types", async () => {
      renderView({
        catalogEntity: "database",
        catalogPrimary: compositeKey(["prod", "postgres"]),
      });

      await screen.findByText("Recent matching spans");
      expect(screen.queryByText("Time by dependency")).not.toBeInTheDocument();
      expect(fetchDependencyBreakdown).not.toHaveBeenCalled();
    });

    it("is hidden at the breakdown drill-in depth", async () => {
      fetchCatalogEntities.mockImplementation(async (entityType) => {
        if (entityType.identity[0] === "span.name") {
          return {
            groups: [
              group(["GET /health"], 400, 0, 5, 9, "1700000000000000000"),
            ],
            truncated: false,
          };
        }
        return { groups: [], truncated: false };
      });
      renderView({ catalogSecondary: "GET /health" });

      await screen.findByText("Recent matching spans");
      expect(screen.queryByText("Time by dependency")).not.toBeInTheDocument();
    });
  });
});
