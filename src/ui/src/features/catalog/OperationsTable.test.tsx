import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";
import { OperationsTable } from "./OperationsTable";
import * as catalogApi from "../../api/catalog";
import * as operationSeriesApi from "../../api/operationSeries";
import type { CatalogEntity } from "../../api/catalog";
import type { EntityTypeDef } from "./entityTypes";

vi.mock("../../api/catalog", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/catalog")>();
  return { ...actual, fetchCatalogEntities: vi.fn() };
});
vi.mock("../../api/operationSeries", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/operationSeries")>();
  return { ...actual, fetchOperationSeries: vi.fn() };
});

const fetchCatalogEntities = vi.mocked(catalogApi.fetchCatalogEntities);
const fetchOperationSeries = vi.mocked(operationSeriesApi.fetchOperationSeries);

const breakdownEntity: EntityTypeDef = {
  id: "service::span.name",
  label: "Operations",
  singular: "Operations",
  identity: ["span.name"],
};

const range = { fromMs: 0, toMs: 3_600_000 };
const pinned = [{ field: "service.name", value: "checkout" }];

function op(name: string, n: number): CatalogEntity {
  return {
    values: [name],
    observations: [{ source: "traces", count: n }],
    lastNs: "1700003600000000000",
    red: { traces: n, errors: Math.round(n * 0.01), p50Ms: 20, p95Ms: 80 },
  };
}

function manyOps(count: number): CatalogEntity[] {
  return Array.from({ length: count }, (_, i) =>
    op(`operation-${i}`, count - i),
  );
}

beforeEach(() => {
  fetchCatalogEntities.mockReset();
  fetchOperationSeries.mockReset();
  fetchOperationSeries.mockResolvedValue(new Map());
});

afterEach(() => {
  vi.restoreAllMocks();
});

function renderTable(rows: CatalogEntity[], onRowClick = vi.fn()) {
  fetchCatalogEntities.mockResolvedValue({ entities: rows, truncated: false });
  renderWithClient(
    <OperationsTable
      entity={breakdownEntity}
      range={range}
      rangeKey="rk"
      rangeSeconds={3600}
      pinned={pinned}
      onRowClick={onRowClick}
    />,
  );
  return onRowClick;
}

describe("OperationsTable", () => {
  it("shows only the top 8 by rate and says so, with a toggle to show the rest", async () => {
    renderTable(manyOps(12));
    await waitFor(() =>
      expect(screen.getByText("top 8 of 12 by rate")).toBeInTheDocument(),
    );
    const rowsShown = screen.getAllByRole("row").length - 1; // minus header
    expect(rowsShown).toBe(8);

    const toggle = screen.getByRole("button", {
      name: "Show all 12 operations",
    });
    await userEvent.click(toggle);

    expect(screen.getByText("12 operations, by rate")).toBeInTheDocument();
    expect(screen.getAllByRole("row").length - 1).toBe(12);
    expect(
      screen.getByRole("button", { name: "Show top 8" }),
    ).toBeInTheDocument();
  });

  it("shows every row and no toggle when there are 8 or fewer", async () => {
    renderTable(manyOps(5));
    await waitFor(() =>
      expect(screen.getByText("5 operations, by rate")).toBeInTheDocument(),
    );
    expect(
      screen.queryByRole("button", { name: /Show/ }),
    ).not.toBeInTheDocument();
  });

  it("filters by substring, case-insensitively, and hides the toggle while filtering", async () => {
    renderTable([
      op("GET /cart", 10),
      op("POST /checkout", 5),
      op("GET /health", 1),
    ]);
    await waitFor(() =>
      expect(screen.getByText("3 operations, by rate")).toBeInTheDocument(),
    );

    await userEvent.type(
      screen.getByPlaceholderText("Filter operations…"),
      "get",
    );

    await waitFor(() =>
      expect(screen.getByText("2 of 3 match")).toBeInTheDocument(),
    );
    expect(screen.getByText("GET /cart")).toBeInTheDocument();
    expect(screen.getByText("GET /health")).toBeInTheDocument();
    expect(screen.queryByText("POST /checkout")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Show/ }),
    ).not.toBeInTheDocument();
  });

  it("shows an empty state when the filter matches nothing", async () => {
    renderTable([op("GET /cart", 10)]);
    await waitFor(() =>
      expect(screen.getByText("GET /cart")).toBeInTheDocument(),
    );

    await userEvent.type(
      screen.getByPlaceholderText("Filter operations…"),
      "zzz",
    );

    await waitFor(() =>
      expect(
        screen.getByText("No operations match “zzz”."),
      ).toBeInTheDocument(),
    );
  });

  it("renders a sparkline per row from useOperationSeries", async () => {
    fetchOperationSeries.mockResolvedValue(
      new Map([
        [
          "GET /cart",
          [
            { tMs: 1, value: 2 },
            { tMs: 2, value: 3 },
          ],
        ],
      ]),
    );
    renderTable([op("GET /cart", 10)]);
    const row = (await screen.findByText("GET /cart")).closest("tr")!;
    expect(within(row).getByRole("img")).toBeInTheDocument();
  });

  it("drills in on row click", async () => {
    const onRowClick = renderTable([op("GET /cart", 10)]);
    await userEvent.click(await screen.findByText("GET /cart"));
    expect(onRowClick).toHaveBeenCalledWith(["GET /cart"]);
  });
});
