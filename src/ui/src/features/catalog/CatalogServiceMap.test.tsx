import { fireEvent, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as serviceGraphApi from "../../api/serviceGraph";
import { renderWithClient } from "../../test/render";
import { CatalogServiceMap } from "./CatalogServiceMap";
import type { ServiceGraphResult } from "../../api/serviceGraph";

vi.mock("../../api/serviceGraph", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/serviceGraph")>();
  return { ...actual, fetchServiceGraph: vi.fn() };
});

const fetchServiceGraph = vi.mocked(serviceGraphApi.fetchServiceGraph);

afterEach(() => {
  fetchServiceGraph.mockReset();
});

const range = { fromMs: 0, toMs: 3_600_000 };

const baseResult: ServiceGraphResult = {
  graph: {
    nodes: [
      {
        id: "service:api-gateway",
        name: "api-gateway",
        kind: "service",
        request_rate: 24,
        error_rate: 0.001,
        p95_ns: 30_000_000,
      },
      {
        id: "service:checkout",
        name: "checkout",
        kind: "service",
        request_rate: 20,
        error_rate: 0.03,
        p95_ns: 190_000_000,
      },
      {
        id: "external:database:orders-db",
        name: "orders-db",
        kind: "external",
        dependency_kind: "database",
      },
    ],
    edges: [
      {
        source: "service:api-gateway",
        target: "service:checkout",
        count: 6_480,
        rate: 20,
        error_rate: 0.006,
        p95_ns: 190_000_000,
      },
      {
        source: "service:checkout",
        target: "external:database:orders-db",
        count: 9_100,
        rate: 2.5,
        error_rate: 0,
        p95_ns: 25_000_000,
      },
    ],
    dropped_nodes: 0,
  },
  warnings: [],
};

function renderMap(update = vi.fn()) {
  const utils = renderWithClient(
    <CatalogServiceMap range={range} rangeKey="r" update={update} />,
  );
  return { ...utils, update };
}

describe("CatalogServiceMap", () => {
  it("renders every node from the whole-tenant graph", async () => {
    fetchServiceGraph.mockResolvedValue(baseResult);
    renderMap();
    expect(fetchServiceGraph).toHaveBeenCalledWith(range, {});
    await screen.findByText("api-gateway");
    expect(screen.getByText("checkout")).toBeInTheDocument();
    expect(screen.getByText("orders-db")).toBeInTheDocument();
  });

  it("hides external nodes when the hide-external toggle is on", async () => {
    fetchServiceGraph.mockResolvedValue(baseResult);
    renderMap();
    await screen.findByText("orders-db");
    fireEvent.click(screen.getByRole("checkbox", { name: /hide external/i }));
    expect(screen.queryByText("orders-db")).not.toBeInTheDocument();
    expect(screen.getByText("checkout")).toBeInTheDocument();
  });

  it("opens a side panel with the node's figures, callers and dependencies", async () => {
    fetchServiceGraph.mockResolvedValue(baseResult);
    renderMap();
    fireEvent.click(await screen.findByRole("button", { name: /checkout/ }));
    const panel = screen.getByRole("complementary");
    expect(panel).toHaveTextContent("20/s");
    expect(panel).toHaveTextContent("3%");
    expect(panel).toHaveTextContent("190 ms");
    expect(panel).toHaveTextContent("api-gateway");
    expect(panel).toHaveTextContent("orders-db");
  });

  it("opens the clicked node's service page, keeping the time range", async () => {
    fetchServiceGraph.mockResolvedValue(baseResult);
    const { update } = renderMap();
    fireEvent.click(await screen.findByRole("button", { name: /checkout/ }));
    fireEvent.click(screen.getByRole("button", { name: "Service page" }));
    expect(update).toHaveBeenCalledWith(
      { catalogPrimary: "checkout", catalogSecondary: "" },
      { push: true },
    );
  });

  it("opens Traces filtered to the node's service", async () => {
    fetchServiceGraph.mockResolvedValue(baseResult);
    const { update } = renderMap();
    fireEvent.click(await screen.findByRole("button", { name: /checkout/ }));
    fireEvent.click(screen.getByRole("button", { name: "Traces" }));
    expect(update).toHaveBeenCalledWith(
      {
        signal: "traces",
        traceFilters: [{ field: "service.name", value: "checkout" }],
      },
      { push: true },
    );
  });

  it("opens Errors filtered to the node's service", async () => {
    fetchServiceGraph.mockResolvedValue(baseResult);
    const { update } = renderMap();
    fireEvent.click(await screen.findByRole("button", { name: /checkout/ }));
    fireEvent.click(screen.getByRole("button", { name: "Errors" }));
    expect(update).toHaveBeenCalledWith(
      {
        signal: "errors",
        filters: [{ label: "service.name", op: "=", value: "checkout" }],
      },
      { push: true },
    );
  });

  it("shows the node-cap and correlate truncation warnings above the map", async () => {
    fetchServiceGraph.mockResolvedValue({
      graph: { ...baseResult.graph, dropped_nodes: 5 },
      warnings: [
        {
          code: "correlate_row_limit",
          message: "The span join was truncated at its row cap.",
        },
      ],
    });
    renderMap();
    expect(
      await screen.findByText("The span join was truncated at its row cap."),
    ).toBeInTheDocument();
  });

  it("shows an empty state when the tenant has no service graph", async () => {
    fetchServiceGraph.mockResolvedValue({
      graph: { nodes: [], edges: [], dropped_nodes: 0 },
      warnings: [],
    });
    renderMap();
    expect(
      await screen.findByText("No services seen in this range"),
    ).toBeInTheDocument();
  });

  it("shows an error state when the graph query fails", async () => {
    fetchServiceGraph.mockRejectedValue(new Error("boom"));
    renderMap();
    expect(await screen.findByText(/boom/)).toBeInTheDocument();
  });
});
