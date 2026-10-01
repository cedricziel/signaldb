import { fireEvent, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as serviceGraphApi from "../../api/serviceGraph";
import { renderWithClient } from "../../test/render";
import { ServiceNeighborhood } from "./ServiceNeighborhood";
import type { ServiceGraph } from "../../api/gen";
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

const graphWithCallers: ServiceGraph = {
  nodes: [
    {
      id: "service:api-gateway",
      name: "api-gateway",
      kind: "service",
      request_rate: 5,
      error_rate: 0,
      p95_ns: 10_000_000,
    },
    {
      id: "service:checkout",
      name: "checkout",
      kind: "service",
      request_rate: 4,
      error_rate: 0.01,
      p95_ns: 20_000_000,
    },
    {
      id: "external:database:postgres",
      name: "postgres",
      kind: "external",
      dependency_kind: "database",
    },
  ],
  edges: [
    {
      source: "service:api-gateway",
      target: "service:checkout",
      count: 300,
      rate: 5,
      error_rate: 0,
      p95_ns: 10_000_000,
    },
    {
      source: "service:checkout",
      target: "external:database:postgres",
      count: 200,
      rate: 3,
      error_rate: 0.02,
      p95_ns: 5_000_000,
    },
  ],
  dropped_nodes: 0,
};

const resultWithCallers: ServiceGraphResult = {
  graph: graphWithCallers,
  warnings: [],
};

function renderNeighborhood(update = vi.fn()) {
  const utils = renderWithClient(
    <ServiceNeighborhood
      serviceName="checkout"
      range={range}
      rangeKey="r"
      update={update}
    />,
  );
  return { ...utils, update };
}

describe("ServiceNeighborhood", () => {
  it("scopes the graph query to the service with depth 1", async () => {
    fetchServiceGraph.mockResolvedValue(resultWithCallers);
    renderNeighborhood();
    await screen.findByText("api-gateway");
    expect(fetchServiceGraph).toHaveBeenCalledWith(range, {
      focus: "checkout",
      depth: 1,
    });
  });

  it("renders the map with the focus node selected", async () => {
    fetchServiceGraph.mockResolvedValue(resultWithCallers);
    renderNeighborhood();
    const focusNode = await screen.findByRole("button", {
      name: /checkout/,
      pressed: true,
    });
    expect(focusNode).toBeInTheDocument();
  });

  it("labels an external node with its dependency kind", async () => {
    fetchServiceGraph.mockResolvedValue(resultWithCallers);
    renderNeighborhood();
    const externalNode = await screen.findByRole("button", {
      name: /postgres/,
    });
    expect(externalNode).toHaveTextContent("database");
  });

  it("colours a neighbour's status dot by its error rate, not any error at all", async () => {
    fetchServiceGraph.mockResolvedValue(resultWithCallers);
    renderNeighborhood();
    const checkoutNode = await screen.findByRole("button", {
      name: /checkout/,
    });
    // 1% error rate on `checkout` — warn, not critical.
    expect(checkoutNode.querySelector(".sg-node-dot")).toHaveClass(
      "sg-node-dot-warn",
    );
    const gatewayNode = screen.getByRole("button", { name: /api-gateway/ });
    expect(gatewayNode.querySelector(".sg-node-dot")).toHaveClass(
      "sg-node-dot-healthy",
    );
  });

  it("navigates to a clicked neighbour's service page", async () => {
    fetchServiceGraph.mockResolvedValue(resultWithCallers);
    const { update } = renderNeighborhood();
    const callerNode = await screen.findByRole("button", {
      name: /api-gateway/,
    });
    fireEvent.click(callerNode);
    expect(update).toHaveBeenCalledWith(
      { catalogPrimary: "api-gateway", catalogSecondary: "" },
      { push: true },
    );
  });

  it("shows a no-callers state when the service has no incoming edges", async () => {
    fetchServiceGraph.mockResolvedValue({
      graph: {
        nodes: [
          {
            id: "service:checkout",
            name: "checkout",
            kind: "service",
            request_rate: 4,
            error_rate: 0,
            p95_ns: 1_000_000,
          },
          {
            id: "external:database:postgres",
            name: "postgres",
            kind: "external",
          },
        ],
        edges: [
          {
            source: "service:checkout",
            target: "external:database:postgres",
            count: 200,
            rate: 3,
            error_rate: 0,
            p95_ns: 5_000_000,
          },
        ],
        dropped_nodes: 0,
      },
      warnings: [],
    });
    renderNeighborhood();
    expect(
      await screen.findByText("No callers seen in the time range"),
    ).toBeInTheDocument();
  });

  it("switches to the table view and lists callers and dependencies as rows", async () => {
    fetchServiceGraph.mockResolvedValue(resultWithCallers);
    renderNeighborhood();
    await screen.findByText("api-gateway");
    fireEvent.click(screen.getByRole("button", { name: "Table" }));
    const rows = screen.getAllByRole("row");
    const cells = rows.map((r) => r.textContent);
    expect(cells.some((c) => c?.includes("api-gateway"))).toBe(true);
    expect(cells.some((c) => c?.includes("postgres"))).toBe(true);
  });
});
