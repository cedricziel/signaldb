import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import {
  graphScale,
  MIN_GRAPH_SCALE,
  ServiceGraph,
  type ServiceGraphEdge,
  type ServiceGraphNode,
} from "./ServiceGraph";

const NODES: ServiceGraphNode[] = [
  { id: "gateway", label: "gateway", metricLine: "120ms" },
  { id: "payments", label: "payments", metricLine: "40ms", failed: true },
  { id: "db", label: "postgres", external: true },
];

const EDGES: ServiceGraphEdge[] = [
  { from: "gateway", to: "payments", count: 12, failed: true },
  { from: "payments", to: "db", count: 12 },
];

describe("ServiceGraph", () => {
  it("shows a node's latency beside its name, apart from the truncating label", () => {
    render(
      <ServiceGraph
        nodes={[
          {
            id: "checkout",
            label: "checkout-service-with-a-long-name",
            latency: "p95 30 ms",
          },
        ]}
        edges={[]}
      />,
    );
    const node = screen.getByRole("button", { name: /checkout/ });
    expect(node.querySelector(".sg-node-label")).toHaveTextContent(
      "checkout-service-with-a-long-name",
    );
    expect(node.querySelector(".sg-node-latency")).toHaveTextContent(
      "p95 30 ms",
    );
  });

  it("renders a node per service and calls onNodeClick with its id", () => {
    const onNodeClick = vi.fn();
    render(
      <ServiceGraph nodes={NODES} edges={EDGES} onNodeClick={onNodeClick} />,
    );
    expect(screen.getByRole("button", { name: /gateway/ })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /payments/ }));
    expect(onNodeClick).toHaveBeenCalledWith("payments");
  });

  it("marks the selected node pressed", () => {
    render(<ServiceGraph nodes={NODES} edges={EDGES} selected="payments" />);
    expect(screen.getByRole("button", { name: /payments/ })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(screen.getByRole("button", { name: /gateway/ })).toHaveAttribute(
      "aria-pressed",
      "false",
    );
  });

  it("colours a node's status dot by its error rate, not any error at all", () => {
    const nodes: ServiceGraphNode[] = [
      { id: "healthy", label: "api-gateway", errorRate: 0.001 },
      { id: "warn", label: "checkout", errorRate: 0.01 },
      { id: "critical", label: "payments", errorRate: 0.05 },
    ];
    render(<ServiceGraph nodes={nodes} edges={[]} />);
    const dot = (name: string) =>
      screen
        .getByRole("button", { name: new RegExp(name) })
        .querySelector(".sg-node-dot");
    expect(dot("api-gateway")).toHaveClass("sg-node-dot-healthy");
    expect(dot("checkout")).toHaveClass("sg-node-dot-warn");
    expect(dot("payments")).toHaveClass("sg-node-dot-critical");
  });

  it("shows no status dot for an external node", () => {
    render(<ServiceGraph nodes={NODES} edges={EDGES} />);
    expect(
      screen
        .getByRole("button", { name: /postgres/ })
        .querySelector(".sg-node-dot"),
    ).toBeNull();
  });

  it("falls back to a critical dot for a bare failed flag (no rate known)", () => {
    render(<ServiceGraph nodes={NODES} edges={EDGES} />);
    expect(
      screen
        .getByRole("button", { name: /payments/ })
        .querySelector(".sg-node-dot"),
    ).toHaveClass("sg-node-dot-critical");
  });

  it("draws each edge with a direction arrow coloured to match its severity", () => {
    const { container } = render(<ServiceGraph nodes={NODES} edges={EDGES} />);
    const line = container.querySelector(".sg-edge-critical")!;
    const markerEnd = line.getAttribute("marker-end")!;
    expect(markerEnd).toMatch(/^url\(#.+-critical\)$/);
    const markerId = markerEnd.slice(4, -1);
    const marker = container.querySelector(markerId);
    expect(marker?.tagName).toBe("marker");
    expect(marker?.getAttribute("markerUnits")).toBe("userSpaceOnUse");
    expect(Number(marker?.getAttribute("markerWidth"))).toBeGreaterThanOrEqual(
      10,
    );
  });

  it("scales the graph down to fit a container narrower than its layout", () => {
    // Six layers (a chain of six services) lays out wider than the 1200px
    // container width/setup.ts stubs every element to — this must shrink to
    // fit rather than overflow it.
    const nodes: ServiceGraphNode[] = "abcdef".split("").map((id) => ({
      id,
      label: id,
    }));
    const edges: ServiceGraphEdge[] = [
      { from: "a", to: "b", count: 1 },
      { from: "b", to: "c", count: 1 },
      { from: "c", to: "d", count: 1 },
      { from: "d", to: "e", count: 1 },
      { from: "e", to: "f", count: 1 },
    ];
    const { container } = render(<ServiceGraph nodes={nodes} edges={edges} />);
    const host = container.querySelector(".service-graph-host") as HTMLElement;
    const scale = Number(
      /scale\(([\d.]+)\)/.exec(host.style.transform)?.[1] ?? "1",
    );
    expect(scale).toBeGreaterThan(0);
    expect(scale).toBeLessThan(1);
  });

  it("stops shrinking at the readable floor and lets the box scroll", () => {
    // Twelve layers are far wider than the stubbed 1200px container: the
    // fit scale would be ~0.5, which puts node names near 6px.
    const ids = "abcdefghijkl".split("");
    const nodes: ServiceGraphNode[] = ids.map((id) => ({ id, label: id }));
    const edges: ServiceGraphEdge[] = ids
      .slice(1)
      .map((to, i) => ({ from: ids[i]!, to, count: 1 }));
    const { container } = render(<ServiceGraph nodes={nodes} edges={edges} />);
    const host = container.querySelector(".service-graph-host") as HTMLElement;
    expect(host.style.transform).toBe(`scale(${MIN_GRAPH_SCALE})`);
    const viewport = container.querySelector(
      ".service-graph-viewport",
    ) as HTMLElement;
    expect(parseFloat(viewport.style.width)).toBeGreaterThan(1200);
  });

  it("marks external nodes distinctly and can hide them", () => {
    const { rerender } = render(<ServiceGraph nodes={NODES} edges={EDGES} />);
    expect(
      screen.getByRole("button", { name: /postgres/ }).className,
    ).toContain("external");
    rerender(<ServiceGraph nodes={NODES} edges={EDGES} hideExternal />);
    expect(screen.queryByRole("button", { name: /postgres/ })).toBeNull();
  });

  it("shows the node-cap note when the graph was truncated", () => {
    render(
      <ServiceGraph
        nodes={NODES}
        edges={EDGES}
        capped={{ shown: 3, total: 10 }}
      />,
    );
    expect(
      screen.getByText(/Showing the busiest 3 of 10 services/),
    ).toBeInTheDocument();
  });

  it("shows the empty state for no nodes", () => {
    render(<ServiceGraph nodes={[]} edges={[]} />);
    expect(screen.getByRole("status")).toBeInTheDocument();
  });

  it("shows a loading state", () => {
    render(<ServiceGraph nodes={[]} edges={[]} loading />);
    expect(screen.getByText("Loading…")).toBeInTheDocument();
  });

  it("shows an error state", () => {
    render(<ServiceGraph nodes={[]} edges={[]} error="failed to load" />);
    expect(screen.getByRole("alert")).toHaveTextContent("failed to load");
  });
});

describe("graphScale", () => {
  it("keeps a graph that fits at its natural size", () => {
    expect(graphScale(800, 600)).toBe(1);
  });

  it("scales a wider graph down to fit", () => {
    expect(graphScale(900, 1000)).toBeCloseTo(0.9);
  });

  it("never goes below the readable floor", () => {
    expect(graphScale(300, 1000)).toBe(MIN_GRAPH_SCALE);
  });

  it("draws at 1 before the container is measured", () => {
    expect(graphScale(0, 1000)).toBe(1);
  });
});
