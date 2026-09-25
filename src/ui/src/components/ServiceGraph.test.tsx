import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import {
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
    expect(dot("api-gateway")).toBeNull();
    expect(dot("checkout")).toHaveClass("sg-node-dot-warn");
    expect(dot("payments")).toHaveClass("sg-node-dot-critical");
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
    expect(container.querySelector(markerId)?.tagName).toBe("marker");
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
