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

  it("marks a failed node with the failed indicator", () => {
    render(<ServiceGraph nodes={NODES} edges={EDGES} />);
    expect(
      screen.getByRole("button", { name: /payments/ }).className,
    ).toContain("failed");
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
