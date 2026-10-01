import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import {
  ServiceGraph,
  type ServiceGraphEdge,
  type ServiceGraphNode,
} from "./ServiceGraph";

const meta = {
  title: "Components/ServiceGraph",
  component: ServiceGraph,
} satisfies Meta<typeof ServiceGraph>;

export default meta;
type Story = StoryObj<typeof meta>;

const SYSTEM_NODES: ServiceGraphNode[] = [
  { id: "web", label: "web", metricLine: "2.1k req/s · p95 90ms" },
  {
    id: "api-gateway",
    label: "api-gateway",
    metricLine: "1.9k req/s · p95 60ms",
  },
  { id: "orders", label: "orders", metricLine: "800 req/s · p95 45ms" },
  { id: "payments", label: "payments", metricLine: "300 req/s · p95 120ms" },
  { id: "inventory", label: "inventory", metricLine: "500 req/s · p95 20ms" },
  {
    id: "notifications",
    label: "notifications",
    metricLine: "150 req/s · p95 15ms",
  },
  {
    id: "postgres",
    label: "postgres",
    external: true,
    metricLine: "primary db",
  },
  {
    id: "stripe",
    label: "stripe.com",
    external: true,
    metricLine: "payment API",
  },
];

const SYSTEM_EDGES: ServiceGraphEdge[] = [
  { from: "web", to: "api-gateway", count: 2100, errorRate: 0.001 },
  { from: "api-gateway", to: "orders", count: 1800, errorRate: 0.002 },
  { from: "api-gateway", to: "payments", count: 300, errorRate: 0.006 },
  { from: "orders", to: "inventory", count: 500, errorRate: 0 },
  { from: "orders", to: "postgres", count: 800, errorRate: 0.001 },
  {
    from: "payments",
    to: "stripe",
    count: 300,
    errorRate: 0.03,
    metricLine: "p95 300ms",
  },
  { from: "orders", to: "notifications", count: 150, errorRate: 0 },
];

export const WholeSystem: Story = {
  args: { nodes: SYSTEM_NODES, edges: SYSTEM_EDGES },
};

export const OneHopNeighbourhood: Story = {
  args: {
    nodes: SYSTEM_NODES.filter((n) =>
      ["api-gateway", "orders", "payments"].includes(n.id),
    ),
    edges: SYSTEM_EDGES.filter(
      (e) => e.from === "api-gateway" || e.to === "api-gateway",
    ),
  },
};

const TRACE_NODES: ServiceGraphNode[] = [
  { id: "api-gateway", label: "api-gateway", metricLine: "12ms in service" },
  {
    id: "payments",
    label: "payments",
    metricLine: "80ms in service",
    failed: true,
  },
  { id: "inventory", label: "inventory", metricLine: "20ms in service" },
];

const TRACE_EDGES: ServiceGraphEdge[] = [
  { from: "api-gateway", to: "payments", count: 1, failed: true },
  { from: "api-gateway", to: "inventory", count: 1, failed: false },
];

export const SingleTrace: Story = {
  args: { nodes: TRACE_NODES, edges: TRACE_EDGES },
};

export const SelectedNode: Story = {
  args: { nodes: SYSTEM_NODES, edges: SYSTEM_EDGES, selected: "payments" },
};

export const ErrorEdgeThresholds: Story = {
  args: {
    nodes: [
      { id: "a", label: "a" },
      { id: "b", label: "neutral (<0.5%)" },
      { id: "c", label: "warn (>=0.5%)" },
      { id: "d", label: "critical (>=2%)" },
    ],
    edges: [
      { from: "a", to: "b", count: 100, errorRate: 0.001 },
      { from: "a", to: "c", count: 100, errorRate: 0.01 },
      { from: "a", to: "d", count: 100, errorRate: 0.05 },
    ],
  },
};

export const NodeStatusDots: Story = {
  args: {
    nodes: [
      { id: "a", label: "healthy (<0.5%)", errorRate: 0.001 },
      { id: "b", label: "warn (>=0.5%)", errorRate: 0.01 },
      { id: "c", label: "critical (>=2%)", errorRate: 0.05 },
    ],
    edges: [
      { from: "a", to: "b", count: 100 },
      { from: "b", to: "c", count: 100 },
    ],
  },
};

/** A graph wider than its host must scale down to fit rather than clip —
 * `WholeSystem`'s eight nodes laid out over several layers, forced into a
 * panel far narrower than their natural width. */
export const NarrowContainer: Story = {
  args: { nodes: SYSTEM_NODES, edges: SYSTEM_EDGES },
  decorators: [
    (Story) => (
      <div style={{ width: 360, border: "1px dashed #999" }}>
        <Story />
      </div>
    ),
  ],
};

export const ExternalNodesShown: Story = {
  args: { nodes: SYSTEM_NODES, edges: SYSTEM_EDGES, hideExternal: false },
};

export const ExternalNodesHidden: Story = {
  args: { nodes: SYSTEM_NODES, edges: SYSTEM_EDGES, hideExternal: true },
};

export const Empty: Story = {
  args: { nodes: [], edges: [] },
};

export const NodeCapWarning: Story = {
  args: {
    nodes: SYSTEM_NODES,
    edges: SYSTEM_EDGES,
    capped: { shown: SYSTEM_NODES.length, total: 350 },
  },
};

export const Loading: Story = {
  args: { nodes: [], edges: [], loading: true },
};

export const ErrorState: Story = {
  args: { nodes: [], edges: [], error: "Couldn't load the service graph." },
};

export const Interactive: Story = {
  args: { nodes: SYSTEM_NODES, edges: SYSTEM_EDGES },
  render: function Render(args) {
    const [selected, setSelected] = useState<string | null>(null);
    return (
      <ServiceGraph
        {...args}
        selected={selected}
        onNodeClick={(id) => setSelected(id === selected ? null : id)}
      />
    );
  },
};
