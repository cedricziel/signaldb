import type { Meta, StoryObj } from "@storybook/react-vite";
import { KpiCard, KpiStrip } from "./KpiCard";
import { Sparkline } from "./Sparkline";

const meta = {
  title: "Components/KpiCard",
  component: KpiCard,
} satisfies Meta<typeof KpiCard>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Basic: Story = {
  args: { label: "Requests", value: "1,204", unit: "req/s" },
};

export const WithGoodChange: Story = {
  args: {
    label: "Throughput",
    value: "812",
    unit: "req/s",
    change: { text: "+6% vs prev", direction: "up", tone: "good" },
  },
};

export const WithBadChange: Story = {
  args: {
    label: "Error rate",
    value: "2.1%",
    valueTone: "error",
    change: { text: "+6% vs prev", direction: "up", tone: "bad" },
    detail: "last 15 minutes",
  },
};

export const WithSparkline: Story = {
  args: {
    label: "Latency p95",
    value: "120",
    unit: "ms",
    change: { text: "-3% vs prev", direction: "down", tone: "good" },
  },
  render: (args) => (
    <KpiCard {...args}>
      <Sparkline
        points={[10, 14, 9, 22, 18, 30, 25].map((v, i) => ({ x: i, v }))}
        width="100%"
        tone="accent"
      />
    </KpiCard>
  ),
};

export const Strip: Story = {
  render: () => (
    <KpiStrip>
      <KpiCard label="Requests" value="1,204" unit="req/s" />
      <KpiCard
        label="Error rate"
        value="2.1%"
        valueTone="error"
        change={{ text: "+6% vs prev", direction: "up", tone: "bad" }}
      />
      <KpiCard label="p95" value="120" unit="ms" detail="last 15 minutes" />
    </KpiStrip>
  ),
};
