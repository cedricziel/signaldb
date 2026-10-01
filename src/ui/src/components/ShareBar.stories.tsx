import type { Meta, StoryObj } from "@storybook/react-vite";
import { ShareBar } from "./ShareBar";

const meta = {
  title: "Components/ShareBar",
  component: ShareBar,
} satisfies Meta<typeof ShareBar>;

export default meta;
type Story = StoryObj<typeof meta>;

const segments = [
  { key: "database", value: 300, color: "var(--svc-a)", label: "Database" },
  { key: "http", value: 100, color: "var(--svc-b)", label: "HTTP" },
  { key: "rpc", value: 60, color: "var(--svc-c)", label: "RPC" },
  { key: "other", value: 20, color: "var(--faint)", label: "Other" },
];

export const Segments: Story = {
  args: { segments },
};

export const SegmentsWithLegend: Story = {
  args: { segments, legend: true },
};

export const SingleFill: Story = {
  args: { fraction: 0.62 },
};

export const SingleFillError: Story = {
  args: { fraction: 0.9, fillColor: "var(--err)" },
};
