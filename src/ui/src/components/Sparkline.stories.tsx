import type { Meta, StoryObj } from "@storybook/react-vite";
import { Sparkline } from "./Sparkline";
import { formatValue } from "../lib/vizFormat";

const series = [10, 14, 9, 22, 18, 30, 25, 40, 35, 50].map((v, i) => ({
  x: i * 60_000,
  v,
}));

const meta = {
  title: "Components/Sparkline",
  component: Sparkline,
  args: {
    points: series,
    formatValue: (v: number) => formatValue(v),
    formatLabel: (x: number) => `t+${x / 60_000}m`,
  },
} satisfies Meta<typeof Sparkline>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Line: Story = {};

export const Bar: Story = {
  args: { variant: "bar", tone: "accent" },
};

export const ErrorTone: Story = {
  args: { tone: "error" },
};

export const AccentTone: Story = {
  args: { tone: "accent" },
};

export const Stretched: Story = {
  args: { width: "100%" },
  decorators: [
    (Story) => (
      <div style={{ width: 320 }}>
        <Story />
      </div>
    ),
  ],
};

export const Empty: Story = {
  args: { points: [], emptyText: "No data in this window" },
};
