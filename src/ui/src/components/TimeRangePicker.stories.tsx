import type { Meta, StoryObj } from "@storybook/react-vite";
import { TimeRangePicker } from "./TimeRangePicker";

const meta = {
  title: "Components/TimeRangePicker",
  component: TimeRangePicker,
} satisfies Meta<typeof TimeRangePicker>;

export default meta;
type Story = StoryObj<typeof meta>;

export const KnownPreset: Story = {
  args: {
    range: { type: "relative", seconds: 3600 },
    onChange: () => {},
  },
};

export const CustomRelative: Story = {
  args: {
    range: { type: "relative", seconds: 1800 },
    onChange: () => {},
  },
};

export const Absolute: Story = {
  args: {
    range: {
      type: "absolute",
      fromMs: Date.parse("2026-01-01T00:00:00Z"),
      toMs: Date.parse("2026-01-01T01:00:00Z"),
    },
    onChange: () => {},
  },
};
