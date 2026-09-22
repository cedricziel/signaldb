import type { Meta, StoryObj } from "@storybook/react-vite";
import { VizTooltip } from "./VizTooltip";

const HOST = { width: 400, height: 240 };

const meta = {
  title: "Components/VizTooltip",
  component: VizTooltip,
  decorators: [
    (Story) => (
      <div
        style={{ position: "relative", width: HOST.width, height: HOST.height }}
      >
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof VizTooltip>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    anchor: { x: 100, y: 40 },
    host: HOST,
    title: "2024-03-05 14:07",
    rows: [
      { swatch: "red", label: "error", value: "12 lines" },
      { swatch: "blue", label: "info", value: "525 lines" },
      { label: "gap", value: "–", muted: true },
    ],
    footer: { label: "total", value: "537 lines" },
  },
};

export const StringFooter: Story = {
  args: {
    anchor: { x: 100, y: 40 },
    host: HOST,
    title: "checkout",
    rows: [{ swatch: "green", label: "p50", value: "12ms" }],
    footer: "1,204 samples",
  },
};
