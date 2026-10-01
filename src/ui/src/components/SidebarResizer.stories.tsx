import type { Meta, StoryObj } from "@storybook/react-vite";
import { sidebarWidth, spanDetailWidth } from "../lib/sidebarWidth";
import { SidebarResizer } from "./SidebarResizer";

const meta = {
  title: "Components/SidebarResizer",
  component: SidebarResizer,
  decorators: [
    (Story) => (
      <div style={{ position: "relative", width: 300, height: 200 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof SidebarResizer>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Sidebar: Story = {
  args: { panel: sidebarWidth },
};

export const SpanDetail: Story = {
  args: { panel: spanDetailWidth },
};
