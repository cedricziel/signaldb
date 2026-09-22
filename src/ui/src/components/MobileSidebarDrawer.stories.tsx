import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import {
  MobileFiltersToggle,
  MobileSidebarDrawer,
} from "./MobileSidebarDrawer";

const meta = {
  title: "Components/MobileSidebarDrawer",
  component: MobileSidebarDrawer,
} satisfies Meta<typeof MobileSidebarDrawer>;

export default meta;
type Story = StoryObj<typeof meta>;

function Interactive() {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <MobileFiltersToggle open={open} onToggle={() => setOpen((v) => !v)} />
      <MobileSidebarDrawer open={open} onClose={() => setOpen(false)}>
        <div>sidebar content</div>
      </MobileSidebarDrawer>
    </div>
  );
}

export const InteractiveToggle: StoryObj = {
  render: () => <Interactive />,
};

export const OpenByDefault: Story = {
  args: {
    open: true,
    onClose: () => {},
    children: <div>sidebar content</div>,
  },
};

export const RightSide: Story = {
  args: {
    open: true,
    onClose: () => {},
    side: "right",
    children: <div>sidebar content</div>,
  },
};
