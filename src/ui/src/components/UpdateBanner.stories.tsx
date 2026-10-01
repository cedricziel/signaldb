import { useEffect } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { resetUpdateState, setUpdateAvailable } from "../lib/pwaUpdate";
import { UpdateBanner } from "./UpdateBanner";

/** Marks an update as pending for the story's lifetime; Reload is a no-op. */
function Pending() {
  useEffect(() => {
    setUpdateAvailable(async () => {});
    return resetUpdateState;
  }, []);
  return <UpdateBanner />;
}

const meta = {
  title: "Components/UpdateBanner",
  component: UpdateBanner,
} satisfies Meta<typeof UpdateBanner>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Hidden: Story = {};

export const Visible: Story = {
  render: () => <Pending />,
};
