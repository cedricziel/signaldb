import type { Meta, StoryObj } from "@storybook/react-vite";
import { CopyValueButton } from "./CopyValueButton";

const meta = {
  title: "Components/CopyValueButton",
  component: CopyValueButton,
} satisfies Meta<typeof CopyValueButton>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    value: "checkout",
    label: "value for service",
  },
};
