import type { Meta, StoryObj } from "@storybook/react-vite";
import { within, userEvent } from "storybook/test";
import { ConfirmButton } from "./ConfirmButton";

const meta = {
  title: "Components/ConfirmButton",
  component: ConfirmButton,
} satisfies Meta<typeof ConfirmButton>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Idle: Story = {
  args: {
    label: "Delete",
    prompt: "Delete dataset staging?",
    onConfirm: () => {},
  },
};

export const Confirming: Story = {
  args: {
    label: "Delete",
    prompt: "Delete dataset staging?",
    onConfirm: () => {},
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await userEvent.click(canvas.getByRole("button", { name: "Delete" }));
  },
};

export const Disabled: Story = {
  args: {
    label: "Delete",
    prompt: "Delete dataset staging?",
    onConfirm: () => {},
    disabled: true,
  },
};
