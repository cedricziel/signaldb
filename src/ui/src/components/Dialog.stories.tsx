import type { Meta, StoryObj } from "@storybook/react-vite";
import { Dialog } from "./Dialog";

const meta = {
  title: "Components/Dialog",
  component: Dialog,
} satisfies Meta<typeof Dialog>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    label: "Test dialog",
    onClose: () => {},
    children: (
      <>
        <p>Delete this dataset?</p>
        <button type="button">Confirm</button>
        <button type="button">Cancel</button>
      </>
    ),
  },
};

export const NotDismissible: Story = {
  args: {
    label: "Message only",
    children: <p>Nothing to press here.</p>,
  },
};
