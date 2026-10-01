import type { Meta, StoryObj } from "@storybook/react-vite";
import { DemoButton } from "./DemoButton";

const meta = {
  title: "Components/DemoButton",
  component: DemoButton,
  args: {
    username: "demo@example.com",
    password: "demo",
    onAuthenticated: () => {},
  },
} satisfies Meta<typeof DemoButton>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {};
