import type { Meta, StoryObj } from "@storybook/react-vite";
import { LoginCard } from "./LoginCard";

const meta = {
  title: "Components/LoginCard",
  component: LoginCard,
} satisfies Meta<typeof LoginCard>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    as: "h1",
    title: "Sign in",
    hint: "Use your account to explore logs, traces and metrics.",
    children: <p>Sign-in controls go here.</p>,
  },
};

export const NoHint: Story = {
  args: {
    as: "h1",
    title: "Choose a tenant",
    children: <p>Tenant picker goes here.</p>,
  },
};
