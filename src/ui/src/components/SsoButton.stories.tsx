import type { Meta, StoryObj } from "@storybook/react-vite";
import { SsoButton } from "./SsoButton";

const meta = {
  title: "Components/SsoButton",
  component: SsoButton,
} satisfies Meta<typeof SsoButton>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Okta: Story = {
  args: { name: "Okta", startUrl: "/ui/session/oidc/start" },
};

export const Google: Story = {
  args: { name: "Google", startUrl: "/ui/session/oidc/start" },
};
