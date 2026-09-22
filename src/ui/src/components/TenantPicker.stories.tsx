import type { Meta, StoryObj } from "@storybook/react-vite";
import type { SessionMembership } from "../api/session";
import { TenantPicker } from "./TenantPicker";

const memberships: SessionMembership[] = [
  { tenant_id: "acme", name: "Acme Corp", role: "admin" },
  { tenant_id: "globex", name: "Globex", role: "member" },
  { tenant_id: "initech", name: "Initech", role: "viewer" },
];

const meta = {
  title: "Components/TenantPicker",
  component: TenantPicker,
} satisfies Meta<typeof TenantPicker>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: { memberships, onPicked: () => {}, busy: false },
};

export const Busy: Story = {
  args: { memberships, onPicked: () => {}, busy: true },
};
